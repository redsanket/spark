package hadooptest.storm;

import static org.junit.Assert.assertEquals;
import hadooptest.SerialTests;
import hadooptest.TestSessionStorm;
import hadooptest.Util;
import hadooptest.workflow.storm.topology.bolt.CheckSubjectBolt;
import hadooptest.workflow.storm.topology.spout.CheckSubjectSpout;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Arrays;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.TopologySummary;
import backtype.storm.topology.TopologyBuilder;

@Category(SerialTests.class)
public class TestAutoCred extends TestSessionStorm {

    @AfterClass
    public static void cleanup() throws Exception {
        stop();
    }

    @Test
    public void AutoCredTest() throws Exception{
        StormTopology topology = buildTopology();

        String topoName = "auto-cred-topology-test";
        String outputLoc = "/homes/hadoopqa/autocred"; //TODO change this to use a shared directory or soemthing, so we can get to it simply
                           
        // Remove the file, if there
        try {
            File file = new File(outputLoc);
            file.delete();
        } catch(Exception e) {
        }
        Config config = new Config();
        config.put("test.output.location",outputLoc);
        config.setDebug(true);
        config.setNumWorkers(2);
        config.put(Config.NIMBUS_TASK_TIMEOUT_SECS, 200);
        config.put("name.to.use","me");
        config.put(Config.TOPOLOGY_AUTO_CREDENTIALS, Arrays.asList("hadooptest.workflow.storm.topology.NamedAutoCredentials"));
        //TODO turn this into a utility that has a conf setting
        File jar = new File(conf.getProperty("WORKSPACE") + "/topologies/target/topologies-1.0-SNAPSHOT-jar-with-dependencies.jar");
        cluster.submitTopology(jar, topoName, config, topology);
        try {
            int uptime = 20;
            int cur_uptime = 0;

            cur_uptime = getUptime(topoName);

            if (cur_uptime < uptime){
                Util.sleep(uptime - cur_uptime);
            }
            
            cur_uptime = getUptime(topoName);

            while (cur_uptime < uptime){
                Util.sleep(1);
                cur_uptime = getUptime(topoName);
            }
            
        } finally {
            cluster.killTopology(topoName);
        }
        File outputFile = new File(outputLoc);
        int tryCount = 60;
        while (tryCount > 0 && !outputFile.exists()) {
            logger.info("Waiting for output.  Attempt " + (61-tryCount));
            tryCount -= 1;
            Util.sleep(1);
        }
        // If the file doesn't exist, we'll blow up trying to read it.
        BufferedReader reader = new BufferedReader(new FileReader(outputLoc));
        String line; 
        //get results
        while ((line = reader.readLine()) != null) {
            String[] parts = line.split("\t");
            logger.info(Arrays.toString(parts));
            assertEquals(3, parts.length);
            assertEquals("me", parts[1]);
            assertEquals("me", parts[2]);
        }
        reader.close();
    }    
        
    public static StormTopology buildTopology() {
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("cs_spout", new CheckSubjectSpout(), 1);
        
        builder.setBolt("cs_bolt", new CheckSubjectBolt(), 1)
                .globalGrouping("cs_spout");
        
        return builder.createTopology();
    }    
}
