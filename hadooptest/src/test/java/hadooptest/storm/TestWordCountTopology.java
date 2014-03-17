package hadooptest.storm;

import static org.junit.Assert.assertEquals;
import hadooptest.SerialTests;
import hadooptest.TestSessionStorm;
import hadooptest.Util;
import hadooptest.workflow.storm.topology.bolt.Aggregator;
import hadooptest.workflow.storm.topology.bolt.SplitSentence;
import hadooptest.workflow.storm.topology.bolt.WordCount;
import hadooptest.workflow.storm.topology.spout.FiniteSentenceSpout;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.TopologySummary;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

@Category(SerialTests.class)
public class TestWordCountTopology extends TestSessionStorm {
    @BeforeClass
    public static void setup() throws Exception {
        start();
    }

    @AfterClass
    public static void cleanup() throws Exception {
        stop();
    }

    public TopologySummary getTS(String name) throws Exception {
        for (TopologySummary ts: cluster.getClusterInfo().get_topologies()) {
            if (name.equals(ts.get_name())) {
                return ts;
            }
        }
        throw new IllegalArgumentException("Topology "+name+" does not appear to be up yet");
    }
 
    public int getUptime(String name) throws Exception {
        return getTS(name).get_uptime_secs();
    }


    @Test
    public void WordCountTopologyTest() throws Exception{
        StormTopology topology = buildTopology();

        String topoName = "wc-topology-test";
        String outputLoc = "/tmp/wordcount"; //TODO change this to use a shared directory or soemthing, so we can get to it simply
                           
        Config config = new Config();
        config.put("test.output.location",outputLoc);
        config.setDebug(true);
        config.setNumWorkers(3);
        config.put(Config.NIMBUS_TASK_TIMEOUT_SECS, 200);
        //TODO turn this into a utility that has a conf setting
        File jar = new File(conf.getProperty("STORM_TEST_HOME") + "/target/hadooptest-ci-1.0-SNAPSHOT-test-jar-with-dependencies.jar");
        cluster.submitTopology(jar, topoName, config, topology);
        try {
            
            int uptime = 10;
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
            
            //TopologyInfo topo = getTS(topoName);
       
            BufferedReader reader = new BufferedReader(new FileReader(outputLoc));
        
            //get results
            String line;
            HashMap<String, Integer> resultWordCount = new HashMap<String, Integer>();
            System.out.println("Read results from: "+outputLoc);

            while ((line = reader.readLine()) != null) {
                if (line.length()>0){
                    String word = line.split(":")[0];
                    Integer count = Integer.parseInt(line.split(":")[1].trim());
                    resultWordCount.put(word, count);
                }
            }
            reader.close();
        
            //get expected results
            String file = conf.getProperty("STORM_TEST_HOME") + "/resources/storm/testinputoutput/WordCountFromFile/expected_results";
            reader = new BufferedReader(new FileReader(file));
        
            HashMap<String, Integer> expectedWordCount = new HashMap<String, Integer>();
            logger.info("Read epected results from: "+ file);

            while ((line = reader.readLine()) != null) {
                String word = line.split(":")[0];
                logger.info("Word = " + word);
                Integer count = Integer.parseInt(line.split(":")[1].trim());
                logger.info("Count = " + count);
                expectedWordCount.put(word, count);                       
            }
            reader.close();
        
            assertEquals (expectedWordCount.size(), resultWordCount.size());
        
            //int num_spouts = cluster.getExecutors(topo, "sentence_spout").size();
            int num_spouts = 1; //This must match the topology
            logger.info("Number of spouts: " + num_spouts);
            for (String key: expectedWordCount.keySet()){
                //for WordCount from file the output depends on how many spouts are created
                assertEquals(key, (int)expectedWordCount.get(key)*num_spouts, (int)resultWordCount.get(key));
            }
        } finally {
            cluster.killTopology(topoName);
        }
    }    
        
    public static StormTopology buildTopology() {
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("sentence_spout", new FiniteSentenceSpout(), 1);
        
        builder.setBolt("split", new SplitSentence(), 8)
                 .shuffleGrouping("sentence_spout");
        
        builder.setBolt("count", new WordCount(), 12)
                 .fieldsGrouping("split", new Fields("word"));
        
        builder.setBolt("aggregator", new Aggregator())
                .globalGrouping("count");
        
        return builder.createTopology();
    }    
}
