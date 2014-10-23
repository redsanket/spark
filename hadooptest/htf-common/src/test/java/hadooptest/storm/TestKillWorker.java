package hadooptest.storm;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import hadooptest.SerialTests;
import hadooptest.TestSessionStorm;
import hadooptest.Util;
import hadooptest.cluster.storm.ModifiableStormCluster;
import hadooptest.cluster.storm.StormDaemon;
import hadooptest.workflow.storm.topology.bolt.ExclaimBolt;
import hadooptest.cluster.storm.ModifiableStormCluster;
import java.io.File;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.List;
import java.util.HashSet;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.Bolt;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.generated.ExecutorSummary;

@Category(SerialTests.class)
public class TestKillWorker extends TestSessionStorm {
    private static String function = "exclaim";
    static int totalSlots = 0;
    static ModifiableStormCluster mc;

    @BeforeClass
    public static void setup() throws Exception {
        cluster.setDrpcAclForFunction(function);
        mc = (ModifiableStormCluster)cluster;
    }
    
    @AfterClass
    public static void cleanup() throws Exception {
        if (mc != null) {
            // don't need to restart bc these config changes are dynamically loaded
            mc.resetConfigs();
        }
        stop();
    }

    public void testDrpcFunction() throws Exception{
        logger.info("About to check drpc");
        String expectedResult = "hello";
        String drpcResult = cluster.DRPCExecute( function, expectedResult );
        logger.info("The value for hello is " + drpcResult );
        for (int i = 0 ; i < totalSlots ; i++ ) {
            expectedResult += "!";
        }
        assertTrue(drpcResult!=null && drpcResult.equals(expectedResult));
    }

    @Test(timeout=900000)
    public void KillWorkerTest() throws Exception{
        // Figure out how many slots are in the cluster  (workers * (slots/worker))
        int numSupervisors = cluster.lookupRole(StormDaemon.SUPERVISOR).size();
        logger.info("Number of supervisors : " + Integer.toString(numSupervisors) );
        String[] returnValue = exec.runProcBuilder(new String[] { "yinst", "set", "ystorm.supervisor_slots_ports" }, true);
        Pattern p = Pattern.compile("([0-9]*)\n");
        Matcher m = p.matcher(returnValue[1]);
        assertTrue("Could not find yinst pattern in output", m.find());
        logger.info("Parsed value of " + m.group(1));
        int slotsPerWorker = Integer.parseInt(m.group(1));
        logger.info("Number of slots / worker : " + Integer.toString(slotsPerWorker) );
        totalSlots = numSupervisors * slotsPerWorker;
        StormTopology topology = buildTopology();

        String topoName = "KillWorkerTest";
                           
        Config config = new Config();
        config.setDebug(true);
        config.setNumWorkers(totalSlots);
        config.put(Config.NIMBUS_TASK_TIMEOUT_SECS, 200);
        config.put(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD, "123:456");
        //TODO turn this into a utility that has a conf setting
        File jar = new File(conf.getProperty("WORKSPACE") + "/topologies/target/topologies-1.0-SNAPSHOT-jar-with-dependencies.jar");
        cluster.submitTopology(jar, topoName, config, topology);


        try {
            logger.info("About to wait for it");
            final String topoId = getFirstTopoIdForName(topoName);
            waitForTopoUptimeSeconds(topoId, 20);

            TopologyInfo topInfo = cluster.getTopologyInfo(topoId);
            List<ExecutorSummary> executors = topInfo.get_executors();

            HashSet<String> procs = new HashSet<String>();

            for ( int i = 0 ; i < executors.size() ; i++ ) {
                ExecutorSummary e = executors.get(i);
                String it = e.get_host() + ":" + e.get_port();
                logger.info("host:port = " + it );
                logger.info("executor = " + e );
                procs.add(it);
            }
            for ( String s : procs ) {
                String[] args = s.split(":");
                logger.info("From set " + s );
                String pid =  mc.getWorkerPid(args[0], args[1]);
                logger.info("PID for " + s + "=" +  pid );
                // Let's kill it!
                String[] output = exec.runProcBuilder(new String[] { "ssh", args[0], "kill", "-9", pid }, true);
                assertTrue( "kill of " + args[0] + " " + pid + " failed.", output[0].equals("0"));
                logger.info("Killed " + s + ".   Waiting for it to come back up.");
                Util.sleep(5);
                
                int retryCount = 30;
                while ( retryCount > 0 && mc.getWorkerPid(args[0], args[1]) == null ) {
                    Util.sleep(5);
                    retryCount -= 1;
                }
                assertTrue( "Worker did not restart", retryCount > 0);
                logger.info("Sleeping a bit to let everything connect.");
                Util.sleep(10);
                logger.info("About to test drpc function");
                testDrpcFunction();
            }
        } finally {
            cluster.killTopology(topoName);
        }
    }
        
    public StormTopology buildTopology() throws Exception {

        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder(function);
        //Add a linear string of Exclaim bolts
        builder.addBolt(new ExclaimBolt(), 1);
        for(int i=1 ; i<totalSlots ; i++) {
            builder.addBolt(new ExclaimBolt(), 1).fieldsGrouping( new Fields("id"));
        }

        return builder.createRemoteTopology();
    }
}
