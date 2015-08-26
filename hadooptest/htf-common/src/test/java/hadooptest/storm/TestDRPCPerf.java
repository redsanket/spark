package hadooptest.storm;

import hadooptest.storm.DRPCClient;
import static org.junit.Assume.assumeTrue;
import hadooptest.SerialTests;
import hadooptest.TestSessionStorm;
import hadooptest.cluster.storm.ModifiableStormCluster;
import hadooptest.workflow.storm.topology.bolt.EchoBolt;
import hadooptest.automation.utils.http.JSONUtil;
import hadooptest.workflow.storm.topology.grouping.YidGrouping;

import hadooptest.Util;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import static org.junit.Assert.*;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import backtype.storm.drpc.LinearDRPCTopologyBuilder;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.TopologySummary;

import backtype.storm.StormSubmitter;
import backtype.storm.drpc.DRPCSpout;
import backtype.storm.drpc.ReturnResults;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;
import backtype.storm.topology.TopologyBuilder;

@SuppressWarnings("deprecation")
@Category(SerialTests.class)
public class TestDRPCPerf extends TestSessionStorm {
    static ModifiableStormCluster mc;
    static String function = "echo";
    private backtype.storm.Config _conf;
    private backtype.storm.Config _conf2;

    @BeforeClass
    public static void setup() throws Exception {
        assumeTrue(cluster instanceof ModifiableStormCluster);
        mc = (ModifiableStormCluster)cluster;

        cluster.setDrpcAclForFunction(function);
    }

    @AfterClass
    public static void cleanup() throws Exception {
        if (mc != null) {
            mc.resetConfigs();
        }
        stop();
    }

    public TestDRPCPerf(){
        _conf = new backtype.storm.Config();
        _conf.putAll(backtype.storm.utils.Utils.readStormConfig());

        _conf2 = new backtype.storm.Config();
    }

    @Test(timeout=600000)
    public void TestDRPCPerformance() throws Exception{
        logger.info("Starting TestDRPCPerformance");
        StormTopology topology = buildTopology();

        String topoName = "drpc-echo-topology";

        _conf2.setDebug(true);
        _conf2.setNumWorkers(3);

        File jar = new File(conf.getProperty("WORKSPACE") + "/topologies/target/topologies-1.0-SNAPSHOT-jar-with-dependencies.jar");
        try {
            cluster.submitTopology(jar, topoName, _conf2, topology);
        } catch (Exception e) {
            logger.error("Couldn't launch topology: ", e );
            throw new Exception(e);
        }

        List<String> servers = (List<String>) _conf.get(backtype.storm.Config.DRPC_SERVERS);
        if(servers == null || servers.isEmpty()) {
            cluster.killTopology(topoName);
            throw new RuntimeException("No DRPC servers configured for topology");
        }

        String drpcHttpsPort = (String)mc.getConf("ystorm.drpc_https_port", hadooptest.cluster.storm.StormDaemon.DRPC);
        String DRPCURI = "https://" + servers.get(0) + ":" + drpcHttpsPort + "/drpc/" + function;
        String [] drpcURIs = new String[] { DRPCURI };
        // Let's set up the test cert
        DRPCClient.setUpTrustStore( new String[] { "/etc/grid-keytabs/testcert" }, drpcURIs );
         
        try {
            // Give it some time to come up, and then launch a bunch of threads to beat on it.
            logger.info("Sleeping for a bit to let DRPC invocate");
            Util.sleep(10);

            String v1Role = "yahoo.grid_re.storm." + conf.getProperty("CLUSTER_NAME");
            String [] ycaV1Roles = new String [] { v1Role };

            logger.info("Launch lots of threads to beat on the server.  Let's see how long it takes.");

            int numThreads = 32;

            ArrayList<DRPCClient> dcList = new ArrayList<DRPCClient>();
            long startTime = System.currentTimeMillis();
            for (int i = 0 ; i < numThreads ; i++ ) {
                DRPCClient dc = new DRPCClient(1024, 512, ycaV1Roles, drpcURIs, i, false );
                try {
                    dc.start();
                } catch (Exception e) {
                    logger.warn("Got an exception when starting thread " + i, e);
                }
                dcList.add( dc );
            }
            logger.info("Wait for them all.");
            for (int i = 0 ; i < numThreads ; i++ ) {
                DRPCClient dc = dcList.get(i);
                try {
                    dc.join();
                } finally {
                    dc.halt();
                }
                logger.info("Finished a halt on " + i );
            }
            logger.info("It took " + (System.currentTimeMillis() - startTime) + " millis");
            assertTrue("Took longer then 100 seconds", (System.currentTimeMillis() - startTime) < 100000);
        } finally {
            DRPCClient.revertTrustStore();
            cluster.killTopology(topoName);
        }
    }
    
    public StormTopology buildTopology() throws Exception {

        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder(function);
        builder.addBolt(new EchoBolt(), 3);

        return builder.createRemoteTopology();
    }
}
