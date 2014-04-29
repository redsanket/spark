package hadooptest.storm;

import java.io.File;
import java.io.IOException;

import static org.junit.Assume.*;
import java.net.URI;
import java.util.HashSet;
import java.util.HashMap;
import java.util.List;
import java.io.IOException;

import java.nio.file.Paths;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import hadooptest.SerialTests;
import hadooptest.TestSessionStorm;
import hadooptest.cluster.storm.ModifiableStormCluster;
import hadooptest.Util;
import hadooptest.workflow.storm.topology.bolt.ExclaimBolt;

import backtype.storm.generated.*;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;

@SuppressWarnings("deprecation")
@Category(SerialTests.class)
public class TestBasicDRPC extends TestSessionStorm {
    static ModifiableStormCluster mc;
    private backtype.storm.Config _conf;

    @BeforeClass
    public static void setup() throws Exception {
        assumeTrue(cluster instanceof ModifiableStormCluster);
        mc = (ModifiableStormCluster)cluster;
    }

    @AfterClass
    public static void cleanup() throws Exception {
        if (mc != null) {
            mc.resetConfigsAndRestart();
        }
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

    public TestBasicDRPC(){
        _conf = new backtype.storm.Config();
        _conf.putAll(backtype.storm.utils.Utils.readStormConfig());
    }

    @Test
    public void TestDRPCTopologyHTTP() throws Exception{
        logger.info("Starting TestDRPCTopologyHTTP");
        StormTopology topology = buildTopology();

        String topoName = "drpc-topology-test1";

        _conf.setDebug(true);
        _conf.setNumWorkers(3);

        File jar = new File(conf.getProperty("WORKSPACE") + "/topologies/target/topologies-1.0-SNAPSHOT-jar-with-dependencies.jar");
        try {
            cluster.submitTopology(jar, topoName, _conf, topology);
        } catch (Exception e) {
            logger.error("Couldn't launch topology: " + e );
            throw new Exception();
        }

        boolean passed = false;
        // Configure the Jetty client to talk to the RS.  TODO:  Add API to the registry stub to do all this for us.....
        // Create and start client
        HttpClient client = new HttpClient();
        client.setIdleTimeout(30000);
        try {
            client.start();
        } catch (Exception e) {
            cluster.killTopology(topoName);
            throw new IOException("Could not start Http Client", e);
        }
        // Get the URI to ping
        List<String> servers = (List<String>) _conf.get(backtype.storm.Config.DRPC_SERVERS);
        if(servers == null || servers.isEmpty()) {
            cluster.killTopology(topoName);
            throw new RuntimeException("No DRPC servers configured for topology");   
        }

        String DRPCURI = "http://" + servers.get(0) + ":" + _conf.get(backtype.storm.Config.DRPC_HTTP_PORT) + "/drpc/exclamation/hello";
        logger.info("Attempting to connect to drpc server with " + DRPCURI);

        // Let's try for 3 minutes, or until we get a 200 back.
        boolean done = false;
        int tryCount = 200;
        while (!done && tryCount > 0) {
            Thread.sleep(1000);
            Request req;
            try {
                req = client.newRequest(DRPCURI);
            } catch (Exception e) {
                TestSessionStorm.logger.error("Request failed to URL " + DRPCURI);
                cluster.killTopology(topoName);
                throw new IOException("Could not start build request", e);
            }
            ContentResponse resp = null;
            TestSessionStorm.logger.warn("Trying to get status at " + DRPCURI);
            try {
                resp = req.send();
            } catch (Exception e) {
                tryCount -= 1;
            }

            if (resp != null && resp.getStatus() == 200) {
                done = true;
                // Check the response value.  Should be "hello!"
                String respString = resp.getContentAsString();
                logger.info("Got back " + respString + " from drpc.");
                if (respString.equals("hello!")) {
                    passed = true;
                }
            }
        }
        
        cluster.killTopology(topoName);

        // Stop client
        try {
            client.stop();
        } catch (Exception e) {
            throw new IOException("Could not stop http client", e);
        }

        // Did we fail?
        if (!done) {
            throw new IOException("Timed out trying to get DRPC response\n");
        }

        if (!passed) {
            logger.error("A test case failed.  Throwing error");
            throw new Exception();
        }
    }
    
    @Test
    public void TestDRPCTopologyHTTPPut() throws Exception{
        logger.info("Starting TestDRPCTopologyHTTPPost");
        StormTopology topology = buildTopology();

        String topoName = "drpc-topology-test-post";

        _conf.setDebug(true);
        _conf.setNumWorkers(3);

        File jar = new File(conf.getProperty("WORKSPACE") + "/topologies/target/topologies-1.0-SNAPSHOT-jar-with-dependencies.jar");
        try {
            cluster.submitTopology(jar, topoName, _conf, topology);
        } catch (Exception e) {
            logger.error("Couldn't launch topology: " + e );
            throw new Exception();
        }
        
        String inputFileDir = new String(conf.getProperty("WORKSPACE") + "/htf-common/resources/storm/testinputoutput/TestBasicDRPC/input.txt");
        java.nio.file.Path inputPath = Paths.get(inputFileDir);

        boolean passed = false;
        // Configure the Jetty client to talk to the RS.  TODO:  Add API to the registry stub to do all this for us.....
        // Create and start client
        HttpClient client = new HttpClient();
        client.setIdleTimeout(30000);
        try {
            client.start();
        } catch (Exception e) {
            cluster.killTopology(topoName);
            throw new IOException("Could not start Http Client", e);
        }
        // Get the URI to ping
        List<String> servers = (List<String>) _conf.get(backtype.storm.Config.DRPC_SERVERS);
        if(servers == null || servers.isEmpty()) {
            cluster.killTopology(topoName);
            throw new RuntimeException("No DRPC servers configured for topology");   
        }

        String DRPCURI = "http://" + servers.get(0) + ":" + _conf.get(backtype.storm.Config.DRPC_HTTP_PORT) + "/drpc/exclamation";
        logger.info("Attempting to connect to drpc server with " + DRPCURI);

        // Let's try for 3 minutes, or until we get a 200 back.
        boolean done = false;
        int tryCount = 200;
        while (!done && tryCount > 0) {
            Thread.sleep(1000);
            ContentResponse putResp = null;
            
            try {
                putResp = client.POST(DRPCURI).file(inputPath).send();
            } catch (Exception e) {
                TestSessionStorm.logger.error("Put failed to URL " + DRPCURI + e);
                cluster.killTopology(topoName);
                throw new IOException("Could not put to DRPC", e);
            }

            if (putResp != null && putResp.getStatus() == 200) {
                done = true;
                // Check the response value.  Should be "Hello World!"
                String respString = putResp.getContentAsString();
                logger.info("Got back " + respString + " from drpc.");
                if (respString.equals("Hello World!")) {
                    passed = true;
                }
            }
        }
        
        cluster.killTopology(topoName);

        // Stop client
        try {
            client.stop();
        } catch (Exception e) {
            throw new IOException("Could not stop http client", e);
        }

        // Did we fail?
        if (!done) {
            throw new IOException("Timed out trying to get DRPC response\n");
        }

        if (!passed) {
            logger.error("A test case failed.  Throwing error");
            throw new Exception();
        }
    }

    public StormTopology buildTopology() throws Exception {

        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("exclamation");
        builder.addBolt(new ExclaimBolt(), 3);

        return builder.createRemoteTopology();
    }
}
