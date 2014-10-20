package hadooptest.storm;

import static org.junit.Assume.assumeTrue;
import hadooptest.SerialTests;
import hadooptest.TestSessionStorm;
import hadooptest.cluster.storm.ModifiableStormCluster;
import hadooptest.workflow.storm.topology.bolt.ExclaimBolt;
import hadooptest.automation.utils.http.JSONUtil;
import hadooptest.workflow.storm.topology.grouping.YidGrouping;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.InetAddress;
import java.net.Socket;
import java.io.InputStreamReader;
import java.io.BufferedReader;
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
import backtype.storm.testing.ForwardingMetricsConsumer;
import backtype.storm.topology.TopologyBuilder;

@SuppressWarnings("deprecation")
@Category(SerialTests.class)
public class TestBasicDRPC extends TestSessionStorm {
    static ModifiableStormCluster mc;
    private backtype.storm.Config _conf;
    private backtype.storm.Config _conf2;

    @BeforeClass
    public static void setup() throws Exception {
        assumeTrue(cluster instanceof ModifiableStormCluster);
        mc = (ModifiableStormCluster)cluster;

        cluster.setDrpcInvocationAuthAclForFunction("exclamation", "hadoopqa");
        cluster.setDrpcInvocationAuthAclForFunction("jsonpost", "hadoopqa");
        String v1Role = "yahoo.grid_re.storm." + conf.getProperty("CLUSTER_NAME");
        cluster.setDrpcClientAuthAclForFunction("exclamation", "hadoopqa," +v1Role );
        cluster.setDrpcClientAuthAclForFunction("jsonpost", "hadoopqa," +v1Role );
    }

    @AfterClass
    public static void cleanup() throws Exception {
        if (mc != null) {
            mc.resetConfigs();
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

        _conf2 = new backtype.storm.Config();
    }

    public boolean secureMode() throws Exception {
        String filter = null;
        filter = (String)_conf.get("drpc.http.filter");
                
        if ( filter != null && filter.equals("yjava.servlet.filter.YCAFilter")) {
            return true;
        }
        return false;
    }

    @Test(timeout=600000)
    public void TestDRPCTopologyHTTP() throws Exception{
        logger.info("Starting TestDRPCTopologyHTTP");
        StormTopology topology = buildTopology();

        String topoName = "drpc-topology-test1";

        _conf2.setDebug(true);
        _conf2.setNumWorkers(3);

        File jar = new File(conf.getProperty("WORKSPACE") + "/topologies/target/topologies-1.0-SNAPSHOT-jar-with-dependencies.jar");
        try {
            cluster.submitTopology(jar, topoName, _conf2, topology);
        } catch (Exception e) {
            logger.error("Couldn't launch topology: ", e );
            throw new Exception(e);
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
                if (secureMode()) {
                    String v1Role = "yahoo.grid_re.storm." + conf.getProperty("CLUSTER_NAME");
                    String ycaCert = com.yahoo.spout.http.Util.getYcaV1Cert(v1Role);
                    req = client.newRequest(DRPCURI).header("Yahoo-App-Auth", ycaCert);
                } else {
                    req = client.newRequest(DRPCURI);
                }
            } catch (Exception e) {
                TestSessionStorm.logger.error("Request failed to URL " + DRPCURI, e);
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
    
    @Test(timeout=600000)
    public void TestDRPCTopologyHTTPPut() throws Exception{
        logger.info("Starting TestDRPCTopologyHTTPPost");
        StormTopology topology = buildTopology();

        String topoName = "drpc-topology-test-post";

        _conf2.setDebug(true);
        _conf2.setNumWorkers(3);

        File jar = new File(conf.getProperty("WORKSPACE") + "/topologies/target/topologies-1.0-SNAPSHOT-jar-with-dependencies.jar");
        try {
            cluster.submitTopology(jar, topoName, _conf2, topology);
        } catch (Exception e) {
            logger.error("Couldn't launch topology: ", e );
            throw new Exception(e);
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
        Exception ex = null;
        while (!done && tryCount > 0) {
            Thread.sleep(1000);
            ContentResponse putResp = null;
            
            try {
                if (secureMode()) {
                    String v1Role = "yahoo.grid_re.storm." + conf.getProperty("CLUSTER_NAME");
                    String ycaCert = com.yahoo.spout.http.Util.getYcaV1Cert(v1Role);
                    putResp = client.POST(DRPCURI).header("Yahoo-App-Auth", ycaCert).file(inputPath).send();
                } else {
                    putResp = client.POST(DRPCURI).file(inputPath).send();
                }
            } catch (Exception e) {
                TestSessionStorm.logger.error("POST failed to URL " + DRPCURI, e);
                ex = e;
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
                if ( ex != null ) {
                    throw new IOException("Could not put to DRPC", ex);
                } else {
                    throw new IOException("Timed out trying to get DRPC response\n");
                }
        }

        if (!passed) {
            logger.error("A test case failed.  Throwing error");
            throw new Exception();
        }
    }

    @Test(timeout=600000)
    public void TestDRPCTopologyJSONPost() throws Exception{
        logger.info("Starting TestDRPCTopologyJSONPost");
        StormTopology topology = buildRamTopology();

        String topoName = "drpc-topology-json-post";

        File jar = new File(conf.getProperty("WORKSPACE") + "/topologies/target/topologies-1.0-SNAPSHOT-jar-with-dependencies.jar");
        try {
            backtype.storm.Config stormConfig = new backtype.storm.Config();
                stormConfig.setDebug(true);
                stormConfig.put(backtype.storm.Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, 8);
                stormConfig.put(backtype.storm.Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 32);
                stormConfig.put(backtype.storm.Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
                stormConfig.put(backtype.storm.Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384);
                stormConfig.setNumWorkers(3);
                stormConfig.setNumAckers(3);
                cluster.submitTopology(jar, topoName, stormConfig, topology);
        } catch (Exception e) {
            logger.error("Couldn't launch topology: ", e );
            throw new Exception(e);
        }

        String inputFileDir = new String(conf.getProperty("WORKSPACE") + "/htf-common/resources/storm/testinputoutput/TestBasicDRPC/input.json");
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

        String DRPCURI = "http://" + servers.get(0) + ":" + _conf.get(backtype.storm.Config.DRPC_HTTP_PORT) + "/drpc/jsonpost";
        logger.info("Attempting to connect to drpc server with " + DRPCURI);

        // Let's try for 3 minutes, or until we get a 200 back.
        int numPosts = 3;
        boolean done = false;
        while (numPosts > 0) {
            int tryCount = 200;
            Exception ex = null;
            done = false;
            while (!done && tryCount > 0) {
                Thread.sleep(1000);
                ContentResponse putResp = null;
                
                try {
                    if (secureMode()) {
                        String v1Role = "yahoo.grid_re.storm." + conf.getProperty("CLUSTER_NAME");
                        String ycaCert = com.yahoo.spout.http.Util.getYcaV1Cert(v1Role);
                        putResp = client.POST(DRPCURI).header("Yahoo-App-Auth", ycaCert).header("Content-Type", "application/json").file(inputPath).send();
                    } else {
                        putResp = client.POST(DRPCURI).header("Content-Type", "application/json").file(inputPath).send();
                    }
                } catch (Exception e) {
                    TestSessionStorm.logger.error("POST failed to URL " + DRPCURI, e);
                    ex = e;
                }

                if (putResp != null && putResp.getStatus() == 200) {
                    done = true;
                    // Check the response value.  Should be "Hello World!"
                    String respString = putResp.getContentAsString();
                    logger.info("Got back " + respString + " from drpc.");
                    if (respString.contains("login")) {
                        passed = true;
                    }
                }
            }
            // Did we fail?
            if (!done) {
                    cluster.killTopology(topoName);
                    // Stop client
                    try {
                        client.stop();
                    } catch (Exception e) {
                        throw new IOException("Could not stop http client", e);
                    }

                    if ( ex != null ) {
                        throw new IOException("Could not put to DRPC", ex);
                    } else {
                        throw new IOException("Timed out trying to get DRPC response\n");
                    }
            }
            numPosts -= 1;
            Thread.sleep(1000);
        }
        
        cluster.killTopology(topoName);

        // Stop client
        try {
            client.stop();
        } catch (Exception e) {
            throw new IOException("Could not stop http client", e);
        }

/*
        String DRPCURI = "http://" + servers.get(0) + ":" + _conf.get(backtype.storm.Config.DRPC_HTTP_PORT) + "/drpc/jsonpost";
        logger.info("Attempting to connect to drpc server with " + DRPCURI);

        // Let's do what Ram did, and just call out to curl and hit it with the same data.
        String myJSONPost = "'{\"src\":\"mobile_web\",\"asn\":\"5089\",\"login\":\"1545227ad14cc686e8325319df5e09bc\"}'";

        // Give topology enough time to come up.
        logger.info("Sleep for 1 seconds to allow topology to register DRPC");
        Thread.sleep(10000);
        String[] returnValue = null;
        boolean done = false;
        int tryCount = 200;
        while (!done && tryCount > 0) {
            if (secureMode()) {
                logger.info("Attempting to connect to secure drpc server with " + DRPCURI);
                logger.info("Attempting to connect to secure drpc server with json" + myJSONPost);
                String v1Role = "yahoo.grid_re.storm." + conf.getProperty("CLUSTER_NAME");
                String ycaCert = com.yahoo.spout.http.Util.getYcaV1Cert(v1Role);
                returnValue = exec.runProcBuilder(new String[] { "curl", "-X", "POST", "-H", "\"Yahoo-App-Auth:" + ycaCert + "\"", "-H", "\"Content-Type: application/json\"", "--data", myJSONPost, DRPCURI } , true);
                logger.info("returnValue[0] = " + returnValue[0]);
                logger.info("returnValue[1] = " + returnValue[1]);
                logger.info("returnValue[2] = " + returnValue[2]);
            } else {
                returnValue = exec.runProcBuilder(new String[] { "curl", "-X", "POST", "-H", "Content-Type: application/json", "--data", myJSONPost, DRPCURI } , true);
            }

            if ( returnValue[0].equals("0") ) {
                //done = true;
                Thread.sleep(10000);
                tryCount -= 1;
            } else {
                logger.warn("curl returned error.  Sleeping and retrying");
                Thread.sleep(10000);
                tryCount -= 1;
            }
        }
        */
    }


    @Test(timeout=600000)
    public void TestMetrics() throws Exception{
        logger.info("Starting TestMetrics");
        ServerSocket ss = new ServerSocket(0);
        int port = ss.getLocalPort();
        String dest = InetAddress.getLocalHost().getCanonicalHostName()+":"+ss.getLocalPort();

        StormTopology topology = buildTopology();

        String topoName = "drpc-metrics-test1";

        _conf2.setDebug(true);
        _conf2.setNumWorkers(3);
        _conf2.registerMetricsConsumer(ForwardingMetricsConsumer.class, dest, 1);
        _conf2.put(backtype.storm.Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS, 15); //15 seconds to make the test run faster

        File jar = new File(conf.getProperty("WORKSPACE") + "/topologies/target/topologies-1.0-SNAPSHOT-jar-with-dependencies.jar");
        try {
            cluster.submitTopology(jar, topoName, _conf2, topology);
        } catch (Exception e) {
            logger.error("Couldn't launch topology: ", e );
            throw new Exception(e);
        }

        Socket s = ss.accept();
        BufferedReader r = new BufferedReader(new InputStreamReader(s.getInputStream()));

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
                if (secureMode()) {
                    String v1Role = "yahoo.grid_re.storm." + conf.getProperty("CLUSTER_NAME");
                    String ycaCert = com.yahoo.spout.http.Util.getYcaV1Cert(v1Role);
                    req = client.newRequest(DRPCURI).header("Yahoo-App-Auth", ycaCert);
                } else {
                    req = client.newRequest(DRPCURI);
                }
            } catch (Exception e) {
                TestSessionStorm.logger.error("Request failed to URL " + DRPCURI, e);
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
        String line;
        tryCount = 0;
        boolean foundRecv = false;
        boolean foundSend = false;
        while ((!foundRecv || !foundSend) && tryCount < 200 && (line = r.readLine()) != null) {
          TestSessionStorm.logger.info(line);
          //Right now we are just checking that we got metrics back, and they look like they are in the right form
          String[] parts = line.split("\t", 6);
          assertEquals(6, parts.length);
          //[0] is ts
          //[1] is host/port
          //[2] is task id
          //[3] is bolt/spout name
          //[4] is metric name
          //[5] is metric value
          //We are looking for __send-iconnection and __recv-iconnection
          if ("__recv-iconnection".equals(parts[4])) {
            foundRecv = true;
            assertEquals("-1", parts[2]);
            assertEquals("__system", parts[3]);
            assertTrue(parts[5].contains("enqueued"));
            assertTrue(parts[5].contains("pending"));
            assertTrue(parts[5].contains("dequeuedMessages"));
          }
          if ("__send-iconnection".equals(parts[4])) {
            foundSend = true;
            assertEquals("-1", parts[2]);
            assertEquals("__system", parts[3]);
            assertTrue(parts[5].contains("enqueued"));
            assertTrue(parts[5].contains("sent"));
            assertTrue(parts[5].contains("lostOnSend"));
            assertTrue(parts[5].contains("dest"));
            assertTrue(parts[5].contains("queue_length"));
            assertTrue(parts[5].contains("reconnects"));
            assertTrue(parts[5].contains("src"));
          }

          tryCount++;
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
        assertTrue(foundRecv);
        assertTrue(foundSend);
    }


    public StormTopology buildRamTopology() throws Exception {
            TopologyBuilder builder = new TopologyBuilder();

            DRPCSpout spout = new DRPCSpout("jsonpost");
            builder.setSpout("drpc", spout, 1);
            builder.setBolt("return", new ReturnResults(),1).customGrouping("drpc", new YidGrouping());
            
            return builder.createTopology();
    }

    public StormTopology buildTopology() throws Exception {

        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("exclamation");
        builder.addBolt(new ExclaimBolt(), 3);

        return builder.createRemoteTopology();
    }
}
