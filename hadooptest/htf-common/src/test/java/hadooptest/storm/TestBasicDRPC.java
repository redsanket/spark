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

import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import backtype.storm.drpc.LinearDRPCTopologyBuilder;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.TopologySummary;
import backtype.storm.generated.KillOptions;

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
    private HttpClient _http_client;
    private String _drpc_base;
    private String _topo_name;

    @BeforeClass
    public static void setup() throws Exception {
        assumeTrue(cluster instanceof ModifiableStormCluster);
        mc = (ModifiableStormCluster)cluster;

        cluster.setDrpcAclForFunction("exclamation");
        cluster.setDrpcAclForFunction("jsonpost");
    }

    @AfterClass
    public static void cleanup() throws Exception {
        if (mc != null) {
            mc.resetConfigs();
        }
        stop();
    }

    @Before
    public void topoSetup() throws Exception {
        _http_client = new HttpClient();
        _http_client.setIdleTimeout(30000);
        _http_client.start();
        // Get the URI to ping
        List<String> servers = (List<String>) _conf.get(backtype.storm.Config.DRPC_SERVERS);
        if(servers == null || servers.isEmpty()) {
            throw new RuntimeException("No DRPC servers configured");   
        }
        String httpPort = (String)mc.getConf("ystorm.drpc_http_port", hadooptest.cluster.storm.StormDaemon.DRPC);
        _drpc_base = "http://" + servers.get(0) + ":" + httpPort + "/drpc/";
    }

    @After
    public void topoCleanup() throws Exception {
        if (_http_client != null) {
            _http_client.stop();
            _http_client = null;
        }
        if (_topo_name != null) {
            KillOptions ko = new KillOptions();
            ko.set_wait_secs(0);
            cluster.killTopology(_topo_name, ko);
            _topo_name = null;
        }
    }

    public TestBasicDRPC(){
        _conf = new backtype.storm.Config();
        _conf.putAll(backtype.storm.utils.Utils.readStormConfig());

        _conf2 = new backtype.storm.Config();
    }


    public void submitTopology(StormTopology topology, String topoName, backtype.storm.Config topoConf) throws Exception{
        File jar = new File(conf.getProperty("WORKSPACE") + "/topologies/target/topologies-1.0-SNAPSHOT-jar-with-dependencies.jar");
        try {
            cluster.submitTopology(jar, topoName, topoConf, topology);
            _topo_name = topoName;
        } catch (Exception e) {
            logger.error("Couldn't launch topology: ", e );
            throw e;
        }
    }

    public ContentResponse drpcTryLoop(String uri) throws Exception {
        return drpcTryLoop(uri, null, null, null);
    }

    public ContentResponse drpcTryLoop(String uri, HttpMethod method, String inputFile, String contentType) throws Exception {
        boolean done = false;
        int tryCount = 200;
        Exception other = null;
        while (!done && tryCount > 0) {
            Request req;
            try {
                req = _http_client.newRequest(uri);
                if (isDrpcSecure()) {
                    String v1Role = "yahoo.grid_re.storm." + conf.getProperty("CLUSTER_NAME");
                    String ycaCert = getYcaV1Cert(v1Role);
                    req = req.header("Yahoo-App-Auth", ycaCert);
                }
                if (method != null) {
                    req.method(method);
                }
                if (contentType != null) {
                    req.header("Content-Type", contentType);
                }
                if (inputFile != null) {
                    java.nio.file.Path inputPath = Paths.get(inputFile);
                    req.file(inputPath);
                }
            } catch (Exception e) {
                TestSessionStorm.logger.error("Request failed to URL " + uri, e);
                throw e;
            }
            ContentResponse resp = null;
            TestSessionStorm.logger.warn("Trying " + req);
            try {
                resp = req.send();
            } catch (Exception e) {
                tryCount -= 1;
                other = e;
            }

            if (resp != null && resp.getStatus() == 200) {
                done = true;
                return resp;
            }
            Thread.sleep(1000);
        }
        if (other != null) {
            throw new Exception("Could not get a proper response back from "+uri, other);
        } else {
            throw new Exception("Could not get a proper response back from "+uri);
        }
    }

    @Test(timeout=600000)
    public void TestDRPCTopologyHTTP() throws Exception{
        logger.info("Starting TestDRPCTopologyHTTP");

        _conf2.setDebug(true);
        _conf2.setNumWorkers(3);
        submitTopology(buildTopology(), "drpc-topology-test1", _conf2);

        boolean passed = false;
        String DRPCURI = _drpc_base + "exclamation/hello";
        logger.info("Attempting to connect to drpc server with " + DRPCURI);

        ContentResponse resp = drpcTryLoop(DRPCURI);
        // Check the response value.  Should be "hello!"
        String respString = resp.getContentAsString();
        logger.info("Got back " + respString + " from drpc.");
        assertEquals("hello!", respString);
    }
    
    @Test(timeout=600000)
    public void TestDRPCTopologyHTTPPost() throws Exception{
        logger.info("Starting TestDRPCTopologyHTTPPost");

        _conf2.setDebug(true);
        _conf2.setNumWorkers(3);
        submitTopology(buildTopology(), "drpc-topology-test-post", _conf2);

        String inputFileDir = new String(conf.getProperty("WORKSPACE") + "/htf-common/resources/storm/testinputoutput/TestBasicDRPC/input.txt");
        java.nio.file.Path inputPath = Paths.get(inputFileDir);

        boolean passed = false;
        String DRPCURI = _drpc_base + "exclamation";
        logger.info("Attempting to connect to drpc server with " + DRPCURI);

        ContentResponse resp = drpcTryLoop(DRPCURI, HttpMethod.POST, inputFileDir, null);
        // Check the response value.  Should be "Hello World!"
        String respString = resp.getContentAsString();
        logger.info("Got back " + respString + " from drpc.");
        assertEquals("Hello World!", respString);
    }

    @Test(timeout=600000)
    public void TestDRPCTopologyJSONPost() throws Exception{
        logger.info("Starting TestDRPCTopologyJSONPost");

        backtype.storm.Config stormConfig = new backtype.storm.Config();
        stormConfig.setDebug(true);
        stormConfig.put(backtype.storm.Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, 8);
        stormConfig.put(backtype.storm.Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 32);
        stormConfig.put(backtype.storm.Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
        stormConfig.put(backtype.storm.Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384);
        stormConfig.setNumWorkers(3);
        stormConfig.setNumAckers(3);
        submitTopology(buildRamTopology(), "drpc-topology-json-post", stormConfig);

        String inputFileDir = new String(conf.getProperty("WORKSPACE") + "/htf-common/resources/storm/testinputoutput/TestBasicDRPC/input.json");
        String DRPCURI = _drpc_base + "jsonpost";
        logger.info("Attempting to connect to drpc server with " + DRPCURI);

        for (int i = 0; i < 3; i++) {
            ContentResponse resp = drpcTryLoop(DRPCURI, HttpMethod.POST, inputFileDir, "application/json");
            String respString = resp.getContentAsString();
            logger.info("Got back " + respString + " from drpc.");
            assertTrue(respString.contains("login"));
        }
    }


    @Test(timeout=600000)
    public void TestMetrics() throws Exception{
        logger.info("Starting TestMetrics");
        ServerSocket ss = new ServerSocket(0);
        int port = ss.getLocalPort();
        String dest = InetAddress.getLocalHost().getCanonicalHostName()+":"+ss.getLocalPort();

        _conf2.setDebug(true);
        _conf2.setNumWorkers(3);
        _conf2.registerMetricsConsumer(ForwardingMetricsConsumer.class, dest, 1);
        _conf2.put(backtype.storm.Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS, 15); //15 seconds to make the test run faster
        submitTopology(buildTopology(), "drpc-metrics-test1", _conf2);

        Socket s = ss.accept();
        BufferedReader r = new BufferedReader(new InputStreamReader(s.getInputStream()));

        String DRPCURI = _drpc_base + "exclamation/hello";
        ContentResponse resp = drpcTryLoop(DRPCURI);
        String respString = resp.getContentAsString();
        logger.info("Got back " + respString + " from drpc.");
        assertEquals("hello!", respString);

        String line;
        int tryCount = 0;
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
