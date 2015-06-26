package hadooptest.storm.security;

import backtype.storm.drpc.DRPCSpout;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;
import backtype.storm.drpc.ReturnResults;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.TopologySummary;
import backtype.storm.topology.TopologyBuilder;
import hadooptest.SerialTests;
import hadooptest.TestSessionStorm;
import hadooptest.cluster.storm.ModifiableStormCluster;
import hadooptest.workflow.storm.topology.bolt.GetTicketTimeBolt;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.json.simple.JSONValue;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

@SuppressWarnings("deprecation")
@Category(SerialTests.class)
public class TestUploadCreds extends TestSessionStorm {
    public static final String DRPC_FUNCTION_NAME = "getTGTEndTime";
    static ModifiableStormCluster mc;
    private backtype.storm.Config _conf;
    private backtype.storm.Config _conf2;
    private HttpClient httpClient;


    @BeforeClass
    public static void setup() throws Exception {
        assumeTrue(cluster instanceof ModifiableStormCluster);
        mc = (ModifiableStormCluster) cluster;

        cluster.setDrpcInvocationAuthAclForFunction(DRPC_FUNCTION_NAME, "hadoopqa");
        String v1Role = "yahoo.grid_re.storm." + conf.getProperty("CLUSTER_NAME");
        cluster.setDrpcClientAuthAclForFunction(DRPC_FUNCTION_NAME, "hadoopqa," + v1Role);
    }

    @AfterClass
    public static void cleanup() throws Exception {
        if (mc != null) {
            mc.resetConfigs();
        }
        stop();
    }

    public TopologySummary getTS(String name) throws Exception {
        for (TopologySummary ts : cluster.getClusterInfo().get_topologies()) {
            if (name.equals(ts.get_name())) {
                return ts;
            }
        }
        throw new IllegalArgumentException("Topology " + name + " does not appear to be up yet");
    }

    public int getUptime(String name) throws Exception {
        return getTS(name).get_uptime_secs();
    }

    public TestUploadCreds() {
        _conf = new backtype.storm.Config();
        _conf.putAll(backtype.storm.utils.Utils.readStormConfig());

        _conf2 = new backtype.storm.Config();
    }

    @Test(timeout = 700000)
    public void TestUploadCredsUsingDRPC() throws Exception {
        logger.info("Starting TestUploadCreds");
        StormTopology topology = buildTopology();

        String topoName = "upload-creds-test1";

        _conf2.setDebug(true);
        _conf2.setNumWorkers(3);

        File jar = new File(conf.getProperty("WORKSPACE") + "/topologies/target/topologies-1.0-SNAPSHOT-jar-with-dependencies.jar");
        try {
            cluster.submitTopology(jar, topoName, _conf2, topology);
        } catch (Exception e) {
            logger.error("Couldn't launch topology: ", e);
            throw new Exception(e);
        }

        try {

            httpClient = getHttpClient(topoName);
            Long originalTGTEndTime = getTGTEndTimeFromDRPC(topoName);
            logger.info("Original TGT EndTime is: " + originalTGTEndTime.toString());
            //kinit and push new credentials
            pushCreds(topoName);
            Long updatedTGTEndTime = getTGTEndTimeFromDRPC(topoName);

            logger.info("Updated TGT EndTime is: " + updatedTGTEndTime.toString());
            assertFalse("The original TGTEndTime can't be >= updated TGTEndTime",
                    originalTGTEndTime >= updatedTGTEndTime);
        } finally {
            // Stop client
            try {
                httpClient.stop();
            } catch (Exception e) {
                throw new IOException("Could not stop http client", e);
            }
            cluster.killTopology(topoName);
        }
    }

    private void pushCreds(String topoName) throws Exception {
        String[] kinitArgs = new String[]{"kinit", "-kt",
                "/homes/hadoopqa/hadoopqa.dev.headless.keytab", "hadoopqa@DEV.YGRID.YAHOO.COM"};
        exec.runProcBuilderSecurity(kinitArgs, "hadoopqa");

        String[] pushCredsArgs = new String[]{"storm", "upload-credentials", topoName};
        exec.runProcBuilderSecurity(pushCredsArgs, "hadoopqa");
    }

    private String getDRPCRequestUrl(String topoName) {
        // Get the URI to ping
        List<String> servers = (List<String>) _conf.get(backtype.storm.Config.DRPC_SERVERS);
        if (servers == null || servers.isEmpty()) {
            throw new RuntimeException("No DRPC servers configured for topology");
        }
        return "http://" + servers.get(0) + ":" + _conf.get(backtype.storm.Config.DRPC_HTTP_PORT)
                + "/drpc/" + DRPC_FUNCTION_NAME + "/hello";
    }

    private Long getTGTEndTimeFromDRPC(String topoName) throws AuthorizationException, Exception {
        String DRPCURI = getDRPCRequestUrl(topoName);
        logger.info("Attempting to connect to drpc server with " + DRPCURI);

        Long TGTEndTime = 0L;
        boolean done = false;
        int tryCount = 200;
        while (!done && tryCount > 0) {
            Thread.sleep(1000);
            Request req;
            try {
                if (isDrpcSecure()) {
                    String v1Role = "yahoo.grid_re.storm." + conf.getProperty("CLUSTER_NAME");
                    String ycaCert = getYcaV1Cert(v1Role);
                    req = httpClient.newRequest(DRPCURI).header("Yahoo-App-Auth", ycaCert);
                } else {
                    req = httpClient.newRequest(DRPCURI);
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
                TGTEndTime = Long.parseLong(respString);
            }
        }
        // Did we fail?
        if (!done) {
            throw new IOException("Timed out trying to get DRPC response\n");
        }
        return TGTEndTime;
    }

    private HttpClient getHttpClient(String topoName) throws IOException {
        // Let's try for 3 minutes, or until we get a 200 back.
        // Configure the Jetty client to talk to the RS.
        // TODO:  Add API to the registry stub to do all this for us.....
        // Create and start client
        HttpClient client = new HttpClient();
        client.setIdleTimeout(30000);
        try {
            client.start();
        } catch (Exception e) {
            throw new IOException("Could not start Http Client", e);
        }
        return client;
    }

    public StormTopology buildTopology() throws Exception {

        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder(DRPC_FUNCTION_NAME);
        builder.addBolt(new GetTicketTimeBolt(), 3);

        return builder.createRemoteTopology();
    }
}
