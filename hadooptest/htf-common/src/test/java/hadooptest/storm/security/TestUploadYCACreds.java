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
import hadooptest.workflow.storm.topology.bolt.GetYCACertForAppIdBolt;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.junit.Assume.assumeTrue;

@SuppressWarnings("deprecation")
@Category(SerialTests.class)
public class TestUploadYCACreds extends TestSessionStorm {
    public static final String DRPC_FUNCTION_NAME = "getYCACerts";
    static ModifiableStormCluster mc;
    private backtype.storm.Config _conf;
    private backtype.storm.Config _conf2;
    private HttpClient httpClient;
    private String ycaRole = "ystorm.test.yca.users";
    private String ycaV1Role = "yahoo.griduser.hadoopqa";


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

    public TestUploadYCACreds() {
        _conf = new backtype.storm.Config();
        _conf.putAll(backtype.storm.utils.Utils.readStormConfig());

        _conf2 = new backtype.storm.Config();
    }

    @Test(timeout = 700000)
    public void TestUploadYCACredsUsingDRPC() throws Exception {
        logger.info("Starting TestUploadYCACredsUsingDRPC");
        StormTopology topology = buildTopology();

        String topoName = "upload-yca-creds-test1";

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
            //Wait for topology to be up.
            Thread.sleep(10000);
            pushCreds(topoName, ycaRole);
            httpClient = getHttpClient(topoName);
            String updatedYCACert = getYCACertForAppFromDRPC(topoName, ycaRole);
            logger.info("Updated YCA Cert is: " + updatedYCACert);
            assertNotNull("Updated Cert is null", updatedYCACert);
            assertFalse("Updated Cert is empty", updatedYCACert.isEmpty());
            assertTrue("The updated YCA Cert is not a v2", 
                 updatedYCACert.toLowerCase().contains("v=2;a=yahoo." + ycaRole + ";u=hadoopqa"));
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

    private void pushCreds(String topoName, String ycaRole) throws Exception {
        String[] pushCredsArgs = new String[] {
             "storm", "upload-credentials", topoName, 
             "-c", "yahoo.autoyca.v1appid=" + ycaV1Role, 
             "-c", "yahoo.autoyca.appids="+ ycaRole};
        exec.runProcBuilderSecurity(pushCredsArgs, "hadoopqa");
    }

    private String getDRPCRequestUrl(String topoName, String appId) {
        // Get the URI to ping
        List<String> servers = (List<String>) _conf.get(backtype.storm.Config.DRPC_SERVERS);
        if (servers == null || servers.isEmpty()) {
            throw new RuntimeException("No DRPC servers configured for topology");
        }
        return "http://" + servers.get(0) + ":" + _conf.get(backtype.storm.Config.DRPC_HTTP_PORT)
                + "/drpc/" + DRPC_FUNCTION_NAME + "/" + appId;
    }

    private String getYCACertForAppFromDRPC(String topoName, String appId) throws AuthorizationException, Exception {
        String DRPCURI = getDRPCRequestUrl(topoName, appId);
        logger.info("Attempting to connect to drpc server with " + DRPCURI);

        String responseCert = null;
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
            TestSessionStorm.logger.warn("Trying [" + tryCount + "] to get status at " + DRPCURI);
            try {
                resp = req.send();
            } catch (Exception e) {
                TestSessionStorm.logger.error("Got Exception " , e );
                tryCount -= 1;
            }

            if (resp != null && resp.getStatus() == 200) {
                done = true;
                responseCert = resp.getContentAsString();
                logger.info("Got back [" + responseCert + "] from drpc.");
            }
        }
        // Did we fail?
        if (!done) {
            throw new IOException("Timed out trying to get DRPC response\n");
        }
        return responseCert;
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
        builder.addBolt(new GetYCACertForAppIdBolt(), 3);

        return builder.createRemoteTopology();
    }
}
