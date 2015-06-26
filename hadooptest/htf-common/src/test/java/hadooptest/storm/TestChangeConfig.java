package hadooptest.storm;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assume.assumeTrue;
import hadooptest.Util;
import hadooptest.SerialTests;
import hadooptest.TestSessionStorm;
import hadooptest.cluster.storm.ModifiableStormCluster;
import hadooptest.cluster.storm.StormDaemon;
import hadooptest.automation.utils.http.HTTPHandle;
import org.apache.commons.httpclient.HttpMethod;
import hadooptest.automation.utils.http.Response;
import java.util.ArrayList;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SerialTests.class)
public class TestChangeConfig extends TestSessionStorm {
    static ModifiableStormCluster mc;

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

    @Test(timeout=600000)
    public void changeConfigTest() throws Exception {
        backtype.storm.Config theconf = new backtype.storm.Config();
        theconf.putAll(backtype.storm.utils.Utils.readStormConfig());

        Boolean secure = isUISecure();
        String pw = null;
        String user = null;

        // Only get bouncer auth on secure cluster.
        if ( secure ) {
            if (mc != null) {
                user = mc.getBouncerUser();
                pw = mc.getBouncerPassword();
            }
        }
        
        String newUIPort = "8101";
        
        logger.info("Setting a new UI port");
        mc.setConf("ui.port", newUIPort, StormDaemon.UI);
        logger.info("Restart ui");
        mc.restartDaemon(StormDaemon.UI);
        Util.sleep(120);

        logger.info("Asserting test result");
        //TODO lets find a good way to get the different hosts
        HTTPHandle client = new HTTPHandle();
        if ( secure ) {
            client.logonToBouncer(user,pw);
        }
        logger.info("Cookie = " + client.YBYCookie);
        assertNotNull("Cookie is null", client.YBYCookie);
        ArrayList<String> uiNodes = mc.lookupRole(StormDaemon.UI);
        logger.info("Will be connecting to UI at " + uiNodes.get(0));
        String uiURL = "http://" + uiNodes.get(0) + ":8101";
        HttpMethod getMethod = client.makeGET(uiURL, new String(""), null);
        Response response = new Response(getMethod, false);
        logger.info("******* OUTPUT = " + response.getResponseBodyAsString());

        assertTrue("Result of ui get was not zero", response.getStatusCode() == 200);
    }
}
