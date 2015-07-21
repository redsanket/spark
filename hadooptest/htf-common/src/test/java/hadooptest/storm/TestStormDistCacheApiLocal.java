
package hadooptest.storm;

import hadooptest.Util;
import hadooptest.SerialTests;
import hadooptest.TestSessionStorm;

import hadooptest.automation.utils.http.HTTPHandle;
import hadooptest.automation.utils.http.Response;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.httpclient.HttpMethod;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.lang.reflect.Array;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import static org.junit.Assert.*;
import hadooptest.cluster.storm.ModifiableStormCluster;
import static org.junit.Assume.assumeTrue;
import hadooptest.cluster.storm.StormDaemon;
import backtype.storm.blobstore.ClientBlobStore;
import backtype.storm.generated.SettableBlobMeta;

@Category(SerialTests.class)
public class TestStormDistCacheApiLocal extends TestStormDistCacheApi {

    static ModifiableStormCluster mc = (ModifiableStormCluster)cluster;

    @BeforeClass
    public static void setup() throws Exception {
        assumeTrue(cluster instanceof ModifiableStormCluster);
        start();
        mc = (ModifiableStormCluster)cluster;
        cluster.setDrpcAclForFunction("blobstore");
        cluster.setDrpcAclForFunction("permissions");
        cluster.setDrpcAclForFunction("md5");
        if (mc != null) {
            mc.setConf("client_blobstore_class", "backtype.storm.blobstore.NimbusBlobStore");
            mc.setConf("nimbus_blobstore_class", "backtype.storm.blobstore.LocalFsBlobStore");
            mc.setConf("supervisor_blobstore_class", "backtype.storm.blobstore.NimbusBlobStore");
            mc.restartCluster();
        }
    }

    @AfterClass
    public static void cleanup() throws Exception {
        if (mc != null) {
          mc.resetConfigsAndRestart();
        }
        stop();
    }

  @Test(timeout=240000)
  // The purpose of this test is to check whether supervisor crashes if an authorization exception is triggered
  // in the blobstore
  public void testDistCacheAuthAndKeyNotFoundExceptionForSupervisorCrash() throws Exception {
    testDistCacheForSupervisorCrash("u:test:rwa");
  }


  public HTTPHandle bouncerAuthentication() throws Exception {
    backtype.storm.Config theconf = new backtype.storm.Config();
    theconf.putAll(backtype.storm.utils.Utils.readStormConfig());
    Boolean secure = isUISecure();
    ModifiableStormCluster mc = (ModifiableStormCluster) cluster;
    String pw = null;
    String user = null;

    // Only get bouncer auth on secure cluster.
    if (secure) {
      if (mc != null) {
        user = mc.getBouncerUser();
        pw = mc.getBouncerPassword();
      }
    }

    logger.info("Asserting test result");
    //TODO lets find a good way to get the different hosts
    HTTPHandle client = new HTTPHandle();
    if (secure) {
      client.logonToBouncer(user, pw);
    }
    return client;
  }

  public JSONArray getSupervisorsUptime(HTTPHandle client) throws Exception
  {
    Integer port = null;

    logger.info("Cookie = " + client.YBYCookie);
    assertNotNull("Cookie is null", client.YBYCookie);
    ArrayList<String> uiNodes = mc.lookupRole(StormDaemon.UI);
    logger.debug("Will be connecting to UI at " + uiNodes.get(0));
    port = Integer.parseInt((String)mc.getConf("ystorm.ui_port", StormDaemon.UI));
    String uiURL = "http://" + uiNodes.get(0) + ":" + port + "/api/v1/supervisor/summary";

    HttpMethod getMethod = client.makeGET(uiURL, new String(""), null);
    Response response = new Response(getMethod);
    JSONObject obj = response.getJsonObject();
    JSONArray supervisorsUptimeDetails = obj.getJSONArray("supervisors");
    logger.debug("******* OUTPUT = " + response.getResponseBodyAsString());
    return supervisorsUptimeDetails;
  }

  public boolean convertAndCheckUptime(String beforeTopoLaunchUptime, String afterTopoLaunchUptime) {
     String[] btlu = beforeTopoLaunchUptime.split(" ");
     String[] atlu = afterTopoLaunchUptime.split(" ");
     int[] seconds_weight = {86400,3600,60,1};
     int seconds_length = seconds_weight.length;
     int uptimeBeforeLaunch = 0;
     int uptimeAfterLaunch = 0;
     for (int i=btlu.length-1; i > -1; i--) {
       uptimeBeforeLaunch += Integer.parseInt(btlu[i].substring(0, btlu[i].length()-1)) * seconds_weight[--seconds_length];
     }
     seconds_length = seconds_weight.length;
     for (int i=atlu.length-1; i > -1; i--) {
       uptimeAfterLaunch += Integer.parseInt(atlu[i].substring(0, atlu[i].length()-1)) * seconds_weight[--seconds_length];
     }
    return uptimeAfterLaunch < uptimeBeforeLaunch;
  }

  public boolean checkForSupervisorCrash(JSONArray supervisorsUptimeBeforeTopoLaunch, JSONArray supervisorsUptimeAfterTopoLaunch) {
    boolean checkCrash = false;
    if (supervisorsUptimeBeforeTopoLaunch.size() > supervisorsUptimeAfterTopoLaunch.size())
      return checkCrash;

    for (int i=0; i<supervisorsUptimeBeforeTopoLaunch.size(); i++) {
      if (convertAndCheckUptime((String)supervisorsUptimeBeforeTopoLaunch.getJSONObject(i).get("uptime"),
                                (String) supervisorsUptimeAfterTopoLaunch.getJSONObject(i).get("uptime"))) {
        return checkCrash;
      }
    }
    return !checkCrash;
  }

  public void testDistCacheForSupervisorCrash(String blobACLs) throws Exception {
    UUID uuid = UUID.randomUUID();
    String blobKey = uuid.toString() + ".jar";
    String blobContent = "This is integration blob content";
    String fileName = "myFile";

    // Just in case a hung test left a residual topology...
    killAll();

    kinit(conf.getProperty("SECONDARY_KEYTAB"), conf.getProperty("SECONDARY_PRINCIPAL") );
    ClientBlobStore clientBlobStore = getClientBlobStore();
    SettableBlobMeta settableBlobMeta = makeAclBlobMeta(blobACLs);
    createBlobWithContent(blobKey, blobContent, clientBlobStore, settableBlobMeta);
    kinit();

    try {
      // Launch a topology that will read a local file we give it over drpc
      logger.info("About to launch topology");

      HTTPHandle client = bouncerAuthentication();
      JSONArray supervisorUptimeBeforeTopoLaunch = getSupervisorsUptime(client);
      launchBlobStoreTopology(blobKey, fileName);
      // Wait for it to come up
      Util.sleep(30);
      JSONArray supervisorUptimeAfterTopoLaunch = getSupervisorsUptime(client);

      // Test for supervisors not crashing
      assertTrue("Supervisor Crashed", checkForSupervisorCrash(supervisorUptimeBeforeTopoLaunch, supervisorUptimeAfterTopoLaunch));

    } finally {
      killAll();
    }
  }
}
