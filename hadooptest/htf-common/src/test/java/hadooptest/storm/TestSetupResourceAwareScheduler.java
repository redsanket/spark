package hadooptest.storm;

import static org.junit.Assume.assumeTrue;
import static org.junit.Assert.assertTrue;
import hadooptest.SerialTests;
import hadooptest.TestSessionStorm;
import hadooptest.cluster.storm.ModifiableStormCluster;
import backtype.storm.generated.ClusterSummary;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SerialTests.class)
public class TestSetupResourceAwareScheduler extends TestSessionStorm{
    static ModifiableStormCluster mc;

    @BeforeClass
    public static void setup() throws Exception {
        assumeTrue(cluster instanceof ModifiableStormCluster);
        mc = (ModifiableStormCluster)cluster;
        logger.info("Setting to use Default scheduler");
        mc.setConf("storm.scheduler", "backtype.storm.scheduler.DefaultScheduler");
        logger.info("Restart Nimbus");
        mc.restartCluster();
    }

    @AfterClass
    public static void cleanup() throws Exception {
        if (mc != null) {
          mc.resetConfigsAndRestart();
        }
        stop();
    }

    @Test(timeout=600000)
    public void TestSetupResourceAwareScheduler() throws Exception {
      backtype.storm.Config theconf = new backtype.storm.Config();
      theconf.putAll(backtype.storm.utils.Utils.readStormConfig());

      String filter = (String)theconf.get("ui.filter");
      String pw = null;
      String user = null;

      // Only get bouncer auth on secure cluster.
      if ( filter != null ) {
          if (mc != null) {
              user = mc.getBouncerUser();
              pw = mc.getBouncerPassword();
          }
      }
      //storm.scheduler: "backtype.storm.scheduler.multitenant.MultitenantScheduler"
      String newScheduler = "backtype.storm.scheduler.multitenant.MultitenantScheduler";
      logger.info("Setting to use new scheduler");
      mc.setConf("storm.scheduler", newScheduler);
      logger.info("Restart Nimbus");
      mc.restartCluster();
      logger.info("Asserting test result");
      ClusterSummary sum = cluster.getClusterInfo();
      int upTime = sum.get_nimbus_uptime_secs();
      logger.info("Nimbus uptime: "+upTime);
      assertTrue("Nimbus uptime is more than zero", upTime>0);
      
      
    }
}
