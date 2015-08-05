
package hadooptest.storm;

import hadooptest.Util;
import hadooptest.SerialTests;
import hadooptest.TestSessionStorm;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import static org.junit.Assert.*;
import hadooptest.cluster.storm.ModifiableStormCluster;
import static org.junit.Assume.assumeTrue;
import hadooptest.cluster.storm.StormDaemon;
import static org.junit.Assert.assertEquals;

@Category(SerialTests.class)
public class TestPacemakerSanity extends TestSessionStorm {

    static ModifiableStormCluster mc = null;
    static String function = "exclamationTest";
    static String pmNode = null;

    @BeforeClass
    public static void setup() throws Exception {
        assumeTrue(cluster instanceof ModifiableStormCluster);
        start();
        mc = (ModifiableStormCluster)cluster;
        cluster.setDrpcAclForFunction(function);
        if (mc != null) {
            pmNode = mc.lookupRole(StormDaemon.PACEMAKER).get(0);
            mc.setConf("pacemaker_auth_method", "KERBEROS");
            mc.setConf("pacemaker_host", pmNode);
            mc.setConf("pacemaker_kerberos_users", conf.getProperty("PACEMAKER_PRINCIPAL"));
            mc.setConf("storm_cluster_state_store", "org.apache.storm.pacemaker.pacemaker_state_factory");
            mc.startDaemon(StormDaemon.PACEMAKER);
            mc.restartCluster();
        }

    }

    public void launchTestTopology( ) throws Exception {
        String pathToJar = "/home/y/lib64/jars/storm-starter.jar";
        String[] returnValue = exec.runProcBuilder(new String[] { "storm", "jar", pathToJar, 
            "storm.starter.BasicDRPCTopology", function, function }, true);
        assertTrue( "Could not launch topology", returnValue[0].equals("0") );
    }

    @Test(timeout = 600000)
    public void sanityTest() throws Exception {
        // Test and make sure supervisors are alive
        assertTrue("Supervisor did not stay up", !didSupervisorCrash());

        try {
            // Now Launch topology, test topology, kill topology
            launchTestTopology();
            
            Util.sleep(30);

            // Now hit it with DRPC.
            String drpcResult = cluster.DRPCExecute(function, "hello");
            assertEquals( drpcResult, "hello!");
            Util.sleep(10);
            drpcResult = cluster.DRPCExecute(function, "there");
            assertEquals( drpcResult, "there!");
            Util.sleep(10);
            drpcResult = cluster.DRPCExecute(function, "again");
            assertEquals( drpcResult, "again!");
        } finally {
            mc.killTopology(function);
        }
    }

    @AfterClass
    public static void cleanup() throws Exception {
        if (mc != null) {
          mc.resetConfigsAndRestart();
          mc.stopDaemon(StormDaemon.PACEMAKER);
        }
        stop();
    }
}
