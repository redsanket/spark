package hadooptest.storm.security;

import java.io.IOException;

import static org.junit.Assert.*;
import static org.junit.Assume.*;

import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import hadooptest.SerialTests;
import hadooptest.TestSessionStorm;
import hadooptest.cluster.storm.ModifiableStormCluster;
import backtype.storm.generated.*;

@Category(SerialTests.class)
public class TestAuthorization extends TestSessionStorm {
    static ModifiableStormCluster mc;

    @BeforeClass
    public static void setup() throws Exception {
        start();
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

    @Test(expected = AuthorizationException.class)
    public void testDenyAuthorization() throws Exception {
        mc.setConf("nimbus.authorizer", 
                "backtype.storm.security.auth.authorizer.DenyAuthorizer");
        mc.restartCluster();
        cluster.getClusterInfo(); //throws an exception
    }

    @Test
    public void testNoopAuthorization() throws Exception {
        mc.setConf("nimbus.authorizer", 
                "backtype.storm.security.auth.authorizer.NoopAuthorizer");
        mc.restartCluster();
        assertNotNull(cluster.getClusterInfo()); //Just validate that we can talk to nimbus
    }
}
