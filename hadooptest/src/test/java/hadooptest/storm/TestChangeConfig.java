package hadooptest.storm;

import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import hadooptest.SerialTests;
import hadooptest.TestSessionStorm;
import hadooptest.cluster.storm.ModifiableStormCluster;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SerialTests.class)
public class TestChangeConfig extends TestSessionStorm {
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

    @Test
    public void changeConfigTest() throws Exception {
        logger.info("Turning off UI security.");
        mc.unsetConf("ui.filter");
        logger.info("Starting changeConfigTest.");
        
        String newUIPort = "8101";
        
        logger.info("Setting a new UI port");
        mc.setConf("ui.port", newUIPort);
        logger.info("Restart ui");
        mc.restartCluster();

        logger.info("Asserting test result");
        //TODO lets find a good way to get the different hosts
        String[] result = exec.runProcBuilder(new String[] { "wget", "localhost:" + newUIPort });

        assertTrue("Result of wget exec was not zero", result[0].equals("0"));
    }
}
