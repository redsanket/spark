package hadooptest.hadoop.regression;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import hadooptest.TestSession;
import hadooptest.cluster.hadoop.HadoopCluster.Action;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class TestSetupCluster extends TestSession {

    @BeforeClass
    public static void startTestSession() throws IOException {
        TestSession.start();
    }

    @Test
    public void setupCluster() {
        try {

            // Initialize Action, default is STATUS.
            Action action = Action.STATUS;
            String actionStr = System.getProperty("ACTION", "").toUpperCase();
            if ((actionStr != null) && (!actionStr.isEmpty())) {
                action = Action.valueOf(actionStr);
            }
            
            // Initialize component, default is "".
            String component = System.getProperty("COMPONENT", "");
            if ((action == Action.STOP) || (action == Action.START)) {
                if ((component == null) || (component.isEmpty())) {
                    TestSession.logger
                            .error("Required component parameter is missing!!!");
                    System.exit(-1);
                }
            }

            // Initialize node, default node is ""
            String node = System.getProperty("NODE", "");

            // Perform the action.
            switch (action) {
            case STATUS:
                TestSession.logger.info("--> Get Cluster Status:");
                assertTrue(true);
                break;
            case START:
                TestSession.logger.info("--> Start Cluster Component:");
                if ((node == null) || (node.isEmpty())) {
                    cluster.hadoopDaemon(Action.START, component);
                } else {
                    cluster.hadoopDaemon(Action.START, component,
                            new String[] { node });
                }
                break;
            case STOP:
                TestSession.logger.info("--> Stop Cluster Component:");
                if ((node == null) || (node.isEmpty())) {
                    cluster.hadoopDaemon(Action.STOP, component);
                } else {
                    cluster.hadoopDaemon(Action.STOP, component,
                            new String[] { node });
                }
                break;
            case RESET:
                TestSession.logger.info("--> Reset Cluster:");
                assertTrue("Cluster reset failed", TestSession.cluster.reset());
                assertTrue(
                        "Cluster is not off of safemode after cluster reset",
                        TestSession.cluster.waitForSafemodeOff());
                break;
            }

            if (action == Action.RESET) {
                assertTrue("Cluster is not fully up",
                        TestSession.cluster.isFullyUp());
            } else if (action != Action.STATUS) {
                TestSession.cluster.isFullyUp();
            }
        } catch (Exception e) {
            TestSession.logger.error("Exception failure.", e);
            fail();
        }
    }

}