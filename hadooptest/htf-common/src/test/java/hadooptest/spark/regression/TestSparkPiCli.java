package hadooptest.spark.regression;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.workflow.spark.app.AppMaster;
import hadooptest.workflow.spark.app.SparkPi;

import hadooptest.workflow.spark.app.SparkRunSparkSubmit;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SerialTests.class)
public class TestSparkPiCli extends TestSession {

    @BeforeClass
    public static void startTestSession() {
        TestSession.start();
    }


    /*
     * A test for running SparkPi in Standalone mode
     * 
     */
    @Test
    public void runSparkPiTestStandaloneMode() throws Exception {
        SparkPi appUserDefault = new SparkPi();

        appUserDefault.setMaster(AppMaster.YARN_STANDALONE);
        appUserDefault.setWorkerMemory("1g");
        appUserDefault.setNumWorkers(1);
        appUserDefault.setWorkerCores(1);
        String appName = "SparkTestPiCli";
        appUserDefault.setAppName(appName);
        appUserDefault.setShouldPassName(true);
        appUserDefault.setQueueName("default");

        appUserDefault.start();

        assertTrue("SparkPi app (default user) was not assigned an ID within 30 seconds.",
            appUserDefault.waitForID(30));
        assertTrue("SparkPi app ID for sleep app (default user) is invalid.",
            appUserDefault.verifyID());
        assertEquals("SparkPi app name for sleep app is invalid.",
            appName, appUserDefault.getAppName());

        int waitTime = 30;
        assertTrue("App (default user) did not succeed.",
            appUserDefault.waitForSuccess(waitTime));
    }

}
