package hadooptest.spark.regression;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.workflow.spark.app.AppMaster;
import hadooptest.workflow.spark.app.SparkPi;

import hadooptest.workflow.spark.app.SparkRunSparkSubmit;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SerialTests.class)
public class TestSparkPiCliSparkSubmit extends TestSession {

    @BeforeClass
    public static void startTestSession() {
        TestSession.start();
    }


    /*
     * A test for running SparkPi in Standalone mode
     *
     */
    @Test
    public void runSparkPiTestStandaloneModeSparkSubmit() throws Exception {
        SparkRunSparkSubmit appUserDefault = new SparkRunSparkSubmit();

        appUserDefault.setMaster(AppMaster.YARN_STANDALONE);
        appUserDefault.setWorkerMemory("1g");
        appUserDefault.setNumWorkers(1);
        appUserDefault.setWorkerCores(1);
        appUserDefault.setClassName("org.apache.spark.examples.SparkPi");
        String appName = "SparkTestPiCliSparkSubmit";
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

    /*
     * A test for running SparkPi in YARN-client mode
     *
     */
    @Test
    public void runSparkPiTestYarnClientModeSparkSubmit() throws Exception {
        SparkRunSparkSubmit appUserDefault = new SparkRunSparkSubmit();

        appUserDefault.setMaster(AppMaster.YARN_CLIENT);
        appUserDefault.setWorkerMemory("1g");
        appUserDefault.setNumWorkers(1);
        appUserDefault.setWorkerCores(1);
        appUserDefault.setClassName("org.apache.spark.examples.SparkPi");
        String appName = "SparkTestPiCliSparkSubmit";
        appUserDefault.setAppName(appName);
        appUserDefault.setShouldPassName(true);
        appUserDefault.setQueueName("default");

        appUserDefault.start();

        assertTrue("SparkPi app (default user) was not assigned an ID within 30 seconds.",
            appUserDefault.waitForID(30));
        assertTrue("SparkPi app ID for sleep app (default user) is invalid.",
            appUserDefault.verifyID());
        // bug open for this
        //assertEquals("SparkPi app name for sleep app is invalid.",
        //    appName, appUserDefault.getAppName());

        int waitTime = 30;
        assertTrue("App (default user) did not succeed.",
            appUserDefault.waitForSuccess(waitTime));
    }

    /*
     * A test for running SparkPi in Standalone mode
     *
     */
    @Test
    public void runSparkPiTestStandaloneModeSparkSubmitWithDebugLogging() throws Exception {
        SparkRunSparkSubmit appUserDefault = new SparkRunSparkSubmit();

        appUserDefault.setMaster(AppMaster.YARN_STANDALONE);
        appUserDefault.setWorkerMemory("1g");
        appUserDefault.setNumWorkers(1);
        appUserDefault.setWorkerCores(1);
        String localFile = Util.getResourceFullPath("resources/spark/data/log4j.properties");
        appUserDefault.setLog4jFile(localFile);
        appUserDefault.setClassName("org.apache.spark.examples.SparkPi");
        String appName = "SparkTestPiCliSparkSubmit";
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

       // wait a few seconds for yarn to aggregate logs
       Util.sleep(10);

        // confirm DEBUG was actually on
        assertTrue("DEBUG found in log file", appUserDefault.grepLogsCLI("(.*) DEBUG SparkEnv(.*)"));

    }
}
