package hadooptest.spark.regression;

import static org.junit.Assert.assertTrue;

import hadooptest.workflow.spark.app.SparkRunClass;
import hadooptest.workflow.spark.app.SparkRunSparkSubmit;
import org.junit.experimental.categories.Category;
import hadooptest.TestSession;
import hadooptest.workflow.spark.app.SparkRunSparkSubmit;

import org.junit.BeforeClass;
import org.junit.Test;

import hadooptest.Util;

// QE cluster isn't large enough to run in parallel
//@Category(hadooptest.ParallelClassAndMethodTests.class)
@Category(SerialTests.class)
public class TestSparkHackCM extends TestSession {

    private static String localJar = null;

    @BeforeClass
    public static void startTestSession() throws Exception {
        TestSession.start();
        setupTestDir();
    }

    public static void setupTestDir() throws Exception {

        TestSession.cluster.getFS();
        localJar =
            Util.getResourceFullPath("../htf-common/target/htf-common-1.0-SNAPSHOT-tests.jar");
    }

    @Test
    public void runSparkHackCMAuthOff() throws Exception {
        SparkRunSparkSubmit appUserDefault = new SparkRunSparkSubmit();

        appUserDefault.setWorkerMemory("2g");
        // can only use 1 worker
        appUserDefault.setNumWorkers(1);
        appUserDefault.setWorkerCores(1);
        appUserDefault.setClassName("org.apache.spark.examples.SparkRunHackCM");
        appUserDefault.setJarName(localJar);
        appUserDefault.setAppName("runSparkHackCM");
        appUserDefault.setShouldPassName(true);
        String[] argsArray = {"false", "invalid"};
        appUserDefault.setArgs(argsArray);

        appUserDefault.start();

        assertTrue("App (default user) was not assigned an ID within 30 seconds.",
            appUserDefault.waitForID(30));
        assertTrue("App ID for sleep app (default user) is invalid.",
            appUserDefault.verifyID());

        int waitTime = 30;
        assertTrue("Job (default user) did not succeed.",
            appUserDefault.waitForSuccess(waitTime));
    }

    @Test
    public void runSparkHackCM() throws Exception {
        SparkRunSparkSubmit appUserDefault = new SparkRunSparkSubmit();

        appUserDefault.setWorkerMemory("2g");
        // can only use 1 worker
        appUserDefault.setNumWorkers(1);
        appUserDefault.setWorkerCores(1);
        appUserDefault.setClassName("org.apache.spark.examples.SparkRunHackCM");
        appUserDefault.setJarName(localJar);
        appUserDefault.setAppName("runSparkHackCM");
        appUserDefault.setShouldPassName(true);
        String[] argsArray = {"true", "invalid"};
        appUserDefault.setArgs(argsArray);

        appUserDefault.start();

        assertTrue("App (default user) was not assigned an ID within 30 seconds.",
            appUserDefault.waitForID(30));
        assertTrue("App ID for sleep app (default user) is invalid.",
            appUserDefault.verifyID());

        int waitTime = 30;
        assertTrue("Job (default user) did not succeed.",
            appUserDefault.waitForSuccess(waitTime));
    }
}
