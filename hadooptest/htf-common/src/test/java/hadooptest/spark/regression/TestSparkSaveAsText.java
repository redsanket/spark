package hadooptest.spark.regression;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.experimental.categories.Category;
import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.workflow.spark.app.AppMaster;
import hadooptest.workflow.spark.app.SparkRunSparkSubmit;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;

import hadooptest.Util;

@Category(SerialTests.class)
public class TestSparkSaveAsText extends TestSession {

    /**
     * *************************************************************
     * Please set up input file name here *
     * **************************************************************
     */
    private static String localJar = null;
    private static String lrDatafile = "lr_data.txt";
    private static String saveAsFile = "saveAsTextTest.txt";
    private static String saveAsFile2 = "saveAsTextTest2.txt";
    private static String hdfsDir = "/user/" + System.getProperty("user.name") + "/";

    @BeforeClass
    public static void startTestSession() throws Exception {
        TestSession.start();
        setupTestDir();
    }

    @AfterClass
    public static void endTestSession() throws Exception {
        removeTestDir();
    }

    @Before
    public void cleanup() throws Exception {
        TestSession.cluster.getFS().delete(new Path(hdfsDir + saveAsFile), true);
        TestSession.cluster.getFS().delete(new Path(hdfsDir + saveAsFile2), true);
    }


    public static void setupTestDir() throws Exception {

        TestSession.cluster.getFS();
        String localFile = Util.getResourceFullPath("resources/spark/data/" + lrDatafile);
        localJar =
            Util.getResourceFullPath("../htf-common/target/htf-common-1.0-SNAPSHOT-tests.jar");
        System.out.println("LR data File is: " + localFile);
        TestSession.cluster.getFS().copyFromLocalFile(new Path(localFile), new Path(hdfsDir));
    }

    public static void removeTestDir() throws Exception {

        // Delete the file
        TestSession.cluster.getFS().delete(new Path(hdfsDir + lrDatafile), true);
        TestSession.cluster.getFS().delete(new Path(hdfsDir + saveAsFile), true);
        TestSession.cluster.getFS().delete(new Path(hdfsDir + saveAsFile2), true);
    }

    @Test
    public void runSparkSaveAsTextSparkSubmit() throws Exception {
        SparkRunSparkSubmit appUserDefault = new SparkRunSparkSubmit();

        appUserDefault.setWorkerMemory("2g");
        appUserDefault.setNumWorkers(3);
        appUserDefault.setWorkerCores(1);
        appUserDefault.setClassName("hadooptest.spark.regression.SparkSaveAsText");
        appUserDefault.setJarName(localJar);
        String[] argsArray = {lrDatafile, hdfsDir + saveAsFile};
        appUserDefault.setArgs(argsArray);

        appUserDefault.start();

        assertTrue("App (default user) was not assigned an ID within 30 seconds.",
            appUserDefault.waitForID(30));
        assertTrue("App ID for sleep app (default user) is invalid.",
            appUserDefault.verifyID());
        assertEquals("App name for sleep app is invalid.",
            "hadooptest.spark.regression.SparkSaveAsText", appUserDefault.getAppName());

        int waitTime = 30;
        assertTrue("Job (default user) did not succeed.",
            appUserDefault.waitForSuccess(waitTime));
    }

    @Test
    public void runSparkSaveAsTextClientModeSparkSubmit() throws Exception {
        SparkRunSparkSubmit appUserDefault = new SparkRunSparkSubmit();

        appUserDefault.setMaster(AppMaster.YARN_CLIENT);
        appUserDefault.setWorkerMemory("2g");
        appUserDefault.setNumWorkers(3);
        appUserDefault.setWorkerCores(1);
        appUserDefault.setClassName("hadooptest.spark.regression.SparkSaveAsText");
        appUserDefault.setJarName(localJar);
        // tests can run in parallel so we need to use different file name
        String[] argsArray = {lrDatafile, hdfsDir + saveAsFile2};
        appUserDefault.setArgs(argsArray);

        appUserDefault.start();

        assertTrue("App (default user) was not assigned an ID within 30 seconds.",
            appUserDefault.waitForID(30));
        assertTrue("App ID for sleep app (default user) is invalid.",
            appUserDefault.verifyID());
        assertEquals("App name for sleep app is invalid.",
            "SparkSaveAsText", appUserDefault.getAppName());

        int waitTime = 30;
        assertTrue("Job (default user) did not succeed.",
            appUserDefault.waitForSuccess(waitTime));
    }

    @Test
    public void runSparkSaveAsTextSparkSubmitWithExistingOutput() throws Exception {
        SparkRunSparkSubmit appUserDefault = new SparkRunSparkSubmit();

        TestSession.cluster.getFS().mkdirs(new Path(hdfsDir + saveAsFile));

        appUserDefault.setWorkerMemory("2g");
        appUserDefault.setNumWorkers(3);
        appUserDefault.setWorkerCores(1);
        appUserDefault.setClassName("hadooptest.spark.regression.SparkSaveAsText");
        appUserDefault.setJarName(localJar);
        String[] argsArray = {lrDatafile, hdfsDir + saveAsFile};
        appUserDefault.setArgs(argsArray);

        appUserDefault.start();

        assertTrue("App (default user) was not assigned an ID within 30 seconds.",
            appUserDefault.waitForID(30));
        assertTrue("App ID for sleep app (default user) is invalid.",
            appUserDefault.verifyID());
        assertEquals("App name for sleep app is invalid.",
            "hadooptest.spark.regression.SparkSaveAsText", appUserDefault.getAppName());

        int waitTime = 30;
        assertTrue("Job (default user) did not error.",
            appUserDefault.waitForFailure(waitTime));
    }

    @Test
    public void runSparkSaveAsTextWithLoggingSparkSubmit() throws Exception {
        SparkRunSparkSubmit appUserDefault = new SparkRunSparkSubmit();

        appUserDefault.setMaster(AppMaster.YARN_CLIENT);
        appUserDefault.setWorkerMemory("2g");
        appUserDefault.setNumWorkers(3);
        appUserDefault.setWorkerCores(1);
        appUserDefault.setClassName("hadooptest.spark.regression.SparkSaveAsText");
        appUserDefault.setJarName(localJar);

        String localFile = Util.getResourceFullPath("resources/spark/data/log4j.properties");
        appUserDefault.setLog4jFile(localFile);
        String[] argsArray = {lrDatafile, hdfsDir + saveAsFile};
        appUserDefault.setArgs(argsArray);

        appUserDefault.start();

        assertTrue("App (default user) was not assigned an ID within 30 seconds.",
            appUserDefault.waitForID(30));
        assertTrue("App ID for sleep app (default user) is invalid.",
            appUserDefault.verifyID());
        assertEquals("App name for sleep app is invalid.",
            "SparkSaveAsText", appUserDefault.getAppName());

        int waitTime = 30;
        assertTrue("Job (default user) did not succeed.",
            appUserDefault.waitForSuccess(waitTime));

        // wait a few seconds for yarn to aggregate logs
        Util.sleep(10);

        // confirm DEBUG was actually on
        assertTrue("DEBUG found in log file", appUserDefault.grepLogsCLI("(.*) DEBUG SparkEnv(.*)"));
    }
}
