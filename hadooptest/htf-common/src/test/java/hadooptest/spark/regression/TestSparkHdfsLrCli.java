package hadooptest.spark.regression;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.workflow.spark.app.AppMaster;

import hadooptest.workflow.spark.app.SparkRunSparkSubmit;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SerialTests.class)
public class TestSparkHdfsLrCli extends TestSession {

    /**
     * *************************************************************
     * Please set up input file name here *
     * **************************************************************
     */
    private static String localDir = null;
    private static String lrDatafile = "lr_data.txt";
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

    public static void setupTestDir() throws Exception {

        TestSession.cluster.getFS();
        String localFile = Util.getResourceFullPath("resources/spark/data/" + lrDatafile);
        System.out.println("LR data File is: " + localFile);
        TestSession.cluster.getFS().copyFromLocalFile(new Path(localFile), new Path(hdfsDir));
    }

    public static void removeTestDir() throws Exception {

        // Delete the file
        TestSession.cluster.getFS().delete(new Path(hdfsDir + lrDatafile), true);
    }

    /*
     * A test for running a SparkHdfsLR in Standalone mode.
     * 
     */
    @Test
    public void runSparkHdfsLRTestStandaloneModeSparkSubmit() {
        try {
            SparkRunSparkSubmit appUserDefault = new SparkRunSparkSubmit();

            appUserDefault.setMaster(AppMaster.YARN_STANDALONE);
            appUserDefault.setWorkerMemory("2g");
            appUserDefault.setNumWorkers(3);
            appUserDefault.setWorkerCores(1);
            appUserDefault.setClassName("org.apache.spark.examples.SparkHdfsLR");
            String[] argsArray = {lrDatafile, "100"};
            appUserDefault.setArgs(argsArray);

            appUserDefault.start();

            assertTrue("SparkHdfsLR app (default user) was not assigned an ID within 120 seconds.",
                appUserDefault.waitForID(120));
            assertTrue("SparkHdfsLR app ID for sleep app (default user) is invalid.",
                appUserDefault.verifyID());
            assertEquals("SparkHdfsLR app name for sleep app is invalid.",
                "org.apache.spark.examples.SparkHdfsLR", appUserDefault.getAppName());

            int waitTime = 30;
            assertTrue("Job (default user) did not succeed.",
                appUserDefault.waitForSuccess(waitTime));
        } catch (Exception e) {
            TestSession.logger.error("Exception failure.", e);
            fail();
        }
    }

    @Test
    public void runSparkHdfsLRTestYarnClientModeSparkSubmit() {
        try {
            SparkRunSparkSubmit appUserDefault = new SparkRunSparkSubmit();

            appUserDefault.setMaster(AppMaster.YARN_CLIENT);
            appUserDefault.setWorkerMemory("2g");
            appUserDefault.setNumWorkers(3);
            appUserDefault.setWorkerCores(1);
            appUserDefault.setClassName("org.apache.spark.examples.SparkHdfsLR");
            String[] argsArray = {"/user/" + System.getProperty("user.name") + "/" + lrDatafile,
                "100"};
            appUserDefault.setArgs(argsArray);

            appUserDefault.start();

            assertTrue("SparkHdfsLR app (default user) was not assigned an ID within 120 seconds.",
                appUserDefault.waitForID(120));
            assertTrue("SparkHdfsLR app ID for sleep app (default user) is invalid.",
                appUserDefault.verifyID());
            assertEquals("SparkHdfsLR app name for sleep app is invalid.",
                "SparkHdfsLR", appUserDefault.getAppName());

            int waitTime = 30;
            assertTrue("Job (default user) did not succeed.",
                appUserDefault.waitForSuccess(waitTime));
        } catch (Exception e) {
            TestSession.logger.error("Exception failure.", e);
            fail();
        }
    }

    @Test
    public void runSparkHdfsLRTestNonexistHdfsFileSparkSubmit() throws Exception {
        SparkRunSparkSubmit appUserDefault = new SparkRunSparkSubmit();

        appUserDefault.setMaster(AppMaster.YARN_CLIENT);
        appUserDefault.setWorkerMemory("2g");
        appUserDefault.setNumWorkers(3);
        appUserDefault.setWorkerCores(1);
        appUserDefault.setClassName("org.apache.spark.examples.SparkHdfsLR");
        String[] argsArray = {"bogusnonexistentfile.txt", "100"};
        appUserDefault.setArgs(argsArray);

        appUserDefault.start();

        int waitTime = 120;
        assertTrue("Job (default user) did not error.",
            appUserDefault.waitForERROR(waitTime));
    }

}

