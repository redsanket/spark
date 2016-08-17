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

import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;

import hadooptest.Util;

@Category(SerialTests.class)
public class TestSparkJdk64 extends TestSession {

    /**
     * *************************************************************
     * Please set up input file name here *
     * **************************************************************
     */
    private static String localJar = null;
    private static String lrDatafile = "lr_data.txt";
    private static String hdfsDir = "/user/" + System.getProperty("user.name") + "/";

    @BeforeClass
    public static void setupTest() throws Exception {
        setupTestDir();
    }

    @AfterClass
    public static void endTestSession() throws Exception {
        removeTestDir();
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
    }

    /*
     * A test for running pipes and verifiying the properly environment variables are set
     * using jdk64.
     */
    @Test
    public void runSparkPipesEnvVarsJdk64SparkSubmit() throws Exception {
        SparkRunSparkSubmit appUserDefault = new SparkRunSparkSubmit();

        // set memory high enough to require jdk64
        appUserDefault.setWorkerMemory("6g");
        appUserDefault.setMasterMemory("4g");
        appUserDefault.setNumWorkers(1);
        appUserDefault.setWorkerCores(1);
        appUserDefault.setClassName("hadooptest.spark.regression.SparkPipesEnvVars");
        appUserDefault.setJarName(localJar);
        String[] argsArray = {lrDatafile};
        appUserDefault.setArgs(argsArray);

        appUserDefault.start();

        assertTrue("App (default user) was not assigned an ID within 30 seconds.",
            appUserDefault.waitForID(30));
        assertTrue("App ID for sleep app (default user) is invalid.",
            appUserDefault.verifyID());
        assertEquals("App name for sleep app is invalid.",
            "hadooptest.spark.regression.SparkPipesEnvVars", appUserDefault.getAppName());

        int waitTime = 30;
        assertTrue("Job (default user) did not succeed.",
            appUserDefault.waitForSuccess(waitTime));
    }

    @Test
    public void runSparkPipesEnvVarsJdk64SparkSubmitClient() throws Exception {
        SparkRunSparkSubmit appUserDefault = new SparkRunSparkSubmit();

        appUserDefault.setMaster(AppMaster.YARN_CLIENT);

        // set memory high enough to require jdk64
        appUserDefault.setWorkerMemory("6g");
        appUserDefault.setMasterMemory("4g");
        appUserDefault.setNumWorkers(1);
        appUserDefault.setWorkerCores(1);
        appUserDefault.setClassName("hadooptest.spark.regression.SparkPipesEnvVars");
        appUserDefault.setJarName(localJar);
        String[] argsArray = {lrDatafile};
        appUserDefault.setArgs(argsArray);

        appUserDefault.start();

        assertTrue("App (default user) was not assigned an ID within 30 seconds.",
            appUserDefault.waitForID(30));
        assertTrue("App ID for sleep app (default user) is invalid.",
            appUserDefault.verifyID());
        assertEquals("App name for sleep app is invalid.",
            "SparkPipesEnvVars", appUserDefault.getAppName());

        int waitTime = 30;
        assertTrue("Job (default user) did not succeed.",
            appUserDefault.waitForSuccess(waitTime));
    }

}
