package hadooptest.spark.regression;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.experimental.categories.Category;
import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.workflow.spark.app.AppMaster;
import hadooptest.workflow.spark.app.SparkRunSparkSubmit;

import org.apache.hadoop.fs.Path;

import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;

import hadooptest.Util;

@Category(SerialTests.class)
public class TestSparkNewAPIHadoop extends TestSession {

    /**
     * *************************************************************
     * Please set up input file name here *
     * **************************************************************
     */
    private static String localJar = null;
    private static String ctrlBFile = "ctrl_b_data.txt";
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
        String localFile = Util.getResourceFullPath("resources/spark/data/" + ctrlBFile);
        System.out.println("ctrl b file is: " + localFile);
        localJar =
            Util.getResourceFullPath("../htf-common/target/htf-common-1.0-SNAPSHOT-tests.jar");
        TestSession.cluster.getFS().copyFromLocalFile(new Path(localFile), new Path(hdfsDir));
    }

    public static void removeTestDir() throws Exception {

        // Delete the file
        TestSession.cluster.getFS().delete(new Path(hdfsDir + ctrlBFile), true);
    }

    @Test
    public void runSparkNewAPIHadoop() throws Exception {
        SparkRunSparkSubmit appUserDefault = new SparkRunSparkSubmit();

        appUserDefault.setWorkerMemory("2g");
        appUserDefault.setNumWorkers(1);
        appUserDefault.setWorkerCores(1);
        appUserDefault.setClassName("hadooptest.spark.regression.SparkNewAPIHadoopRDD");
        appUserDefault.setJarName(localJar);
        String[] argsArray = {ctrlBFile};
        appUserDefault.setArgs(argsArray);
        appUserDefault.setAppName("runSparkNewAPIHadoop");

        appUserDefault.start();

        assertTrue("App (default user) was not assigned an ID within 30 seconds.",
            appUserDefault.waitForID(30));
        assertTrue("App ID for sleep app (default user) is invalid.",
            appUserDefault.verifyID());
        assertEquals("App name for sleep app is invalid.",
            "hadooptest.spark.regression.SparkNewAPIHadoopRDD", appUserDefault.getAppName());

        int waitTime = 30;
        assertTrue("Job (default user) did not succeed.",
            appUserDefault.waitForSuccess(waitTime));
    }

}
