package hadooptest.spark.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.workflow.spark.app.AppMaster;
import hadooptest.workflow.spark.app.SparkRunSparkSubmit;
import hadooptest.Util;

@Category(SerialTests.class)
public class TestSparkHiveIntegration extends TestSession {

    private static String localJar = null;
    // Note: The hive data file should contain a list of key value pairs in the following format.
    //key1 value1
    //key2 value2
    private static String hiveDataFile = "kv1.txt";

    @BeforeClass
    public static void startTestSession() throws Exception {
        TestSession.start();
        localJar =
                Util.getResourceFullPath("../htf-common/target/htf-common-1.0-SNAPSHOT-tests.jar");
        String dataFile = Util.getResourceFullPath("resources/spark/data/" + hiveDataFile);
        Util.copyFileToHDFS(dataFile, ".", null);
    }

    @AfterClass
    public static void endTestSession() throws Exception {
        String[] filePaths = {hiveDataFile};
        Util.deleteFromHDFS(filePaths);
    }

    //===================================== TESTS ==================================================
    // Perform CRUD Operations using hive.

    @Test
    public void runSparkHiveExample() throws Exception {
        SparkRunSparkSubmit appUserDefault = new SparkRunSparkSubmit();
        appUserDefault.setWorkerMemory("2g");
        appUserDefault.setNumWorkers(3);
        appUserDefault.setWorkerCores(1);
        appUserDefault.setClassName("hadooptest.spark.regression.SparkClusterHiveFull");
        appUserDefault.setJarName(localJar);
        // We no longer need to supply hive-site.xml. Refer YOOZIE-716 and YOOZIE-715
        // appUserDefault.setDistributedCacheFiles("hdfs:///sharelib/v1/hive_conf/libexec/hive/conf/hive-site.xml");
        String[] argsArray = {hiveDataFile};
        appUserDefault.setArgs(argsArray);

        appUserDefault.start();

        assertTrue("App (default user) was not assigned an ID within 30 seconds.",
                appUserDefault.waitForID(30));
        assertTrue("App ID for sleep app (default user) is invalid.",
                appUserDefault.verifyID());
        assertEquals("App name for sleep app is invalid.",
                "hadooptest.spark.regression.SparkClusterHiveFull", appUserDefault.getAppName());

        int waitTime = 90;
        assertTrue("Job (default user) did not succeed.",
                appUserDefault.waitForSuccess(waitTime));
    }

}