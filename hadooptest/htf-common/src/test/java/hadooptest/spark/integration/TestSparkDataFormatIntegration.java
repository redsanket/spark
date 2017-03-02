package hadooptest.spark.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

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
public class TestSparkDataFormatIntegration extends TestSession {

    private static String localJar = null;
    private static String jsonFile = "people.json";
    private static String jsonOpFile = "people_name.json";
    private static String avroFile = "users.avro";
    private static String avroOpFile = "user_name.avro";

    @BeforeClass
    public static void startTestSession() throws Exception {
        TestSession.start();
        // In this case we use the test jar with dependencies which includes the databricks avro jars.
        String testJar = "../htf-common/target/htf-common-1.0-SNAPSHOT-tests-with-dependencies.jar";
        localJar = Util.getResourceFullPath(testJar);

        //Copy the resources to hdfs.
        String jsonFilePath = Util.getResourceFullPath("resources/spark/data/" + jsonFile);
        Util.copyFileToHDFS(jsonFilePath, ".", null);
        String avroFilePath = Util.getResourceFullPath("resources/spark/data/" + avroFile);
        Util.copyFileToHDFS(avroFilePath, ".", null);
    }

    @AfterClass
    public static void endTestSession() throws Exception {
        String[] filePaths = {jsonFile, jsonOpFile, avroFile, avroOpFile};
        Util.deleteFromHDFS(filePaths);
    }

    //===================================== TESTS ==================================================

    @Test
    public void runSparkReadWriteAvro() throws Exception {
        SparkRunSparkSubmit appUserDefault = new SparkRunSparkSubmit();
        appUserDefault.setWorkerMemory("2g");
        appUserDefault.setNumWorkers(3);
        appUserDefault.setWorkerCores(1);
        appUserDefault.setClassName("hadooptest.spark.regression.SparkDataReadWrite");
        appUserDefault.setJarName(localJar);
        appUserDefault.setConf("spark.yarn.security.tokens.hive.enabled=false");
        // We no longer need to supply hive-site.xml. Refer YOOZIE-716 and YOOZIE-715
        //appUserDefault.setDistributedCacheFiles("hdfs:///sharelib/v1/hive_conf/libexec/hive/conf/hive-site.xml");
        String[] argsArray = {jsonFile, jsonOpFile, avroFile, avroOpFile};
        appUserDefault.setArgs(argsArray);

        appUserDefault.start();

        assertTrue("App (default user) was not assigned an ID within 30 seconds.",
                appUserDefault.waitForID(30));
        assertTrue("App ID for sleep app (default user) is invalid.",
                appUserDefault.verifyID());
        assertEquals("App name for sleep app is invalid.",
                "hadooptest.spark.regression.SparkDataReadWrite", appUserDefault.getAppName());

        int waitTime = 90;
        assertTrue("Job (default user) did not succeed.",
                appUserDefault.waitForSuccess(waitTime));
    }

}