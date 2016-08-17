package hadooptest.spark.regression;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.workflow.spark.app.AppMaster;

import hadooptest.workflow.spark.app.SparkRunSparkSubmit;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SerialTests.class)
public class TestSparkPython extends TestSession {

    private static String textFile = "README.md";
    private static String pythonFile = "test.py";
    private static String hdfsDir = "/user/" + System.getProperty("user.name") + "/";
    private static String pythonOutputDir = "testingpythonwordcount";
    private static SparkRunSparkSubmit appUserDefault;

    @BeforeClass
    public static void startTestSession() throws Exception {
        TestSession.start();
        setupTestDir();
    }

    @AfterClass
    public static void endTestSession() throws Exception {
        removeTestDir();
    }

    @After
    public void killJob() throws Exception {
        if ((appUserDefault != null) && (appUserDefault.getYarnState() == YarnApplicationState.RUNNING)) {
            appUserDefault.killCLI();
        }
    }

    public static void setupTestDir() throws Exception {

        TestSession.cluster.getFS();
        String localFile = Util.getResourceFullPath("resources/spark/data/" + textFile);
        System.out.println("text File is: " + localFile);
        TestSession.cluster.getFS().copyFromLocalFile(new Path(localFile), new Path(hdfsDir));
        TestSession.cluster.getFS().delete(new Path(hdfsDir + pythonOutputDir), true);
    }

    public static void removeTestDir() throws Exception {

        // Delete the file
        TestSession.cluster.getFS().delete(new Path(hdfsDir + textFile), true);
        TestSession.cluster.getFS().delete(new Path(hdfsDir + pythonOutputDir), true);
    }

    /*
     * A test for running Spark Python in Client mode.
     * This runs a wordcount in python and outputs a directory to hdfs with results. 
     */
    @Test
    public void runSparkPiTestStandaloneModeSparkSubmitWithDebugLogging() throws Exception {
        appUserDefault = new SparkRunSparkSubmit();

        appUserDefault.setMaster(AppMaster.YARN_CLIENT);
        appUserDefault.setWorkerMemory("1g");
        appUserDefault.setNumWorkers(1);
        appUserDefault.setWorkerCores(1);
        appUserDefault.setShouldPassClass(false);
        appUserDefault.setShouldPassJar(false);
        String appName = "SparkTestPython";
        appUserDefault.setAppName(appName);
        appUserDefault.setShouldPassName(true);
        appUserDefault.setQueueName("default");
        String pythonFileLoc = Util.getResourceFullPath("resources/spark/data/" + pythonFile);
        String[] argsArray = {pythonFileLoc, textFile};
        appUserDefault.setArgs(argsArray);

        appUserDefault.start();

        assertTrue("app (default user) was not assigned an ID within 30 seconds.",
            appUserDefault.waitForID(30));
        assertTrue("app ID for sleep app (default user) is invalid.",
            appUserDefault.verifyID());

        int waitTime = 30;
        assertTrue("App (default user) did not succeed.",
            appUserDefault.waitForSuccess(waitTime));

        String dir = hdfsDir + pythonOutputDir + "/part-00000";
        // make sure it wrote out the text file, thus really succeeded.
        assertTrue("output doesn't exist in hdfs, must have failed", 
                   TestSession.cluster.getFS().exists(new Path(dir)));

        // wait a few seconds for yarn to aggregate logs
        Util.sleep(10);

        // confirm no Exceptions exist in the log
        assertFalse("Exception found in log file", appUserDefault.grepLogsCLI("(.*)Exception(.*)"));
    }
}
