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
    private static String piPythonFile = "pi.py";
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
        String appName = "SparkPiTestStandaloneModeSparkSubmitWithDebugLogging";
        appUserDefault.setAppName(appName);
        appUserDefault.setShouldPassName(true);
        appUserDefault.setQueueName("default");
        String pythonFileLoc = Util.getResourceFullPath("resources/spark/data/" + pythonFile);
        String[] argsArray = {pythonFileLoc, textFile};
        appUserDefault.setArgs(argsArray);

        appUserDefault.start();

        assertTrue("app (default user) was not assigned an ID within 30 seconds.",
            appUserDefault.waitForID(120));
        assertTrue("app ID for sleep app (default user) is invalid.",
            appUserDefault.verifyID());

        int waitTime = 120;
        assertTrue("App (default user) did not succeed.",
            appUserDefault.waitForSuccess(waitTime));

        String dir = hdfsDir + pythonOutputDir + "/part-00000";
        // make sure it wrote out the text file, thus really succeeded.
        assertTrue("output doesn't exist in hdfs, must have failed",
                   TestSession.cluster.getFS().exists(new Path(dir)));
    }

    /*
     * A test for running Spark Python in Cluster mode.
     * This runs pi.py.
     */
    @Test
    public void runSparkPiTestCluster36() throws Exception {
        appUserDefault = new SparkRunSparkSubmit();

        appUserDefault.setMaster(AppMaster.YARN_CLUSTER);
        appUserDefault.setWorkerMemory("1g");
        appUserDefault.setNumWorkers(1);
        appUserDefault.setWorkerCores(1);
        appUserDefault.setShouldPassClass(false);
        appUserDefault.setShouldPassJar(false);
        String appName = "SparkPiTestClusterModePi";
        appUserDefault.setAppName(appName);
        appUserDefault.setShouldPassName(true);
        appUserDefault.setQueueName("default");
        String pythonFileLoc = Util.getResourceFullPath("resources/spark/data/" + piPythonFile);
        String[] argsArray = {pythonFileLoc};
        appUserDefault.setArgs(argsArray);

        appUserDefault.start();

        assertTrue("app (default user) was not assigned an ID within 30 seconds.",
            appUserDefault.waitForID(120));
        assertTrue("app ID for sleep app (default user) is invalid.",
            appUserDefault.verifyID());

        int waitTime = 120;
        assertTrue("App (default user) did not succeed.",
            appUserDefault.waitForSuccess(waitTime));
    }

    /*
     * A test for running Spark Python in Cluster mode, python using apache configs.
     * This runs pi.py.
     */
    @Test
    public void runSparkPiTestClusterApachePythonConfigs() throws Exception {
        appUserDefault = new SparkRunSparkSubmit();

        appUserDefault.setMaster(AppMaster.YARN_CLUSTER);
        appUserDefault.setWorkerMemory("1g");
        appUserDefault.setNumWorkers(1);
        appUserDefault.setWorkerCores(1);
        appUserDefault.setShouldPassClass(false);
        appUserDefault.setShouldPassJar(false);
        String appName = "SparkPiTestClusterModePiApache";
        appUserDefault.setAppName(appName);
        appUserDefault.setShouldPassName(true);
        appUserDefault.setQueueName("default");
        String pythonFileLoc = Util.getResourceFullPath("resources/spark/data/" + piPythonFile);
        String[] argsArray = {pythonFileLoc};
        appUserDefault.setArgs(argsArray);
        appUserDefault.setDistributedCacheArchives("hdfs:///sharelib/v1/python36/python36.tgz#python36");
        appUserDefault.setConf("spark.pyspark.python=./python36/bin/python3.6");
        appUserDefault.setConf("spark.pyspark.driver.python=./python36/bin/python3.6");
        appUserDefault.setConf("spark.executorEnv.LD_LIBRARY_PATH=./python36/lib");
        appUserDefault.setConf("spark.yarn.appMasterEnv.LD_LIBRARY_PATH=./python36/lib");

        appUserDefault.start();

        assertTrue("app (default user) was not assigned an ID within 30 seconds.",
            appUserDefault.waitForID(120));
        assertTrue("app ID for sleep app (default user) is invalid.",
            appUserDefault.verifyID());

        int waitTime = 120;
        assertTrue("App (default user) did not succeed.",
            appUserDefault.waitForSuccess(waitTime));
    }

    /*
     * A test for running Spark Python in Client mode, python using apache configs.
     * This runs pi.py.
     */
    @Test
    public void runSparkPiTestClientApachePythonConfigs() throws Exception {
        appUserDefault = new SparkRunSparkSubmit();

        appUserDefault.setMaster(AppMaster.YARN_CLIENT);
        appUserDefault.setWorkerMemory("1g");
        appUserDefault.setNumWorkers(1);
        appUserDefault.setWorkerCores(1);
        appUserDefault.setShouldPassClass(false);
        appUserDefault.setShouldPassJar(false);
        String appName = "SparkPiTestClientModePiApache";
        appUserDefault.setAppName(appName);
        appUserDefault.setShouldPassName(true);
        appUserDefault.setQueueName("default");
        String pythonFileLoc = Util.getResourceFullPath("resources/spark/data/" + piPythonFile);
        String[] argsArray = {pythonFileLoc};
        appUserDefault.setArgs(argsArray);
        appUserDefault.setDistributedCacheArchives("hdfs:///sharelib/v1/python36/python36.tgz#python36");
        appUserDefault.setConf("spark.pyspark.python=./python36/bin/python3.6");
        appUserDefault.setConf("spark.pyspark.driver.python=/home/y/var/python36/bin/python3.6");
        appUserDefault.setConf("spark.executorEnv.LD_LIBRARY_PATH=./python36/lib");
        appUserDefault.setDriverLibraryPath("/home/y/var/python36/lib");

        appUserDefault.start();

        assertTrue("app (default user) was not assigned an ID within 30 seconds.",
            appUserDefault.waitForID(1200));
        assertTrue("app ID for sleep app (default user) is invalid.",
            appUserDefault.verifyID());

        int waitTime = 120;
        assertTrue("App (default user) did not succeed.",
            appUserDefault.waitForSuccess(waitTime));
    }
}
