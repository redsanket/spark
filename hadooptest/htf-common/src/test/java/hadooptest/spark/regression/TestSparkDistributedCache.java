package hadooptest.spark.regression;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.workflow.spark.app.AppMaster;
import hadooptest.workflow.spark.app.SparkRunSparkSubmit;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.Path;

import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.experimental.categories.Category;
import org.junit.Test;

import hadooptest.Util;

@Category(SerialTests.class)
public class TestSparkDistributedCache extends TestSession {

    /**
     * *************************************************************
     * Please set up input file name here *
     * **************************************************************
     */
    private static String localJar = null;
    private static String singleFile = "singlefile.txt";
    private static String singleArchive = "singlearchive.tgz";
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
        localJar =
            Util.getResourceFullPath("../htf-common/target/htf-common-1.0-SNAPSHOT-tests.jar");
    }

    public static void removeTestDir() throws Exception {

        // Delete the file
        TestSession.cluster.getFS().delete(new Path(hdfsDir + singleFile), true);
    }

    /*-------------------  Using Spark Submit -----------------------------*/
    @Test
    public void runSparkDistributedCacheOneFileSparkSubmit() throws Exception {
        SparkRunSparkSubmit appUserDefault = new SparkRunSparkSubmit();

        appUserDefault.setWorkerMemory("2g");
        appUserDefault.setNumWorkers(3);
        appUserDefault.setWorkerCores(1);
        String localFile = Util.getResourceFullPath("resources/spark/data/singlefile.txt");
        appUserDefault.setDistributedCacheFiles("file://" + localFile);
        appUserDefault.setClassName("hadooptest.spark.regression.SparkDistributedCacheSingleFile");
        appUserDefault.setJarName(localJar);
        String[] argsArray = {"singlefile.txt"};
        appUserDefault.setArgs(argsArray);

        appUserDefault.start();

        assertTrue("App (default user) was not assigned an ID within 30 seconds.",
            appUserDefault.waitForID(30));
        assertTrue("App ID for app (default user) is invalid.",
            appUserDefault.verifyID());

        int waitTime = 30;
        assertTrue("Job (default user) did not succeed.",
            appUserDefault.waitForSuccess(waitTime));

        // also check to make sure staging directory got cleaned up for this app
        Path stagingDir = new Path(hdfsDir + ".sparkStaging/" + appUserDefault.getID());
        TestSession.logger.debug("Staging dir: " + stagingDir.toString());
        assertFalse("staging dirctory should have been removed",
            TestSession.cluster.getFS().exists(stagingDir));
    }

    @Test
    public void runSparkDistributedCacheOneFileWithHashSparkSubmit() throws Exception {
        SparkRunSparkSubmit appUserDefault = new SparkRunSparkSubmit();

        appUserDefault.setWorkerMemory("2g");
        appUserDefault.setNumWorkers(3);
        appUserDefault.setWorkerCores(1);
        String localFile = Util.getResourceFullPath("resources/spark/data/singlefile.txt");
        appUserDefault.setDistributedCacheFiles("file://" + localFile + "#renamed.txt");
        appUserDefault.setClassName("hadooptest.spark.regression.SparkDistributedCacheSingleFile");
        appUserDefault.setJarName(localJar);
        String[] argsArray = {"renamed.txt"};
        appUserDefault.setArgs(argsArray);

        appUserDefault.start();

        assertTrue("App (default user) was not assigned an ID within 30 seconds.",
            appUserDefault.waitForID(30));
        assertTrue("App ID for app (default user) is invalid.",
            appUserDefault.verifyID());

        int waitTime = 30;
        assertTrue("Job (default user) did not succeed.",
            appUserDefault.waitForSuccess(waitTime));
    }

    @Test
    public void runSparkDistributedCacheThreeFilesSparkSubmit() throws Exception {
        SparkRunSparkSubmit appUserDefault = new SparkRunSparkSubmit();

        appUserDefault.setWorkerMemory("2g");
        appUserDefault.setNumWorkers(3);
        appUserDefault.setWorkerCores(1);
        String localFile = Util.getResourceFullPath("resources/spark/data/firstfile.txt");
        String secondFile = Util.getResourceFullPath("resources/spark/data/secondfile.txt");
        String thirdFile = Util.getResourceFullPath("resources/spark/data/thirdfile.txt");
        appUserDefault.setDistributedCacheFiles("file://" + localFile +
            ",file://" + secondFile + "#renamedfile.txt,file://" + thirdFile);
        appUserDefault.setClassName("hadooptest.spark.regression.SparkDistributedCacheThreeFiles");
        appUserDefault.setJarName(localJar);
        String[] argsArray = {"firstfile.txt", "renamedfile.txt", "thirdfile.txt"};
        appUserDefault.setArgs(argsArray);

        appUserDefault.start();

        assertTrue("App (default user) was not assigned an ID within 30 seconds.",
            appUserDefault.waitForID(30));
        assertTrue("App ID for app (default user) is invalid.",
            appUserDefault.verifyID());

        int waitTime = 30;
        assertTrue("Job (default user) did not succeed.",
            appUserDefault.waitForSuccess(waitTime));
    }

    @Test
    public void runSparkDistributedCacheOneFileHashBadFileSparkSubmit() throws Exception {
        SparkRunSparkSubmit appUserDefault = new SparkRunSparkSubmit();

        appUserDefault.setWorkerMemory("2g");
        appUserDefault.setNumWorkers(3);
        appUserDefault.setWorkerCores(1);
        // renmae file to bad filename so its not read
        String localFile = Util.getResourceFullPath("resources/spark/data/singlefile.txt");
        appUserDefault.setDistributedCacheFiles("file://" + localFile + "#badfile.txt");
        appUserDefault.setClassName("hadooptest.spark.regression.SparkDistributedCacheSingleFile");
        appUserDefault.setJarName(localJar);
        String[] argsArray = {"singlefile.txt"};
        appUserDefault.setArgs(argsArray);

        appUserDefault.start();

        assertTrue("App (default user) was not assigned an ID within 30 seconds.",
            appUserDefault.waitForID(30));
        assertTrue("App ID for app (default user) is invalid.",
            appUserDefault.verifyID());

        int waitTime = 30;
        assertTrue("Job (default user) did not fail.",
            appUserDefault.waitForFailure(waitTime));
    }

    @Test
    public void runSparkDistributedCacheNonExistFileSparkSubmit() throws Exception {
        SparkRunSparkSubmit appUserDefault = new SparkRunSparkSubmit();

        appUserDefault.setWorkerMemory("2g");
        appUserDefault.setNumWorkers(3);
        appUserDefault.setWorkerCores(1);
        // renmae file to bad filename so its not read
        appUserDefault.setDistributedCacheFiles("file://nonexistenfile.txt");
        appUserDefault.setClassName("hadooptest.spark.regression.SparkDistributedCacheSingleFile");
        appUserDefault.setJarName(localJar);
        String[] argsArray = {"nonexistentfile.txt"};
        appUserDefault.setArgs(argsArray);

        appUserDefault.start();

        int waitTime = 30;
        assertTrue("Job (default user) did not fail.",
            appUserDefault.waitForERROR(waitTime));
    }

    @Test
    public void runSparkDistributedCacheOneFileFromHdfsSparkSubmit() throws Exception {
        SparkRunSparkSubmit appUserDefault = new SparkRunSparkSubmit();

        // upload file to HDFS
        String localFile = Util.getResourceFullPath("resources/spark/data/singlefile.txt");
        TestSession.cluster.getFS().copyFromLocalFile(new Path(localFile), new Path(hdfsDir));

        appUserDefault.setWorkerMemory("2g");
        appUserDefault.setNumWorkers(3);
        appUserDefault.setWorkerCores(1);
        appUserDefault.setDistributedCacheFiles("hdfs://" + new Path(hdfsDir + singleFile));
        appUserDefault.setClassName("hadooptest.spark.regression.SparkDistributedCacheSingleFile");
        appUserDefault.setJarName(localJar);
        String[] argsArray = {"singlefile.txt"};
        appUserDefault.setArgs(argsArray);

        appUserDefault.start();

        assertTrue("App (default user) was not assigned an ID within 30 seconds.",
            appUserDefault.waitForID(30));
        assertTrue("App ID for app (default user) is invalid.",
            appUserDefault.verifyID());

        int waitTime = 30;
        assertTrue("Job (default user) did not succeed.",
            appUserDefault.waitForSuccess(waitTime));

        // delete from hdfs
        TestSession.cluster.getFS().delete(new Path(hdfsDir + singleFile), true);
    }

    @Test
    public void runSparkDistributedCacheOneArchiveSparkSubmit() throws Exception {
        SparkRunSparkSubmit appUserDefault = new SparkRunSparkSubmit();

        appUserDefault.setWorkerMemory("2g");
        appUserDefault.setNumWorkers(3);
        appUserDefault.setWorkerCores(1);
        String localArchive = Util.getResourceFullPath("resources/spark/data/singlearchive.tgz");
        appUserDefault.setDistributedCacheArchives("file://" + localArchive);
        appUserDefault
            .setClassName("hadooptest.spark.regression.SparkDistributedCacheSingleArchive");
        appUserDefault.setJarName(localJar);
        String[] argsArray = {"singlearchive.tgz"};
        appUserDefault.setArgs(argsArray);

        appUserDefault.start();

        assertTrue("App (default user) was not assigned an ID within 30 seconds.",
            appUserDefault.waitForID(30));
        assertTrue("App ID for app (default user) is invalid.",
            appUserDefault.verifyID());

        int waitTime = 30;
        assertTrue("Job (default user) did not succeed.",
            appUserDefault.waitForSuccess(waitTime));
    }

    @Test
    public void runSparkDistributedCacheOneArchiveWithHashSparkSubmit() throws Exception {
        SparkRunSparkSubmit appUserDefault = new SparkRunSparkSubmit();

        appUserDefault.setWorkerMemory("2g");
        appUserDefault.setNumWorkers(3);
        appUserDefault.setWorkerCores(1);
        String localFile = Util.getResourceFullPath("resources/spark/data/singlearchive.tgz");
        appUserDefault.setDistributedCacheArchives("file://" + localFile + "#renamed.tgz");
        appUserDefault
            .setClassName("hadooptest.spark.regression.SparkDistributedCacheSingleArchive");
        appUserDefault.setJarName(localJar);
        String[] argsArray = {"renamed.tgz"};
        appUserDefault.setArgs(argsArray);

        appUserDefault.start();

        assertTrue("App (default user) was not assigned an ID within 30 seconds.",
            appUserDefault.waitForID(30));
        assertTrue("App ID for app (default user) is invalid.",
            appUserDefault.verifyID());

        int waitTime = 30;
        assertTrue("Job (default user) did not succeed.",
            appUserDefault.waitForSuccess(waitTime));
    }

    @Test
    public void runSparkDistributedCacheThreeArchivesSparkSubmit() throws Exception {
        SparkRunSparkSubmit appUserDefault = new SparkRunSparkSubmit();

        appUserDefault.setWorkerMemory("2g");
        appUserDefault.setNumWorkers(3);
        appUserDefault.setWorkerCores(1);
        String localFile = Util.getResourceFullPath("resources/spark/data/firstarchive.tgz");
        String secondFile = Util.getResourceFullPath("resources/spark/data/secondarchive.tgz");
        String thirdFile = Util.getResourceFullPath("resources/spark/data/thirdarchive.tgz");
        appUserDefault.setDistributedCacheArchives("file://" + localFile +
            ",file://" + secondFile + "#renamedarchive.tgz,file://" + thirdFile);
        appUserDefault
            .setClassName("hadooptest.spark.regression.SparkDistributedCacheThreeArchives");
        appUserDefault.setJarName(localJar);
        String[] argsArray = {"firstarchive.tgz", "renamedarchive.tgz", "thirdarchive.tgz"};
        appUserDefault.setArgs(argsArray);

        appUserDefault.start();

        assertTrue("App (default user) was not assigned an ID within 30 seconds.",
            appUserDefault.waitForID(30));
        assertTrue("App ID for app (default user) is invalid.",
            appUserDefault.verifyID());

        int waitTime = 30;
        assertTrue("Job (default user) did not succeed.",
            appUserDefault.waitForSuccess(waitTime));
    }

    @Test
    public void runSparkDistributedCacheOneArchiveHashBadSparkSubmit() throws Exception {
        SparkRunSparkSubmit appUserDefault = new SparkRunSparkSubmit();

        appUserDefault.setWorkerMemory("2g");
        appUserDefault.setNumWorkers(3);
        appUserDefault.setWorkerCores(1);
        // renmae file to bad filename so its not read
        String localFile = Util.getResourceFullPath("resources/spark/data/singlearchive.tgz");
        appUserDefault.setDistributedCacheArchives("file://" + localFile + "#badfile.tgz");
        appUserDefault
            .setClassName("hadooptest.spark.regression.SparkDistributedCacheSingleArchive");
        appUserDefault.setJarName(localJar);
        String[] argsArray = {"singlearchive.tgz"};
        appUserDefault.setArgs(argsArray);

        appUserDefault.start();

        assertTrue("App (default user) was not assigned an ID within 30 seconds.",
            appUserDefault.waitForID(30));
        assertTrue("App ID for app (default user) is invalid.",
            appUserDefault.verifyID());

        int waitTime = 30;
        assertTrue("Job (default user) did not fail.",
            appUserDefault.waitForFailure(waitTime));
    }

    @Test
    public void runSparkDistributedCacheNonExistArchiveSparkSubmit() throws Exception {
        SparkRunSparkSubmit appUserDefault = new SparkRunSparkSubmit();

        appUserDefault.setWorkerMemory("2g");
        appUserDefault.setNumWorkers(3);
        appUserDefault.setWorkerCores(1);
        // renmae file to bad filename so its not read
        appUserDefault.setDistributedCacheArchives("file://nonexistenarchive.tgz");
        appUserDefault
            .setClassName("hadooptest.spark.regression.SparkDistributedCacheSingleArchive");
        appUserDefault.setJarName(localJar);
        String[] argsArray = {"nonexistentfile.tgz"};
        appUserDefault.setArgs(argsArray);

        appUserDefault.start();

        int waitTime = 30;
        assertTrue("Job (default user) did not fail.",
            appUserDefault.waitForERROR(waitTime));
    }

    @Test
    public void runSparkDistributedCacheOneArchiveFromHdfsSparkSubmit() throws Exception {
        SparkRunSparkSubmit appUserDefault = new SparkRunSparkSubmit();

        // upload file to HDFS
        String localFile = Util.getResourceFullPath("resources/spark/data/singlearchive.tgz");
        TestSession.cluster.getFS().copyFromLocalFile(new Path(localFile), new Path(hdfsDir));

        appUserDefault.setWorkerMemory("2g");
        appUserDefault.setNumWorkers(3);
        appUserDefault.setWorkerCores(1);
        appUserDefault.setDistributedCacheArchives("hdfs://" + new Path(hdfsDir + singleArchive));
        appUserDefault
            .setClassName("hadooptest.spark.regression.SparkDistributedCacheSingleArchive");
        appUserDefault.setJarName(localJar);
        String[] argsArray = {"singlearchive.tgz"};
        appUserDefault.setArgs(argsArray);

        appUserDefault.start();

        assertTrue("App (default user) was not assigned an ID within 30 seconds.",
            appUserDefault.waitForID(30));
        assertTrue("App ID for app (default user) is invalid.",
            appUserDefault.verifyID());

        int waitTime = 30;
        assertTrue("Job (default user) did not succeed.",
            appUserDefault.waitForSuccess(waitTime));

        // delete from hdfs
        TestSession.cluster.getFS().delete(new Path(hdfsDir + singleArchive), true);
    }

}
