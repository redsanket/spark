package hadooptest.spark.regression;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import hadooptest.TestSession;
import hadooptest.workflow.spark.app.AppMaster;
import hadooptest.workflow.spark.app.SparkRunClass;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;

import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;

import hadooptest.Util;

public class TestSparkDistributedCache extends TestSession {

        /****************************************************************
         *  Please set up input file name here *
         ****************************************************************/
        private static String localJar = null;
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
                localJar = Util.getResourceFullPath("../htf-common/target/htf-common-1.0-SNAPSHOT-tests.jar");
                System.out.println("LR data File is: " + localFile);
                TestSession.cluster.getFS().copyFromLocalFile(new Path(localFile), new Path(hdfsDir));
        }

        public static void removeTestDir() throws Exception {

                // Delete the file
                TestSession.cluster.getFS().delete(new Path(hdfsDir + lrDatafile), true);
        }

	
	@Test
	public void runSparkDistributedCacheOneFile() throws Exception {
		SparkRunClass appUserDefault = new SparkRunClass();

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
	}


}
