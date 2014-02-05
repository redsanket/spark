package hadooptest.spark.regression;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import hadooptest.TestSession;
import hadooptest.workflow.spark.app.SparkHdfsLR;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;

import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;

import hadooptest.Util;

public class TestSparkHdfsLrCli extends TestSession {

        /****************************************************************
         *  Please set up input file name here *
         ****************************************************************/
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
	 * A test for running a SparkHdfsLR
	 * 
	 */
	@Test
	public void runSparkHdfsLRTest() {
		try {
			SparkHdfsLR appUserDefault = new SparkHdfsLR();

			appUserDefault.setWorkerMemory("2g");
			appUserDefault.setNumWorkers(3);
			appUserDefault.setWorkerCores(1);
			appUserDefault.setNumIterations(100);
			appUserDefault.setLRDataFile(lrDatafile);

			appUserDefault.start();

			assertTrue("SparkPi app (default user) was not assigned an ID within 30 seconds.", 
					appUserDefault.waitForID(30));
			assertTrue("SparkPiapp ID for sleep app (default user) is invalid.", 
					appUserDefault.verifyID());
            assertEquals("SparkPi app name for sleep app is invalid.", 
                    "Spark", appUserDefault.getAppName());

			int waitTime = 30;
			assertTrue("Job (default user) did not succeed.",
				appUserDefault.waitForSuccess(waitTime));
		}
		catch (Exception e) {
			TestSession.logger.error("Exception failure.", e);
			fail();
		}
	}
	
}
