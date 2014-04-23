package hadooptest.hadoop.regression.yarn;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import hadooptest.TestSession;
import hadooptest.workflow.hadoop.job.TeraGenJob;

import org.junit.BeforeClass;
import org.junit.Test;

public class TestTeraGenCLI extends TestSession {
	
	/****************************************************************
	 *  Please set up input and output directory and file name here *
	 ****************************************************************/
	private static String outputDir = null; 
	
	@BeforeClass
	public static void startTestSession() throws Exception {
		TestSession.start();
		setupTestDir();
	}
	
	public static void setupTestDir() throws Exception {
		TestSession.cluster.getFS();			
		outputDir = "/home/" + TestSession.conf.getProperty("USER") + "/"; 
		System.out.println("Target HDFS Directory is: "+ outputDir + "\n");
		// TestSession.cluster.getFS().mkdirs(new Path(outputDir));
	}

	/*
	 * A test for running a TeraGen job
	 * 
	 * Equivalent to JobSummaryInfo10 in the original shell script YARN regression suite.
	 */
	@Test
	public void runTeraGenTest() {
		try {
			TeraGenJob jobUserDefault = new TeraGenJob();
			
			jobUserDefault.setOutputPath(outputDir);
			
			jobUserDefault.setNumMapTasks(
	                Long.parseLong(
	                        System.getProperty("TERAGEN_NUM_MAP_TASKS", "8000")));
	        /*
	         * 10,000,000,000 rows of 100-byte per row = 1,000,000,000,000 TB
	         */
			jobUserDefault.setNumDataRows(
	                Long.parseLong(
	                        System.getProperty("TERAGEN_NUM_DATA_ROWS", "10000000000")));

			jobUserDefault.start();

			assertTrue("TeraGen job (default user) was not assigned an ID within 10 seconds.", 
					jobUserDefault.waitForID(10));
			assertTrue("TeraGen job ID for TeraGen job (default user) is invalid.", 
					jobUserDefault.verifyID());

			int waitTime = 2;
			assertTrue("Job (default user) did not succeed.",
				jobUserDefault.waitForSuccess(waitTime));

		}
		catch (Exception e) {
			TestSession.logger.error("Exception failure.", e);
			fail();
		}
	}
}
