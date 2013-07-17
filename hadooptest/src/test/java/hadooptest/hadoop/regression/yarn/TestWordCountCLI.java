package hadooptest.hadoop.regression.yarn;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import hadooptest.TestSession;
import hadooptest.cluster.hadoop.DFS;
import hadooptest.workflow.hadoop.job.WordCountJob;

import org.apache.hadoop.fs.Path;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestWordCountCLI extends TestSession {
	
	/****************************************************************
	 *  Please set up input and output directory and file name here *
	 ****************************************************************/
	private static String localDir = null; 
	private static String localFile = "wc_input_new.txt";
	private static String outputDir = null; 
	private static String outputFile = "wc_output";
	
	@BeforeClass
	public static void startTestSession() throws Exception {
		TestSession.start();
		setupTestDir();
	}
	
	public static void setupTestDir() throws Exception {
		
		TestSession.cluster.getFS();	
		
		localDir = "/home/" + System.getProperty("user.name") + "/";
		System.out.println("Target local Directory is: "+ localDir + "\n" + "Target File Name is: " + localFile);
		
		outputDir = "/home/" + TestSession.conf.getProperty("USER") + "/"; 
		System.out.println("Target HDFS Directory is: "+ outputDir + "\n" + "Target File Name is: " + outputFile);
		
		TestSession.cluster.getFS().copyFromLocalFile(new Path(localDir + localFile), new Path(outputDir + localFile));
		// Delete the file, if it exists in the same directory
		TestSession.cluster.getFS().delete(new Path(outputDir+outputFile), true);
	}

	/*
	 * A test for running a Wordcount job
	 * 
	 * Equivalent to JobSummaryInfo10 in the original shell script YARN regression suite.
	 */
	@Test
	public void runWordCountTest() {
		try {
			WordCountJob jobUserDefault = new WordCountJob();
			
			jobUserDefault.setInputFile(outputDir + localFile);
			jobUserDefault.setOutputPath(outputDir + outputFile);

			jobUserDefault.start();

			assertTrue("WordCount job (default user) was not assigned an ID within 10 seconds.", 
					jobUserDefault.waitForID(10));
			assertTrue("WordCount job ID for WordCount job (default user) is invalid.", 
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
