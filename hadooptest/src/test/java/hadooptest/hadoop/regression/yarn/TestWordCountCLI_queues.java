package hadooptest.hadoop.regression.yarn;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import hadooptest.TestSession;
import hadooptest.workflow.hadoop.job.WordCountJob;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.client.YarnClientImpl;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestWordCountCLI_queues extends TestSession {
	
	/****************************************************************
	 *  Please set up input and output directory and file name here *
	 ****************************************************************/
	private static String localDir = null; //"/Users/zhang369/";
	//private static String localDir = "/Users/zhang369/";
	private static String localFile = "wc_input.txt";
	private static String outputDir = null; //"/user/zhang369/";
	//private static String outputDir = "/user/zhang369/";
	private static String outputFile = "wc_output";
	
	private static String qname= null;
	
	@BeforeClass
	public static void startTestSession() throws Exception {
		TestSession.start();
		getQueneInfo();
		setupTestDir();
	}
	
	public static void getQueneInfo() throws Exception {
		
		YarnClientImpl yarnClient = TestSession.cluster.getYarnClient();
		
		List<QueueInfo> queues =  yarnClient.getAllQueues(); 
		assertNotNull("Expected cluster queue(s) not found!!!", queues);		
		System.out.println("queues='" +
        	Arrays.toString(queues.toArray()) + "'");
		System.out.println("Queue 0 name is :" + queues.get(0).getQueueName());
		qname = queues.get(0).getQueueName();
	}
	
	public static void setupTestDir() throws Exception {
		
		TestSession.cluster.getFS();	
		
		localDir = "/Users/" + System.getProperty("user.name") + "/";
		System.out.println("Target local Directory is: "+ localDir + "\n" + "Target File Name is: " + localFile);
		
		outputDir = "/user/" + TestSession.conf.getProperty("USER") + "/"; 
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
			
			jobUserDefault.setQueue(qname);

			jobUserDefault.start();

			assertTrue("WordCount job (default user) was not assigned an ID within 10 seconds.", 
					jobUserDefault.waitForID(100));
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
