/*
 * [ATTENTION!]
 * This test will pass successfully, when submitting a small number of jobs to the cluster
 * (For cluster eomer, the limitation is total < 80 jobs)
 * 		Since this test submits jobs via CLI, therefore, if it starts too many jobs,
 * 		there will be many JVM running at the same time, and same jobs will not be successfully submitted 
 * 		to the cluster, but the console will not show the error at the first place. The check job status part
 * 		will find the error later by showing "Job xxx is not assign an ID in xx sec".
 * For submitting large number of jobs at the same time, please use the TestFloodingQueues in the floodingqueues pacage.
 */
package hadooptest.hadoop.stress.multiqueue;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import hadooptest.TestSession;
import hadooptest.cluster.hadoop.HadoopCluster;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.workflow.hadoop.job.WordCountJob;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.client.YarnClientImpl;
import org.junit.BeforeClass;
import org.junit.Test;



public class TestMultiQueueDurability extends TestSession {

	
	// NOTE: this is a directory and will appear in your home directory in the HDFS
	private static String outputFile = "TestMultiQueueDurability_output";	
	private static String input_string = "Hello world! Run durablity test!";	
	private static int TotalFileNum = 20;

	// location information 
	private static Path inpath = null;
	private static String outputDir = null;
	private static String []qname;
	private static int qNum;
	private int file_count = 0;
	private int input_index;
	private long endTime;
	
	// Parameters from cmd properties file           
	// runMin,runHour,runDay in runtime, queueNum, jobNum
	private static int runMin;
	private static int runHour;
	private static int runDay;
	private static int jobNum;
		
	/*
	 *  Before running the test.
	 *  1. Start the session
	 *  2. Make sure there are more than 2 queues running on the cluster
	 *     and randomly pick 2 queues for the test
	 *  3. Copy the original file from the local machine and generate the 
	 *     input files
	 */
	@BeforeClass
	public static void startTestSession() throws Exception {
		TestSession.start();
		getParameters();
		setupTestConf();
		setupTestDir();
		getQueneInfo();
	}
	
	public static void getParameters() throws Exception {
		
		String workingDir = System.getProperty("user.dir");
		
		Properties prop = new Properties();
		 
    	try {
            //load a properties file
    		prop.load(new FileInputStream(workingDir+"/conf/StressConf/StressTestProp.properties"));
    	} catch (IOException ex) {
    		ex.printStackTrace();
        }

		runMin  = Integer.parseInt(prop.getProperty("Multiqueue.runMin"));
	    runHour = Integer.parseInt(prop.getProperty("Multiqueue.runHour"));
	    runDay  = Integer.parseInt(prop.getProperty("Multiqueue.runDay"));
	    logger.info("===> runMin: "+runMin+",runHour: "+runHour+", runDay: "+runDay + " <===");
	    qNum = Integer.parseInt(prop.getProperty("Multiqueue.queueNum"));
		jobNum = Integer.parseInt(prop.getProperty("Multiqueue.jobNum"));
	    logger.info("===> Queue #: "+ qNum + "Job # per queue: " + jobNum + " <==="); 
	}
	
	
	public static void setupTestConf() throws Exception  {
		
		FullyDistributedCluster cluster =
				(FullyDistributedCluster) TestSession.cluster;
		String component = HadoopCluster.RESOURCE_MANAGER;

		YarnClientImpl yarnClient = new YarnClientImpl();
		yarnClient.init(TestSession.getCluster().getConf());
		yarnClient.start();

		List<QueueInfo> queues =  yarnClient.getAllQueues(); 
		assertNotNull("Expected cluster queue(s) not found!!!", queues);		
		TestSession.logger.info("queues='" +
        	Arrays.toString(queues.toArray()) + "'");
		
		// we need to detect whether there are two queues running
		if (queues.size() >= 2) {
				TestSession.logger.debug("Cluster is already setup properly." 
							+ "Multi-queues are Running." + "Nothing to do.");
				return;
		} else {
				// set up TestSession to default queue numbers, which should be more than 2 queue
				// restart the cluster to get default queue setting
    			cluster.hadoopDaemon("stop", component);
    			cluster.hadoopDaemon("start", component);
 
        		return;        		
		}
	}
	
	public static void getQueneInfo() throws Exception {
		
		qname = new String[qNum];
		
		YarnClientImpl yarnClient = TestSession.cluster.getYarnClient();
		
		List<QueueInfo> queues =  yarnClient.getAllQueues(); 
		assertNotNull("Expected cluster queue(s) not found!!!", queues);		
		logger.info("queues='" +
        	Arrays.toString(queues.toArray()) + "'");
		qNum = Math.min(qNum, queues.size());
		for(int i = 0; i < qNum; i++) {
			qname[i] = queues.get(i).getQueueName();
			logger.info("Queue " + i +" name is :" + qname[i]);
		}
	}
	
	/*
	 *  This function generate input files, and assemble the output path
	 */
	public static void setupTestDir() throws Exception {
		
	    FileSystem myFs = TestSession.cluster.getFS();
		
		outputDir = "/user/" + TestSession.conf.getProperty("USER") + "/"; 
		TestSession.logger.info("Target HDFS Directory is: "+ outputDir + "\n" + "Target File Name is: " + outputFile);
		
		inpath = new Path(outputDir+"/"+"wc_input_foo");
		Path infile = null;
		
		// Check the valid of the input directory in HDFS
		// check if path exists and if so remove it 
	    try {
	       if ( myFs.isDirectory(inpath) ) {
	         myFs.delete(inpath, true);
	         TestSession.logger.info("INFO: deleted input path: " + inpath );
	       }
	    }
	    catch (Exception e) {
	        System.err.println("FAIL: can not remove the input path, can't run wordcount jobs. Exception is: " + e);
	    }
	    // make the input directory
	    try {
	    	 if ( myFs.mkdirs(inpath) ) {
	    		 TestSession.logger.info("INFO: created input path: " + inpath );
	      }
	    }
	    catch (Exception e) {
	         System.err.println("FAIL: can not create the input path, can't run wordcount jobs. Exception is: " + e);
	    }
	    
	    // Print input string
        TestSession.logger.info("Input string is: "+ input_string);  
		
		// Generate different input files for submission later on
		for(int fileNum = 0; fileNum < TotalFileNum; fileNum ++)
		{
			try {
				 infile = new Path(inpath.toString() + "/" + Integer.toString(fileNum) + ".txt" );
		         FSDataOutputStream dostream = FileSystem.create(myFs, infile, new FsPermission("644")); 
		          
		         // generate a set of different input files
		         for(int i= 0; i < 2500*fileNum; i++)
		         		dostream.writeChars(input_string);
		          	
		         dostream.flush();
		         dostream.close();
		    } catch (IOException ioe) {
		        	System.err.println("FAIL: can't create input file for wordcount: " + ioe);
		    }
		}
		// Delete the file, if it exists in the same directory
		TestSession.cluster.getFS().delete(new Path(outputDir+outputFile), true);
	}

	/*
	 * A test for submitting jobNum of jobs to qNum of queues
	 */
	@Test
	public void runWordCountTest() {
		
	    // get current time
	    long startTime = System.currentTimeMillis();
	    logger.info("Current time is: " + startTime/1000);

	    endTime = startTime + runMin*60*1000 + runHour*60*60*1000 + runDay*24*60*60*1000 ;
	    
		while(endTime > System.currentTimeMillis()) {
			
			try {
				// make a 2-D array for all the jobs
			    WordCountJob[][] Jobs = new WordCountJob[qNum][jobNum];
			    
			    // init all jobs
			    for(int i = 0; i < qNum; i++){
			    	for (int j = 0; j < jobNum; j++){
			    		Jobs[i][j] = new WordCountJob();
			    	}
			    }
			    // start all jobs
			    for(int i = 0; i < qNum; i++){
			    	for (int j = 0; j < jobNum; j++){
			    		startJobs(Jobs[i][j], i);
			    	}
			    }
				// check the status of the jobs
			    for(int i = 0; i < qNum; i++){
			    	for (int j = 0; j < jobNum; j++){
			    		assertJobs(Jobs[i][j], i, j);
			    	}
			    }
			}
			catch (Exception e) {
				TestSession.logger.error("Exception failure.", e);
				fail();
			}
		}
	}
	
	/*
	 * This function load job configuration and submit the job to the cluster
	 */
	private void startJobs(WordCountJob jobUserDefault, int i){
		try{
			// Get the random number for randomly choosing input file
			Random random = new Random();
			input_index = random.nextInt(TotalFileNum);
			
			long timeLeftSec = (endTime - System.currentTimeMillis())/1000;
		    logger.info("===> Time remaining : " + timeLeftSec/60/60 + " hours, "+timeLeftSec/60%60+" mins, "+ timeLeftSec%60%60+" sec <===");
			
			String inputFile = inpath.toString() + "/" + Integer.toString(input_index) + ".txt";
			logger.info("Randomly choosed input file is: " + inputFile);
			
			String output = "/" + Integer.toString(i) + "_" + file_count;  
			logger.info("Output file is: " + outputDir + outputFile + output);
			
			jobUserDefault.setInputFile(inputFile);
			jobUserDefault.setOutputPath(outputDir + outputFile + output);
			
			logger.info("Queue index = " + i);
			
			// switch between queues
			jobUserDefault.setQueue(qname[i]);
			file_count++;
			TestSession.logger.info("Filecount now is : " + file_count);
			
			// start the job
			jobUserDefault.start();
			
		} catch (Exception e) {
			TestSession.logger.error("Exception failure.", e);
			fail();
		}
	}
	
	/*
	 * This function is for check the job status and success
	 */
	private void assertJobs(WordCountJob jobUserDefault, int i, int j){	
		try{
			assertTrue("WordCount job (default user) was not assigned an ID within 60 seconds." + "i = " + i +", j = " + j, 
					jobUserDefault.waitForID(60));
			assertTrue("WordCount job ID for WordCount job (default user) is invalid.", 
					jobUserDefault.verifyID());
			int waitTime = 5;
			assertTrue("[!!!!]Job (default user) did not succeed."+ "i = " + i +", j = " + j,
				jobUserDefault.waitForSuccess(waitTime));
			TestSession.logger.info("===> Job(" +i+","+j+")"+"successed! <===");
		} catch (Exception e){
			TestSession.logger.error("Exception failure.", e);
			fail();
		}
	}	
}
