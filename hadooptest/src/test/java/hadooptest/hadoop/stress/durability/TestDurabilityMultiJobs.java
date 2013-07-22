package hadooptest.hadoop.stress.durability;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import hadooptest.TestSession;
import hadooptest.cluster.hadoop.HadoopCluster;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.hadoop.examples.WordCount;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.client.YarnClientImpl;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDurabilityMultiJobs extends TestSession {
	
	// the output folder name on HDFS
	private static String outputFile = "wc_output";
	// input string
	private static String input_string = "Hello world, and run Durability Test with multiJobs!";
	// the number of input files
	private static int TotalFileNum = 20;
		
	// the parameter get from the .properties file
	// ===> runMin, runHour, runDay, jobNum <===
	 	
	// path and counter, etc global variables
	private static Path inpath = null;
	private static String outputDir = null;
	int fileCount = 0;
	int index = 20;
	long endTime;
	
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
		setupTestConf();
		setupTestDir();
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
	 * A test for running a Wordcount job
	 * 
	 * Equivalent to JobSummaryInfo10 in the original shell script YARN regression suite.
	 */
	@Test
	public void runTestDurability() {
		
	    // get current time
	    long startTime = System.currentTimeMillis();
	    TestSession.logger.info("Current time is: " + startTime/1000);
	    
	    String workingDir = System.getProperty("user.dir");
		
		Properties prop = new Properties();
		 
    	try {
            //load a properties file
    		prop.load(new FileInputStream(workingDir+"/conf/StressConf/StressTestProp.properties"));
    	} catch (IOException ex) {
    		ex.printStackTrace();
        }

		int runMin  = Integer.parseInt(prop.getProperty("Durability.runMin"));
	    int runHour = Integer.parseInt(prop.getProperty("Durability.runHour"));
	    int runDay  = Integer.parseInt(prop.getProperty("Durability.runDay"));
	    int jobNum = Integer.parseInt(prop.getProperty("Durability.jobNum"));
	    logger.info("===> runMin: "+runMin+",runHour: "+runHour+", runDay: "+runDay+" <===");

	    endTime = startTime + runMin*60*1000 + runHour*60*60*1000 + runDay*24*60*60*1000 ;

	    TestSession.logger.info("End time is: " + endTime/1000);
	    
		while(endTime > System.currentTimeMillis()) {
			try {
			    WordCount[] Jobs = new WordCount[jobNum];
			    
			    for(int i = 0; i < jobNum; i++){
			    	Jobs[i] = new WordCount();
			    }
			    
				for (int i = 0; i < jobNum; i++){
					startJobs(Jobs[i]);
				}
				
				for (int i = 0; i < jobNum; i++){
					assertJobs(Jobs[i]);
				}
			}
			catch (Exception e) {
				TestSession.logger.error("Exception failure.", e);
				fail();
			}
		}
	}
	
	private void startJobs(WordCount job){
		
		Random myRan = new Random();
		index = myRan.nextInt(TotalFileNum);
		
		try{
			long timeLeftSec = (endTime - System.currentTimeMillis())/1000;
		    logger.info("===> Time remaining : " + timeLeftSec/60/60 + " hours "+timeLeftSec/60%60+" mins "+ timeLeftSec%60%60+" secs <===");
			
			String inputFile = inpath.toString() + "/" + Integer.toString(index) + ".txt";
			TestSession.logger.info("Randomly choosed input file is: " + inputFile);
			
			String output = "/" + Integer.toString(fileCount);
			TestSession.logger.info("Randomly choosed output file is: " + outputDir + outputFile + output);
			fileCount++;
			
			job.setInputFile(inputFile);
			job.setOutputPath(outputDir + outputFile + output);
			
			TestSession.logger.info("===> Start Job [" + Integer.toString(fileCount) + "] <===");	
			job.start();
			
		} catch (Exception e) {
			TestSession.logger.error("Exception failure.", e);
			fail();
		}
	}
	
	private void assertJobs(WordCount jobUserDefault){
		try{
			assertTrue("WordCount job (default user) was not assigned an ID within 20 seconds.", 
					jobUserDefault.waitForID(20));
			assertTrue("WordCount job ID for WordCount job (default user) is invalid.", 
					jobUserDefault.verifyID());
			int waitTime = 2;
			assertTrue("Job (default user) did not succeed.",
				jobUserDefault.waitForSuccess(waitTime));
		} catch (Exception e){
			TestSession.logger.error("Exception failure.", e);
			fail();
		}
	}
}
