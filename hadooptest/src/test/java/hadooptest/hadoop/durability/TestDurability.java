package hadooptest.hadoop.durability;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import hadooptest.TestSession;
import hadooptest.cluster.hadoop.HadoopCluster;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.workflow.hadoop.job.WordCountJob;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Scanner;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.client.YarnClientImpl;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDurability extends TestSession {
	
	/****************************************************************
	 *  Please set up input and output directory and file name here *
	 ****************************************************************/
	// NOTE: the file should appear in you home directory
	private static String localFile = "wc_input_new.txt";
	// NOTE: this is a directory and will appear in your home directory in the HDFS
	private static String outputFile = "wc_output_new";
	
	/****************************************************************
	 *          Please give the string for the input file           *
	 ****************************************************************/
	
	private static String input_string = "Hello world, and run Durability Test";
	
	/****************************************************************
	 *  Configure the total file number that you want to generate   *
	 *                       in the HDFS                            *
	 ****************************************************************/
	private static int TotalFileNum = 20;
		
	/****************************************************************
	 *                  Configure the total runtime                 *
	 ****************************************************************/
	
	// location information 
	private static Path inpath = null;
	private static String outputDir = null;
	private static String localDir = null;
	
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
		
		// show the input and output path
	    // the localDir might be different, check if "/user/", "//Users", or "/home/"
		localDir = "/home/" + System.getProperty("user.name") + "/";
		TestSession.logger.info("Target local Directory is: "+ localDir + "\n" + "Target File Name is: " + localFile);
		
		outputDir = "/user/" + TestSession.conf.getProperty("USER") + "/"; 
		TestSession.logger.info("Target HDFS Directory is: "+ outputDir + "\n" + "Target File Name is: " + outputFile);
		
		inpath = new Path(outputDir+"/"+"wc_input_foo");
		Path infile = null;
		
		// create local input file
		File inputFile = new File(localDir + localFile);
		try{
			if(inputFile.delete()){
				TestSession.logger.info("Input file already exists from previous test, delete it!");
			} else {
				TestSession.logger.info("Input path clear, creating new input file!");
			}
			
			FileUtils.writeStringToFile(new File(localDir + localFile), input_string);
		
		} catch (Exception e) {
			TestSession.logger.error(e);
		}
		
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
	    
	    // Read the local input file
        String s = new Scanner(new File(localDir+localFile) ).useDelimiter("\\A").next();
        TestSession.logger.info("Input string is: "+s);  
		
		
		for(int fileNum = 0; fileNum < TotalFileNum; fileNum ++)
		{
			try {
				 infile = new Path(inpath.toString() + "/" + Integer.toString(fileNum) + ".txt" );
		         FSDataOutputStream dostream = FileSystem.create(myFs, infile, new FsPermission("644")); 
		          
		         // generate a set of different input files
		         for(int i= 0; i < 25*fileNum; i++)
		         		dostream.writeChars(s);
		          	
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
		
		int fileCount = 0;
		int myNum;
		Random myRan = new Random();
		
	    // get current time
	    long startTime = System.currentTimeMillis();
	    TestSession.logger.info("Current time is: " + startTime/1000);
		
	    logger.info("============================ runMin: "+System.getProperty("runMin") 
	    		+",runHour: "+System.getProperty("runHour")+", runDay: "+System.getProperty("runDay"));

	    long endTime = startTime + Integer.parseInt(System.getProperty("runMin"))*60*1000 
	    		+ Integer.parseInt(System.getProperty("runHour"))*60*60*1000 + Integer.parseInt(System.getProperty("runDay"))*24*60*60*1000 ;

	    TestSession.logger.info("End time is: " + endTime/1000);
	    
		while(endTime > System.currentTimeMillis()) {
		
			myNum = myRan.nextInt(TotalFileNum);
			
			try {
				WordCountJob jobUserDefault = new WordCountJob();
				
				long timeLeftSec = (endTime - System.currentTimeMillis())/1000;
			    logger.info("============> Time remaining : " + timeLeftSec/60/60 + " hours "+timeLeftSec/60%60+" mins "+ timeLeftSec%60%60+" secs<============");
				
				String inputFile = inpath.toString() + "/" + Integer.toString(myNum) + ".txt";
				TestSession.logger.info("Randomly choosed input file is: " + inputFile + "\n");
				
				String output = "/" + Integer.toString(fileCount);
				TestSession.logger.info("Randomly choosed output file is: " + outputDir + outputFile + output + "\n");
				
				jobUserDefault.setInputFile(inputFile);
				jobUserDefault.setOutputPath(outputDir + outputFile + output);
	
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
			fileCount++;
		}
	}
}
