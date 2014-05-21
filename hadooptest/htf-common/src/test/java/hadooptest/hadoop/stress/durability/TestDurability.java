package hadooptest.hadoop.stress.durability;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import hadooptest.TestSession;
import hadooptest.workflow.hadoop.job.WordCountJob;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * 
 * Setup Durability.runMin,Durability.runHour,Durability.runDay in runtime 
 * as -DDurability.runMin=1 -DDurability.runHour=2 -DDurability.runDay=3
 *
 */
public class TestDurability extends TestSession {
	
	// the output folder name on HDFS
	private static String outputFile = "wc_output";
	// input string
	private static String input_string = "Hello world, and run Durability Test";
	// the amount of different input files, generated in the HDFS
	private static int TotalFileNum = 20;
	// input and output path
	private static Path inpath = null;
	private static String outputDir = null;
	
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
		setupTestDir();
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
	 * A test for running a word count job in a certain time period
	 */
	@Test
	public void runTestDurability() {
		
		int fileCount = 0;
		int input_index;
		Random myRan = new Random();
		
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
    	// get run time parameter from the .properties file.
		int runMin  = Integer.parseInt(prop.getProperty("Durability.runMin"));
	    int runHour = Integer.parseInt(prop.getProperty("Durability.runHour"));
	    int runDay  = Integer.parseInt(prop.getProperty("Durability.runDay"));
	    logger.info("============>> runMin: "+runMin+",runHour: "+runHour+", runDay: "+runDay);

	    // calculate the ending time
	    long endTime = startTime + runMin*60*1000 + runHour*60*60*1000 + runDay*24*60*60*1000 ;
	    
	    TestSession.logger.info("End time is: " + endTime/1000);
	    
		while(endTime > System.currentTimeMillis()) {
		
			input_index = myRan.nextInt(TotalFileNum);
			
			try {
				WordCountJob jobUserDefault = new WordCountJob();
				
				long timeLeftSec = (endTime - System.currentTimeMillis())/1000;
			    logger.info("============> Time remaining : " + timeLeftSec/60/60 + " hours "+timeLeftSec/60%60+" mins "+ timeLeftSec%60%60+" secs <============");
				
			    // get input and output path
			    String inputFile = inpath.toString() + "/" + Integer.toString(input_index) + ".txt";
				TestSession.logger.info("Randomly choosed input file is: " + inputFile + "\n");
				
				String output = "/" + Integer.toString(fileCount);
				TestSession.logger.info("Randomly choosed output file is: " + outputDir + outputFile + output + "\n");
			    
				// set up job configuration
				jobUserDefault.setInputFile(inputFile);
				jobUserDefault.setOutputPath(outputDir + outputFile + output);
				
				// start the job
				jobUserDefault.start();
				
				// check the status of the job
				assertTrue("WordCount job (default user) was not assigned an ID within 30 seconds.", 
						jobUserDefault.waitForID(30));
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
			// increment the fileCount for no output conflict
			fileCount++;
		}
	}
}
