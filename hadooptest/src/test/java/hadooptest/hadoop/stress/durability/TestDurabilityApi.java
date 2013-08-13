package hadooptest.hadoop.stress.durability;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import hadooptest.TestSession;
import hadooptest.cluster.hadoop.HadoopCluster;
import hadooptest.cluster.hadoop.HadoopCluster.Action;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.workflow.hadoop.job.WordCountAPIFullCapacityJob;
import hadooptest.workflow.hadoop.job.WordCountAPIJob;
import hadooptest.workflow.hadoop.job.WordCountJob;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.client.YarnClientImpl;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * 
 * Setup Durability.runMin,Durability.runHour,Durability.runDay in runtime 
 * as -DDurability.runMin=1 -DDurability.runHour=2 -DDurability.runDay=3
 *
 */
public class TestDurabilityApi extends TestSession {
	
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
    			cluster.hadoopDaemon(Action.STOP, component);
    			cluster.hadoopDaemon(Action.START, component);
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
	 * A test for running a word count job in a certain time period
	 */
	@Test
	public void runTestDurability() {
		int fileCount = 0;
		
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
					
			try {
				
				long timeLeftSec = (endTime - System.currentTimeMillis())/1000;
			    logger.info("============> Time remaining : " + timeLeftSec/60/60 + " hours "+timeLeftSec/60%60+" mins "+ timeLeftSec%60%60+" secs <============");

				logger.info("Number of run rounds is " + fileCount);
				String[] args = {inpath.toString(), outputDir + outputFile+"/"+Integer.toString(fileCount), "1", "1", "default"};
				
//				for (int i = 0; i < args.length; i++){
//			    	TestSession.logger.info("args["+Integer.toString(i) + "]: " + args[i]);
//			    }
				Configuration conf = TestSession.cluster.getConf();
		
				int rc;
				TestSession.cluster.setSecurityAPI("keytab-hadoopqa", "user-hadoopqa");
				rc = ToolRunner.run(conf, new WordCountAPIJob(), args);
				if (rc != 0) {
					TestSession.logger.error("Job failed!!!");
				}
				// increment the fileCount for no output conflict
				fileCount++;
			}catch (Exception e) {
				TestSession.logger.error("Exception failure.", e);
				fail();
			}
		}
	}
}
