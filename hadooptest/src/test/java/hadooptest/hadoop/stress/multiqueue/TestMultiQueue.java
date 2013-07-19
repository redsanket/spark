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

public class TestMultiQueue extends TestSession {
		
	// NOTE: this is a directory and will appear in your home directory in the HDFS
	private static String outputFile = "wc_output_new";
	private static String input_string = "Hello world! Start to test MultiQueue!";	
	private static int TotalFileNum = 20;
	
	// Test configuration global parameter
	private static Path inpath = null;
	private static String outputDir = null;
	private static String []qname;
	private static int qnum = 2;

	
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
		getQueneInfo();
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
	
	/*
	 * This function is used for load target queue name to the qname[] array
	 */
	public static void getQueneInfo() throws Exception {
		
		qname = new String[qnum];
		
		YarnClientImpl yarnClient = TestSession.cluster.getYarnClient();
		
		List<QueueInfo> queues =  yarnClient.getAllQueues(); 
		assertNotNull("Expected cluster queue(s) not found!!!", queues);		
		logger.info("queues='" +
        	Arrays.toString(queues.toArray()) + "'");
		qnum = Math.min(qnum,queues.size());
		for(int i = 0; i < qnum; i++) {
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
	 * A test for submitting word count jobs alternately to two queue one by one.
	 */
	@Test
	public void runWordCountTest() {
		
		int file_count = 0;
		int input_index;
		Random random = new Random();
		
	    // get current time
	    long startTime = System.currentTimeMillis();
	    logger.info("Current time is: " + startTime/1000);
		
	    String workingDir = System.getProperty("user.dir");	
		Properties prop = new Properties();
		 
    	try {
            //load a properties file
    		prop.load(new FileInputStream(workingDir+"/conf/StressConf/StressTestProp.properties"));
    	} catch (IOException ex) {
    		ex.printStackTrace();
        }

		int runMin  = Integer.parseInt(prop.getProperty("Multiqueue.runMin"));
	    int runHour = Integer.parseInt(prop.getProperty("Multiqueue.runHour"));
	    int runDay  = Integer.parseInt(prop.getProperty("Multiqueue.runDay"));
	    logger.info("===>> runMin: " + runMin + ",runHour: " + runHour + ", runDay: " + runDay + " <===");

	    long endTime = startTime + runMin*60*1000 + runHour*60*60*1000 + runDay*24*60*60*1000 ;
	    
		while(endTime > System.currentTimeMillis()) {
		
			input_index = random.nextInt(TotalFileNum);
			
			try {
				WordCountJob jobUserDefault = new WordCountJob();
				
				long timeLeftSec = (endTime - System.currentTimeMillis())/1000;
			    logger.info("===> Time remaining : " + timeLeftSec/60/60 + " hours "+timeLeftSec/60%60+" mins "+ timeLeftSec%60%60+" secs<===");
				
				String inputFile = inpath.toString() + "/" + Integer.toString(input_index) + ".txt";
				logger.info("Randomly choosed input file is: " + inputFile);
				
				String output = "/" + Integer.toString(file_count); 
				logger.info("Output file is: " + outputDir + outputFile + output);
				
				jobUserDefault.setInputFile(inputFile);
				jobUserDefault.setOutputPath(outputDir + outputFile + output);
				
				logger.info("Queue index = " + file_count%qnum);
				
				// switch between queues
				jobUserDefault.setQueue(qname[file_count%qnum]);
	
				jobUserDefault.start();
	
				// check job status
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
			file_count++;
		}
	}
}
