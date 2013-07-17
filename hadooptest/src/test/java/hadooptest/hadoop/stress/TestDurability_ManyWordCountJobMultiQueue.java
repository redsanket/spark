package hadooptest.hadoop.stress;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import hadooptest.TestSession;
import hadooptest.cluster.hadoop.HadoopCluster;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.workflow.hadoop.job.WordCountJob;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
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

public class TestDurability_ManyWordCountJobMultiQueue extends TestSession {
	
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
	static List<QueueInfo> queues;
	
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
		setupQueue();
		setupTestDir();
	}
	public static void setupQueue() throws Exception  {
		
		FullyDistributedCluster cluster =
				(FullyDistributedCluster) TestSession.cluster;
		String component = HadoopCluster.RESOURCE_MANAGER;

		YarnClientImpl yarnClient = new YarnClientImpl();
		yarnClient.init(TestSession.getCluster().getConf());
		yarnClient.start();

		queues =  yarnClient.getAllQueues(); 
		assertNotNull("Expected cluster queue(s) not found!!!", queues);		
		TestSession.logger.info("================= queues ='" +
        	Arrays.toString(queues.toArray()) + "'");
		
		// we need to detect whether there are two queues running
		while (queues.size() < 2){
    			cluster.hadoopDaemon("stop", component);
    			cluster.hadoopDaemon("start", component);
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
	public void runTestDurability() throws IOException, InterruptedException {
		
		DateFormat dateFormat = new SimpleDateFormat("yyyy_MM_dd___HH_mm_ss___");
		Random rand = new Random();
		WordCountJob[] jobs = new WordCountJob[100];
		int queueIndex = 0;
		try {
			for(int i = 0; i < jobs.length; i++){
				queueIndex = queueIndex % queues.size();
				jobs[i] = new WordCountJob();
				jobs[i].setQueue(queues.get(queueIndex).getQueueName());
				logger.info("job "+i+" is using Queue "+queues.get(queueIndex).getQueueName()+", queueIndex = "+queueIndex+",queues.size() = "+queues.size());
				queueIndex++;
				
				String inputFile = inpath.toString() + "/" + Integer.toString(rand.nextInt(TotalFileNum)) + ".txt";
				TestSession.logger.info("Randomly choosed input file is: " + inputFile);
				
				Date date = new Date();
				String output = "/" +dateFormat.format(date).toString()+ Integer.toString(i);
				TestSession.logger.info("===== Output file is: " + outputDir + outputFile + output);
				
				jobs[i].setInputFile(inputFile);
				jobs[i].setOutputPath(outputDir + outputFile + output);
				jobs[i].start();
				
//				assertTrue("WordCount jobs["+i+"] was not assigned an ID within 10 seconds.", 
//						jobs[i].waitForID(10));
//				assertTrue("WordCount job ID for WordCount jobs["+i+"] is invalid.", 
//						jobs[i].verifyID());
			}
		}catch (Exception e) {
			TestSession.logger.error("Exception failure.", e);
			fail();
		}
		checkFileLeak(jobs);
	}
	
	public void checkFileLeak(WordCountJob[] jobs) throws IOException, InterruptedException{
		String pid = curPID().trim();
		logger.info(this.getClass().getName()+" PID is "+pid);
		int nofile = countOpenFile(pid);
		logger.info("=============== Initially Total number of file opened by "+pid+" is "+nofile);

		for(int i = 0; i < jobs.length; i++)
			assertTrue("Job "+i+" did not succeed.",jobs[i].waitForSuccess(20));//waitForSuccess() wait until it finally success is not working as supposed so!

		nofile = countOpenFile(pid);
		logger.info("=============== Finally Total number of file opened by "+pid+" is "+nofile);
//		assertTrue(this.getClass().getName()+" did not clean up all the opened files, "+nofile+" are left opened.",(nofile == 0));
	}
	public int countOpenFile(String pid) throws IOException {
		byte[] bo = new byte[100];
		String[] cmd = {"bash", "-c"," lsof -p "+pid+" | wc -l"};
		Process p = Runtime.getRuntime().exec(cmd);
		p.getInputStream().read(bo);
		return Integer.parseInt(new String(bo).trim());
	}
	public String curPID() throws IOException {

		byte[] bo = new byte[100];
		String[] cmd = {"bash", "-c", "echo $PPID"};
		Process p = Runtime.getRuntime().exec(cmd);
		p.getInputStream().read(bo);
		return new String(bo);
	}
}
