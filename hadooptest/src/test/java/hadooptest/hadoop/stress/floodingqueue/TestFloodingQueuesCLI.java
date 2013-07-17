package hadooptest.hadoop.stress.floodingqueue;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import hadooptest.TestSession;
import hadooptest.cluster.hadoop.HadoopCluster;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.workflow.hadoop.job.WordCountAPIJob;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

import org.apache.commons.io.FileUtils;
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

public class TestFloodingQueuesCLI extends TestSession {
	
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
	 *                  parameters from cmd line	                *
	 ****************************************************************/
	// queueNum, jobNum, runMin, runHour, runDay
	
	
	// location information 
	private static Path inpath = null;
	private static String outputDir = null;
	private static String localDir = null;
	private static String []qName;
	private static int qNum;
	private static int jobNum;
	
	private static int runMin;
	private static int runHour;
	private static int runDay;
	
	@BeforeClass
	public static void startTestSession() throws Exception {
		TestSession.start();
//		setupTestConf();
		getParameters();
		getQueneInfo();
		setupTestDir();
	}
	
	public static void setupTestConf() throws Exception {
		FullyDistributedCluster cluster =
				(FullyDistributedCluster) TestSession.cluster;
		String component = HadoopCluster.RESOURCE_MANAGER;

		/* 
		 * NOTE: Add a check via the Hadoop API or jmx to determine if a single
		 * queue is already in place. If so, skip the following as to not waste
		 *  time.
		 */
		YarnClientImpl yarnClient = new YarnClientImpl();
		yarnClient.init(TestSession.getCluster().getConf());
		yarnClient.start();

		List<QueueInfo> queues =  yarnClient.getAllQueues(); 
		assertNotNull("Expected cluster queue(s) not found!!!", queues);		
		TestSession.logger.info("queues='" +
        	Arrays.toString(queues.toArray()) + "'");
		if ((queues.size() == 1) &&
			(Float.compare(queues.get(0).getCapacity(), 1.0f) == 0)) {
				TestSession.logger.debug("Cluster is already setup properly." +
						"Nothing to do.");
				return;
		}
		
		// Backup the default configuration directory on the Resource Manager
		// component host.
		cluster.getConf(component).backupConfDir();	

		// Copy files to the custom configuration directory on the
		// Resource Manager component host.
		String sourceFile = TestSession.conf.getProperty("WORKSPACE") +
				"/conf/SingleQueueConf/single-queue-capacity-scheduler.xml";
		cluster.getConf(component).copyFileToConfDir(sourceFile,
				"capacity-scheduler.xml");
		cluster.hadoopDaemon("stop", component);
		cluster.hadoopDaemon("start", component);
	}

	public static void getParameters() throws Exception {
		
		String workingDir = System.getProperty("user.dir");
		
		Properties prop = new Properties();
		 
    	try {
            //load a properties file
    		prop.load(new FileInputStream(workingDir+"/conf/StressConf/TestWordCountAPIJob_Durability.properties"));
    	} catch (IOException ex) {
    		ex.printStackTrace();
        }

		runMin  = Integer.parseInt(System.getProperty("runMin"));
	    runHour = Integer.parseInt(System.getProperty("runHour"));
	    runDay  = Integer.parseInt(System.getProperty("runDay"));
	    logger.info("============>>>> runMin: "+runMin+",runHour: "+runHour+", runDay: "+runDay);
	    jobNum = Integer.parseInt(System.getProperty("jobNum"));
	    qNum = Integer.parseInt(System.getProperty("queueNum"));
	    logger.info("============>>>> Job #:: "+jobNum+", Queue #: "+ qNum); 
	}
	
	public static void getQueneInfo() throws Exception {
		
		qName = new String[qNum];
		
		YarnClientImpl yarnClient = TestSession.cluster.getYarnClient();
		
		List<QueueInfo> queues =  yarnClient.getAllQueues(); 
		assertNotNull("Expected cluster queue(s) not found!!!", queues);		
		logger.info("queues='" +
	    	Arrays.toString(queues.toArray()) + "'");
		qNum = Math.min(qNum,queues.size());
		for(int i = 0; i < qNum; i++) {
			qName[i] = queues.get(i).getQueueName();
			logger.info("Queue " + i +" name is :" + qName[i]);
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
	public void runWordCountTest() {
		
	    long startTime = System.currentTimeMillis();
	    long endTime = startTime + runMin*60*1000 + runHour*60*60*1000 + runDay*24*60*60*1000 ;
	    int run_times = 1;
	    
	    logger.info("Current time is: " + startTime/1000);

		while(endTime > System.currentTimeMillis()) {
			try {
			    logger.info("Number of run rounds is " + run_times);
				String[] args = {inpath.toString(), outputDir + outputFile+"/"+Integer.toString(run_times), Integer.toString(jobNum), Integer.toString(qNum)};
				for (int i = 0; i < qNum ; i++){
					args = append(args, qName[i]);
				}
				
		    	for (int i = 0; i < args.length; i++){
		    		TestSession.logger.info("args["+Integer.toString(i) + "]: " + args[i]);
		    	}
				Configuration conf = TestSession.cluster.getConf();
	
				int rc;
				TestSession.cluster.setSecurityAPI("keytab-hadoopqa", "user-hadoopqa");
				rc = ToolRunner.run(conf, new WordCountAPIJob(), args);
				if (rc != 0) {
					TestSession.logger.error("Job failed!!!");
				}
				run_times ++;
			}
			catch (Exception e) {
				TestSession.logger.error("Exception failure.", e);
				fail();
			}
		}
	}
	
	static <T> T[] append(T[] arr, T element) {
	    final int N = arr.length;
	    arr = Arrays.copyOf(arr, N + 1);
	    arr[N] = element;
	    return arr;
	}
}
