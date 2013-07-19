package hadooptest.hadoop.stress.floodingqueue;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import hadooptest.TestSession;
import hadooptest.cluster.hadoop.HadoopCluster;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.workflow.hadoop.job.WordCountAPIJob;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

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

public class TestFloodingQueues extends TestSession {

	// NOTE: this is a directory and will appear in your home directory in the HDFS
	private static String outputFile = "wc_output_new";
	
	private static String input_string = "Hello world, and run Durability Test";

	private static int TotalFileNum = 20;

	private static Path inpath = null;
	private static String outputDir = null;
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

	public static void getParameters() throws Exception {
		
		String workingDir = System.getProperty("user.dir");
		
		Properties prop = new Properties();
		 
    	try {
            //load a properties file
    		prop.load(new FileInputStream(workingDir+"/conf/StressConf/StressTestProp.properties"));
    	} catch (IOException ex) {
    		ex.printStackTrace();
        }

		runMin  = Integer.parseInt(prop.getProperty("wordcountAPI.runMin"));
	    runHour = Integer.parseInt(prop.getProperty("wordcountAPI.runHour"));
	    runDay  = Integer.parseInt(prop.getProperty("wordcountAPI.runDay"));
	    logger.info("============>>>> runMin: "+runMin+",runHour: "+runHour+", runDay: "+runDay);
	    jobNum = Integer.parseInt(prop.getProperty("wordcountAPI.jobNum"));
	    qNum = Integer.parseInt(prop.getProperty("wordcountAPI.queueNum"));
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
