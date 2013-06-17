package hadooptest.hadoop.durability;

import static org.junit.Assert.assertTrue;
import hadooptest.TestSession;
import hadooptest.workflow.hadoop.job.WordCountJob;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestTokenRenewal_existingUgi_webhdfs extends TestSession {
	
    static Configuration conf;
    static Cluster cluster;
    static FileSystem fs;
    static Credentials creds = new Credentials();
    
	private static int TotalFileNum = 20;
	private static Path intPath = null;
	private static String intDir = "/tmp/TestTokenRenewal_Output/"; 
	private static String inputDir = "/tmp/data/";
	private static String inputFile = "wc_input.txt";
	private static String outputFile = "wc_output";
	private static String webhdfs = "webhdfs://gsbl90180.blue.ygrid.yahoo.com:/data/in/wc_input.txt ";

    
	@BeforeClass
	public static void startTestSession() throws Exception {
		
		TestSession.start();
		setupTestConf();
		setupTestDir();
	}
	
	public static void setupTestConf() throws Exception  {
		
		conf = TestSession.getCluster().getConf();
		cluster = new Cluster(conf);
		fs = FileSystem.get(conf);
		
		// list out our config prop change, should be 60 (seconds)
        TestSession.logger.debug("Check the renew property setting, yarn.resourcemanager.delegation.token.renew-interval: " + conf.get("yarn.resourcemanager.delegation.token.renew-interval"));
		
        // don't cancel out tokens so we can use them in job2
        conf.setBoolean("mapreduce.job.complete.cancel.delegation.tokens", false);
	}
	
	public static void setupTestDir() throws Exception {
		
	    FileSystem fs = TestSession.cluster.getFS();

	    // get the file to local first
	    Runtime.getRuntime().exec("hadoop fs -copyToLocal "+webhdfs+(inputDir + inputFile));
		
		logger.info("Input Directory is: "+ inputDir + "\n" + "Input File Name is: " + inputFile);
		logger.info("Intermediate Directory is: "+ intDir + "\n" + "Intermediate File Name is: " + outputFile);
		intPath = new Path(intDir);
		
		// Check the valid of the input directory in HDFS
		// check if path exists and if so remove it 
	    try {
	       if ( fs.isDirectory(intPath) ) {
	         fs.delete(intPath, true);
	         logger.debug("Deleted previous output path: " + intPath );
	       }
	    }
	    catch (Exception e) {
	        logger.error("FAIL: can not remove the previous output path. Exception is: " + e);
	    }
	    
	    // make the input directory
	    try {
	    	 if ( fs.mkdirs(intPath,new FsPermission("777")) ) {
	    		 logger.debug("Created Output path: " + intPath );
	      }
	    }
	    catch (Exception e) {
	         logger.error("FAIL: can not create the output path. Exception is: " + e);
	    }
	   
	    // Read the local input file
        String s = new Scanner(new File(inputDir+inputFile) ).useDelimiter("\\A").next();
        logger.info("Input string is: "+s);  
		
		Path infile = null;
		for(int fileNum = 0; fileNum < TotalFileNum; fileNum ++)
		{
			try {
				 infile = new Path(intPath.toString() + "/" + Integer.toString(fileNum) + ".txt" );
		         FSDataOutputStream dostream = FileSystem.create(fs, infile, new FsPermission("644")); 
		          
		         // generate a set of different input files
		         for(int i= 0; i < 25*fileNum; i++)
		         		dostream.writeChars(s);
		          	
		         dostream.flush();
		         dostream.close();
		    } catch (IOException ioe) {
		        	logger.error("Can't create input file for wordcount: " + ioe);
		    }
		}
	}

	@Test
	public void runTest() throws IOException, InterruptedException {
		
	    int failFlags = 10000; // test result init but not updated yet
		Token<?> mrdt = cluster.getDelegationToken(new Text("mapredqa"));
		creds.addToken(new Text("RM_TOKEN"), mrdt);
	     // get dt with HDFS priv user as renewer
		Token<?> hdfsdt = fs.addDelegationTokens("mapredqa", creds)[0];
	   
		logger.info("mrdt: " + mrdt.getIdentifier());
        logger.info("mrdt kind: " + mrdt.getKind());
        logger.info("mrdt isManaged: " + mrdt.isManaged());
        logger.info("mrdt URL safe string is: " + mrdt.encodeToUrlString() + "\n");
        logger.info("hdfsdt: " + hdfsdt.getIdentifier());
        logger.info("hdfsdt kind: " + hdfsdt.getKind());
        logger.info("hdfsdt isManaged: " + hdfsdt.isManaged());
        logger.info("hdfsdt URL safe string is: " + hdfsdt.encodeToUrlString() + "\n");
        
        // we have 2 tokens now, 1 HDFS_DELEGATION_TOKEN and 1 RM_DELEGATION_TOKEN
        // This should fail, let's try to renew as ourselves
        long renewTimeHdfs = 0, renewTimeRm = 0;
        logger.info("\nLet's try to renew our tokens...");
        logger.info("First our HDFS_DELEGATION_TOKEN: ");
        try { 
        	renewTimeHdfs = hdfsdt.renew(conf); 
        }
        catch (Exception e) { 
        	logger.debug("Success, renew failed as expected since we're not the priv user"); 
        }
        
        if (renewTimeHdfs > 1357252344100L)
        {
            failFlags=failFlags+1; // security failure for HDFS token
            logger.error("FAILED! We were allowed to renew a token as ourselves when renewer is priv user.\nThe renewTimeHdfs we got back is: " + renewTimeHdfs);
        }
        
        logger.info("\nAnd our RM_DELEGATION_TOKEN: ");
        try { renewTimeRm = mrdt.renew(conf); }
        catch (Exception e) { logger.debug("Success, renew failed as expected since we're not the priv user"); }
        if (renewTimeRm > 1357252344100L)
        {
            failFlags=failFlags+2; //security failure for RM token
            logger.error("FAILED! We were allowed to renew a token as ourselves when renewer is priv user.\nThe renewTimeRm we got back is:  " + renewTimeRm + "\n");
        }

        int numTokens = creds.numberOfTokens();
        logger.info("We have a total of " + numTokens  + " tokens");
        logger.info("Dump all tokens currently in our Credentials:");
        logger.info(creds.getAllTokens() + "\n");
        
        logger.info("Trying to submit job1...");
        // job1
		Random random = new Random();
		int input_index = random.nextInt(TotalFileNum);

		WordCountJob job = new WordCountJob();
		String intFile = intPath.toString() + "/" + Integer.toString(input_index) + ".txt";
		logger.info("Randomly choosed intermediate input file is: " + intFile);
		String outpath = "/tmp/outfoo";
		logger.info("Output file is: "+outpath);
		
		job.setInputFile(intFile);
		job.setOutputPath(outpath);
		TestSession.cluster.getFS().delete(new Path(outpath), true);
        job.start();
        
        assertTrue("WordCount job was not assigned an ID within 20 seconds.", job.waitForID(20));
		assertTrue("WordCount job ID for WordCount job is invalid.", job.verifyID());

		int waitTime = 2;
		assertTrue("Job did not succeed.",job.waitForSuccess(waitTime));
		logger.info("Job 1 completion successful");
		
        //job2
        random = new Random();
		input_index = random.nextInt(TotalFileNum);

		job = new WordCountJob();
		outpath="/tmp/outfoo2";
		intFile = intPath.toString() + "/" + Integer.toString(input_index) + ".txt";
		logger.info("Randomly choosed input file is: " + intFile);
		logger.info("Output Directory is: "+outpath);
		
		job.setInputFile(intFile);
		job.setOutputPath(outpath);
		TestSession.cluster.getFS().delete(new Path(outpath), true);
        job.start();

        assertTrue("WordCount job 2 was not assigned an ID within 20 seconds.", job.waitForID(20));
		assertTrue("WordCount job ID for WordCount job (default user) is invalid.", job.verifyID());

		assertTrue("Job 2 did not succeed.",job.waitForSuccess(waitTime));
		logger.info("Job 2 completion successful");
		
		if (numTokens != creds.numberOfTokens()) {
	         logger.info("\nWARNING: number of tokens before and after job submission differs, had " + numTokens + " now have " + creds.numberOfTokens());
	     }
	     logger.info("After job2, we have a total of " + creds.numberOfTokens() + " tokens");
	     logger.info("\nDump all tokens currently in our Credentials:");
	     logger.info(creds.getAllTokens() + "\n");

	}
}



