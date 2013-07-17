package hadooptest.hadoop.stress.tokenRenewal;

import static org.junit.Assert.assertTrue;

import java.io.File;

import hadooptest.TestSession;
import hadooptest.workflow.hadoop.job.WordCountJob;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.junit.BeforeClass;
import org.junit.Test;




public class TestTokenRenewal_existingUgi_oldApi extends TestSession {
	
	/****************************************************************
	 *  Please set up input and output directory and file name here *
	 ****************************************************************/
	// NOTE: the file should appear in you home directory
	private static String localFile = "TTR_input.txt";
	// NOTE: this is a directory and will appear in your home directory in the HDFS
	private static String outputFile = "TTR_output";
	
	private static String input_string = "Hello world! Really???? Are you sure?";

	
	// location information 
	private static String outputDir = null;
	private static String localDir = null;
	
	/*
	 *  Before running the test.
	 *  1. Start the session
	 *  2. Copy the original file from the local machine and generate the 
	 *     input files
	 */
	@BeforeClass
	public static void startTestSession() throws Exception {
		TestSession.start();
		setupTestDir();		
	}
	
	public static void setupTestDir() throws Exception {
		
		// show the input and output path
		localDir = "/home/" + System.getProperty("user.name") + "/";
		logger.info("Target local Directory is: "+ localDir + "\n" + "Target File Name is: " + localFile);
		
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
		
		outputDir = "/user/" + TestSession.conf.getProperty("USER") + "/"; 
		logger.info("Target HDFS Directory is: "+ outputDir + "\n" + "Target File Name is: " + outputFile);
				
		TestSession.cluster.getFS().delete(new Path(outputDir+localFile), true);
	    
		TestSession.cluster.getFS().copyFromLocalFile(new Path(localDir + localFile), new Path(outputDir + localFile)); 
		TestSession.cluster.getFS().delete(new Path(outputDir+outputFile), true);
	}

	/*
	 * A test for running a TestTokenRenewal job
	 * 
	 * Equivalent to JobSummaryInfo10 in the original shell script YARN regression suite.
	 */

	@Test
	public void runTestTokenRenewal1() throws Exception {
			
	    JobConf conf = new JobConf(TestSession.cluster.getConf());
	    JobClient jobclient = new JobClient(conf);
//	    conf.setJarByClass(TokenRenewalTest_existingUgi_oldApi.class);
	    conf.setJobName("TokenRenewalTest_existingUgi_oldApi_job1");

	    FileSystem fs = FileSystem.get(conf);

	    WordCountJob Job1 = new WordCountJob();

		Job1.setInputFile(outputDir + localFile);
		Job1.setOutputPath(outputDir + outputFile +"/job1");

	     // list out our config prop change, should be 60 (seconds)
	    TestSession.logger.info("Check the renew property setting, yarn.resourcemanager.delegation.token.renew-interval: " + conf.get("yarn.resourcemanager.delegation.token.renew-interval"));

	     // don't cancel out tokens so we can use them in job2
	    conf.setBoolean("mapreduce.job.complete.cancel.delegation.tokens", false);

	     // get dt with RM priv user as renewer
	    Token<DelegationTokenIdentifier> mrdt = jobclient.getDelegationToken(new Text("mapredqa"));
	    conf.getCredentials().addToken(new Text("MR_TOKEN"), mrdt);
	     // get dt with HDFS priv user as renewer
	    Token<? extends TokenIdentifier> hdfsdt = fs.getDelegationToken("mapredqa");
	    conf.getCredentials().addToken(new Text("HDFS_TOKEN"), hdfsdt);
	    
	    TestSession.logger.info("mrdt: " + mrdt.getIdentifier());
	    TestSession.logger.info("mrdt kind: " + mrdt.getKind());
	     //private method        TestSession.logger.info("mrdt Renewer: " + mrdt.getRenewer() + "\n");
	    TestSession.logger.info("mrdt isManaged: " + mrdt.isManaged());
	    TestSession.logger.info("mrdt URL safe string is: " + mrdt.encodeToUrlString() + "\n");
	    TestSession.logger.info("hdfsdt: " + hdfsdt.getIdentifier());
	    TestSession.logger.info("hdfsdt kind: " + hdfsdt.getKind());
	     //private method        TestSession.logger.info("hdfsdt Renewer: " + hdfsdt.getRenewer() + "\n");
	    TestSession.logger.info("hdfsdt isManaged: " + hdfsdt.isManaged());
	    TestSession.logger.info("hdfsdt URL safe string is: " + hdfsdt.encodeToUrlString() + "\n");

	     // we have 2 tokens now, 1 HDFS_DELEGATION_TOKEN and 1 RM_DELEGATION_TOKEN
	     // This should fail, let's try to renew as ourselves 
	    long renewTimeHdfs = 0, renewTimeRm = 0;
	    TestSession.logger.info("\nLet's try to renew our tokens...");
	    TestSession.logger.info("First our HDFS_DELEGATION_TOKEN: ");
	    try { renewTimeHdfs = hdfsdt.renew(conf); }
	    catch (Exception e) { TestSession.logger.info("Success, renew failed as expected since we're not the priv user"); }
	    if (renewTimeHdfs > 1357252344100L)
	    {
	    	TestSession.logger.error("FAILED! We were allowed to renew a token as ourselves when renewer is priv user.\nThe renewTimeHdfs we got back is: " + renewTimeHdfs);
	    }

	    TestSession.logger.info("\nAnd our RM_DELEGATION_TOKEN: "); 
	    try { renewTimeRm = mrdt.renew(conf); }
	    catch (Exception e) { TestSession.logger.info("Success, renew failed as expected since we're not the priv user"); }
	    if (renewTimeRm > 1357252344100L) 
	    { 
	    	TestSession.logger.error("FAILED! We were allowed to renew a token as ourselves when renewer is priv user.\nThe renewTimeRm we got back is:  " + renewTimeRm + "\n");
	    }

	    int numTokens = conf.getCredentials().numberOfTokens();
	    TestSession.logger.info("We have a total of " + numTokens  + " tokens");
	    TestSession.logger.info("Dump all tokens currently in our Credentials:");
	    TestSession.logger.info(conf.getCredentials().getAllTokens() + "\n");

	    TestSession.logger.info("Trying to submit job1...");
	     
	    Job1.start();

	    assertTrue("Job1  was not assigned an ID within 10 seconds.", 
					Job1.waitForID(10));
	    assertTrue("Job1 is invalid.", 
					Job1.verifyID());

	    int waitTime = 2;
	    assertTrue("Job1 did not succeed.",
					Job1.waitForSuccess(waitTime));

	    if (numTokens != conf.getCredentials().numberOfTokens()) {
	         TestSession.logger.warn("\nWARNING: number of tokens before and after job submission differs, had " + numTokens + " now have " + conf.getCredentials().numberOfTokens());
	    }
	    TestSession.logger.info("We have a total of " + conf.getCredentials().numberOfTokens() + " tokens");
	    TestSession.logger.info("\nDump all tokens currently in our Credentials:");
	    TestSession.logger.info(conf.getCredentials().getAllTokens() + "\n");

	    // run a second job which should use the existing tokens, should see renewals 
	    // happen at 0.80*60 seconds
		WordCountJob Job2 = new WordCountJob();

		Job2.setInputFile(outputDir + localFile);
		Job2.setOutputPath(outputDir + outputFile +"/job2");

		Job2.start();
		 
		assertTrue("Job2  was not assigned an ID within 10 seconds.", 
					Job2.waitForID(10));
		assertTrue("Job2 is invalid.", 
					Job2.verifyID());

		waitTime = 2;
		assertTrue("Job2 did not succeed.",
					Job2.waitForSuccess(waitTime));
	     
	    if (numTokens != conf.getCredentials().numberOfTokens()) {
	        TestSession.logger.warn("\nWARNING: number of tokens before and after job submission differs, had " + numTokens + " now have " + conf.getCredentials().numberOfTokens());
	    }
	    TestSession.logger.info("We have a total of " + conf.getCredentials().numberOfTokens() + " tokens");
	    TestSession.logger.info("\nDump all tokens currently in our Credentials:");
	    TestSession.logger.info(conf.getCredentials().getAllTokens() + "\n");
	}
}	
	 