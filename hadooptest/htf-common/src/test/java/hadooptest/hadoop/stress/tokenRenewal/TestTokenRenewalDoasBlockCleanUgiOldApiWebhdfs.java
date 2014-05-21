/*
 *  [ATTENTION!]
 *  There is a race condition problem for this test.
 *  This test may succeed normally, or fail on the "Redirecting to job history server" error
 *  Waiting for the bug fixed in Hadoop core.
 */
package hadooptest.hadoop.stress.tokenRenewal;

import static org.junit.Assert.assertTrue;
import hadooptest.TestSession;
import hadooptest.cluster.hadoop.HadoopCluster;
import hadooptest.workflow.hadoop.job.WordCountJob;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestTokenRenewalDoasBlockCleanUgiOldApiWebhdfs extends TestSession {

	// NOTE: the file should appear in you home directory
	private static String localFile = "TTR_input.txt";
	// NOTE: this is a directory and will appear in your home directory in the HDFS
	private static String outputFile = "TTR_output";
	// NOTE: this is the name node of your cluster that you currently test your code on
	private static String hdfsNode;
	private static String webhdfsAddr;
	private static String input_string = "Hello world! Run token renewal tests!";

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
		// get webhdfs node addr from the properties file
		String workingDir = System.getProperty("user.dir");	
		Properties prop = new Properties();	 
    	try {
            //load a properties file
    		prop.load(new FileInputStream(workingDir+"/conf/StressConf/StressTestProp.properties"));
    	} catch (IOException ex) {
    		ex.printStackTrace();
        }
		
		hdfsNode = TestSession.cluster.getNode(HadoopCluster.NAMENODE).getHostname();
	    logger.info("===> HDFS node addr.: "+ hdfsNode + " <===");
		
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
		logger.info("Target Directory is: " + outputDir + localFile+ "Target File is: " + outputDir + outputFile);
		
	    webhdfsAddr = "webhdfs://" + hdfsNode + ":" + outputDir + localFile;
	    
		logger.info("Target Directory is: " + webhdfsAddr);
				
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
	    FileSystem fs = FileSystem.get(conf);


	    // list out our config prop change, should be 60 (seconds)
	    TestSession.logger.info("Check the renew property setting, yarn.resourcemanager.delegation.token.renew-interval: " + conf.get("yarn.resourcemanager.delegation.token.renew-interval"));
	    // don't cancel our tokens so we can use them in second
	    conf.setBoolean("mapreduce.job.complete.cancel.delegation.tokens", false);

	    UserGroupInformation ugiOrig = UserGroupInformation.createRemoteUser("hadoopqa@DEV.YGRID.YAHOO.COM");
	    //UserGroupInformation ugiOrig = UserGroupInformation.createRemoteUser("patwhite@DS.CORP.YAHOO.COM");

	    Credentials creds = new Credentials();
	    // get dt with RM priv user as renewer
	    Token<DelegationTokenIdentifier> mrdt = jobclient.getDelegationToken(new Text("GARBAGE1_mapredqa"));
	    //phw conf.getCredentials().addToken(new Text("MR_TOKEN"), mrdt);
	    creds.addToken(new Text("MR_TOKEN"), mrdt);
	    // get an HDFS token
	    Token<?> myTokenHdfsFs = fs.addDelegationTokens("mapredqa", creds)[0];

	    // let's see what we got...
	    // MR token
	    TestSession.logger.info("mrdt: " + mrdt.getIdentifier());
	    TestSession.logger.info("mrdt kind: " + mrdt.getKind());
	      //private method        TestSession.logger.info("mrdt Renewer: " + mrdt.getRenewer() + "\n");
	    TestSession.logger.info("mrdt isManaged: " + mrdt.isManaged());
	    TestSession.logger.info("mrdt URL safe string is: " + mrdt.encodeToUrlString() + "\n");
	    // HDFS token
	    TestSession.logger.info("myTokenHdfsFs: " + myTokenHdfsFs.getIdentifier());
	    TestSession.logger.info("myTokenHdfsFs kind: " + myTokenHdfsFs.getKind());
	      //private method        TestSession.logger.info("myTokenHdfsFs Renewer: " + myTokenHdfsFs.getRenewer() + "\n");
	    TestSession.logger.info("myTokenHdfsFs isManaged: " + myTokenHdfsFs.isManaged());
	    TestSession.logger.info("myTokenHdfsFs URL safe string is: " + myTokenHdfsFs.encodeToUrlString() + "\n");

	    // add creds to UGI, this adds the RM token, the HDFS token was added already as part
	    // of the addDelegationTokens()
	    ugiOrig.addCredentials(creds);
	    TestSession.logger.info("From OriginalUser... my Creds say i'm: " + UserGroupInformation.getCurrentUser() + " and I have " + creds.numberOfTokens() + " tokens");

	     // we have 2 tokens now, 1 HDFS_DELEGATION_TOKEN and 1 RM_DELEGATION_TOKEN
	     // This should fail, let's try to renew as ourselves 
	    long renewTimeHdfs = 0, renewTimeRm = 0;
	    TestSession.logger.info("\nLet's try to renew our tokens...");
	    TestSession.logger.info("First our HDFS_DELEGATION_TOKEN: ");
	    try { renewTimeHdfs = myTokenHdfsFs.renew(conf); }
	    catch (Exception e) { TestSession.logger.info("Success, renew failed as expected since we're not the priv user"); }
	    if (renewTimeHdfs > 1357252344100L)
	    {
	      TestSession.logger.info("FAILED! We were allowed to renew a token as ourselves when renewer is priv user.\nThe renewTimeHdfs we got back is: " + renewTimeHdfs);
	    }


	    TestSession.logger.info("\nAnd our RM_DELEGATION_TOKEN: ");
	    try { renewTimeRm = mrdt.renew(conf); }
	    catch (Exception e) { TestSession.logger.info("Success, renew failed as expected since we're not the priv user"); }
	    if (renewTimeRm > 1357252344100L)
	    {
	      TestSession.logger.info("FAILED! We were allowed to renew a token as ourselves when renewer is priv user.\nThe renewTimeRm we got back is:  " + renewTimeRm + "\n");
	    }

	    int numTokens = ugiOrig.getCredentials().numberOfTokens();
	    TestSession.logger.info("We have a total of " + numTokens  + " tokens");
	    TestSession.logger.info("Dump all tokens currently in our Credentials:");
	    TestSession.logger.info(ugiOrig.getCredentials().getAllTokens() + "\n");


	    // instantiate a seperate object to use for submitting jobs, using
	    // our shiney new tokens
	    DoasUser du = new DoasUser(ugiOrig);
	    du.go();

	    // back to our original context, our two doAs jobs should have ran as the specified
	    // proxy user, dump our existing credentials 
	    TestSession.logger.info("Back from the doAs block to original context... my Creds say i'm: " + UserGroupInformation.getCurrentUser() + " and I now have " + creds.numberOfTokens() + " tokens");
	    TestSession.logger.info("\nDump all tokens currently in our Credentials:");
	    TestSession.logger.info(ugiOrig.getCredentials().getAllTokens() + "\n");
	}
	
	  // class DoasUser
	  // this is used create a new UGI (new security context) for running wordcount jobs
	  // using the credentials passed into said ugi
	  public class DoasUser {

	    private Credentials doasCreds;
	    UserGroupInformation ugi;

	    //constructor - ugi to use is passed in, uses existing creds that come in with said ugi
	    DoasUser (UserGroupInformation ugi) {
	      doasCreds = ugi.getCredentials();

	      this.ugi = ugi;
	    }

	    public void go() throws Exception {

	      // run as the original user
	    	String retVal = ugi.doAs(new PrivilegedExceptionAction<String>() {
	        public String run() throws Exception {
	           // this fails with expired token before Tom's fix on top of Sid's token renewal patch in 23.6 
	           ugi.addCredentials(doasCreds);
	           TestSession.logger.info("From doasUser before running jobs...  my Creds say i'm: " + UserGroupInformation.getCurrentUser() + " and I have " + doasCreds.numberOfTokens() + " tokens");

	           // setup and run a wordcount job, this should use the tokens passed into
	           // this doAs block, job should succeed
	           
	           // submit the job, this should automatically get us a jobhistory token,
	           // but does not seem to do so...
	           TestSession.logger.info("\nTrying to submit doAs job1...");
	           WordCountJob Job1 = new WordCountJob();
		  	     
		   	   Job1.setInputFile(outputDir + localFile);
		   	   Job1.setOutputPath(outputDir + outputFile +"/job1");
		   		 
		   	   TestSession.logger.info("Trying to submit job1...");
		   	   Job1.start();
		   		 
		   	   assertTrue("Job1  was not assigned an ID within 10 seconds.", 
		   				Job1.waitForID(10));
		   	   assertTrue("Job1 is invalid.", 
		   				Job1.verifyID());
	
		   	   int waitTime = 2;
		   	   assertTrue("Job1 did not succeed.",
		   				Job1.waitForSuccess(waitTime));
	           TestSession.logger.info("After doasUser first job... my Creds say i'm: " + UserGroupInformation.getCurrentUser() + " and I now have " + doasCreds.numberOfTokens() + " tokens");


	           // setup and run another wordcount job, this should exceed the token renewal time of 60 seconds
	           // and cause all of our passed-in tokens to be renewed, job should also succeed
	           WordCountJob Job2 = new WordCountJob();
		  	     
		   	   Job2.setInputFile(outputDir + localFile);
		   	   Job2.setOutputPath(outputDir + outputFile +"/job2");
		   		 
		   	   TestSession.logger.info("Trying to submit job2...");
		   	   Job2.start();
		   		 
		   	   assertTrue("Job2  was not assigned an ID within 10 seconds.", 
		   				Job2.waitForID(10));
		   	   assertTrue("Job2 is invalid.", 
		   				Job2.verifyID());
	
		   	   waitTime = 2;
		   	   assertTrue("Job2 did not succeed.",
		   				Job2.waitForSuccess(waitTime));

	           TestSession.logger.info("After doasUser second job... my Creds say i'm: " + UserGroupInformation.getCurrentUser() + " and I now have " + doasCreds.numberOfTokens() + " tokens");

	           return "This is the doAs block";
	          }
	        });
	      // back out of the go() method, no longer running as the doAs proxy user
	      TestSession.logger.info(retVal);
	      // write out our tokens back out of doas scope
	      //doasCreds.writeTokenStorageFile(new Path("/tmp/tokenfile_doas_out"), doasConf);
	     }

	  }

}
	 