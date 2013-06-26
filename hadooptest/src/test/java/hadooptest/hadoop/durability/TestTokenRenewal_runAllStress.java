package hadooptest.hadoop.durability;

import static org.junit.Assert.assertTrue;
import hadooptest.TestSession;
import hadooptest.workflow.hadoop.job.WordCountJob;

import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.junit.BeforeClass;
import org.junit.Test;



public class TestTokenRenewal_runAllStress extends TestSession {
	
	/****************************************************************
	 *  Please set up input and output directory and file name here *
	 ****************************************************************/
	// NOTE: the file should appear in you home directory
	private static String localFile = "TTR_input.txt";
	// NOTE: this is a directory and will appear in your home directory in the HDFS
	private static String outputFile = "TTR_output";
	// NOTE: this is the name node of your cluster that you currently test your code on
	private static String hdfsNode = "gsbl90628.blue.ygrid.yahoo.com";

	/****************************************************************
	 *      Please set up how long you want ALL tests to run        *
	 ****************************************************************/
	private static long runTimeMin = 5; 
	private static long runTimeHour = 0;
	private static long runTimeDay = 0;
	
	// location information 
	private static String outputDir = null;
	private static String localDir = null;
	private static int jobcount = 0;
	private static String webhdfsAddr;
	
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
	public void TestTokenRenewal() throws Exception{
		
	    // get current time
	    long startTime = System.currentTimeMillis();
	    TestSession.logger.info("Current time is: " + startTime/1000);
		
	    long endTime = startTime + runTimeMin*60*1000 + runTimeHour*60*60*1000 + runTimeDay*24*60*60*1000;
	    
	    TestSession.logger.info("End time is: " + endTime/1000);
		
		while(endTime > System.currentTimeMillis()) {
			
			System.out.println("Time remaining(in sec): " + (endTime - System.currentTimeMillis())/1000);
			
			existingUgi(null);
			existingUgi("webhdfs");
			
			existingUgi_oldApi(null);
			existingUgi_oldApi("webhdfs");
			
	//		System.out.println("\n\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!doasBlock_cleanUgi!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n\n");
	//		doasBlock_cleanUgi(null);
	//		System.out.println("\n\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!doasBlock_cleanUgi_webhdfs!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n\n");
	//		doasBlock_cleanUgi("webhdfs");
			
	//		System.out.println("\n\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!doasBlock_cleanUgi_oldApi!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n\n");
	//		doasBlock_cleanUgi_oldApi(null);
	//		System.out.println("\n\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!doasBlock_cleanUgi_oldApi_webhdfs!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n\n");
	//		doasBlock_cleanUgi_oldApi("webhdfs");
			
			System.out.println("\n\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!doasBlock_cleanUgi_proxyUser!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n\n");
			doasBlock_cleanUgi_proxyUser(null, null);
			System.out.println("\n\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!doasBlock_cleanUgi_proxyUser_webhdfs!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n\n");		
			doasBlock_cleanUgi_proxyUser("webhdfs", null);
	
			System.out.println("\n\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!doasBlock_cleanUgi_proxyUser_CurrentUser!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n\n");
			doasBlock_cleanUgi_proxyUser(null, "CurrentUser");
			System.out.println("\n\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!doasBlock_cleanUgi_proxyUser_CurrentUser_webhdfs!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n\n");
			doasBlock_cleanUgi_proxyUser("webhdfs", "CurrentUser");
		}
	}
	
	
	public void existingUgi(String type) throws Exception {
		// existing ugi
		Credentials creds = new Credentials();
	    Configuration conf = TestSession.cluster.getConf();    
	    Cluster cluster = new Cluster(conf);
	    FileSystem fs = FileSystem.get(conf);
	    WordCountJob Job1 = new WordCountJob();
	     
	    
	    if(type == "webhdfs")
    		Job1.setInputFile(webhdfsAddr);
	    else
			Job1.setInputFile(outputDir + localFile);
		Job1.setOutputPath(outputDir + outputFile +"/job" + Integer.toString(jobcount));
		jobcount++;
		
//		System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n" +
//							"jobcount = " + jobcount + "\n" +
//							"Path = " + outputDir + outputFile +"/job" + Integer.toString(jobcount));
		
	    // list out our config prop change, should be 60 (seconds)
	    System.out.println("Check the renew property setting, yarn.resourcemanager.delegation.token.renew-interval: " + conf.get("yarn.resourcemanager.delegation.token.renew-interval"));

	     // don't cancel out tokens so we can use them in job2
	    conf.setBoolean("mapreduce.job.complete.cancel.delegation.tokens", false);
	    		 
	    // get dt with RM priv user as renewer
	    // need to get RM token from Cluster, can't get one from Job, weird eh?
	    Token<?> mrdt = cluster.getDelegationToken(new Text("mapredqa"));
	    creds.addToken(new Text("RM_TOKEN"), mrdt);
	    // get dt with HDFS priv user as renewer
	    Token<?> hdfsdt = fs.addDelegationTokens("mapredqa", creds)[0];

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
	    try { renewTimeHdfs = hdfsdt.renew(conf); 
	    	TestSession.logger.info("Renew time for HDFS = " + "renewTimeHDFS"); }
	    catch (Exception e) { TestSession.logger.info("Success, renew failed as expected since we're not the priv user"); }
	    if (renewTimeHdfs > 1357252344100L)  
	    {
	      TestSession.logger.error("FAILED! We were allowed to renew a token as ourselves when renewer is priv user.\nThe renewTimeHdfs we got back is: " + renewTimeHdfs);
	    }

	    TestSession.logger.info("\nAnd our RM_DELEGATION_TOKEN: "); 
	    try { renewTimeRm = mrdt.renew(conf); 
  				TestSession.logger.info("Renew time for HDFS = " + "renewTimeHDFS");}
	    catch (Exception e) { TestSession.logger.info("Success, renew failed as expected since we're not the priv user"); }
	    if (renewTimeRm > 1357252344100L) 
	    {
	      TestSession.logger.error("FAILED! We were allowed to renew a token as ourselves when renewer is priv user.\nThe renewTimeRm we got back is:  " + renewTimeRm + "\n");
	    }

	    int numTokens = creds.numberOfTokens();
	    TestSession.logger.info("We have a total of " + numTokens  + " tokens");
	    TestSession.logger.info("Dump all tokens currently in our Credentials:");
	    TestSession.logger.info(creds.getAllTokens() + "\n");

	    TestSession.logger.info("Trying to submit job1...");
	     
	    Job1.start();
	     
		assertTrue("Job1  was not assigned an ID within 10 seconds.", 
					Job1.waitForID(10));
		assertTrue("Job1 is invalid.", 
					Job1.verifyID());

		int waitTime = 2;
		assertTrue("Job1 did not succeed.",
					Job1.waitForSuccess(waitTime));
	     
	    if (numTokens != creds.numberOfTokens()) {
	         TestSession.logger.info("\nWARNING: number of tokens before and after job submission differs, had " + numTokens + " now have " + creds.numberOfTokens());
	    }
	    TestSession.logger.info("After job1, we have a total of " + creds.numberOfTokens() + " tokens");
	    TestSession.logger.info("\nDump all tokens currently in our Credentials:");
	    TestSession.logger.info(creds.getAllTokens() + "\n");
	     
	    // run a second job which should use the existing tokens, should see renewals 
	    // happen at 0.80*60 seconds
	     //Configuration conf2 = new Configuration();
	     
	    WordCountJob Job2 = new WordCountJob();
	     
	    if(type == "webhdfs")
    		Job2.setInputFile(webhdfsAddr);
	    else
			Job2.setInputFile(outputDir + localFile);
		Job2.setOutputPath(outputDir + outputFile +"/job" + Integer.toString(jobcount));
		jobcount++;
		 
		TestSession.logger.info("Trying to submit job2...");
		Job2.start();
		 
		assertTrue("Job2  was not assigned an ID within 10 seconds.", 
					Job2.waitForID(10));
		assertTrue("Job2 is invalid.", 
					Job2.verifyID());

		waitTime = 2;
		assertTrue("Job2 did not succeed.",
					Job2.waitForSuccess(waitTime));
		 
		if (numTokens != creds.numberOfTokens()) {
	         TestSession.logger.warn("\nWARNING: number of tokens before and after job submission differs, had " + numTokens + " now have " + creds.numberOfTokens());
	    }
	    TestSession.logger.info("After job2, we have a total of " + creds.numberOfTokens() + " tokens");
	    TestSession.logger.info("\nDump all tokens currently in our Credentials:");
	    TestSession.logger.info(creds.getAllTokens() + "\n");	
	}
	
	
	public void existingUgi_oldApi(String type) throws Exception {
			
	    JobConf conf = new JobConf(TestSession.cluster.getConf());
	    JobClient jobclient = new JobClient(conf);
//	    conf.setJarByClass(TokenRenewalTest_existingUgi_oldApi.class);
	    conf.setJobName("TokenRenewalTest_existingUgi_oldApi_job1");

	    FileSystem fs = FileSystem.get(conf);

	    WordCountJob Job1 = new WordCountJob();

	    if(type == "webhdfs")
    		Job1.setInputFile(webhdfsAddr);
	    else
			Job1.setInputFile(outputDir + localFile);
		Job1.setOutputPath(outputDir + outputFile +"/job" + Integer.toString(jobcount));
		jobcount++;

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

	    if(type == "webhdfs")
    		Job2.setInputFile(webhdfsAddr);
	    else
			Job2.setInputFile(outputDir + localFile);
		Job2.setOutputPath(outputDir + outputFile +"/job" + Integer.toString(jobcount));
		jobcount++;

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
	
	
	public void doasBlock_cleanUgi(String type) throws Exception {
		Configuration conf = new Configuration();
	    Cluster cluster = new Cluster(conf);
	    FileSystem fs = FileSystem.get(conf);

	    // list out our config prop change, should be 60 (seconds)
	    TestSession.logger.info("Check the renew property setting, yarn.resourcemanager.delegation.token.renew-interval: " + conf.get("yarn.resourcemanager.delegation.token.renew-interval"));
	    // don't cancel our tokens so we can use them in second
	    conf.setBoolean("mapreduce.job.complete.cancel.delegation.tokens", false);

	    UserGroupInformation ugiOrig = UserGroupInformation.createRemoteUser("hadoopqa@DEV.YGRID.YAHOO.COM");
	    //UserGroupInformation ugiOrig = UserGroupInformation.createRemoteUser("patwhite@DS.CORP.YAHOO.COM");

	    // renewer is me
	    //Token<? extends TokenIdentifier> myTokenRm = new Token(cluster.getDelegationToken(new Text(ugiOrig.getShortUserName())) );
	    //Token<? extends TokenIdentifier> myTokenHdfsFs = new Token(fs.getDelegationToken(ugiOrig.getShortUserName()) );

	    // renewer is not valid, shouldn't matter now after 23.6 design change for renewer
	    Token<? extends TokenIdentifier> myTokenRm = cluster.getDelegationToken(new Text("GARBAGE1_mapredqa"));
	    // Note well, need to use fs.addDelegationTokens() to get an HDFS DT that is recognized by the fs,
	    // trying to use fs.getDelegationToken() appears to work but doesn't, fs still auto fetches a token
	    // so said token is not being recognized

	    Credentials creds = new Credentials();
	    creds.addToken(new Text("MyTokenAliasRM"), myTokenRm);

	    // TODO: should capture this list and iterate over it, not grab first element...
	    Token<?> myTokenHdfsFs = fs.addDelegationTokens("mapredqa", creds)[0];

	    // let's see what we got...
	    TestSession.logger.info("myTokenRm: " + myTokenRm.getIdentifier());
	    TestSession.logger.info("myTokenRm kind: " + myTokenRm.getKind() + "\n");
	    TestSession.logger.info("myTokenHdfsFs: " + myTokenHdfsFs.getIdentifier());
	    TestSession.logger.info("myTokenHdfsFs kind: " + myTokenHdfsFs.getKind() + "\n");

	    // add creds to UGI, this adds the two RM tokens, the HDFS token was added already as part
	    // of the addDelegationTokens()
	    ugiOrig.addCredentials(creds);
	    TestSession.logger.info("From OriginalUser... my Creds say i'm: " + UserGroupInformation.getCurrentUser() + " and I have " + creds.numberOfTokens() + " tokens");

	    // write our tokenfile
	    //creds.writeTokenStorageFile(new Path("/tmp/tokenfile_orig"), conf);

	    // we have 2 tokens now, 1 HDFS_DELEGATION_TOKEN and 1 RM_DELEGATION_TOKEN, let's verify 
	    // we can't renew them, since renewers don't match 

	    long renewTimeHdfs = 0, renewTimeRm = 0;
	    TestSession.logger.info("\nLet's try to renew our tokens, should fail since renewers don't match...");
	    TestSession.logger.info("First our HDFS_DELEGATION_TOKEN: ");
	    try { renewTimeHdfs = myTokenHdfsFs.renew(conf); }
	    catch (Exception e) { TestSession.logger.info("Success, renew failed as expected since we're not the priv user"); }
	    if (renewTimeHdfs > 1357252344100L) 
	    {
	      TestSession.logger.error("FAILED! We were allowed to renew a token as ourselves when renewer is priv user.\nThe renewTimeHdfs we got back is: " + renewTimeHdfs);
	    }

	    TestSession.logger.info("\nOur first RM_DELEGATION_TOKEN: ");
	    try { renewTimeRm = myTokenRm.renew(conf); }
	    catch (Exception e) { TestSession.logger.info("Success, renew failed as expected since we're not the priv user"); }
	    if (renewTimeRm > 1357252344100L) 
	    {
	      TestSession.logger.error("FAILED! We were allowed to renew a token as ourselves when renewer is priv user.\nThe renewTimeRm we got back is:  " + renewTimeRm + "\n");
	    }

	    int numTokens = ugiOrig.getCredentials().numberOfTokens();
	    TestSession.logger.info("We have a total of " + numTokens  + " tokens");
	    TestSession.logger.info("Dump all tokens currently in our Credentials:");
	    TestSession.logger.info(ugiOrig.getCredentials().getAllTokens() + "\n");

	    // instantiate a seperate object to use for submitting jobs, using
	    // our shiney new tokens
	    DoasUser_cleanUgi du = new DoasUser_cleanUgi(ugiOrig);
	    du.go(type);

	    // back to our original context, our two doAs jobs should have ran as the specified
	    // proxy user, dump our existing credentials 
	    TestSession.logger.info("Back from the doAs block to original context... my Creds say i'm: " + UserGroupInformation.getCurrentUser() + " and I now have " + creds.numberOfTokens() + " tokens");
	    TestSession.logger.info("\nDump all tokens currently in our Credentials:");
	    TestSession.logger.info(ugiOrig.getCredentials().getAllTokens() + "\n");

	}
	
	 // class DoasUser
	 // this is used create a new UGI (new security context) for running wordcount jobs
	 // using the credentials passed into said ugi
	public class DoasUser_cleanUgi {
		
	    private Credentials doasCreds;
//	    private Configuration doasConf = TestSession.cluster.getConf();
	    private String _type;
	    UserGroupInformation ugi;

	    //constructor - get creds passed in, need to create your own ugi
	    DoasUser_cleanUgi (Credentials creds) {
	      doasCreds = creds;
	      	 
	      //try { ugi = UserGroupInformation.getLoginUser(); }
	      //user remote....             try { ugi = UserGroupInformation.getCurrentUser(); }
	      try { ugi = UserGroupInformation.createRemoteUser("zhang369"); }
	      catch (Exception e) { TestSession.logger.error("Failed, couldn't get UGI object" + e); }

	      // check for valid passed-in Cred object
	      if (doasCreds == null) {
	         TestSession.logger.info("\nTotal Bummer: No doasCreds dude...\n");
	      } else {
	    	  TestSession.logger.info("\tdoasCreds listing:\n" + doasCreds.getAllTokens());
	      }
	    }

	    //constructor - ugi to use is passed in, uses existing creds that come in with said ugi
	    DoasUser_cleanUgi (UserGroupInformation ugi) {
	      doasCreds = ugi.getCredentials();

	      this.ugi = ugi;
	    }

	    public void go(String type) throws Exception {

	    	_type = type;
	    	// run as the original user
		    ugi.doAs(new PrivilegedExceptionAction<String>() {
		        public String run() throws Exception {
		           // this fails with expired token before Tom's fix on top of Sid's token renewal patch in 23.6 
		           ugi.addCredentials(doasCreds);
		           TestSession.logger.info("From doasUser before running jobs...  my Creds say i'm: " + UserGroupInformation.getCurrentUser() + " and I have " + doasCreds.numberOfTokens() + " tokens");
		         
		           // write out our tokens in doas scope
		           //Path tokenFile = new Path("/tmp/tokenfile_doas_in");
		           //doasCreds.writeTokenStorageFile(new Path("/tmp/tokenfile_doas_in"), doasConf);
		           //FileSystem doasFs = FileSystem.get(doasConf);
		           //doasFs.setPermission(tokenFile, new FsPermission("777"));
	
		           // setup and run a wordcount job, this should use the tokens passed into
		           // this doAs block, job should succeed
		           
		           
		            WordCountJob Job1 = new WordCountJob();
		  	     
		    	    if(_type == "webhdfs")
		        		Job1.setInputFile(webhdfsAddr);
		    	    else
		    			Job1.setInputFile(outputDir + localFile);
		    		Job1.setOutputPath(outputDir + outputFile +"/job" + Integer.toString(jobcount));
		    		jobcount++;
			   		 
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
		  	     
			   	    if(_type == "webhdfs")
			    		Job2.setInputFile(webhdfsAddr);
				    else
						Job2.setInputFile(outputDir + localFile);
		    		Job2.setOutputPath(outputDir + outputFile +"/job" + Integer.toString(jobcount));
		    		jobcount++;
			   		 
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
		           TestSession.logger.info("\nDump all tokens currently in our Credentials:");
		           TestSession.logger.info(ugi.getCredentials().getAllTokens() + "\n");
	
		           return "This is the doAs block";
		          }
		        });
	      // back out of the go() method, no longer running as the doAs proxy user

	      // write out our tokens back out of doas scope
	      //doasCreds.writeTokenStorageFile(new Path("/tmp/tokenfile_doas_out"), doasConf);
	     }

	  }
	
	public void doasBlock_cleanUgi_oldApi(String type) throws Exception {
		JobConf conf = new JobConf(TestSession.cluster.getConf());
	    JobClient jobclient = new JobClient(conf);
	    FileSystem fs = FileSystem.get(conf);


	    // list out our config prop change, should be 60 (seconds)
	    System.out.println("Check the renew property setting, yarn.resourcemanager.delegation.token.renew-interval: " + conf.get("yarn.resourcemanager.delegation.token.renew-interval"));
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
	    System.out.println("mrdt: " + mrdt.getIdentifier());
	    System.out.println("mrdt kind: " + mrdt.getKind());
	      //private method        System.out.println("mrdt Renewer: " + mrdt.getRenewer() + "\n");
	    System.out.println("mrdt isManaged: " + mrdt.isManaged());
	    System.out.println("mrdt URL safe string is: " + mrdt.encodeToUrlString() + "\n");
	    // HDFS token
	    System.out.println("myTokenHdfsFs: " + myTokenHdfsFs.getIdentifier());
	    System.out.println("myTokenHdfsFs kind: " + myTokenHdfsFs.getKind());
	      //private method        System.out.println("myTokenHdfsFs Renewer: " + myTokenHdfsFs.getRenewer() + "\n");
	    System.out.println("myTokenHdfsFs isManaged: " + myTokenHdfsFs.isManaged());
	    System.out.println("myTokenHdfsFs URL safe string is: " + myTokenHdfsFs.encodeToUrlString() + "\n");

	    // add creds to UGI, this adds the RM token, the HDFS token was added already as part
	    // of the addDelegationTokens()
	    ugiOrig.addCredentials(creds);
	    System.out.println("From OriginalUser... my Creds say i'm: " + UserGroupInformation.getCurrentUser() + " and I have " + creds.numberOfTokens() + " tokens");

	     // we have 2 tokens now, 1 HDFS_DELEGATION_TOKEN and 1 RM_DELEGATION_TOKEN
	     // This should fail, let's try to renew as ourselves 
	     long renewTimeHdfs = 0, renewTimeRm = 0;
	     System.out.println("\nLet's try to renew our tokens...");
	     System.out.println("First our HDFS_DELEGATION_TOKEN: ");
	     try { renewTimeHdfs = myTokenHdfsFs.renew(conf); }
	     catch (Exception e) { System.out.println("Success, renew failed as expected since we're not the priv user"); }
	    if (renewTimeHdfs > 1357252344100L)
	    {
	      System.out.println("FAILED! We were allowed to renew a token as ourselves when renewer is priv user.\nThe renewTimeHdfs we got back is: " + renewTimeHdfs);
	    }


	     System.out.println("\nAnd our RM_DELEGATION_TOKEN: ");
	     try { renewTimeRm = mrdt.renew(conf); }
	     catch (Exception e) { System.out.println("Success, renew failed as expected since we're not the priv user"); }
	    if (renewTimeRm > 1357252344100L)
	    {
	      System.out.println("FAILED! We were allowed to renew a token as ourselves when renewer is priv user.\nThe renewTimeRm we got back is:  " + renewTimeRm + "\n");
	    }

	     int numTokens = ugiOrig.getCredentials().numberOfTokens();
	     System.out.println("We have a total of " + numTokens  + " tokens");
	     System.out.println("Dump all tokens currently in our Credentials:");
	     System.out.println(ugiOrig.getCredentials().getAllTokens() + "\n");


	    // instantiate a seperate object to use for submitting jobs, using
	    // our shiney new tokens
	    DoasUser_cleanUgi_oldApi du = new DoasUser_cleanUgi_oldApi(ugiOrig);
	    du.go(type);

	    // back to our original context, our two doAs jobs should have ran as the specified
	    // proxy user, dump our existing credentials 
	    System.out.println("Back from the doAs block to original context... my Creds say i'm: " + UserGroupInformation.getCurrentUser() + " and I now have " + creds.numberOfTokens() + " tokens");
	    System.out.println("\nDump all tokens currently in our Credentials:");
	    System.out.println(ugiOrig.getCredentials().getAllTokens() + "\n");
	}
	
	 
	  // class DoasUser
	  // this is used create a new UGI (new security context) for running wordcount jobs
	  // using the credentials passed into said ugi
	 public class DoasUser_cleanUgi_oldApi {

	    private Credentials doasCreds;
	    UserGroupInformation ugi;
	    private String _type;

	    //constructor - ugi to use is passed in, uses existing creds that come in with said ugi
	    DoasUser_cleanUgi_oldApi (UserGroupInformation ugi) {
	      doasCreds = ugi.getCredentials();

	      this.ugi = ugi;
	    }

	    public void go(String type) throws Exception {

	    	_type = type;
	        // run as the original user
	    	ugi.doAs(new PrivilegedExceptionAction<String>() {
		        public String run() throws Exception {
		           // this fails with expired token before Tom's fix on top of Sid's token renewal patch in 23.6 
		        	ugi.addCredentials(doasCreds);
		        	TestSession.logger.info("From doasUser before running jobs...  my Creds say i'm: " + UserGroupInformation.getCurrentUser() + " and I have " + doasCreds.numberOfTokens() + " tokens");
	
		        	// setup and run a wordcount job, this should use the tokens passed into
		        	// this doAs block, job should succeed
		        	//JobConf doasConf = new JobConf();
	
		        	// submit the job, this should automatically get us a jobhistory token,
		        	// but does not seem to do so...
		        	TestSession.logger.info("\nTrying to submit doAs job1...");
		        	WordCountJob Job1 = new WordCountJob();
			  	     
		    	    if(_type == "webhdfs")
		        		Job1.setInputFile(webhdfsAddr);
		    	    else
		    			Job1.setInputFile(outputDir + localFile);
		    		Job1.setOutputPath(outputDir + outputFile +"/job" + Integer.toString(jobcount));
		    		jobcount++;
			   		 
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
			  	     
		    	    if(_type == "webhdfs")
		        		Job2.setInputFile(webhdfsAddr);
		    	    else
		    			Job2.setInputFile(outputDir + localFile);
		    		Job2.setOutputPath(outputDir + outputFile +"/job" + Integer.toString(jobcount));
		    		jobcount++;
			   		 
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
	      // TestSession.logger.info(retVal);
	      // write out our tokens back out of doas scope
	      //doasCreds.writeTokenStorageFile(new Path("/tmp/tokenfile_doas_out"), doasConf);
	     }

	  }

	 public void doasBlock_cleanUgi_proxyUser(String type, String user) throws Exception {
			Configuration conf = TestSession.cluster.getConf();//new Configuration();
		    Cluster cluster = new Cluster(conf);
		    FileSystem fs = FileSystem.get(conf);

		    // list out our config prop change, should be 60 (seconds)
		    System.out.println("Check the renew property setting, yarn.resourcemanager.delegation.token.renew-interval: " + conf.get("yarn.resourcemanager.delegation.token.renew-interval"));
		    // don't cancel our tokens so we can use them in second
		    conf.setBoolean("mapreduce.job.complete.cancel.delegation.tokens", false);

		    UserGroupInformation ugiOrig = UserGroupInformation.createRemoteUser("hadoopqa@DEV.YGRID.YAHOO.COM");
		    //UserGroupInformation ugiOrig = UserGroupInformation.createRemoteUser("patwhite@DS.CORP.YAHOO.COM");

		    // renewer is me
		    //Token<? extends TokenIdentifier> myTokenRm = new Token(cluster.getDelegationToken(new Text(ugiOrig.getShortUserName())) );
		    //Token<? extends TokenIdentifier> myTokenHdfsFs = new Token(fs.getDelegationToken(ugiOrig.getShortUserName()) );

		    // renewer is not valid, shouldn't matter now after 23.6 design change for renewer
		    Token<? extends TokenIdentifier> myTokenRm = cluster.getDelegationToken(new Text("GARBAGE1_mapredqa"));
		    // Note well, need to use fs.addDelegationTokens() to get an HDFS DT that is recognized by the fs,
		    // trying to use fs.getDelegationToken() appears to work but doesn't, fs still auto fetches a token
		    // so said token is not being recognized

		    Credentials creds = new Credentials();
		    creds.addToken(new Text("MyTokenAliasRM"), myTokenRm);

		    // TODO: should capture this list and iterate over it, not grab first element...
		    Token<?> myTokenHdfsFs = fs.addDelegationTokens("mapredqa", creds)[0];

		    // let's see what we got...
		    System.out.println("myTokenRm: " + myTokenRm.getIdentifier());
		    System.out.println("myTokenRm kind: " + myTokenRm.getKind() + "\n");
		    System.out.println("myTokenHdfsFs: " + myTokenHdfsFs.getIdentifier());
		    System.out.println("myTokenHdfsFs kind: " + myTokenHdfsFs.getKind() + "\n");

		    // add creds to UGI, this adds the two RM tokens, the HDFS token was added already as part
		    // of the addDelegationTokens()
		    ugiOrig.addCredentials(creds);
		    System.out.println("From OriginalUser... my Creds say i'm: " + UserGroupInformation.getCurrentUser() + " and I have " + creds.numberOfTokens() + " tokens");

		    // write our tokenfile
		    //creds.writeTokenStorageFile(new Path("/tmp/tokenfile_orig"), conf);

		    // we have 2 tokens now, 1 HDFS_DELEGATION_TOKEN and 1 RM_DELEGATION_TOKEN, let's verify 
		    // we can't renew them, since renewers don't match 

		    long renewTimeHdfs = 0, renewTimeRm = 0;
		    System.out.println("\nLet's try to renew our tokens, should fail since renewers don't match...");
		    System.out.println("First our HDFS_DELEGATION_TOKEN: ");
		    try { renewTimeHdfs = myTokenHdfsFs.renew(conf); }
		    catch (Exception e) { System.out.println("Success, renew failed as expected since we're not the priv user"); }
		    if (renewTimeHdfs > 1357252344100L)  
		    {
		      System.out.println("FAILED! We were allowed to renew a token as ourselves when renewer is priv user.\nThe renewTimeHdfs we got back is: " + renewTimeHdfs);
		    }

		    System.out.println("\nOur first RM_DELEGATION_TOKEN: ");
		    try { renewTimeRm = myTokenRm.renew(conf); }
		    catch (Exception e) { System.out.println("Success, renew failed as expected since we're not the priv user"); }
		    if (renewTimeRm > 1357252344100L) 
		    {
		      System.out.println("FAILED! We were allowed to renew a token as ourselves when renewer is priv user.\nThe renewTimeRm we got back is:  " + renewTimeRm + "\n");
		    }

		    int numTokens = ugiOrig.getCredentials().numberOfTokens();
		    System.out.println("We have a total of " + numTokens  + " tokens");
		    System.out.println("Dump all tokens currently in our Credentials:");
		    System.out.println(ugiOrig.getCredentials().getAllTokens() + "\n");

		    // instantiate a seperate object to use for submitting jobs, but don't use
		    // the tokens we got since they won't work in the doAs due to mismatching
		    // user authentications
		    DoasUser_cleanUgi_proxyUser du = new DoasUser_cleanUgi_proxyUser(user);
		    du.go(type);

		    // back to our original context, our two doAs jobs should have ran as the specified
		    // proxy user, dump our existing credentials 
		    System.out.println("Back from the doAs block to original context... my Creds say i'm: " + UserGroupInformation.getCurrentUser() + " and I now have " + creds.numberOfTokens() + " tokens");
		    System.out.println("\nDump all tokens currently in our Credentials:");
		    System.out.println(ugiOrig.getCredentials().getAllTokens() + "\n");

		}
		
		  // class DoasUser
		  // this is used create a new UGI (new security context) for running wordcount jobs
		  // using the credentials passed into said ugi
	 public class DoasUser_cleanUgi_proxyUser {
		    // DO NOT instantiate objects here, dummy, declare for scope but don't snag 'em yet
		    // gotta be in the doas run method to get proxy user's context

		    private Credentials doasCreds;
		    private Cluster doasCluster;
		    private Configuration doasConf;
		    private FileSystem doasFs;
		    private UserGroupInformation ugi;
		    private String _type;
		    
		    // constructor - nothing is passed in, just use the doAs block to get 
		    // a new UGI (new security context)
		    DoasUser_cleanUgi_proxyUser (String user) throws Exception {
		      // DO NOT instantiate objects here either, dummy, gotta be in the doas run method to getƒ
		      // proxy user's context

		      // get a proxy UGI for hadoopqa, since no creds are passed in we have to get tokens
		      // using the TGT fallback for the login user
		    try { 
		    	if(user == "CurrentUser")
		    		ugi = UserGroupInformation.createProxyUser("mapredqa@DEV.YGRID.YAHOO.COM", UserGroupInformation.getCurrentUser());
		    	else
		    		ugi = UserGroupInformation.createProxyUser("mapredqa@DEV.YGRID.YAHOO.COM", UserGroupInformation.getLoginUser()); 
		    }
		    //tryCurrentUser    try { ugi = UserGroupInformation.createProxyUser("hadoopqa@DEV.YGRID.YAHOO.COM", UserGroupInformation.getLoginUser()); }
		    catch (Exception e) { 
		    	System.out.println("Failed, couldn't get UGI object for proxy user: " + e); }
		    }

		    //constructor - ugi to use is passed in, uses existing creds that come in with said ugi
		    DoasUser_cleanUgi_proxyUser (UserGroupInformation ugi) {
		    	doasCreds = ugi.getCredentials();
		    	this.ugi = ugi;
		    }

		    public void go(String type) throws Exception {
		    	_type = type;
		        // run as the original user
		    	ugi.doAs(new PrivilegedExceptionAction<String>() {
			          public String run() throws Exception {
	
			            // MUST instantiate objs here in the doas go() run method to get the proxy context
			            // might not have to be in the run method but just be in the go() method, need
			            // to try that...
			            doasConf = TestSession.cluster.getConf();
			            doasCluster = new Cluster(doasConf);
			            doasCreds = new Credentials();
			            doasFs = FileSystem.get(doasConf);
			            
			            // get RM and HDFS token within our proxy ugi context, these we should be able to use
			            // renewer is not valid, shouldn't matter now after 23.6 design change for renewer
			            Token<?> doasRmToken = doasCluster.getDelegationToken(new Text("GARBAGE1_mapredqa"));
	
			            doasCreds.addToken(new Text("MyDoasTokenAliasRM"), doasRmToken);
	
			            // TODO: should capture this list and iterate over it, not grab first element...
			            Token<?> doasHdfsToken = doasFs.addDelegationTokens("mapredqa", doasCreds)[0];
	
			            // let's see what we got...
			            System.out.println("doasRmToken: " + doasRmToken.getIdentifier());
			            System.out.println("doasRmToken kind: " + doasRmToken.getKind() + "\n");
			            System.out.println("doasHdfsToken: " + doasHdfsToken.getIdentifier());
			            System.out.println("doasHdfsToken kind: " + doasHdfsToken.getKind() + "\n");
	
			            // add creds to UGI, this adds the two RM tokens, the HDFS token was added already as part
			            // of the addDelegationTokens()
			            ugi.addCredentials(doasCreds);
			            System.out.println("From DoasProxyUser... my Creds say i'm: " + UserGroupInformation.getCurrentUser() + " and I have " + doasCreds.numberOfTokens() + " tokens");
	
			           
			            // setup and run a wordcount job, this should use the tokens passed into
			            // this doAs block, job should succeed
			            WordCountJob Job1 = new WordCountJob();
				  	     
			    	    if(_type == "webhdfs")
			        		Job1.setInputFile(webhdfsAddr);
			    	    else
			    			Job1.setInputFile(outputDir + localFile);
			    		Job1.setOutputPath(outputDir + outputFile +"/job" + Integer.toString(jobcount));
			    		jobcount++;
				   		 
				   		TestSession.logger.info("Trying to submit job1...");
				   		Job1.start();
				   		 
				   		assertTrue("Job1  was not assigned an ID within 10 seconds.", 
				   					Job1.waitForID(10));
				   		assertTrue("Job1 is invalid.", 
				   					Job1.verifyID());
			
				   		int waitTime = 2;
				   		assertTrue("Job1 did not succeed.",
				   					Job1.waitForSuccess(waitTime));
	
			            System.out.println("\nAfter doasUser first job... my Creds say i'm: " + UserGroupInformation.getCurrentUser() + " and I now have " + doasCreds.numberOfTokens() + " tokens");
			             
			            // setup and run another wordcount job, this should exceed the token renewal time of 60 seconds
			            // and cause all of our passed-in tokens to be renewed, job should also succeed
			            WordCountJob Job2 = new WordCountJob();
				  	     
			    	    if(_type == "webhdfs")
			        		Job2.setInputFile(webhdfsAddr);
			    	    else
			    			Job2.setInputFile(outputDir + localFile);
			    		Job2.setOutputPath(outputDir + outputFile +"/job" + Integer.toString(jobcount));
			    		jobcount++;
					   		 
					   	TestSession.logger.info("Trying to submit job2...");
					   	Job2.start();
					   		 
					   	assertTrue("Job2  was not assigned an ID within 10 seconds.", 
					   				Job2.waitForID(10));
					   	assertTrue("Job2 is invalid.", 
					   				Job2.verifyID());
				
					   	waitTime = 2;
					   	assertTrue("Job2 did not succeed.",
					   				Job2.waitForSuccess(waitTime));
	
			            System.out.println("\nAfter doasUser second job... my Creds say i'm: " + UserGroupInformation.getCurrentUser() + " and I now have " + doasCreds.numberOfTokens() + " tokens");
			            System.out.println("\nDump all tokens currently in our Credentials:");
			            System.out.println(ugi.getCredentials().getAllTokens() + "\n");
	
			            return "This is the doAs block";
		            }
		          });
		       }
		    }
}
	 