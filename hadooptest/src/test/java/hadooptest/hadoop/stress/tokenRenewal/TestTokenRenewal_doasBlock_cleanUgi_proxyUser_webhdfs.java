package hadooptest.hadoop.stress.tokenRenewal;

import static org.junit.Assert.assertTrue;
import hadooptest.TestSession;
import hadooptest.workflow.hadoop.job.WordCountJob;

import java.io.File;
import java.security.PrivilegedExceptionAction;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.junit.BeforeClass;
import org.junit.Test;



public class TestTokenRenewal_doasBlock_cleanUgi_proxyUser_webhdfs extends TestSession {
	
	/****************************************************************
	 *  Please set up input and output directory and file name here *
	 ****************************************************************/
	// NOTE: the file should appear in you home directory
	private static String localFile = "TTR_input.txt";
	// NOTE: this is a directory and will appear in your home directory in the HDFS
	private static String outputFile = "TTR_output";
	// NOTE: this is the name node of your cluster that you currently test your code on
	private static String hdfsNode = "gsbl90628.blue.ygrid.yahoo.com";
	private static String webhdfsAddr;
	
	/****************************************************************
	 *          Please give the string for the input file           *
	 ****************************************************************/
	
	private static String input_string = "Hello world! Run doasBlock cleanUgi proxyUser webhfs!";
	
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
		
		outputDir = "/user/" + TestSession.conf.getProperty("USER") + "/"; 
		logger.info("Target Directory is: " + outputDir + localFile+ "Target File is: " + outputDir + outputFile);
		
	    webhdfsAddr = "webhdfs://" + hdfsNode + ":" + outputDir + localFile;
	    
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
	    DoasUser du = new DoasUser();
	    du.go();

	    // back to our original context, our two doAs jobs should have ran as the specified
	    // proxy user, dump our existing credentials 
	    System.out.println("Back from the doAs block to original context... my Creds say i'm: " + UserGroupInformation.getCurrentUser() + " and I now have " + creds.numberOfTokens() + " tokens");
	    System.out.println("\nDump all tokens currently in our Credentials:");
	    System.out.println(ugiOrig.getCredentials().getAllTokens() + "\n");

	}
	
	  // class DoasUser
	  // this is used create a new UGI (new security context) for running wordcount jobs
	  // using the credentials passed into said ugi
	public class DoasUser {
	    // DO NOT instantiate objects here, dummy, declare for scope but don't snag 'em yet
	    // gotta be in the doas run method to get proxy user's context

	    private Credentials doasCreds;
	    private Cluster doasCluster;
	    private Configuration doasConf;
	    private FileSystem doasFs;
	    private UserGroupInformation ugi;

	    // constructor - nothing is passed in, just use the doAs block to get 
	    // a new UGI (new security context)
	    DoasUser () throws Exception {
	      // DO NOT instantiate objects here either, dummy, gotta be in the doas run method to getƒ
	      // proxy user's context

	      // get a proxy UGI for hadoopqa, since no creds are passed in we have to get tokens
	      // using the TGT fallback for the login user
	      try { ugi = UserGroupInformation.createProxyUser("mapredqa@DEV.YGRID.YAHOO.COM", UserGroupInformation.getLoginUser()); 
//	    		System.out.println("!!!!!!!!!!!!!!!!!!!!!ugi info!!!!!!!!!!!!!!!!!!!\n" +  
//							ugi.getCredentials().getAllTokens() + "\n" +  
//							ugi.getCredentials().numberOfTokens() + "\n"+  
//							ugi.getCredentials().getAllTokens());
    		
	      }
	      //tryCurrentUser    try { ugi = UserGroupInformation.createProxyUser("hadoopqa@DEV.YGRID.YAHOO.COM", UserGroupInformation.getLoginUser()); }
	      catch (Exception e) { System.out.println("Failed, couldn't get UGI object for proxy user: " + e); }
	    }

	    //constructor - ugi to use is passed in, uses existing creds that come in with said ugi
	    DoasUser (UserGroupInformation ugi) {
	      doasCreds = ugi.getCredentials();

	      this.ugi = ugi;
	    }

	    public void go() throws Exception {

	        // run as the original user
	        String retVal = ugi.doAs(new PrivilegedExceptionAction<String>() {
	          public String run() throws Exception {

	            // MUST instantiate objs here in the doas go() run method to get the proxy context
	            // might not have to be in the run method but just be in the go() method, need
	            // to try that...
	            doasConf = TestSession.cluster.getConf();
	            doasCluster = new Cluster(doasConf);
	            doasCreds = new Credentials();
	            doasFs = FileSystem.get(doasConf);
	            
//	      		System.out.println("!!!!!!!!!!!!!!!!!!!!!Token!!!!!!!!!!!!!!!!!!!\n" +  	      				
//						"All tokens = " + ugi.getCredentials().getAllTokens() + "\n" +  
//						"Number of tokens = " + ugi.getCredentials().numberOfTokens() + "\n"+
//						"Login users = " + UserGroupInformation.getLoginUser() + "\n");

	            // get RM and HDFS token within our proxy ugi context, these we should be able to use
	            // renewer is not valid, shouldn't matter now after 23.6 design change for renewer
	            Token<?> doasRmToken = doasCluster.getDelegationToken(new Text("GARBAGE1_mapredqa"));
	            
//	      		System.out.println("!!!!!!!!!!!!!!!!!!!!!Token!!!!!!!!!!!!!!!!!!!\n" +  
//	      							"Encode to String = " + doasRmToken.encodeToUrlString() +   "\n" +  
//	      							"Token Kind = " + doasRmToken.getKind() + "\n"+  
//	      							"Ident = " + doasRmToken.getIdentifier() + "\n"+ 
//	      							"Get service = " + doasRmToken.getService());

	            doasCreds.addToken(new Text("MyDoasTokenAliasRM"), doasRmToken);

//	      		System.out.println("!!!!!!!!!!!!!!!!!!!!!ugi info0000!!!!!!!!!!!!!!!!!!!\n" +  
//	      							ugi.getCredentials().getAllTokens() + "\n" +  
//	      							ugi.getCredentials().numberOfTokens() + "\n"+  
//	      							ugi.getCredentials().getAllTokens());


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
		  	     
		   		Job1.setInputFile(webhdfsAddr);
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

	             System.out.println("\nAfter doasUser first job... my Creds say i'm: " + UserGroupInformation.getCurrentUser() + " and I now have " + doasCreds.numberOfTokens() + " tokens");
	             
	             // setup and run another wordcount job, this should exceed the token renewal time of 60 seconds
	             // and cause all of our passed-in tokens to be renewed, job should also succeed
	             WordCountJob Job2 = new WordCountJob();
		  	     
			   		Job2.setInputFile(webhdfsAddr);
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

	             System.out.println("\nAfter doasUser second job... my Creds say i'm: " + UserGroupInformation.getCurrentUser() + " and I now have " + doasCreds.numberOfTokens() + " tokens");
	             System.out.println("\nDump all tokens currently in our Credentials:");
	             System.out.println(ugi.getCredentials().getAllTokens() + "\n");

	             return "This is the doAs block";
	            }
	          });
	        
	       System.out.println(retVal);
	        // back out of the go() method, no longer running as the doAs proxy user

	        // write out our tokens back out of doas scope
	        //doasCreds.writeTokenStorageFile(new Path("/tmp/tokenfile_doas_out"), doasConf);
	       }

	    }
}
	 