// TokenRenewalTest_doasBlock_cleanUgi - acquire and attempt renew of HDFS and RM DT tokens 
// from a current user context but using a new remoteUser UGI to clear the security context.
// The token renewal should fail since renewers don't match, the tokens should be valid and
// can be passed to a doAs block where these tokens are the only credentials available to
// submit jobs. 
//
// 20130107 phw
// Run two wordcount jobs from within a doAs block, with no valid TGT and using tokens passed
// for job submission. Wordcounts are set to run slightly longer than the token renewal
// period so the second job relies on automatic renewal by the RM and NN to have valid
// tokens.
//
// Config needs: property 'yarn.resourcemanager.delegation.token.renew-interval' was
//               reduced to 60 (seconds) to force token renewal between jobs, actual
//               setting is in mSec so it's '60000'.
//
// Input needs: the wordcount needs some sizeable text input in hdfs at '/data/in'
//
// Output Expectations: token acquisition success with renewal failure, these creds are 
// added to the new ugi, this ugi is passed to a soad block where we try to run two wordcount
// jobs in succession. We should see teh same token behavior as we do in the normal (non doas)
// case, which that Rm and HDFS DTs are acquired, after job submission we automatically 
// get an MR (job history) DT token, in addition to having the existing acquired tokens.
// After a predefiend timeout, <10 minutes for RM and <1 minute for HDFS, the tokens are 
// automatically cancelled.

import java.io.*;
import java.util.*;
import java.net.*;
import java.security.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.*;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.security.*;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.security.*;
import org.apache.hadoop.security.token.*;
import org.apache.hadoop.security.token.delegation.*;
import org.apache.hadoop.tools.*;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;

public class TokenRenewalTest_doasBlock_cleanUgi extends Configured implements Tool {

  int failFlags = 10000; // test result init but not updated yet

  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(new TokenRenewalTest_doasBlock_cleanUgi(), args);
    System.exit(ret-10000); // if no errors, this will be 0
  }

  public int run(String [] args) throws Exception {

    Configuration conf = new Configuration();
    Cluster cluster = new Cluster(conf);
    DFSClient dfs = new DFSClient(conf);
    FileSystem fs = FileSystem.get(conf);

    // list out our config prop change, should be 60 (seconds)
    System.out.println("Check the renew property setting, yarn.resourcemanager.delegation.token.renew-interval: " + conf.get("yarn.resourcemanager.delegation.token.renew-interval"));
    // don't cancel our tokens so we can use them in second
    conf.setBoolean("mapreduce.job.complete.cancel.delegation.tokens", false);

    UserGroupInformation ugiOrig = UserGroupInformation.createRemoteUser("dfsload@DEV.YGRID.YAHOO.COM");
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
      failFlags=failFlags+1; // security failure for HDFS token
      System.out.println("FAILED! We were allowed to renew a token as ourselves when renewer is priv user.\nThe renewTimeHdfs we got back is: " + renewTimeHdfs);
    }

    System.out.println("\nOur first RM_DELEGATION_TOKEN: ");
    try { renewTimeRm = myTokenRm.renew(conf); }
    catch (Exception e) { System.out.println("Success, renew failed as expected since we're not the priv user"); }
    if (renewTimeRm > 1357252344100L) 
    {
      failFlags=failFlags+2; //security failure for RM token  
      System.out.println("FAILED! We were allowed to renew a token as ourselves when renewer is priv user.\nThe renewTimeRm we got back is:  " + renewTimeRm + "\n");
    }

    int numTokens = ugiOrig.getCredentials().numberOfTokens();
    System.out.println("We have a total of " + numTokens  + " tokens");
    System.out.println("Dump all tokens currently in our Credentials:");
    System.out.println(ugiOrig.getCredentials().getAllTokens() + "\n");

    // instantiate a seperate object to use for submitting jobs, using
    // our shiney new tokens
    DoasUser du = new DoasUser(ugiOrig);
    du.go();

    // back to our original context, our two doAs jobs should have ran as the specified
    // proxy user, dump our existing credentials 
    System.out.println("Back from the doAs block to original context... my Creds say i'm: " + UserGroupInformation.getCurrentUser() + " and I now have " + creds.numberOfTokens() + " tokens");
    System.out.println("\nDump all tokens currently in our Credentials:");
    System.out.println(ugiOrig.getCredentials().getAllTokens() + "\n");

    return(failFlags);
  }


  // 
  // class DoasUser
  // this is used create a new UGI (new security context) for running wordcount jobs
  // using the credentials passed into said ugi
  public class DoasUser {

    private Credentials doasCreds;
    private Configuration doasConf = new Configuration();
    UserGroupInformation ugi;

    //constructor - get creds passed in, need to create your own ugi
    DoasUser (Credentials creds) {
      doasCreds = creds;
 
      //try { ugi = UserGroupInformation.getLoginUser(); }
      //user remote....             try { ugi = UserGroupInformation.getCurrentUser(); }
      try { ugi = UserGroupInformation.createRemoteUser("patwhite"); }
      catch (Exception e) { System.out.println("Failed, couldn't get UGI object" + e); }

      // check for valid passed-in Cred object
      if (doasCreds == null) {
         System.out.println("\nTotal Bummer: No doasCreds dude...\n");
      } else {
 	 System.out.println("\tdoasCreds listing:\n" + doasCreds.getAllTokens());
      }
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
           // this fails with expired token before Tom's fix on top of Sid's token renewal patch in 23.6 
           ugi.addCredentials(doasCreds);
           System.out.println("From doasUser before running jobs...  my Creds say i'm: " + UserGroupInformation.getCurrentUser() + " and I have " + doasCreds.numberOfTokens() + " tokens");
         
           // write out our tokens in doas scope
           //Path tokenFile = new Path("/tmp/tokenfile_doas_in");
           //doasCreds.writeTokenStorageFile(new Path("/tmp/tokenfile_doas_in"), doasConf);
           //FileSystem doasFs = FileSystem.get(doasConf);
           //doasFs.setPermission(tokenFile, new FsPermission("777"));

           // setup and run a wordcount job, this should use the tokens passed into
           // this doAs block, job should succeed
           Job jobDoas = new Job(getConf());
           jobDoas.setJarByClass(TokenRenewalTest_doasBlock_cleanUgi.class);
           jobDoas.setJobName("TokenRenewalTest_doasBlock_cleanUgi_wordcountOrigUser_job1");

           jobDoas.setOutputKeyClass(Text.class);
           jobDoas.setOutputValueClass(IntWritable.class);

           jobDoas.setMapperClass(Map.class);
           jobDoas.setCombinerClass(Reduce.class);
           jobDoas.setReducerClass(Reduce.class);

           jobDoas.setInputFormatClass(TextInputFormat.class);
           jobDoas.setOutputFormatClass(TextOutputFormat.class);

           Path outpath = new Path("/tmp/outfoo");
           if ( outpath.getFileSystem(getConf()).isDirectory(outpath) ) {
             outpath.getFileSystem(getConf()).delete(outpath, true);
             System.out.println("Info: deleted output path: " + outpath );
           }
           FileInputFormat.setInputPaths(jobDoas, new Path("/data/in"));
           FileOutputFormat.setOutputPath(jobDoas, outpath);

           // submit the job, this should automatically get us a jobhistory token,
           // but does not seem to do so...
           jobDoas.submit();

           System.out.print("...wait while the doAs job runs.");
           while ( !jobDoas.isComplete() ) {
             System.out.print(".");
             Thread.sleep(2000);
           }
           if ( jobDoas.isSuccessful() ) {
               System.out.println("Job completion successful");
               // open perms on the output
               outpath.getFileSystem(getConf()).setPermission(outpath, new FsPermission("777"));
               outpath.getFileSystem(getConf()).setPermission(outpath.suffix("/part-r-00000"), new FsPermission("777"));
           } else {
               failFlags=failFlags+10; // first job failed
               System.out.println("Job failed");
           }

           System.out.println("After doasUser first job... my Creds say i'm: " + UserGroupInformation.getCurrentUser() + " and I now have " + doasCreds.numberOfTokens() + " tokens");

           
           // setup and run another wordcount job, this should exceed the token renewal time of 60 seconds
           // and cause all of our passed-in tokens to be renewed, job should also succeed
           Job jobDoas2 = new Job(getConf());
           jobDoas2.setJarByClass(TokenRenewalTest_doasBlock_cleanUgi.class);
           jobDoas2.setJobName("TokenRenewalTest_doasBlock_cleanUgi_wordcountDoasUser_job2");

           jobDoas2.setOutputKeyClass(Text.class);
           jobDoas2.setOutputValueClass(IntWritable.class);

           jobDoas2.setMapperClass(Map.class);
           jobDoas2.setCombinerClass(Reduce.class);
           jobDoas2.setReducerClass(Reduce.class);

           jobDoas2.setInputFormatClass(TextInputFormat.class);
           jobDoas2.setOutputFormatClass(TextOutputFormat.class);

           Path outpath2 = new Path("/tmp/outfoo2");
           if ( outpath2.getFileSystem(getConf()).isDirectory(outpath2) ) {
             outpath2.getFileSystem(getConf()).delete(outpath2, true);
             System.out.println("Info: deleted output path2: " + outpath2 );
           }
           FileInputFormat.setInputPaths(jobDoas2, new Path("/data/in"));
           FileOutputFormat.setOutputPath(jobDoas2, outpath2);

           // submit the second job, this should also automatically get us a 
           // jobhistory token, but doesn't...
           jobDoas2.submit();

           System.out.print("...wait while the second doAs job runs.");
           while ( !jobDoas2.isComplete() ) {
             System.out.print(".");
             Thread.sleep(2000);
           }
           if ( jobDoas2.isSuccessful() ) {
               System.out.println("Job 2 completion successful");
               // open perms on the output
               outpath2.getFileSystem(getConf()).setPermission(outpath2, new FsPermission("777"));
               outpath2.getFileSystem(getConf()).setPermission(outpath2.suffix("/part-r-00000"), new FsPermission("777"));
           } else {
               failFlags=failFlags+20; // second job failed
               System.out.println("Job 2 failed");
           }

           System.out.println("After doasUser second job... my Creds say i'm: " + UserGroupInformation.getCurrentUser() + " and I now have " + doasCreds.numberOfTokens() + " tokens");
           System.out.println("\nDump all tokens currently in our Credentials:");
           System.out.println(ugi.getCredentials().getAllTokens() + "\n");

           return "This is the doAs block";
          }
        });
      // back out of the go() method, no longer running as the doAs proxy user

      // write out our tokens back out of doas scope
      //doasCreds.writeTokenStorageFile(new Path("/tmp/tokenfile_doas_out"), doasConf);
     }

  }

  // Mapper and Reducer classes for the wordcount job
  public static class Map
      extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);
      while (tokenizer.hasMoreTokens()) {
        word.set(tokenizer.nextToken());
        context.write(word, one);
      }
    }
  }
  public static class Reduce
      extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values,
        Context context) throws IOException, InterruptedException {

      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      context.write(key, new IntWritable(sum));
    }
  }

}
