// TokenRenewalTest_doasBlock_cleanUgi_oldApi - acquire and attempt renew of HDFS and RM DT tokens 
// from a current user context but using a new remoteUser UGI to clear the security context.
// The token renewal should fail since renewers don't match, the tokens should be valid and
// can be passed to a doAs block where these tokens are the only credentials available to
// submit jobs. 
// 
// This is using the old v1 style Job APIs, JobClient, JobConf, JobRunner. Behavior is
// different than with the newer APIs, like Job, one note is that the older API auto
// fetches a jobhistory token where the new does not seem to do so.
//
// 20130108 phw
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
import java.security.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.*;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.security.*;
import org.apache.hadoop.security.token.*;
import org.apache.hadoop.security.token.delegation.*;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;

public class TokenRenewalTest_doasBlock_cleanUgi_oldApi_webhdfs extends Configured implements Tool {

  int failFlags = 10000; // test result init but not updated yet

  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(new TokenRenewalTest_doasBlock_cleanUgi_oldApi_webhdfs(), args);
    System.exit(ret-10000); // if no errors, this will be 0
  }

  public int run(String [] args) throws Exception {

    JobConf conf = new JobConf(TokenRenewalTest_doasBlock_cleanUgi_oldApi_webhdfs.class);
    JobClient jobclient = new JobClient(conf);
    DFSClient dfs = new DFSClient(conf);
    FileSystem fs = FileSystem.get(conf);


    // list out our config prop change, should be 60 (seconds)
    System.out.println("Check the renew property setting, yarn.resourcemanager.delegation.token.renew-interval: " + conf.get("yarn.resourcemanager.delegation.token.renew-interval"));
    // don't cancel our tokens so we can use them in second
    conf.setBoolean("mapreduce.job.complete.cancel.delegation.tokens", false);

    UserGroupInformation ugiOrig = UserGroupInformation.createRemoteUser("dfsload@DEV.YGRID.YAHOO.COM");
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
      failFlags=failFlags+1; // security failure for HDFS token
      System.out.println("FAILED! We were allowed to renew a token as ourselves when renewer is priv user.\nThe renewTimeHdfs we got back is: " + renewTimeHdfs);
    }

     System.out.println("\nAnd our RM_DELEGATION_TOKEN: ");
     try { renewTimeRm = mrdt.renew(conf); }
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
           System.out.println("From doasUser before running jobs...  my Creds say i'm: " + UserGroupInformation.getCurrentUser() + " and I have " + doasCreds.numberOfTokens() + " tokens");

           // setup and run a wordcount job, this should use the tokens passed into
           // this doAs block, job should succeed
           JobConf doasConf = new JobConf(TokenRenewalTest_doasBlock_cleanUgi_oldApi_webhdfs.class);
           JobClient doasJobclient = new JobClient(doasConf);

           doasConf.setJarByClass(TokenRenewalTest_doasBlock_cleanUgi_oldApi_webhdfs.class);
           doasConf.setJobName("TokenRenewalTest_doasBlock_cleanUgi_oldApi_webhdfs_wordcountDoasUser_job1");

           doasConf.setOutputKeyClass(Text.class);
           doasConf.setOutputValueClass(IntWritable.class);

           doasConf.setMapperClass(Map.class);
           doasConf.setCombinerClass(Reduce.class);
           doasConf.setReducerClass(Reduce.class);

           doasConf.setInputFormat(TextInputFormat.class);
           doasConf.setOutputFormat(TextOutputFormat.class);

           Path outpath = new Path("/tmp/outfoo");
           if ( outpath.getFileSystem(getConf()).isDirectory(outpath) ) {
             outpath.getFileSystem(getConf()).delete(outpath, true);
             System.out.println("Info: deleted output path: " + outpath );
           }
           FileInputFormat.setInputPaths(doasConf, new Path("/data/in"));
           FileOutputFormat.setOutputPath(doasConf, outpath);

           // submit the job, this should automatically get us a jobhistory token,
           // but does not seem to do so...
           System.out.println("\nTrying to submit doAs job1...");
           RunningJob runningjob1 = doasJobclient.submitJob(doasConf);


           System.out.print("...wait while the doAs job1 runs.");
           while ( !runningjob1.isComplete() ) {
             System.out.print(".");
             Thread.sleep(2000);
           }
           if ( runningjob1.isSuccessful() ) {
               System.out.println("Job completion successful");
               // open perms on the output
               outpath.getFileSystem(doasConf).setPermission(outpath, new FsPermission("777"));
               outpath.getFileSystem(doasConf).setPermission(outpath.suffix("/part-00000"), new FsPermission("777"));
           } else {
               failFlags=failFlags+10; // first job failed
               System.out.println("Job  job1 failed");
           }

           System.out.println("After doasUser first job... my Creds say i'm: " + UserGroupInformation.getCurrentUser() + " and I now have " + doasCreds.numberOfTokens() + " tokens");


           // setup and run another wordcount job, this should exceed the token renewal time of 60 seconds
           // and cause all of our passed-in tokens to be renewed, job should also succeed
           JobConf doasConf2 = new JobConf(TokenRenewalTest_doasBlock_cleanUgi_oldApi_webhdfs.class);
           JobClient doasJobclient2 = new JobClient(doasConf2);

           doasConf.setJarByClass(TokenRenewalTest_doasBlock_cleanUgi_oldApi_webhdfs.class);
           doasConf.setJobName("TokenRenewalTest_doasBlock_cleanUgi_oldApi_webhdfs_wordcountDoasUser_job2");

           doasConf.setOutputKeyClass(Text.class);
           doasConf.setOutputValueClass(IntWritable.class);

           doasConf.setMapperClass(Map.class);
           doasConf.setCombinerClass(Reduce.class);
           doasConf.setReducerClass(Reduce.class);

           doasConf.setInputFormat(TextInputFormat.class);
           doasConf.setOutputFormat(TextOutputFormat.class);

           Path outpath2 = new Path("/tmp/outfoo2");
           if ( outpath2.getFileSystem(getConf()).isDirectory(outpath2) ) {
             outpath2.getFileSystem(getConf()).delete(outpath2, true);
             System.out.println("Info: deleted output path: " + outpath2 );
           }
           FileInputFormat.setInputPaths(doasConf2, new Path("/data/in"));
           FileOutputFormat.setOutputPath(doasConf2, outpath2);

           // submit the job, this should automatically get us a jobhistory token,
           // but does not seem to do so...
           System.out.println("\nTrying to submit doAs job2...");
           RunningJob runningjob2 = doasJobclient2.submitJob(doasConf2);

           System.out.print("...wait while the doAs job2 runs.");
           while ( !runningjob2.isComplete() ) {
             System.out.print(".");
             Thread.sleep(2000);
           }
           if ( runningjob2.isSuccessful() ) {
               System.out.println("Job completion successful");
               // open perms on the output
               outpath2.getFileSystem(doasConf).setPermission(outpath2, new FsPermission("777"));
               outpath2.getFileSystem(doasConf).setPermission(outpath2.suffix("/part-00000"), new FsPermission("777"));
           } else {
               failFlags=failFlags+20; // second job failed
               System.out.println("Job  job2 failed");
           }

           System.out.println("After doasUser second job... my Creds say i'm: " + UserGroupInformation.getCurrentUser() + " and I now have " + doasCreds.numberOfTokens() + " tokens");

           return "This is the doAs block";
          }
        });
      // back out of the go() method, no longer running as the doAs proxy user

      // write out our tokens back out of doas scope
      //doasCreds.writeTokenStorageFile(new Path("/tmp/tokenfile_doas_out"), doasConf);
     }

  }

    //
    // the mapper and reducer classes
    // this are as-is from the original v1 wordcount example code
    //
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
      private final static IntWritable one = new IntWritable(1);
      private Text word = new Text();
 
      public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
          word.set(tokenizer.nextToken());
          output.collect(word, one);
        }
      }
    }
    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
      public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        int sum = 0;
        while (values.hasNext()) {
          sum += values.next().get();
        }
        output.collect(key, new IntWritable(sum));
      }
    }

}
