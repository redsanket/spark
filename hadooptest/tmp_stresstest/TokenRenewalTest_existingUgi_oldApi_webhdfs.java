// TokenRenewalTest_existingUgi_oldApi - acquire, renew and use HDFS and RM DT tokens as a 
// normal user. A goal of this test is to use the same approach Core recommended to set the
// correct renewer, and implemented in Oozie 3.3.1.0.1301022110. 
//
// Using the old mapred API (JobCient, JobConf, JobRunner) since new API does not get
// jobhistory tokens implicitly, and there does not seem to be a way to request one, and
// also this is what Oozie is using.
//
// Oozie reference code: 
// http://svn.apache.org/viewvc/oozie/trunk/core/src/main/java/org/apache/oozie/action/hadoop/ \
//        JavaActionExecutor.java?revision=1427979&view=markup
//
// 20130103 phw
// Run a wordcount job using RM and HDFS delegation tokens that are explicitly gotten by the 
// current user. Try to renew the tokens before the job, confirm they fail renewal since renewer
// does not match current user and then run the job
//
// Config needs: property 'yarn.resourcemanager.delegation.token.renew-interval' was
//               reduced to 60 (seconds) to force token renewal between jobs, actual
//               setting is in mSec so it's '60000'.
//
// Input needs: the wordcount needs some sizeable text input in hdfs at '/data/in'
//
// Output Expectations: token acquisition success with renewal failure, job run using acquired
//                      credentials is successful, job output perm changes success
//

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.security.*;
import org.apache.hadoop.security.token.*;
import org.apache.hadoop.security.token.delegation.*;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;

 	
public class TokenRenewalTest_existingUgi_oldApi_webhdfs {

   public static void main(String[] args) throws Exception {
     int failFlags = 10000; // test result init but not updated yet
     JobConf conf = new JobConf(TokenRenewalTest_existingUgi_oldApi_webhdfs.class);
     JobClient jobclient = new JobClient(conf);
     conf.setJarByClass(TokenRenewalTest_existingUgi_oldApi_webhdfs.class);
     conf.setJobName("TokenRenewalTest_existingUgi_oldApi_webhdfs_job1");

     FileSystem fs = FileSystem.get(conf);

     conf.setOutputKeyClass(Text.class);
     conf.setOutputValueClass(IntWritable.class);

     conf.setMapperClass(Map.class);
     conf.setCombinerClass(Reduce.class);
     conf.setReducerClass(Reduce.class);

     conf.setInputFormat(TextInputFormat.class);
     conf.setOutputFormat(TextOutputFormat.class);

     Path outpath = new Path("/tmp/outfoo");
     if ( outpath.getFileSystem(conf).isDirectory(outpath) ) {
       outpath.getFileSystem(conf).delete(outpath, true);
       System.out.println("Info: deleted output path: " + outpath );
     }
     FileInputFormat.setInputPaths(conf, new Path("webhdfs://gsbl90565/data/in"));
     FileOutputFormat.setOutputPath(conf, outpath);

     // list out our config prop change, should be 60 (seconds)
     System.out.println("Check the renew property setting, yarn.resourcemanager.delegation.token.renew-interval: " + conf.get("yarn.resourcemanager.delegation.token.renew-interval"));

     // don't cancel out tokens so we can use them in job2
     conf.setBoolean("mapreduce.job.complete.cancel.delegation.tokens", false);

     // get dt with RM priv user as renewer
     Token<DelegationTokenIdentifier> mrdt = jobclient.getDelegationToken(new Text("mapredqa"));
     conf.getCredentials().addToken(new Text("MR_TOKEN"), mrdt);
     // get dt with HDFS priv user as renewer
     Token<? extends TokenIdentifier> hdfsdt = fs.getDelegationToken("mapredqa");
     conf.getCredentials().addToken(new Text("HDFS_TOKEN"), hdfsdt);

     System.out.println("mrdt: " + mrdt.getIdentifier());
     System.out.println("mrdt kind: " + mrdt.getKind());
     //private method        System.out.println("mrdt Renewer: " + mrdt.getRenewer() + "\n");
     System.out.println("mrdt isManaged: " + mrdt.isManaged());
     System.out.println("mrdt URL safe string is: " + mrdt.encodeToUrlString() + "\n");
     System.out.println("hdfsdt: " + hdfsdt.getIdentifier());
     System.out.println("hdfsdt kind: " + hdfsdt.getKind());
     //private method        System.out.println("hdfsdt Renewer: " + hdfsdt.getRenewer() + "\n");
     System.out.println("hdfsdt isManaged: " + hdfsdt.isManaged());
     System.out.println("hdfsdt URL safe string is: " + hdfsdt.encodeToUrlString() + "\n");

     // we have 2 tokens now, 1 HDFS_DELEGATION_TOKEN and 1 RM_DELEGATION_TOKEN
     // This should fail, let's try to renew as ourselves 
     long renewTimeHdfs = 0, renewTimeRm = 0;
     System.out.println("\nLet's try to renew our tokens...");
     System.out.println("First our HDFS_DELEGATION_TOKEN: ");
     try { renewTimeHdfs = hdfsdt.renew(conf); }
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

     int numTokens = conf.getCredentials().numberOfTokens();
     System.out.println("We have a total of " + numTokens  + " tokens");
     System.out.println("Dump all tokens currently in our Credentials:");
     System.out.println(conf.getCredentials().getAllTokens() + "\n");

     System.out.println("Trying to submit job1...");
     RunningJob runningjob1 = jobclient.submitJob(conf);
     // odd, runJob() does not get a jobhistory (MR_DELEGATION_TOKEN) token...
     //jobclient.runJob(conf);

     System.out.print("...wait while job1 runs.");
     while ( !runningjob1.isComplete() ) {
       System.out.print(".");
       Thread.sleep(2000);
     }
     if ( runningjob1.isSuccessful() ) {
         System.out.println("Job completion successful");
         // open perms on the output
         outpath.getFileSystem(conf).setPermission(outpath, new FsPermission("777"));
         outpath.getFileSystem(conf).setPermission(outpath.suffix("/part-00000"), new FsPermission("777"));
     } else {
         failFlags=failFlags+10; // first job failed
         System.out.println("Job job1 failed");
     }

     if (numTokens != conf.getCredentials().numberOfTokens()) {
         System.out.println("\nWARNING: number of tokens before and after job submission differs, had " + numTokens + " now have " + conf.getCredentials().numberOfTokens());
     }
     System.out.println("We have a total of " + conf.getCredentials().numberOfTokens() + " tokens");
     System.out.println("\nDump all tokens currently in our Credentials:");
     System.out.println(conf.getCredentials().getAllTokens() + "\n");

     // run a second job which should use the existing tokens, should see renewals 
     // happen at 0.80*60 seconds
     conf.setJobName("TokenRenewalTest_existingUgi_oldApi_webhdfs_job2");
     if ( outpath.getFileSystem(conf).isDirectory(outpath) ) {
       outpath.getFileSystem(conf).delete(outpath, true);
       System.out.println("Info: deleted output path: " + outpath );
     }
     System.out.println("\nTrying to submit job2...");
     RunningJob runningjob2 = jobclient.submitJob(conf);

     System.out.print("...wait while job2 runs.");
     while ( !runningjob2.isComplete() ) {
       System.out.print(".");
       Thread.sleep(2000);
     }
     if ( runningjob2.isSuccessful() ) {
         System.out.println("Job completion successful");
         // open perms on the output
         outpath.getFileSystem(conf).setPermission(outpath, new FsPermission("777"));
         outpath.getFileSystem(conf).setPermission(outpath.suffix("/part-00000"), new FsPermission("777"));
     } else {
         failFlags=failFlags+20; // second job failed
         System.out.println("Job job2 failed");
     }
     if (numTokens != conf.getCredentials().numberOfTokens()) {
         System.out.println("\nWARNING: number of tokens before and after job submission differs, had " + numTokens + " now have " + conf.getCredentials().numberOfTokens());
     }
     System.out.println("We have a total of " + conf.getCredentials().numberOfTokens() + " tokens");
     System.out.println("\nDump all tokens currently in our Credentials:");
     System.out.println(conf.getCredentials().getAllTokens() + "\n");

     System.exit(failFlags-10000); // if no errors, this will be 0
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

