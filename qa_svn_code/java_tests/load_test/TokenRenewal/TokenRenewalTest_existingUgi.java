// TokenRenewalTest_existingUgi - acquire, renew and use HDFS and RM DT tokens as a 
// normal user. A goal of this test is to use the same approach Core recommended to set the
// correct renewer, and implemented in Oozie 3.3.1.0.1301022110. 
//
// 20130104 phw
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
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.security.*;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.security.*;
import org.apache.hadoop.security.token.*;
import org.apache.hadoop.security.token.delegation.*;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;

 	
public class TokenRenewalTest_existingUgi {

   public static void main(String[] args) throws Exception {
     int failFlags = 10000; // test result init but not updated yet
     Credentials creds = new Credentials();
     Configuration conf = new Configuration();
     Job job = new Job(conf);
     Cluster cluster = new Cluster(conf);
     FileSystem fs = FileSystem.get(conf);

     job.setJarByClass(TokenRenewalTest_existingUgi.class);
     job.setJobName("TokenRenewalTest_existingUgi_job1");

     job.setOutputKeyClass(Text.class);
     job.setOutputValueClass(IntWritable.class);

     job.setMapperClass(Map.class);
     job.setCombinerClass(Reduce.class);
     job.setReducerClass(Reduce.class);

     job.setInputFormatClass(TextInputFormat.class);
     job.setOutputFormatClass(TextOutputFormat.class);

     Path outpath = new Path("/tmp/outfoo");
     if ( outpath.getFileSystem(conf).isDirectory(outpath) ) {
       outpath.getFileSystem(conf).delete(outpath, true);
       System.out.println("Info: deleted output path: " + outpath );
     }
     FileInputFormat.setInputPaths(job, new Path("/data/in"));
     FileOutputFormat.setOutputPath(job, outpath);

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

     int numTokens = creds.numberOfTokens();
     System.out.println("We have a total of " + numTokens  + " tokens");
     System.out.println("Dump all tokens currently in our Credentials:");
     System.out.println(creds.getAllTokens() + "\n");

     System.out.println("Trying to submit job1...");
     job.submit();

     System.out.print("...wait while job1 runs.");
     while ( !job.isComplete() ) {
       System.out.print(".");
       Thread.sleep(2000);
     }
     if ( job.isSuccessful() ) {
         System.out.println("Job completion successful");
         // open perms on the output
         outpath.getFileSystem(conf).setPermission(outpath, new FsPermission("777"));
         outpath.getFileSystem(conf).setPermission(outpath.suffix("/part-r-00000"), new FsPermission("777"));
     } else {
         failFlags=failFlags+10; // first job failed
         System.out.println("Job job1 failed");
     }

     if (numTokens != creds.numberOfTokens()) {
         System.out.println("\nWARNING: number of tokens before and after job submission differs, had " + numTokens + " now have " + creds.numberOfTokens());
     }
     System.out.println("After job1, we have a total of " + creds.numberOfTokens() + " tokens");
     System.out.println("\nDump all tokens currently in our Credentials:");
     System.out.println(creds.getAllTokens() + "\n");


     // run a second job which should use the existing tokens, should see renewals 
     // happen at 0.80*60 seconds
     Configuration conf2 = new Configuration();
     Job job2 = new Job(conf2);

     job2.setJarByClass(TokenRenewalTest_existingUgi.class);
     job2.setJobName("TokenRenewalTest_existingUgi_job2");

     job2.setOutputKeyClass(Text.class);
     job2.setOutputValueClass(IntWritable.class);

     job2.setMapperClass(Map.class);
     job2.setCombinerClass(Reduce.class);
     job2.setReducerClass(Reduce.class);

     job2.setInputFormatClass(TextInputFormat.class);
     job2.setOutputFormatClass(TextOutputFormat.class);

     Path outpath2 = new Path("/tmp/outfoo2");
     if ( outpath2.getFileSystem(conf2).isDirectory(outpath2) ) {
       outpath2.getFileSystem(conf2).delete(outpath2, true);
       System.out.println("Info: deleted output path: " + outpath2 );
     }
     FileInputFormat.setInputPaths(job2, new Path("/data/in"));
     FileOutputFormat.setOutputPath(job2, outpath2);

     System.out.println("Trying to submit job2...");
     job2.submit();

     System.out.print("...wait while job2 runs.");
     while ( !job2.isComplete() ) {
       System.out.print(".");
       Thread.sleep(2000);
     }
     if ( job2.isSuccessful() ) {
         System.out.println("Job completion successful");
         // open perms on the output
         outpath.getFileSystem(conf2).setPermission(outpath, new FsPermission("777"));
         outpath.getFileSystem(conf2).setPermission(outpath.suffix("/part-r-00000"), new FsPermission("777"));
     } else {
         failFlags=failFlags+20; // second job failed
         System.out.println("Job job2 failed");
     }

     if (numTokens != creds.numberOfTokens()) {
         System.out.println("\nWARNING: number of tokens before and after job submission differs, had " + numTokens + " now have " + creds.numberOfTokens());
     }
     System.out.println("After job2, we have a total of " + creds.numberOfTokens() + " tokens");
     System.out.println("\nDump all tokens currently in our Credentials:");
     System.out.println(creds.getAllTokens() + "\n");

    System.exit(failFlags-10000); // if no errors, this will be 0
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

