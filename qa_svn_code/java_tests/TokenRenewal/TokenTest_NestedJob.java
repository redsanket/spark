// TokenTest_NestedJob.java
// token testcase: launch a job which launches another job from its mapper 
//
// - configure and setup a job from main
// - obtain an RM token and add it to existing ugi's credentials 
// - submit initial job, using the map task 'JobLaunchMapper'
// - 'JobLaunchMapper' in turn configures and submits a second job, using another
//    map task 'NopMapper'
// - 'NopMapper' is a do-nothing mapper simply to ensure the inner job can
//    complete tasks
//
//  Need to be sure there is at least one input file in the first job's input path
//  otherwise the mapper is not called and the job will succeed by not having
//  anything to do.
//
//  Note that the number of lines (tuples) in the first job's input (/tmp/in1) will
//  determine the number of JobLaunchMapper invocations because the map task is called
//  for each tuple. Probably want to set the input to a single file with 3 or 4 lines,
//  to generate that number of inner job launches.
//
// usage: hadoop --config <hadoop conf>  jar TokenTest_NestedJob.jar TokenTest_NestedJob 
//
//
// Token info; here's the tokens that were obtained by the Outer and Inner jobs, and
// their tasks:
//
// Outer job, AM has these tokens:
//     MR_DELEGATION_TOKEN (job history)
//     RM_DELEGATION_TOKEN (RM comm)
//     HDFS_DELEGATION_TOKEN (hdfs comm)
//     YARN_APPLICATION_TOKEN (RM token used by AM to talk to RM)
//
// Outer job, Task (ie outer mapper) has these tokens:
//     MR_DELEGATION_TOKEN (job history)
//     mapreduce.job (job submission umbilical)
//     RM_DELEGATION_TOKEN (RM comm)
//     HDFS_DELEGATION_TOKEN (hdfs comm)
//
// Inner job, AM has these tokens:
//     MR_DELEGATION_TOKEN (job history)
//     mapreduce.job (job submission umbilical)
//     RM_DELEGATION_TOKEN (RM comm)
//     HDFS_DELEGATION_TOKEN (hdfs comm)
//     YARN_APPLICATION_TOKEN (RM token used by AM to talk to RM)
//
// Inner job, Task (ie inner mapper) has these tokens:
//     MR_DELEGATION_TOKEN (job history)
//     mapreduce.job (job submission umbilical)
//     RM_DELEGATION_TOKEN (RM comm)
//     HDFS_DELEGATION_TOKEN (hdfs comm)
//
// 20130517phw

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.*;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.*;
import org.apache.hadoop.security.token.*;
import org.apache.hadoop.security.token.delegation.*;

public class TokenTest_NestedJob extends Configured {

   // first map task, used be the initial job launched in main, this configs and launches its
   // own job, "NestedMapJob"
   public static class JobLaunchMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
     public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
         throws IOException {

       System.out.println("JobLaunchMapper: in the mapper");

       JobConf conf2 = new JobConf();
       conf2.setJarByClass(TokenTest_NestedJob.class);
       conf2.setJobName("NestedMapJob");

       conf2.setOutputKeyClass(Text.class);
       conf2.setOutputValueClass(IntWritable.class);

       conf2.setMapperClass(NopMapper.class);

       // check for output dir2 and delete it if there
       FileSystem fs2 = FileSystem.get(conf2);
       Path out2 = new Path("/tmp/out2");
       if ( fs2.isDirectory(out2))  {
         fs2.delete(out2, true);
         System.out.println("Info: deleted output path: " + out2 );
       }
       FileInputFormat.setInputPaths(conf2, new Path("/tmp/in2"));
       FileOutputFormat.setOutputPath(conf2, new Path("/tmp/out2"));

       // submit the second (the inner) job
       JobClient.runJob(conf2);

       System.out.println("JobLaunchMapper: leaving mapper, bye bye");
     }
   }
   // second mapper, a do-nothing map task to ensure the second job can complete tasks
   public static class NopMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
     public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
         throws IOException {

       System.out.println("NopMapper: in the mapper");
       System.out.println("NopMapper: leaving mapper, that's all folks");

     }
   }


   // main, configs and launches the first job, with some utility work to make sure
   // the out path is gone, gets an RM token and adds it to credentials for use by
   // the second job, lists all its creds before the first job submit 
   public static void main(String[] args) throws Exception {

     JobConf conf = new JobConf();
     JobClient jobclient = new JobClient(conf);
     conf.setJarByClass(TokenTest_NestedJob.class);
     conf.setJobName("OriginalUserJob");

     conf.setOutputKeyClass(Text.class);
     conf.setOutputValueClass(IntWritable.class);

     conf.setMapperClass(JobLaunchMapper.class);

     // need this? is this is the correct property?
     // don't cancel our tokens so we can use them in second
     conf.setBoolean("mapreduce.job.complete.cancel.delegation.tokens", false);

     // check for output dir and delete it if there
     FileSystem fs = FileSystem.get(conf);
     Path out = new Path("/tmp/out1");
     if ( fs.isDirectory(out))  {
       fs.delete(out, true);
       System.out.println("Info: deleted output path: " + out );
     }

     FileInputFormat.setInputPaths(conf, new Path("/tmp/in1"));
     FileOutputFormat.setOutputPath(conf, out);

     Credentials creds = new Credentials();
     // get an RM token and add it to our creds
     Token<? extends TokenIdentifier> RmToken = jobclient.getDelegationToken(new Text("RmRenewerShouldNotMatterNow")) ;
     // must add token to the conf object in order to pass it into the inner job
     conf.getCredentials().addToken(new Text("MyRmToken"), RmToken);

     // let's see what we got for credentials...
     System.out.println("RmToken: " + RmToken.getIdentifier());
     System.out.println("RmToken kind: " + RmToken.getKind() + "\n");
     System.out.println("\tDump of my creds:\n" + creds.getAllTokens());

     // submit the first (the outer) job
     jobclient.runJob(conf);
   }
}

