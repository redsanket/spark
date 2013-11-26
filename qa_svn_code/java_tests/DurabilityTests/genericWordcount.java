import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.*;

// 
// class genericWordcount
// a fairly self-contained class that you can call to run a simple wordcount job,
// this uses the old (mapred) API which is the primary flavor of job code run
// still.
//
// inputs: none, but assumes there is accessible input data in dfs at "/data/in"
//
// outputs: boolean pass/fail result for the job, output is generated in 
// dfs at "/tmp/outfoo" but it's not really used for anything other than a
// quick data validity check to help determine job pass/fail 
// 
public class genericWordcount {
   boolean retStatus = false;
 
   public boolean runGenericWordcount() throws Exception {
     JobConf conf = new JobConf(genericWordcount.class);
     JobClient jobclient = new JobClient(conf);
     conf.setJarByClass(genericWordcount.class);
     conf.setJobName("genericWordcount_job");

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
     Path inpath = new Path("/data/in");
     FileInputFormat.setInputPaths(conf, inpath);
     FileOutputFormat.setOutputPath(conf, outpath);

     // generate some input data
     boolean gendataStat = false;
     GenData gendata = new GenData();
     gendataStat = gendata.generate(fs, inpath);
     System.out.println("INFO: gendata returns: " + gendataStat);

     System.out.println("Trying to submit genericWordcount job...");
     RunningJob runningjob1 = jobclient.submitJob(conf);

     System.out.print("...wait while genericWordcount job runs.");
     while ( !runningjob1.isComplete() ) {
       System.out.print(".");
       Thread.sleep(5000);
     }
     if ( runningjob1.isSuccessful() ) {
         System.out.println("Job completion successful");
         // open perms on the output
         outpath.getFileSystem(conf).setPermission(outpath, new FsPermission("777"));
         outpath.getFileSystem(conf).setPermission(outpath.suffix("/part-00000"), new FsPermission("777"));
         retStatus = true;
     } else {
         System.out.println("WARN: genericWordcount job failed");
         // we failed
         retStatus = false;
     }

     // report our overall result
     return retStatus;
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

  // 
  // class GenData
  // generate some input data for the wordcount job
  //
  public class GenData {
    FileSystem myFs;
    Path myInPath;

    boolean generate(FileSystem fs, Path inpath) {

      myFs = fs;
      myInPath = inpath;
      // check if path exists and if so rm it 
      try {
        if ( myFs.isDirectory(inpath) ) {
          myFs.delete(inpath, true);
          System.out.println("INFO: deleted input path: " + inpath );
        }
      }
      catch (Exception e) {
          System.err.println("FAIL: can not remove the input path, can't run wordcount jobs. Exception is: " + e);
      }
      // make the input dir
      try {
        if ( myFs.mkdirs(inpath) ) {
          System.out.println("INFO: created input path: " + inpath );
        }
      }
      catch (Exception e) {
          System.err.println("FAIL: can not create the input path, can't run wordcount jobs. Exception is: " + e);
      }
 
      // create 100 new files at the new path and gen some data into each
      int wrcount = 0;
      int fileNum;
      for (fileNum=1; fileNum<=100; fileNum++) {
        Path infile = new Path(inpath.toString()+"/infile"+Integer.toString(fileNum));
        //System.out.println("DEBUG: THE PATH IS:  " + infile);
        try {
          FSDataOutputStream dostream = new FSDataOutputStream(myFs.create(myFs, infile, new FsPermission("644"))); 
          String s = "this is a string of words that can be counted which is just swell and keen and how about that sandwich";
          dostream.writeChars(s);
          dostream.flush();
          dostream.close();
        } catch (IOException ioe) {
          System.err.println("FAIL: can't create input file for wordcount: " + ioe);
        }
      }
      ////System.out.println("INFO: wrote " + wrcount + " chars to file " + dostream);

      return true;

    }
  }

}

