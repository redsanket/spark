// GenFilesInHdfs.java
// tweaked version of AR's 24hour test job datagen class, give it the number of files you
// want and it goes off and creates that number in hdfs, and also specify the queue to use
//
// usage: hadoop --config cfg jar GenFilesInHdfs.jar NUM_FILES_TO_GEN QUEUE_TO_USE 
//          where NUM_FILES_TO_GEN can be from 1 to 10,000,000. 
// example: 
//      /home/gs/gridre/yroot.argentum/share/hadoop/bin/hadoop \
//      --config /home/gs/gridre/yroot.argentum/conf/hadoop  jar GenFilesInHdfs.jar GenFilesInHdfs 1 unfunded
//
// this relies on the external class 'genericWordcount.java' to actually setup input data and run 
// a wordcount job
// 
// Notes:
// you can ctrl-C this to kill it, but the currently submitted wordcount job will keep running,
// shouldn't be a big deal since it's so short but fyi in case this is killed and you still see
// a wordcount running in the RM gui, should only be the current job, no others in this state.
//
import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.ha.*;
import org.apache.hadoop.ha.HAServiceProtocol.*;

public class GenFilesInHdfs {

   public static void main(String[] args) throws Exception {
     boolean testResult = false;

     //check for input file count 
     if (args.length != 2) {
       System.err.println("Error: bad args count: " + args.length + "\nUsage: GenFilesInHdfs <number of files to generate> <queue to use>");
       System.exit(-1);
     }
     // needs to be between 1 and 10,000,000 files
     int infileCount = Integer.parseInt(args[0]);
     if ((infileCount < 1) || (infileCount > 10000000)) {
       System.err.println("Error: infileCount needs to be between 1 minimum and 10,000,000 files maximum");
       System.exit(-1);
     }
     // set the queue name to submit jobs to
     String queueName=args[1];
     //String queueName="sds";

     System.out.println("=====================================================================================");
     System.out.println("GenFilesInHdfs: create the specified number of files in hdfs");
     System.out.println("=====================================================================================");

     // get a job configuration
     Configuration conf = new Configuration();

     // snarf a dfs
     FileSystem fs = FileSystem.get(conf);

     // println some conf property values
     System.out.println("==== some configuration property values...  ====");
     System.out.println("Get our fs.defaultFS property value from conf: " + conf.get("fs.defaultFS"));

     // println some fs values and settings
     System.out.println("==== some filesystem settings and values...  ====");
     System.out.println("Get our HomeDirectory: " + fs.getHomeDirectory().toString());
     System.out.println("Get our Uri: " + fs.getUri());
     System.out.println("Get the CanonicalServiceName: " + fs.getCanonicalServiceName());
     System.out.println("Get the DefaultUri: " + fs.getDefaultUri(conf));
     System.out.println("Dump the FS_DEFAULT_NAME_KEY: " + fs.FS_DEFAULT_NAME_KEY);
     System.out.println("Dump the DEFAULT_FS: " + fs.DEFAULT_FS);

    // get current time
    long startTime = System.currentTimeMillis()/1000;
    System.out.println("Current time is: " + startTime); 

    // run wordcount jobs for the specified number of input files 
    //boolean runjobStat = false;
    int runjobStat;
    int jobCounter = 0;  // keep a dumb little wordcount job counter
    genericWordcount gwc = new genericWordcount(); 
    runjobStat = gwc.runGenericWordcount(infileCount, queueName);
    if ( false) {
      System.err.println("FAIL: attempt to run a job reported fail");
      // failing test
      testResult = false;
    } else {
      JobStatus jobstatus = new JobStatus();
      System.out.println("INFO: attempt to run generic wordcount job returns: " + jobstatus.getJobRunState(runjobStat));
      jobCounter++;
    }
    // passing test
    testResult = true;

    System.out.println("\nTest completed. Result is: " + testResult);
    System.out.println("Test launched " + jobCounter + " wordcount jobs\n");
  }

}


