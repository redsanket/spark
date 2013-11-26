// ARtest.java
// tweaked version of 24hour test for AR, run jobs that each submit two wordcounts, job1
// uses 100 files, job2 uses 200 files to generate variability in runtime
//
// usage: hadoop --config cfg jar run24hour_job.jar run24hour_job HOURS_TO_RUN
//          where HOURS_TO_RUN can be from 1 hour to 200, times are arbitrary, no reason we
//          can't increase as long as we tweak the time math
// example: 
//      /home/gs/gridre/yroot.elrond/share/hadoop/bin/hadoop \
//      --config /home/gs/gridre/yroot.elrond/conf/hadoop  jar run24hour_job.jar run24hour_job 1
//
// this relies on the external class 'genericWordcount.java' to actually setup input data and run 
// a wordcount job
// 
// expected operation is that you launch this guy and it just runs wordcount jobs with 60 sec
// sleeps inbetween and it completes once enough time is elapsed. A regression failure of the 
// original bug would be jobs run fine until the TGT renew period is reached and then new 
// submissions fail.
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

public class ARtest {

   public static void main(String[] args) throws Exception {
     boolean testResult = false;

     //check for input file count 
     if (args.length != 2) {
       System.err.println("Error: bad args count: " + args.length + "\nUsage: ARtest <input file count> <queue name>");
       System.exit(-1);
     }
     // needs to be between 1 and 1,000,000 files
     int infileCount = Integer.parseInt(args[0]);
     if ((infileCount < 1) || (infileCount > 1000000)) {
       System.err.println("Error: infileCount needs to be between 1 minimum and 1,000,000 files maximum");
       System.exit(-1);
     }
     // set the queue name to submit jobs to
     String queueName=args[1];

     System.out.println("=====================================================================================");
     System.out.println("ARtest.java: submit jobs for specified number of files");
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
    boolean runjobStat = false;
    int jobCounter = 0;  // keep a dumb little wordcount job counter
    genericWordcount gwc = new genericWordcount(); 
    runjobStat = gwc.runGenericWordcount(infileCount, queueName);
    if (runjobStat != true) {
      System.err.println("FAIL: attempt to run a job reported fail");
      // failing test
      testResult = false;
    } else {
      System.out.println("INFO: attempt to run generic wordcount job returns: " + runjobStat);
      jobCounter++;
    }
    // passing test
    testResult = true;

    System.out.println("\nTest completed. Result is: " + testResult);
    System.out.println("Test completed " + jobCounter + " wordcount jobs\n");
  }

}

