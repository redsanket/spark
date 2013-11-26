// run24hour_job
// this is a durability test in general but specifically checks for regression from bug 4994229 aka
// the "AB token bug". This issue wasn't tokens per se but rather TGT renewal, we weren't renewing
// a user's TGT so once we got within 0.80*KerbTgtExpire, new user job submissions would fail.
// 13 July 2012 - phw - generic java for hadoop .23 
// 16 Jan 2013 - phw - cleanup, fixing, tweaks and break the wc job to an external class
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

public class run24hour_job {

   public static void main(String[] args) throws Exception {
     boolean testResult = true;
     boolean runningOk = true;

     //check for how long to run, in hours
     if (args.length != 1) {
       System.err.println("Error: bad args count: " + args.length + "\nUsage: run24hour_job <hours_to_run>");
       System.exit(-1);
     }
     // needs to be between 1 and 200 hours
     int timeToRun = Integer.parseInt(args[0]);
     if ((timeToRun < 1) || (timeToRun > 200)) {
       System.err.println("Error: timeToRun needs to be between 1 hour minimum and 200 hours maximum");
       System.exit(-1);
     }
     // get user's desired runtime in seconds, timeToRun x 60 mins x 60 secs
     long endTime = (System.currentTimeMillis()/1000)+(timeToRun*60*60);

     System.out.println("=====================================================================================");
     System.out.println("run24hour_job.java: long duration job that submits jobs for specified number of hours");
     System.out.println("                    this is a regression testcase for the AB \"TGT renewal issue\"");
     System.out.println("=====================================================================================");

     System.out.println("INFO: timeToRun is " + timeToRun + " hours\n");
     System.out.println("INFO: endTime is going to be " + endTime + " seconds\n");

     // get a job configuration
     Configuration conf = new Configuration();

     // snarf a dfs
     FileSystem fs = FileSystem.get(conf);

     // println some conf property values
     System.out.println("==== some configuration property values...  ====");
     System.out.println("Get our fs.defaultFS property value from conf: " + conf.get("fs.defaultFS"));
     System.out.println("Get our dfs.datanode.failed.volumes.tolerated property value from conf: " + conf.get("dfs.datanode.failed.volumes.tolerated"));
     System.out.println("Get our yarn.nodemanager.disk-health-checker.min-healthy-disksproperty value from conf: " + conf.get("yarn.nodemanager.disk-health-checker.min-healthy-disks"));

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

    // run short wordcount jobs for the specified length of time
    boolean runjobStat = false;
    int jobCounter = 0;  // keep a dumb little wordcount job counter
    genericWordcount gwc = new genericWordcount(); 
    while (!isTimeUp(endTime) && runningOk) {
      runjobStat = gwc.runGenericWordcount();
      if (runjobStat != true) {
        System.err.println("FAIL: attempt to run a job reported fail");
        // failing test
        runningOk = false;
        testResult = false;
       } else {
        System.out.println("INFO: attempt to run generic wordcount job returns: " + runjobStat);
      }
      jobCounter++;
    }

    System.out.println("\nTest ran for " + (endTime - startTime) + " seconds");
    System.out.println("\nTest completed " + jobCounter + " wordcount jobs");
    System.out.println("\nTest completed. Result is: " + testResult);
  }

  // quickie check if we've run long enough
  // inputs: endTime, need the target end time
  // output; bool, true if we've exceeded the target time
  public static boolean isTimeUp(long endTime) {
    boolean checkTimeUp = false;
    long currentTime = System.currentTimeMillis()/1000;

    if (currentTime > endTime) {
      // yup
      checkTimeUp = true;
    } else {
      // nope
      checkTimeUp = false;
    }

    System.out.println("endTime is: " + endTime);
    System.out.println("currentTime is: " + currentTime);
    System.out.println("\n Diff of time remaining is: " + ((-1)*(currentTime - endTime)) + " seconds");

    return checkTimeUp;
  }

}

