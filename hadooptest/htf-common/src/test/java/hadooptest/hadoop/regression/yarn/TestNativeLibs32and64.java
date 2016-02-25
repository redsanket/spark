package hadooptest.hadoop.regression.yarn;

// Tests to verify the success and fail cases when running jobs that explicitly load native and GPL
// libraries, for 32 and 64 bit libraries, in 32 and 64 bit JVM environments. The goal is to confirm 
// the libs and the environments align correctly, and when they do not, we see the errors we expect.
//
// In the success case we expect to see the libraries are correctly loaded, while in the fail cases 
// where the libs and JVMs are misaligned (32 with 64 bit, or 64 with 32bit), we expect to see specific 
// errors in the task logs. We must check the task logs for the job, the RM and NM logs do not have the
// messages we need.
//
// These cases are specific to 2.2.x, the logging behavior is very different for the success cases and
// for some of the GPL message strings between 0.23 and 2.2.x. These tests have been validated on the
// following hadoop release: hadoop-2.2.1.3.1312161930 (argentum, 20131217phw)

import hadooptest.TestSession;
import hadooptest.cluster.hadoop.HadoopCluster;
import hadooptest.cluster.hadoop.HadoopCluster.Action;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.workflow.hadoop.job.GenericJob;
import hadooptest.workflow.hadoop.job.JobState;
import hadooptest.node.hadoop.HadoopNode;
import hadooptest.automation.constants.HadooptestConstants;
//import hadooptest.hadoop.regression.yarn.capacityScheduler.CapacitySchedulerBaseClass;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Enumeration;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.File;

import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl; // H2.x, this test is 2.x only
import org.apache.hadoop.mapred.TaskReport;
import org.junit.Assert;
import org.junit.Assume; 
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Ignore;

public class TestNativeLibs32and64 extends TestSession {

    // job config for the native library tests
    private static final String[] PARAMS_JVM32_NATIVELIB32 = {
       "-Dyarn.app.mapreduce.am.env=JAVA_HOME=/home/gs/java/jdk32/current",
       "-Dmapreduce.map.env=JAVA_HOME=/home/gs/java/jdk32/current",
       "-Dmapreduce.reduce.env=JAVA_HOME=/home/gs/java/jdk32/current",
       "-Dmapreduce.admin.user.env=LD_LIBRARY_PATH=/home/gs/hadoop/current/lib/native/Linux-i386-32",
       "-Dyarn.app.mapreduce.am.admin.user.env=LD_LIBRARY_PATH=/home/gs/hadoop/current/lib/native/Linux-i386-32"
    };
    private static final String[] PARAMS_JVM32_NATIVELIB64 = {
       "-Dyarn.app.mapreduce.am.env=JAVA_HOME=/home/gs/java/jdk32/current",
       "-Dmapreduce.map.env=JAVA_HOME=/home/gs/java/jdk32/current",
       "-Dmapreduce.reduce.env=JAVA_HOME=/home/gs/java/jdk32/current",
       "-Dmapreduce.admin.user.env=LD_LIBRARY_PATH=/home/gs/hadoop/current/lib/native/Linux-amd64-64",
       "-Dyarn.app.mapreduce.am.admin.user.env=LD_LIBRARY_PATH=/home/gs/hadoop/current/lib/native/Linux-amd64-64"
    };
    private static final String[] PARAMS_JVM64_NATIVELIB32 = {
       "-Dyarn.app.mapreduce.am.env=JAVA_HOME=/home/gs/java/jdk64/current",
       "-Dmapreduce.map.env=JAVA_HOME=/home/gs/java/jdk64/current",
       "-Dmapreduce.reduce.env=JAVA_HOME=/home/gs/java/jdk64/current",
       "-Dmapreduce.admin.user.env=LD_LIBRARY_PATH=/home/gs/hadoop/current/lib/native/Linux-i386-32",
       "-Dyarn.app.mapreduce.am.admin.user.env=LD_LIBRARY_PATH=/home/gs/hadoop/current/lib/native/Linux-i386-32"
    };
    private static final String[] PARAMS_JVM64_NATIVELIB64 = {
		"-Dyarn.app.mapreduce.am.env=JAVA_HOME=/home/gs/java/jdk64/current",
		"-Dmapreduce.map.env=JAVA_HOME=/home/gs/java/jdk64/current",
		"-Dmapreduce.reduce.env=JAVA_HOME=/home/gs/java/jdk64/current",
		"-Dmapreduce.admin.user.env=LD_LIBRARY_PATH=/home/gs/hadoop/current/lib/native/Linux-amd64-64",
		"-Dyarn.app.mapreduce.am.admin.user.env=LD_LIBRARY_PATH=/home/gs/hadoop/current/lib/native/Linux-amd64-64"
	};
    private static final String[] PARAMS_JVMDEFAULT_NATIVELIBDEFAULT = {
        ""
     };
    
    // job configs for java7 and java8 64bit jobs, 64bit support does have a 'current' 64bit
    // symlink but explicit use of java7 or java8 requires setting this in the user job
    //
    // java7, note that the QE Flubber path and symlinks differ from production, production added
    // a new set of paths for 'java7 64' while Flubber still uses the existing 64bit paths
    //private static final String[] PARAMS_JAVA7_JVM64_NATIVELIB64 = {
    //            "-Dyarn.app.mapreduce.am.env=JAVA_HOME=/home/gs/java7/jdk64",
    //            "-Dmapreduce.map.env=JAVA_HOME=/home/gs/java7/jdk64",
    //            "-Dmapreduce.reduce.env=JAVA_HOME=/home/gs/java7/jdk64",
    //            "-Dmapreduce.admin.user.env=LD_LIBRARY_PATH=/home/gs/hadoop/current/lib/native/Linux-amd64-64",
    //            "-Dyarn.app.mapreduce.am.admin.user.env=LD_LIBRARY_PATH=/home/gs/hadoop/current/lib/native/Linux-amd64-64"
    //    };
    private static final String[] PARAMS_JAVA7_JVM64_NATIVELIB64 = {
                "-Dyarn.app.mapreduce.am.env=JAVA_HOME=/home/gs/java/jdk64/current",
                "-Dmapreduce.map.env=JAVA_HOME=/home/gs/java/jdk64/current",
                "-Dmapreduce.reduce.env=JAVA_HOME=/home/gs/java/jdk64/current",
                "-Dmapreduce.admin.user.env=LD_LIBRARY_PATH=/home/gs/hadoop/current/lib/native/Linux-amd64-64",
                "-Dyarn.app.mapreduce.am.admin.user.env=LD_LIBRARY_PATH=/home/gs/hadoop/current/lib/native/Linux-amd64-64"
        };
    // java8, QE Flubber and Production almost match for java8 64, Flubber uses an additional 'current' symlink
    // where prod does not, ie the prod path is '/home/gs/java8/jdk64'
    private static final String[] PARAMS_JAVA8_JVM64_NATIVELIB64 = {
                "-Dyarn.app.mapreduce.am.env=JAVA_HOME=/home/gs/java8/jdk64/current",
                "-Dmapreduce.map.env=JAVA_HOME=/home/gs/java8/jdk64/current",
                "-Dmapreduce.reduce.env=JAVA_HOME=/home/gs/java8/jdk64/current",
                "-Dmapreduce.admin.user.env=LD_LIBRARY_PATH=/home/gs/hadoop/current/lib/native/Linux-amd64-64",
                "-Dyarn.app.mapreduce.am.admin.user.env=LD_LIBRARY_PATH=/home/gs/hadoop/current/lib/native/Linux-amd64-64"
        };

    // utility settings
    private static final String[] PARAMS_FAILMODE = {
		"-Dyarn.app.mapreduce.am.job.node-blacklisting.enable=false"
	};
	
    private static final String[] PARAMS_DEBUG_ENABLE = {
		"-Dyarn.app.mapreduce.am.log.level=DEBUG",
		"-Dmapreduce.map.log.level=DEBUG",
		"-Dmapreduce.reduce.log.level=DEBUG"
	};
    private static final String[] PARAMS_HADOOPCMD_SLEEP = {
		"-m 1 -r 1 -mt 1000 -rt 1000"
    };

    private static String nm;
		
	@BeforeClass
	public static void startTestSession() throws Exception{
		TestSession.start();		

		setupTestConf();
	}

	public static void setupTestConf() throws Exception {
		FullyDistributedCluster cluster =
				(FullyDistributedCluster) TestSession.cluster;
		String component = HadoopCluster.NODEMANAGER;
		TestSession.logger.info("We have " +  cluster.getNodes(component).size() + " nodemanagers");
		if (cluster.getNodes(component).size() != 0) {
			//TestSession.logger.debug("Let's try to dump our NMs: "); 
			Hashtable<String,HadoopNode> nms = cluster.getNodes(component);
			Enumeration<String> e = nms.keys();
			
			// shut all the NMs down and then restart just one NM, to force task execution for 
			// predictable log analysis
			TestSession.logger.info("Shutting down all NMs...");
			cluster.hadoopDaemon(Action.STOP, component);
			
			// start one NM and check if it's up
			String nmToUse[] = { e.nextElement() };
			nm = nmToUse[0];
			TestSession.logger.info("Restart NM on: " + nmToUse[0]);			
			cluster.hadoopDaemon(Action.START, component, nmToUse);
			// TODO doesn't do what i expect, returns false unless all NMs are up
			if (cluster.isComponentUpOnSingleHost(component, nmToUse[0])) {
				TestSession.logger.info("NM on " + nmToUse[0] + " is up and running.");
			} else {
				TestSession.logger.error("NM on " + nmToUse[0] + " is not up!");
			}
		} else {
			TestSession.logger.error("No NodeManagers available!");
		}
				
		YarnClientImpl yarnClient = new YarnClientImpl();
		yarnClient.init(TestSession.getCluster().getConf());
		yarnClient.start();

                //
                // copy cap sched conf
                //
                String replacementConfigFile = TestSession.conf.getProperty("WORKSPACE") + "/htf-common/resources/hadooptest/" +
                      "hadoop/regression/yarn/capacityScheduler/capacity-scheduler_UserLimitFactor.xml";
                TestSession.logger.info("Copying over canned cap sched file localted @:" + replacementConfigFile);
                // Backup config and replace file, for Resource Manager
                cluster.getConf(HadooptestConstants.NodeTypes.RESOURCE_MANAGER).backupConfDir();
                cluster.getConf(HadooptestConstants.NodeTypes.RESOURCE_MANAGER).copyFileToConfDir(replacementConfigFile, "capacity-scheduler.xml");
                // Bounce node
                cluster.hadoopDaemon(Action.STOP, HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
                cluster.hadoopDaemon(Action.START, HadooptestConstants.NodeTypes.RESOURCE_MANAGER);

                Thread.sleep(10000);
      }

    
        //
	// individual test cases, mix of 32/64 JVM and Libs, plus the default case
        //

        // skip this test, 32bit JVMs no longer supported
        @Ignore
	@Test
        public void testNativeLibsJVM32Libs32() throws Exception{ testNativeLibsPos("testNativeLibsVM32Libs32", PARAMS_JVM32_NATIVELIB32, PARAMS_DEBUG_ENABLE); }

        // skip this test, 32bit JVMs no longer supported
        @Ignore
	@Test
        public void testNativeLibsJVM32Libs64() throws Exception{ testNativeLibsNeg("testNativeLibsVM32Libs64", PARAMS_JVM32_NATIVELIB64, PARAMS_DEBUG_ENABLE); }

	@Test public void testNativeLibsJVM64Libs32() throws Exception{ testNativeLibsNeg("testNativeLibsVM64Libs32", PARAMS_JVM64_NATIVELIB32, PARAMS_DEBUG_ENABLE); }
	@Test public void testNativeLibsJVM64Libs64() throws Exception{ testNativeLibsPos("testNativeLibsVM64Libs64", PARAMS_JVM64_NATIVELIB64, PARAMS_DEBUG_ENABLE); }
	@Test public void testNativeLibsDefault()    throws Exception{ testNativeLibsDefault("testNativeLibsDefault", PARAMS_JVMDEFAULT_NATIVELIBDEFAULT, PARAMS_DEBUG_ENABLE); }

        //
	// individual test cases, explicit use of java7/8 64bit
        //
        @Test public void testNativeLibsJAVA7JVM64Libs64() throws Exception{ testNativeLibsPos("testNativeLibsJava7VM64Libs64", PARAMS_JAVA7_JVM64_NATIVELIB64, PARAMS_DEBUG_ENABLE); }

        @Test public void testNativeLibsJAVA8JVM64Libs64() throws Exception{ 
                // only run java8 test if the cluster environment supports JDK8
                String JAVA8_PATH = "/home/gs/gridre/yroot." + TestSession.cluster.getClusterName() + "/share/yjava_jdk/java";
                TestSession.logger.info("check if exist: " + JAVA8_PATH); 
                File f = new File (JAVA8_PATH);
                org.junit.Assume.assumeTrue( f.exists() && f.isDirectory() );
                testNativeLibsPos("testNativeLibsJava8VM64Libs64", PARAMS_JAVA8_JVM64_NATIVELIB64, PARAMS_DEBUG_ENABLE); 
              }

	
	// test definition for positive cases, where JDK and Libs align, 32 bit or 64 bit
	protected void testNativeLibsPos(String testname, String[] PARAMS_JVM_NATIVELIB, 
			       String[] PARAMS_DEBUG_ENABLE) throws Exception {
		
		String[] TEST_JVM_AND_LIBS = PARAMS_JVM_NATIVELIB;
		String[] TEST_DEBUG_MODE = PARAMS_DEBUG_ENABLE;
		
		StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
		StackTraceElement e = stacktrace[2];//maybe this number needs to be corrected
		String methodName = e.getMethodName();
		TestSession.logger.info("Test Case: " + methodName);
		
		String testDesc=
            "Test use of 32/64 bit native libraries with 32/64 bit JDK, Positive Cases";
		TestSession.logger.debug(testDesc);

		// Submit a sleep job
		GenericJob job = new GenericJob();
        job.setJobJar(TestSession.cluster.getConf().getHadoopProp("HADOOP_TEST_JAR"));
        job.setJobName("sleep");
        
        ArrayList<String> jobArgs = new ArrayList<String>();
        jobArgs.addAll(Arrays.asList(TEST_JVM_AND_LIBS));
        jobArgs.addAll(Arrays.asList(TEST_DEBUG_MODE));
        jobArgs.addAll(Arrays.asList(PARAMS_HADOOPCMD_SLEEP));

		job.setJobArgs(jobArgs.toArray(new String[0]));
        job.start();
        job.waitForID(600);
        boolean result = job.waitFor(JobState.SUCCEEDED, 300);
		
		// check the resulting job's state
		if (result) {
			TestSession.logger.info("Job state is SUCCEEDED");
		} else {
			TestSession.logger.error("Failed to run Sleep job for 32/64bit NativeLibs positive test "
		       +testname);
			Assert.fail();
		}
		
		// get the TaskId, there should only be one mapper
		TaskReport[] taskreports = job.getMapTasksStatus();
		TestSession.logger.debug("TaskReport has " + taskreports.length + " elements");
		TestSession.logger.info("TaskReport element: " + taskreports[0].getTaskId());
		
		verifyTaskLog(nm, taskreports[0].getTaskID().toString(), "checksuccess");
	}
	
	// test definition for negative cases, where JDK and Libs do not align, 32/64 or 64/32
	protected void testNativeLibsNeg(String testname, String[] PARAMS_JVM_NATIVELIB, 
			       String[] PARAMS_DEBUG_ENABLE) throws Exception {
		
		String[] TEST_JVM_AND_LIBS = PARAMS_JVM_NATIVELIB;
		String[] TEST_DEBUG_MODE = PARAMS_DEBUG_ENABLE;
		
		StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
		StackTraceElement e = stacktrace[2];//maybe this number needs to be corrected
		String methodName = e.getMethodName();
		TestSession.logger.info("Test Case: " + methodName);
		
		String testDesc=
            "Test use of 32/64 bit native libraries with 32/64 bit JDK, Negative Cases";
		TestSession.logger.debug(testDesc);

		// Submit a sleep job
		GenericJob job = new GenericJob();
        job.setJobJar(TestSession.cluster.getConf().getHadoopProp("HADOOP_TEST_JAR"));
        job.setJobName("sleep");
        
        ArrayList<String> jobArgs = new ArrayList<String>();
        jobArgs.addAll(Arrays.asList(TEST_JVM_AND_LIBS));
        jobArgs.addAll(Arrays.asList(TEST_DEBUG_MODE));
        jobArgs.addAll(Arrays.asList(PARAMS_FAILMODE));
        jobArgs.addAll(Arrays.asList(PARAMS_HADOOPCMD_SLEEP));

		job.setJobArgs(jobArgs.toArray(new String[0]));
        job.start();
        job.waitForID(600);
        // fail status can take >10 minutes due to AM blacklisting, PARAMS_FAILMODE
        // should avoid this
		boolean result = job.waitFor(JobState.FAILED, 600);
		
		// check the resulting job's state, should fail
		if (result) {
			TestSession.logger.info("Job state is FAILED, as expected");
		} else {
			TestSession.logger.error("Unable to run Sleep job for 32/64bit NativeLibs negative test "
		       +testname);
			Assert.fail();
		}
	
		// get the TaskId, there will be multiple mapper attempts due to retry, but they 
		// all should show the lib load failures so we can check the first one's logs
		TaskReport[] taskreports = job.getMapTasksStatus();
		TestSession.logger.debug("TaskReport has " + taskreports.length + " elements");
		TestSession.logger.info("TaskReport element: " + taskreports[0].getTaskId());
		
		verifyTaskLog(nm, taskreports[0].getTaskId().toString(), "checkfail");	
	}
	
	// test definition for default case, where jdk and libs should default to 32 bits, and
	// no logging is generated as long as all loads and resolvers work
	protected void testNativeLibsDefault(String testname, String[] PARAMS_JVM_NATIVELIB, 
			       String[] PARAMS_DEBUG_ENABLE) throws Exception {
		
		String[] TEST_JVM_AND_LIBS = PARAMS_JVM_NATIVELIB;
		String[] TEST_DEBUG_MODE = PARAMS_DEBUG_ENABLE;
		
		StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
		StackTraceElement e = stacktrace[2];//maybe this number needs to be corrected
		String methodName = e.getMethodName();
		TestSession.logger.info("Test Case: " + methodName);
		
		String testDesc=
            "Test use of 32/64 bit native libraries with 32/64 bit JDK, Positive Cases";
		TestSession.logger.debug(testDesc);

		// Submit a sleep job
		GenericJob job = new GenericJob();
        job.setJobJar(TestSession.cluster.getConf().getHadoopProp("HADOOP_TEST_JAR"));
        job.setJobName("sleep");
        
        ArrayList<String> jobArgs = new ArrayList<String>();
        jobArgs.addAll(Arrays.asList(TEST_JVM_AND_LIBS));
        jobArgs.addAll(Arrays.asList(TEST_DEBUG_MODE));
        jobArgs.addAll(Arrays.asList(PARAMS_HADOOPCMD_SLEEP));

		job.setJobArgs(jobArgs.toArray(new String[0]));
        job.start();
        job.waitForID(600);
        boolean result = job.waitFor(JobState.SUCCEEDED, 300);
		
		// check the resulting job's state
		if (result) {
			TestSession.logger.info("Job state is SUCCEEDED");
		} else {
			TestSession.logger.error("Failed to run Sleep job for 32/64bit NativeLibs default test "
		       +testname);
			Assert.fail();
		}
		
		// get the TaskId, there should only be one mapper
		TaskReport[] taskreports = job.getMapTasksStatus();
		TestSession.logger.debug("TaskReport has " + taskreports.length + " elements");
		TestSession.logger.info("TaskReport element: " + taskreports[0].getTaskId());
		
		verifyTaskLog(nm, taskreports[0].getTaskId().toString(), "checksuccessdefault");	
	}
	
    // check the the NM's log for the appropriate messages where the libs were, or
    // were not, found and loaded
    // 
    // This takes the NM hostname, the taskid from the job, and the messages to check 
    // for (success or fail messages)
    protected void verifyTaskLog(String hostname, String taskid, String checkmode) throws Exception {
    		ArrayList<String> patterns = new ArrayList<String>();
            boolean foundpattern = false;
            
            // build up our task log path from the taskid we were given
            String appid = taskid.substring(5,23);
            // 2.6 added the possiblity of an epoch field in container name, need to account for it
            String logfiles = "/home/gs/var/log/mapredqa/userlogs/application_"+appid+"/container*_"+appid+"_01_000001/*";
            TestSession.logger.info("NM log path is: " + logfiles);
            
            // Load up the patterns we want to look for in the task's logs
            //
            // the success case's message(s)
            if (checkmode.equals("checksuccess")) {
            		 patterns.add("org.apache.hadoop.util.NativeCodeLoader: Loaded the native-hadoop library");
            // the fail case's error message(s)
            } else if (checkmode.equals("checkfail")){
            		patterns.add("org.apache.hadoop.util.NativeCodeLoader: Failed to load native-hadoop with error: java.lang.UnsatisfiedLinkError");
            		patterns.add("java.lang.RuntimeException: native-lzo library not available");
            // the default case's messages which we do not want to see in the logs
            } else if (checkmode.equals("checksuccessdefault")) {
        		patterns.add("org.apache.hadoop.util.NativeCodeLoader: Failed to load native-hadoop with error: java.lang.UnsatisfiedLinkError");
        		patterns.add("java.lang.RuntimeException: native-lzo library not available");
            }

            // the default case is special in 2.x, no logging is generated as long as no errors were
            // found trying to load libraries, so we have to look for the inverse case of no errors
            // when we want to checksuccessdefault
            if (!checkmode.equals("checksuccessdefault")) {
            	Iterator<String> itr = patterns.iterator();
            	while (itr.hasNext()) {
            		// check for each log pattern
            		TestSession.logger.debug("Verify check for each message in task log...");
            		foundpattern = grepPatternOnHost(itr.next(), logfiles, hostname);
            
            		// as long as we find each individual pattern, we are ok
            		if (!foundpattern){
            			TestSession.logger.error("NM log patterns were not found, VERIFY FAILED");
            			Assert.fail();
            		} else {
            			TestSession.logger.debug("NM log pattern was found");
            		}
            	}
            } else {
               	Iterator<String> itr = patterns.iterator();
            	while (itr.hasNext()) {
            		// default job, check for no errors
            		TestSession.logger.debug("Verify check for default job, look for no errors in task log...");
            		foundpattern = grepPatternOnHost(itr.next(), logfiles, hostname);
            
            		// as long as we do not find each individual pattern, we are ok
            		if (foundpattern){
            			TestSession.logger.error("NM log errors were found, VERIFY FAILED");
            			Assert.fail();
            		} else {
            			TestSession.logger.debug("NM error pattern was not found");
            		}
            	}
            }
    }
    
    // utility method for verification, this takes a pattern string, filename and hostname
    // and runs a remote grep for given pattern within the filename, on the remote host
    protected boolean grepPatternOnHost(String pattern, String filename, String hostname) 
    		throws Exception {
    	
    	boolean isFound = false;
    	
    	String[] pdshCmd = { HadooptestConstants.ShellCommand.PDSH, "-w", hostname };
        ArrayList<String> temp = new ArrayList<String>();
        
        String testCmd[] = { "/bin/grep", 
                        pattern, 
                        filename };
        temp.addAll(Arrays.asList(pdshCmd));
        temp.addAll(Arrays.asList(testCmd));
        String[] commandStrings = temp.toArray(new String[pdshCmd.length
                                    + testCmd.length]);
        String responseLine;
        Process p;
        try {
                p = Runtime.getRuntime().exec(commandStrings);
                p.waitFor();

                BufferedReader r = new BufferedReader(new InputStreamReader(
                        p.getInputStream()));
                while ((responseLine = r.readLine()) != null) {
                        TestSession.logger.info("RESPONSE_BEGIN:" + responseLine + "RESPONSE_END\n");
                        if (responseLine.contains(pattern)) {
                              isFound = true;
                                 break;
                        }
                }
        } catch (IOException e) {
                throw new RuntimeException(e);
        } catch (InterruptedException e) {
                e.printStackTrace();
        }
        
        return isFound;
    }
		
}
