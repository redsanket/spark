/*
 * YAHOO!
 */

package hadooptest.regression.yarn;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import hadooptest.config.testconfig.PseudoDistributedConfiguration;
import hadooptest.cluster.pseudodistributed.PseudoDistributedCluster;

/*
 * A test suite used to exercise the ability to kill tasks.
 */
public class MapredKillTask {

	private String jobID = "0";
	private static PseudoDistributedConfiguration conf;
	private static PseudoDistributedCluster cluster;
	
	/******************* CLASS BEFORE/AFTER ***********************/
	
	/*
	 * Configuration and cluster setup that should happen before running any of the tests in the class instance.
	 */
	@BeforeClass
	public static void startCluster() throws FileNotFoundException, IOException {
		
		conf = new PseudoDistributedConfiguration();
		conf.set("mapreduce.map.maxattempts", "4");
		conf.set("mapreduce.reduce.maxattempts", "4");
		conf.write();

		cluster = new PseudoDistributedCluster(conf);
		cluster.start();
	}
	
	/*
	 * Cluster cleanup that should happen after running tests in the class instance.
	 */
	@AfterClass
	public static void stopCluster() throws IOException {
		cluster.stop();
		conf.cleanup();
	}
	
	/******************* TEST BEFORE/AFTER ***********************/
	
	/*
	 * Before each test, we much initialize the sleep job and verify that its job ID is valid.
	 */
	@Before
	public void initTestJob() {
		this.jobID = this.submitSleepJob();
		assertTrue("Sleep job ID is invalid.", 
				this.verifyJobID(this.jobID));
	}
	
	/*
	 * After each test, we must reset the state of the cluster to a known default state.
	 */
	@After
	public void resetClusterState() {
		if (this.killJob(this.jobID) && this.jobID != "0") {
			System.out.println("Cleaned up latent job by killing it: " + this.jobID);
		}
		else {
			System.out.println("Job was already killed or never started, no need to clean up: " + this.jobID);
		}
		
		this.jobID = "0";
	}
	
	/******************* TESTS ***********************/
	
	/*
	 * A test which attempts to kill a running task from a sleep job.
	 */
	@Test
	public void killRunningTask() {		
		String taskID = this.getMapTaskAttemptID(this.jobID);
		assertTrue("Killed task message doesn't exist, we weren't able to kill the task.", 
				this.killTaskAttempt(taskID));
	}
	
	/*
	 * A test which attempts to kill a task from a sleep job which has already been killed.
	 */
	@Test
	public void killTaskOfAlreadyKilledJob() {
		
		assertTrue("Was not able to kill the job.", 
				this.killJob(this.jobID));
		
		String taskID = this.getMapTaskAttemptID(this.jobID);
		assertTrue("Killed task message doesn't exist, we weren't able to kill the task.", 
				this.killTaskAttempt(taskID));
	}
	
	/*
	 * A test which attempts to kill a task from a sleep job which has failed.
	 */
	@Test
	public void killTaskOfAlreadyFailedJob() {
		
		assertTrue("Was not able to fail the job.", 
				this.failJob(this.jobID));
		
		String taskID = this.getMapTaskAttemptID(this.jobID);
		assertTrue("Killed task message doesn't exist, we weren't able to kill the task.", 
				this.killTaskAttempt(taskID));
	}
	
	/*
	 * A test which attempts to kill a task from a sleep job which has already succeeded.
	 */
	@Test
	public void killTaskOfAlreadyCompletedJob() {
		
		assertTrue("Job did not succeed.", 
				this.verifyJobSuccess(this.jobID));
		
		String taskID = this.getMapTaskAttemptID(this.jobID);
		assertTrue("Killed task message doesn't exist, we weren't able to kill the task.", 
				this.killTaskAttempt(taskID));
	}
	
	/******************* END TESTS ***********************/
	
	/******************* CONVENIENCE METHODS *********************/
	
	/*
	 * Submits a default sleep job.
	 * 
	 * @return String The ID of the sleep job.
	 */
	private String submitSleepJob() {
		return submitSleepJob(10, 10, 50000, 50000, 1);
	}
	
	/*
	 * Submits a sleep job.
	 * 
	 * @return String The ID of the sleep job.
	 */
	private String submitSleepJob(int m_param, int r_param, int mt_param, int rt_param, int numJobs) {			
		Process hadoopProc = null;
		String jobID = "";
		//String taskAttemptID = "";
		
		String hadoop_version = "0.23.4"; // should come from prop variable in fw conf
		String hadoop_install = "/Users/rbernota/workspace/eclipse/branch-0.23.4/hadoop-dist/target/hadoop-0.23.4"; // this should come from env $HADOOP_INSTALL or prop variable in fw conf
		String hadoop_mapred_test_jar = hadoop_install + "/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-" + hadoop_version + "-tests.jar";
		String artifacts_dir = "/Users/rbernota/workspace/artifacts";
		String hadoop_conf_dir = "/Users/rbernota/workspace/hadoop/test/pseudodistributed_configs/test/";
		String hadoop_exe = hadoop_install + "/bin/hadoop";
		String user = "rbernota"; // not sure where this should go... probably in fw conf for now, but possibly extract from system.
		
		for (int i = 0; i < numJobs; i++) {			
			String hadoopCmd = hadoop_exe + " --config " + hadoop_conf_dir 
					+ " jar " + hadoop_mapred_test_jar 
					+ " sleep -Dmapreduce.job.user.name=" + user 
					+ " -m " + m_param 
					+ " -r " + r_param 
					+ " -mt " + mt_param 
					+ " -rt " + rt_param
					+ " > " + artifacts_dir + "/sleep." + i + ".log";
			
			System.out.println("COMMAND: " + hadoop_exe + hadoopCmd);
			
			String jobPatternStr = " - Running job: (.*)$";
			Pattern jobPattern = Pattern.compile(jobPatternStr);
			
			//String mapTaskPatternStr = " - Starting task: (.*)$";
			//Pattern mapTaskPattern = Pattern.compile(mapTaskPatternStr);
			
			try {
				hadoopProc = Runtime.getRuntime().exec(hadoopCmd);
				//hadoopProc.waitFor();
				BufferedReader reader=new BufferedReader(new InputStreamReader(hadoopProc.getInputStream())); 
				String line=reader.readLine(); 
				while(line!=null) 
				{ 
					System.out.println(line); 
					
					Matcher jobMatcher = jobPattern.matcher(line);
					//Matcher mapTaskMatcher = mapTaskPattern.matcher(line);
					
					if (jobMatcher.find()) {
						jobID = jobMatcher.group(1);
						System.out.println("JOB ID: " + jobID);
						break;
					}
					//else if (mapTaskMatcher.find()) {
					//	taskAttemptID = mapTaskMatcher.group(1);
					//	System.out.println("TASK ATTEMPT ID: " + taskAttemptID);
					//	break;
					//}
					
					line=reader.readLine();
				} 
			}
			catch (Exception e) {
				if (hadoopProc != null) {
					hadoopProc.destroy();
				}
				e.printStackTrace();
			}
		}

		//this.mapTaskID = taskAttemptID;
		
		return jobID;
	}
	
	/*
	 * Verifies a job ID is a valid ID for the expected format.
	 * 
	 * @param jobID The ID of the job to verify.
	 * @return boolean Whether the job is valid or not.
	 */
	private boolean verifyJobID(String jobID) {
		if (jobID == "0") {
			System.out.println("JOB ID DID NOT MATCH FORMAT AND WAS ZERO");
			return false;
		}

		String jobPatternStr = "job_(.*)$";
		Pattern jobPattern = Pattern.compile(jobPatternStr);
		
		Matcher jobMatcher = jobPattern.matcher(jobID);
		
		if (jobMatcher.find()) {
			jobID = jobMatcher.group(1);
			System.out.println("JOB ID MATCHED EXPECTED FORMAT");
			System.out.println("JOB ID: " + jobID);
			return true;
		}
		else {
			System.out.println("JOB ID DID NOT MATCH FORMAT");
			return false;
		}
	}
	
	private boolean verifyJobSuccess(String jobID) {
		// Runs Hadoop to check for the SUCCEEDED state of the job 
		
		// check for job success here
		Process mapredProc = null;
		
		String hadoop_install = "/Users/rbernota/workspace/eclipse/branch-0.23.4/hadoop-dist/target/hadoop-0.23.4"; // this should come from env $HADOOP_INSTALL or prop variable in fw conf
		String hadoop_conf_dir = "/Users/rbernota/workspace/hadoop/test/pseudodistributed_configs/test/";
		String mapred_exe = hadoop_install + "/bin/mapred";
		
		String mapredCmd = mapred_exe + " --config " + hadoop_conf_dir + " job -status " + jobID;
		
		System.out.println(mapredCmd);

		String mapredPatternStrSuccess = "(.*)(Job state: SUCCEEDED)(.*)";
		Pattern mapredPatternSuccess = Pattern.compile(mapredPatternStrSuccess);
		
		String mapredPatternStrFailed = "(.*)(Job state: FAILED)(.*)";
		Pattern mapredPatternFailed = Pattern.compile(mapredPatternStrFailed);
		
		String mapredPatternStrKilled = "(.*)(Job state: KILLED)(.*)";
		Pattern mapredPatternKilled = Pattern.compile(mapredPatternStrKilled);

		String mapredPatternStrPrep = "(.*)(Job state: PREP)(.*)";
		Pattern mapredPatternPrep = Pattern.compile(mapredPatternStrPrep);

		String mapredPatternStrRunning = "(.*)(Job state: RUNNING)(.*)";
		Pattern mapredPatternRunning = Pattern.compile(mapredPatternStrRunning);

		// Give the sleep job 5 minutes to complete
		for (int i = 0; i <= 50; i++) {
		
			try {
				mapredProc = Runtime.getRuntime().exec(mapredCmd);
				BufferedReader reader=new BufferedReader(new InputStreamReader(mapredProc.getInputStream())); 
				String line=reader.readLine(); 
				while(line!=null) 
				{ 
					System.out.println(line); 

					Matcher mapredMatcherSuccess = mapredPatternSuccess.matcher(line);
					Matcher mapredMatcherFailed = mapredPatternFailed.matcher(line);
					Matcher mapredMatcherKilled = mapredPatternKilled.matcher(line);
					Matcher mapredMatcherPrep = mapredPatternPrep.matcher(line);
					Matcher mapredMatcherRunning = mapredPatternRunning.matcher(line);

					if (mapredMatcherSuccess.find()) {
						System.out.println("JOB " + jobID + " SUCCEEDED");
						return true;
					}
					else if (mapredMatcherFailed.find()) {
						System.out.println("JOB " + jobID + " FAILED");
						return false;
					}
					else if (mapredMatcherKilled.find()) {
						System.out.println("JOB " + jobID + " WAS KILLED");
						return false;
					}
					else if (mapredMatcherPrep.find()) {
						System.out.println("JOB " + jobID + " IS STILL IN PREP STATE");
					}
					else if (mapredMatcherRunning.find()) {
						System.out.println("JOB " + jobID + " IS STILL RUNNING");
					}

					line=reader.readLine();
				} 
			}
			catch (Exception e) {
				if (mapredProc != null) {
					mapredProc.destroy();
				}
				e.printStackTrace();
			}

			try {
				Thread.currentThread().sleep(10000);
			}
			catch (InterruptedException ie) {
				System.out.println("Couldn't sleep the current Thread.");
			}
		}

		System.out.println("JOB " + jobID + " didn't SUCCEED within the 5 minute timeout window.");
		return false;
	}
	
	/*
	 * Get the task attempt ID associated with the specified job ID.
	 * 
	 * @param jobID The ID of the job to associate with the task attempt.
	 * @return String The ID of the task attempt.
	 */
	private String getMapTaskAttemptID(String jobID) {
		// Get the task attempt ID given a job ID
		String taskID = "0"; //should get the real taskID here
		
		String taskIDExtractStr = "(job)(.*)";
		Pattern taskIDPattern = Pattern.compile(taskIDExtractStr);
		
		Matcher taskIDMatcher = taskIDPattern.matcher(jobID);
		
		while (taskIDMatcher.find()) {
			taskID = "attempt" + taskIDMatcher.group(2) + "_m_00000_0";
		}
		
		//local myAttemptId1=$(echo $myjobId |sed 's/job/attempt/g'|sed 's/$/_m_00000_0/g') 
		
		System.out.println("MAP TASK ID = " + taskID);
		
		return taskID;
	}
	
	private String getReduceTaskAttemptID(String jobID) {
		// Get the task attempt ID given a job ID
		String taskID = "0"; //should get the real taskID here
		
		String taskIDExtractStr = "(job)(.*)";
		Pattern taskIDPattern = Pattern.compile(taskIDExtractStr);
		
		Matcher taskIDMatcher = taskIDPattern.matcher(jobID);
		
		while (taskIDMatcher.find()) {
			taskID = "attempt" + taskIDMatcher.group(2) + "_r_00000_0";
		}
		
		//local myAttemptId1=$(echo $myjobId |sed 's/job/attempt/g'|sed 's/$/_m_00000_0/g') 
		
		System.out.println("REDUCE TASK ID = " + taskID);
		
		return taskID;
	}
	
	/*
	 * Kills the task attempt associated with the specified task ID.
	 * 
	 * @param jobID The ID of the job matching the task attempt.
	 * @return boolean Whether the task attempt was killed or not.
	 */
	private boolean killTaskAttempt(String taskID) {
		
		Process mapredProc = null;
		
		String hadoop_install = "/Users/rbernota/workspace/eclipse/branch-0.23.4/hadoop-dist/target/hadoop-0.23.4"; // this should come from env $HADOOP_INSTALL or prop variable in fw conf
		String hadoop_conf_dir = "/Users/rbernota/workspace/hadoop/test/pseudodistributed_configs/test/";
		String mapred_exe = hadoop_install + "/bin/mapred";
		
		String mapredCmd = mapred_exe + " --config " + hadoop_conf_dir + " job -kill-task " + taskID;
		
		System.out.println(mapredCmd);

		String mapredPatternStr = "(.*)(Killed task " + taskID + ")(.*)";
		Pattern mapredPattern = Pattern.compile(mapredPatternStr);
		
		try {
			mapredProc = Runtime.getRuntime().exec(mapredCmd);
			BufferedReader reader=new BufferedReader(new InputStreamReader(mapredProc.getInputStream())); 
			String line=reader.readLine(); 
			while(line!=null) 
			{ 
				System.out.println(line); 
				
				Matcher mapredMatcher = mapredPattern.matcher(line);
				
				if (mapredMatcher.find()) {
					System.out.println("TASK ATTEMPT " + taskID + " WAS KILLED");
					return true;
				}
				
				line=reader.readLine();
			} 
		}
		catch (Exception e) {
			if (mapredProc != null) {
				mapredProc.destroy();
			}
			e.printStackTrace();
		}
		
		System.out.println("TASK ATTEMPT " + taskID + " WAS NOT KILLED");
		return false;
	}
	
	/*
	 * Fails the task attempt associated with the specified task ID.
	 * 
	 * @param jobID The ID of the job matching the task attempt.
	 * @return boolean Whether the task attempt was killed or not.
	 */
	private boolean failTaskAttempt(String taskID) {
		
		Process mapredProc = null;
		
		String hadoop_install = "/Users/rbernota/workspace/eclipse/branch-0.23.4/hadoop-dist/target/hadoop-0.23.4"; // this should come from env $HADOOP_INSTALL or prop variable in fw conf
		String hadoop_conf_dir = "/Users/rbernota/workspace/hadoop/test/pseudodistributed_configs/test/";
		String mapred_exe = hadoop_install + "/bin/mapred";
		
		String mapredCmd = mapred_exe + " --config " + hadoop_conf_dir + " job -fail-task " + taskID;
		
		System.out.println(mapredCmd);

		String mapredPatternStr = "(.*)(Killed task " + taskID + " by failing it)(.*)";
		Pattern mapredPattern = Pattern.compile(mapredPatternStr);
		
		try {
			mapredProc = Runtime.getRuntime().exec(mapredCmd);
			BufferedReader reader=new BufferedReader(new InputStreamReader(mapredProc.getInputStream())); 
			String line=reader.readLine(); 
			while(line!=null) 
			{ 
				System.out.println(line); 
				
				Matcher mapredMatcher = mapredPattern.matcher(line);
				
				if (mapredMatcher.find()) {
					System.out.println("TASK ATTEMPT " + taskID + " WAS FAILED");
					return true;
				}
				
				line=reader.readLine();
			} 
		}
		catch (Exception e) {
			if (mapredProc != null) {
				mapredProc.destroy();
			}
			e.printStackTrace();
		}
		
		System.out.println("TASK ATTEMPT " + taskID + " WAS NOT FAILED");
		return false;
	}
	
	/*
	 * Fail a job.
	 * 
	 * @param jobID The ID of the job to fail.
	 * @return boolean Whether the job was failed or not.
	 */
	private boolean failJob(String jobID) {
		// Fail job with given ID
		
		/*
		 * 
		 *    local myjobId=$1
   local myAttemptId1=$(echo $myjobId |sed 's/job/attempt/g'|sed 's/$/_m_000000_0/g')
   local myAttemptId2=$(echo $myjobId |sed 's/job/attempt/g'|sed 's/$/_m_000000_1/g')
   local myAttemptId3=$(echo $myjobId |sed 's/job/attempt/g'|sed 's/$/_m_000000_2/g')
   local myAttemptId4=$(echo $myjobId |sed 's/job/attempt/g'|sed 's/$/_m_000000_3/g')
   local myAttemptIds=" $myAttemptId1 $myAttemptId2 $myAttemptId3 $myAttemptId4"
   for myAttemptId in $myAttemptIds; do
      failGivenAttemptId $myAttemptId
   done
		 * 
		 */
		String taskID; //should get the real taskID here
		
		String taskIDExtractStr = "(job)(.*)";
		Pattern taskIDPattern = Pattern.compile(taskIDExtractStr);
		
		Matcher taskIDMatcher = taskIDPattern.matcher(jobID);
		
		while (taskIDMatcher.find()) {
			for (int i = 0; i < 4; i++) {
				taskID = "attempt" + taskIDMatcher.group(2) + "_m_000000_" + Integer.toString(i);
				if (! this.failTaskAttempt(taskID)) {
					return false;
				}
			}
		}
		

		// Get job status

		Process mapredProc = null;
		
		String hadoop_install = "/Users/rbernota/workspace/eclipse/branch-0.23.4/hadoop-dist/target/hadoop-0.23.4"; // this should come from env $HADOOP_INSTALL or prop variable in fw conf
		String hadoop_conf_dir = "/Users/rbernota/workspace/hadoop/test/pseudodistributed_configs/test/";
		String mapred_exe = hadoop_install + "/bin/mapred";
		
		String mapredCmd = mapred_exe + " --config " + hadoop_conf_dir + " job -status " + jobID;
		
		System.out.println(mapredCmd);

		String mapredPatternStr = "(.*)(Job state: FAILED)(.*)";
		Pattern mapredPattern = Pattern.compile(mapredPatternStr);
		
		// Greps the job status output to see if it failed the job 

		try {
			mapredProc = Runtime.getRuntime().exec(mapredCmd);
			BufferedReader reader=new BufferedReader(new InputStreamReader(mapredProc.getInputStream())); 
			String line=reader.readLine(); 
			while(line!=null) 
			{ 
				System.out.println(line); 
				
				Matcher mapredMatcher = mapredPattern.matcher(line);
				
				if (mapredMatcher.find()) {
					System.out.println("JOB " + jobID + " WAS FAILED");
					return true;
				}
				
				line=reader.readLine();
			} 
		}
		catch (Exception e) {
			if (mapredProc != null) {
				mapredProc.destroy();
			}
			e.printStackTrace();
		}

		System.out.println("JOB " + jobID + " WAS NOT FAILED");
		return false; // job didn't fail
	}

	/*
	 * Kill a job.
	 * 
	 * @param jobID The ID of the job to kill.
	 * @return boolean Whether the job was killed or not.
	 */
	private boolean killJob(String jobID) {

		Process mapredProc = null;
		
		String hadoop_install = "/Users/rbernota/workspace/eclipse/branch-0.23.4/hadoop-dist/target/hadoop-0.23.4"; // this should come from env $HADOOP_INSTALL or prop variable in fw conf
		String hadoop_conf_dir = "/Users/rbernota/workspace/hadoop/test/pseudodistributed_configs/test/";
		String mapred_exe = hadoop_install + "/bin/mapred";
		
		String mapredCmd = mapred_exe + " --config " + hadoop_conf_dir + " job -kill " + jobID;
		
		System.out.println(mapredCmd);

		String mapredPatternStr = "(.*)(Killed job " + jobID + ")(.*)";
		Pattern mapredPattern = Pattern.compile(mapredPatternStr);
		
		try {
			mapredProc = Runtime.getRuntime().exec(mapredCmd);
			BufferedReader reader=new BufferedReader(new InputStreamReader(mapredProc.getInputStream())); 
			String line=reader.readLine(); 
			while(line!=null) 
			{ 
				System.out.println(line); 
				
				Matcher mapredMatcher = mapredPattern.matcher(line);
				
				if (mapredMatcher.find()) {
					System.out.println("JOB " + jobID + " WAS KILLED");
					return true;
				}
				
				line=reader.readLine();
			} 
		}
		catch (Exception e) {
			if (mapredProc != null) {
				mapredProc.destroy();
			}
			e.printStackTrace();
		}
		
		System.out.println("JOB " + jobID + " WAS NOT KILLED");
		return false;
	}
	
}
