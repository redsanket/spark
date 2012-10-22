/*
 * YAHOO!
 */

package hadooptest.regression.yarn;

import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

/*
 * A test suite used to exercise the ability to kill tasks.
 */
public class MapredKillTask {

	private String jobID = "0";
	private String mapTaskID = "0";
	private String reduceTaskID = "0";
	
	/******************* CLASS BEFORE/AFTER ***********************/
	
	/*
	 * Configuration and cluster setup that should happen before running any of the tests in the class instance.
	 */
	@BeforeClass
	public static void startCluster() {
		// set configuration
		// start the cluster
	}
	
	/*
	 * Cluster cleanup that should happen after running tests in the class instance.
	 */
	@AfterClass
	public static void stopCluster() {
		// stop the cluster
		// clean up configuration
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
		this.jobID = "0";
		
		// make sure any sleep jobs are finished/failed/killed
	}
	
	/******************* TESTS ***********************/
	
	/*
	 * A test which attempts to kill a running task from a sleep job.
	 */
	@Test
	public void killRunningTask() {		
		assertTrue("Killed task message doesn't exist, we weren't able to kill the task.", 
				this.killTaskAttempt(this.jobID));
	}
	
	/*
	 * A test which attempts to kill a task from a sleep job which has already been killed.
	 */
	@Test
	public void killTaskOfAlreadyKilledJob() {
		
		assertTrue("Was not able to kill the job.", 
				this.killJob(this.jobID));
		
		String taskID = this.getTaskAttemptID(this.jobID);
		assertTrue("Killed task message doesn't exist, we weren't able to kill the task.", 
				this.killTaskAttempt(taskID));
	}
	
	/*
	 * A test which attempts to kill a task from a sleep job which has failed.
	 */
	@Test
	public void killTaskOfAlreadyFailedJob() {
		
		assertTrue("Was not able to fail the job.", this.failJob(this.jobID));
		
		String taskID = this.getTaskAttemptID(this.jobID);
		assertTrue("Killed task message doesn't exist, we weren't able to kill the task.", 
				this.killTaskAttempt(taskID));
	}
	
	/*
	 * A test which attempts to kill a task from a sleep job which has already succeeded.
	 */
	@Test
	public void killTaskOfAlreadyCompletedJob() {
		
		assertTrue("Job did not succeed.", this.verifyJobSuccess(this.jobID));
		
		String taskID = this.getTaskAttemptID(this.jobID);
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
		String taskAttemptID = "";
		
		String hadoop_version = "0.23.4"; // should come from prop variable in fw conf
		String hadoop_install = "/Users/rbernota/workspace/eclipse/branch-0.23.4/hadoop-dist/target/hadoop-0.23.4"; // this should come from env $HADOOP_INSTALL or prop variable in fw conf
		String hadoop_mapred_test_jar = hadoop_install + "/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-" + hadoop_version + "-tests.jar";
		String artifacts_dir = "/Users/rbernota/workspace/artifacts";
		String hadoop_conf_dir = "/Users/rbernota/workspace/conf";
		String hadoop_exe = hadoop_install + "/bin/hadoop";
		String user = "rbernota"; // not sure where this should go... probably in fw conf for now, but possibly extract from system.
		
		for (int i = 0; i < numJobs; i++) {
			//createFile $ARTIFACTS_DIR/sleep.$i.log
			this.createArtifactsFile(artifacts_dir, i);
			
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
			
			String mapTaskPatternStr = " - Starting task: (.*)$";
			Pattern mapTaskPattern = Pattern.compile(mapTaskPatternStr);
			
			try {
				hadoopProc = Runtime.getRuntime().exec(hadoopCmd);
				//hadoopProc.waitFor();
				BufferedReader reader=new BufferedReader(new InputStreamReader(hadoopProc.getInputStream())); 
				String line=reader.readLine(); 
				while(line!=null) 
				{ 
					System.out.println(line); 
					line=reader.readLine();
					
					Matcher jobMatcher = jobPattern.matcher(line);
					Matcher mapTaskMatcher = mapTaskPattern.matcher(line);
					
					if (jobMatcher.find()) {
						jobID = jobMatcher.group(1);
						System.out.println("JOB ID: " + jobID);
					}
					else if (mapTaskMatcher.find()) {
						taskAttemptID = mapTaskMatcher.group(1);
						System.out.println("TASK ATTEMPT ID: " + taskAttemptID);
						break;
					}
				} 
			}
			catch (Exception e) {
				if (hadoopProc != null) {
					hadoopProc.destroy();
				}
				e.printStackTrace();
			}
		}

		this.mapTaskID = taskAttemptID;
		
		return jobID;
	}
	
	/*
	 * Create an artifact file
	 * 
	 * @param String The location of the artifact directory
	 * @param int The unique identifying integer for the filename
	 */
	private void createArtifactsFile(String artifactsDir, int fileInstance) {
		// touch $1 > /dev/null 2>&1 && echo "File $1 created."
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
		boolean jobSuccess = false;
		
		// check for job success here
				
		return jobSuccess;
	}
	
	/*
	 * Get the task attempt ID associated with the specified job ID.
	 * 
	 * @param jobID The ID of the job to associate with the task attempt.
	 * @return String The ID of the task attempt.
	 */
	private String getTaskAttemptID(String jobID) {
		// Get the task attempt ID given a job ID
		String taskID = "0"; //should get the real taskID here
		
		return taskID;
	}
	
	/*
	 * Kills the task attempt associated with the specified job ID.
	 * 
	 * @param jobID The ID of the job matching the task attempt.
	 * @return boolean Whether the task attempt was killed or not.
	 */
	private boolean killTaskAttempt(String taskID) {
		// Kill task attempt with known ID

		/*
		 *    $HADOOP_MAPRED_CMD --config $HADOOP_CONF_DIR job -kill-task $1
   				# resp=`$HADOOP_MAPRED_CMD --config $HADOOP_CONF_DIR job -fail-task $1 | grep Killed`
   				# echo " The response of the fail task is $resp "
		 */
		taskID = this.mapTaskID;
		
		Process mapredProc = null;
		
		String hadoop_version = "0.23.4"; // should come from prop variable in fw conf
		String hadoop_install = "/Users/rbernota/workspace/eclipse/branch-0.23.4/hadoop-dist/target/hadoop-0.23.4"; // this should come from env $HADOOP_INSTALL or prop variable in fw conf
		String hadoop_conf_dir = "/Users/rbernota/workspace/conf";
		String mapred_exe = hadoop_install + "/bin/mapred";
		
		String mapredCmd = mapred_exe + " --config " + hadoop_conf_dir + " job -kill-task " + taskID;
		
		System.out.println(mapredCmd);

		String mapredPatternStr = "Killed";
		Pattern mapredPattern = Pattern.compile(mapredPatternStr);
		
		try {
			mapredProc = Runtime.getRuntime().exec(mapredCmd);
			BufferedReader reader=new BufferedReader(new InputStreamReader(mapredProc.getInputStream())); 
			String line=reader.readLine(); 
			while(line!=null) 
			{ 
				System.out.println(line); 
				line=reader.readLine();
				
				Matcher mapredMatcher = mapredPattern.matcher(line);
				
				if (mapredMatcher.find()) {
					System.out.println("TASK ATTEMPT WAS KILLED");
					return true;
				}
			} 
		}
		catch (Exception e) {
			if (mapredProc != null) {
				mapredProc.destroy();
			}
			e.printStackTrace();
		}
		
		System.out.println("TASK ATTEMPT WAS NOT KILLED");
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
		
		// Get job status
		
		// Greps the job status output to see if it failed the job 
		
		return true; // return if the job was failed or not
	}

	/*
	 * Kill a job.
	 * 
	 * @param jobID The ID of the job to kill.
	 * @return boolean Whether the job was killed or not.
	 */
	private boolean killJob(String jobID) {
		// Kill job with given ID
		
		/*
		 * 
		 *    local captureFile=$2
   if [ "X"$captureFile"X" == "XX" ] ; then
      $HADOOP_MAPRED_CMD --config $HADOOP_CONF_DIR job -kill $1 |tail -1 |cut -f 1 | awk -F" " '{print $1}'
   else
      $HADOOP_MAPRED_CMD --config $HADOOP_CONF_DIR job -kill $1 > $captureFile 2>&1
   fi
		 * 
		 */
		
		return true; // return if the job was killed or not
	}
	
}
