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

/*
 * A test suite used to exercise the ability to kill tasks.
 */
public class MapredKillTask {

	private int jobID = 0;
	
	/******************* CLASS BEFORE/AFTER ***********************/
	
	/*
	 * Configuration and cluster setup that should happen before running any of the tests in the class instance.
	 */
	@BeforeClass
	public void startCluster() {
		// set configuration
		// start the cluster
	}
	
	/*
	 * Cluster cleanup that should happen after running tests in the class instance.
	 */
	@AfterClass
	public void stopCluster() {
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
		this.jobID = 0;
		
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
		
		int taskID = this.getTaskAttemptID(this.jobID);
		assertTrue("Killed task message doesn't exist, we weren't able to kill the task.", 
				this.killTaskAttempt(taskID));
	}
	
	/*
	 * A test which attempts to kill a task from a sleep job which has failed.
	 */
	@Test
	public void killTaskOfAlreadyFailedJob() {
		
		assertTrue("Was not able to fail the job.", this.failJob(this.jobID));
		
		int taskID = this.getTaskAttemptID(this.jobID);
		assertTrue("Killed task message doesn't exist, we weren't able to kill the task.", 
				this.killTaskAttempt(taskID));
	}
	
	/*
	 * A test which attempts to kill a task from a sleep job which has already succeeded.
	 */
	@Test
	public void killTaskOfAlreadyCompletedJob() {
		
		assertTrue("Job did not succeed.", this.verifyJobSuccess(this.jobID));
		
		int taskID = this.getTaskAttemptID(this.jobID);
		assertTrue("Killed task message doesn't exist, we weren't able to kill the task.", 
				this.killTaskAttempt(taskID));
	}
	
	/******************* END TESTS ***********************/
	
	/******************* CONVENIENCE METHODS *********************/
	
	/*
	 * Submits a default sleep job.
	 * 
	 * @return int The ID of the sleep job.
	 */
	private int submitSleepJob() {
		return submitSleepJob(10, 10, 50000, 50000, 1);
	}
	
	/*
	 * Submits a sleep job.
	 * 
	 * @return int The ID of the sleep job.
	 */
	private int submitSleepJob(int m_param, int r_param, int mt_param, int rt_param, int numJobs) {
		int sleepJobID = 0;
		
		// submit sleep job
		
		Process hadoopProc = null;
		
		String hadoop_version = "0.23.4"; // should come from prop variable in fw conf
		String hadoop_install = "/Users/rbernota/workspace/eclipse/branch-0.23.4/hadoop-dist/target/hadoop-0.23.4"; // this should come from env $HADOOP_INSTALL or prop variable in fw conf
		String hadoop_mapred_test_jar = hadoop_install + "/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-" + hadoop_version + "-tests.jar";
		String artifacts_dir = "";
		String hadoop_conf_dir = "";
		
		String user = "rbernota"; // not sure where this should go... probably in fw conf for now, but possibly extract from system.
		
		for (int i = 0; i < numJobs; i++) {
			//createFile $ARTIFACTS_DIR/sleep.$i.log
			this.createArtifactsFile(artifacts_dir, i);
			
			String hadoopCmd = "--config " + hadoop_conf_dir 
					+ " jar " + hadoop_mapred_test_jar 
					+ " sleep -Dmapreduce.job.user.name=" + user 
					+ " -m " + m_param 
					+ " -r " + r_param 
					+ " -mt " + mt_param 
					+ " -rt " + rt_param 
					+ " > " + artifacts_dir + "/sleep." + i + ".log";
			try {
				hadoopProc = Runtime.getRuntime().exec(new String[] {"hadoop", hadoopCmd} );
			}
			catch (Exception e) {
				if (hadoopProc != null) {
					hadoopProc.destroy();
				}
				e.printStackTrace();
			}
		}

		// get job id
		
		/*
		 
		    local myloc="$ARTIFACTS_DIR/sleep.0.log"
   if [ ! -z $1 ] ; then
      myloc=$1
   fi
   for (( i=0; i < 10; i++ )); do
      local myjobId=`cat $myloc |grep "Running job"|awk -F'job:' '{print $2}'`
      # Stripping of the leading blank space quick and easy way
      myjobId=`echo $myjobId`
      if [ ! "X${myjobId}X" == "XX" ] ; then
         break
      else
         sleep 10
      fi
   done
   validateJobId $myjobId
   if [ $? -ne 0 ] ; then
      echo "0"
   else
      echo $myjobId
   fi
   # return $?
		 
		 */
		return sleepJobID;
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
	 * Verifies a job ID exists.
	 * 
	 * @param jobID The ID of the job to verify.
	 * @return boolean Whether the job is valid or not.
	 */
	private boolean verifyJobID(int jobID) {
		// verify job ID
		// make sure it isn't 0, plus...
		
		
		/*
		 *    local myjobId=$1
   pattern=`echo $myjobId |grep job_ `
   if [ -z "$pattern" ] ; then
      # echo " The job id does meet the format"
      return 1
   else
      # echo " The job id meets the format "
      return 0
   fi
		*/
		
		if (true/*valid*/) {
			return true;
		}
		else {
			return false;
		}
	}
	
	private boolean verifyJobSuccess(int jobID) {
		// Runs Hadoop to check for the SUCCEEDED state of the job 
		boolean jobSuccess = false;
		
		// check for job success here
				
		return jobSuccess;
	}
	
	/*
	 * Get the task attempt ID associated with the specified job ID.
	 * 
	 * @param jobID The ID of the job to associate with the task attempt.
	 * @return int The ID of the task attempt.
	 */
	private int getTaskAttemptID(int jobID) {
		// Get the task attempt ID given a job ID
		int taskID = 0; //should get the real taskID here
		
		return taskID;
	}
	
	/*
	 * Kills the task attempt associated with the specified job ID.
	 * 
	 * @param jobID The ID of the job matching the task attempt.
	 * @return boolean Whether the task attempt was killed or not.
	 */
	private boolean killTaskAttempt(int jobID) {
		// Kill task attempt with known ID

		/*
		 *    $HADOOP_MAPRED_CMD --config $HADOOP_CONF_DIR job -kill-task $1
   				# resp=`$HADOOP_MAPRED_CMD --config $HADOOP_CONF_DIR job -fail-task $1 | grep Killed`
   				# echo " The response of the fail task is $resp "
		 */
		
		
		// Look for "killed task" message
		
		if (true/* message exists */) {
			return true;
		}
		else {
			return false;
		}
	}
	
	/*
	 * Fail a job.
	 * 
	 * @param jobID The ID of the job to fail.
	 * @return boolean Whether the job was failed or not.
	 */
	private boolean failJob(int jobID) {
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
	private boolean killJob(int jobID) {
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
