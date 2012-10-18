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
	
	/***** CONVENIENCE METHODS *****/
	
	/*
	 * Submits a sleep job.
	 * 
	 * @return int The ID of the sleep job.
	 */
	private int submitSleepJob() {
		int sleepJobID = 0;
		
		// submit sleep job
		
		// get job id
		
		return sleepJobID;
	}
	
	/*
	 * Verifies a job ID exists.
	 * 
	 * @param jobID The ID of the job to verify.
	 * @return boolean Whether the job is valid or not.
	 */
	private boolean verifyJobID(int jobID) {
		// verify job ID
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
		
		return true; // return if the job was killed or not
	}
	
}
