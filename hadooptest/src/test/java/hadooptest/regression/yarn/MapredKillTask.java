package hadooptest.regression.yarn;

import org.junit.Test;
import org.junit.Assert;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class MapredKillTask {

	@Test
	public void killRunningTask() {
		
		int jobID = this.submitSleepJob();
		
		if (! this.verifyJobID(jobID)) {
			Assert.fail("Sleep job ID is invalid.");
		}
		
		if (! this.killTaskAttempt(jobID)) {
			Assert.fail("Killed task message doesn't exist, we weren't able to kill the task.");
		}
	}
	
	@Test
	public void killTaskOfAlreadyKilledJob() {
		
		int jobID = this.submitSleepJob();
		
		if (! this.verifyJobID(jobID)) {
			Assert.fail("Sleep job ID is invalid.");
		}
		
		// Kill job with given ID
		if (true/* job not killed */) {
			// fail test
		}
		
		// get job task attempt ID
		int taskID = 0;
		
		if (! this.killTaskAttempt(taskID)) {
			Assert.fail("Killed task message doesn't exist, we weren't able to kill the task.");
		}
		
	}
	
	@Test
	public void killTaskOfAlreadyFailedJob() {

		int jobID = this.submitSleepJob();
		
		if (! this.verifyJobID(jobID)) {
			Assert.fail("Sleep job ID is invalid.");
		}
		
		// Fail job with given ID
		
		// Get job status
		
		// Greps the job status output to see if it failed the job 
		
		if (true/* job did not fail */) {
			// fail test
		}
		
		// get job task attempt ID
		int taskID = 0;
		
		if (! this.killTaskAttempt(taskID)) {
			Assert.fail("Killed task message doesn't exist, we weren't able to kill the task.");
		}
	}
	
	@Test
	public void killTaskOfAlreadyCompletedJob() {

		int jobID = this.submitSleepJob();
		
		if (! this.verifyJobID(jobID)) {
			Assert.fail("Sleep job ID is invalid.");
		}
		
		// Runs Hadoop to check for the SUCCEEDED state of the job 
		
		// get job task attempt ID
		int taskID = 0;
		
		if (! this.killTaskAttempt(taskID)) {
			Assert.fail("Killed task message doesn't exist, we weren't able to kill the task.");
		}
	}
	
	/***** CONVENIENCE METHODS *****/
	
	private int submitSleepJob() {
		int jobID = 0;
		
		// submit sleep job
		
		// get job id
		
		return jobID;
	}
	
	private boolean verifyJobID(int jobID) {
		// verify job ID
		if (true/*valid*/) {
			return true;
		}
		else {
			return false;
		}
	}
	
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
	
}
