package hadooptest.cluster.standalone;

import hadooptest.cluster.JobState;
import hadooptest.cluster.Job;

public class StandaloneJob implements Job {

	public String ID = "0";	// The ID of the job.
	
	/*
	 * Class Constructor.
	 */
	public StandaloneJob() {
		super();
	}
	
	/*
	 * Submit the job to the cluster.
	 * 
	 * @return String the ID of the job submitted.
	 */
	public String submit() {
		
	}
	
	/*
	 * Fails the job.
	 * 
	 * @return boolean Whether the job was successfully failed.
	 */
	public boolean fail() {
		
	}
	
	/*
	 * Kills the job.
	 * 
	 * @return boolean Whether the job was successfully killed.
	 */
	public boolean kill() {
		
	}
	
	/*
	 * Verifies that the job ID matches the expected format.
	 * 
	 * @return boolean Whether the job ID matches the expected format.
	 */
	public boolean verifyID() {
		
	}
	
	/*
	 * Waits for the job to succeed, and returns true for success.
	 * 
	 * @return boolean whether the job succeeded
	 */
	public boolean waitForSuccess() {
		
	}
	
	/*
	 * Waits for the specified number of seconds for the job to 
	 * succeed, and returns true for success.
	 * 
	 * @param seconds The number of seconds to wait for the success state.
	 */
	public boolean waitForSuccess(int seconds) {
		
	}
	
	/*
	 * Returns the state of the job in the JobState format.
	 * 
	 * @return JobState The state of the job.
	 */
	public JobState state() {
		
	}
}
