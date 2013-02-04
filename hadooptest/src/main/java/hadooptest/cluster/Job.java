/*
 * YAHOO!
 */

package hadooptest.cluster;

import java.io.FileNotFoundException;
import java.io.IOException;

/*
 * An interface which should represent the base capability of any job
 * submitted to a cluster.
 */
public interface Job {

	/*
	 * The ID of the job.
	 * 
	 * @return String The job ID.
	 */
	public String ID = "0";
	
	/*
	 * Submit the job to the cluster.
	 * 
	 * @return String the ID of the job submitted.
	 */
	public String submit();
	
	/*
	 * Submit a job to the cluster, while being able to specify the job parameters.
	 * 
	 * @param mappers The "-m" param to the sleep job (number of mappers)
	 * @param reducers The "-r" param to the sleep job (number of reducers)
	 * @param mt_param The "-rt" param to the sleep job (map time)
	 * @param rt_param The "-mt" param to the sleep job (reduce time)
	 * @param numJobs The number of sleep jobs to run.
	 * @param map_memory The memory assigned for map jobs.  If -1, it will use the default.
	 * @param reduce_memory The memory assigned for reduce jobs.  If -1, it will use the default.
	 * 
	 * @return String the ID of the sleep job that was submitted to the pseudodistributed cluster.
	 */
	public String submit(int mappers, int reducers, int map_time,
			int reduce_time, int numJobs, int map_memory, int reduce_memory);
	
	/*
	 * Submit a fail job to the cluster, while being able to specify whether the mappers or reducers should fail.
	 * 
	 * @param failMappers set to true if the mappers should fail
	 * @param failReducers set to true if the reducers should fail
	 * 
	 * @return String the ID of the sleep job that was submitted to the cluster.
	 */
	public String submit(boolean failMappers, boolean failReducers);
	
	/*
	 * Fails the job.
	 * 
	 * @return boolean Whether the job was successfully failed.
	 */
	public boolean fail();

	/*
	 * Fails the job.
	 * 
	 * @param max_attempts The maximum number of map task attempts to fail before 
	 * 						assuming that the job should have failed.
	 * 
	 * @return boolean Whether the job was successfully failed.
	 */
	public boolean fail(int max_attempts);
	
	/*
	 * Kills the job.
	 * 
	 * @return boolean Whether the job was successfully killed.
	 */
	public boolean kill();
	
	/*
	 * Verifies that the job ID matches the expected format.
	 * 
	 * @return boolean Whether the job ID matches the expected format.
	 */
	public boolean verifyID();
	
	/*
	 * Waits for the job to succeed, and returns true for success.
	 * 
	 * @return boolean whether the job succeeded
	 */
	public boolean waitForSuccess();
	
	/*
	 * Waits for the specified number of seconds for the job to 
	 * succeed, and returns true for success.
	 * 
	 * @param seconds The number of seconds to wait for the success state.
	 */
	public boolean waitForSuccess(int seconds);
	
	/*
	 * Returns the state of the job in the JobState format.
	 * 
	 * @return JobState The state of the job.
	 */
	public JobState state();
	
	/*
	 * Get the map task attempt ID associated with the job ID.
	 * 
	 * @param jobID The ID of the job to associate with the task attempt.
	 * @return String The ID of the task attempt.
	 */
	public String getMapTaskAttemptID();
	
	/*
	 * Get the reduce task attempt ID associated with the job ID.
	 * 
	 * @return String The ID of the task attempt.
	 */
	public String getReduceTaskAttemptID();
	
	/*
	 * Kills the task attempt associated with the specified task ID.
	 * 
	 * @param taskID The ID of the task attempt to kill.
	 * @return boolean Whether the task attempt was killed or not.
	 */
	public boolean killTaskAttempt(String taskID);
	
	/*
	 * Finds whether the job summary info in the summary info log file exists.
	 * 
	 * @param status The status of the job
	 * @param jobName The name of the job
	 * @param user The job user
	 * @param queue The queue for the job
	 * 
	 * @return boolean Whether the job summary info was found in the summary info log file or not
	 */
	public boolean findSummaryInfo(String status, String jobName, String user, String queue) throws FileNotFoundException, IOException;
	
	/*
	 * Sets a user for the job other than the default.
	 * 
	 * @param user The user to override the default user with.
	 */
	public void setUser(String user);
	
	/*
	 * Sets a queue for the job other than the default.
	 * 
	 * @param queue The queue to override the default queue with.
	 */
	public void setQueue(String queue);
	
}

