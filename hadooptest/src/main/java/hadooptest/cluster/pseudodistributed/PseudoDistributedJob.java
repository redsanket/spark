/*
 * YAHOO!
 */

package hadooptest.cluster.pseudodistributed;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import hadooptest.cluster.Job;
import hadooptest.cluster.JobState;

/*
 * A class which represents a base pseudodistributed cluster job.
 */
public abstract class PseudoDistributedJob implements Job {
	/*
	 * The ID of the job.
	 * 
	 * @return String The job ID.
	 */
	public String ID = "0";
	public JobState state;

	private final String HADOOP_INSTALL = "/Users/rbernota/workspace/eclipse/branch-0.23.4/hadoop-dist/target/hadoop-0.23.4";
	private final String CONFIG_BASE_DIR = "/Users/rbernota/workspace/hadoop/test/pseudodistributed_configs/test/";
	
	public PseudoDistributedJob() {
		super();
	}
	
	/*
	 * Returns the state of the job in the JobState format.
	 * 
	 * @return JobState The state of the job.
	 */
	public JobState state() {
		return state;
	}
	
	/*
	 * Verifies that the job ID matches the expected format.
	 * 
	 * @return boolean Whether the job ID matches the expected format.
	 */
	public boolean verifyID() {
		if (this.ID == "0") {
			System.out.println("JOB ID DID NOT MATCH FORMAT AND WAS ZERO");
			return false;
		}

		String jobPatternStr = "job_(.*)$";
		Pattern jobPattern = Pattern.compile(jobPatternStr);
		
		Matcher jobMatcher = jobPattern.matcher(this.ID);
		
		if (jobMatcher.find()) {
			System.out.println("JOB ID MATCHED EXPECTED FORMAT");
			System.out.println("JOB ID: " + this.ID);
			return true;
		}
		else {
			System.out.println("JOB ID DID NOT MATCH FORMAT");
			return false;
		}
	}
	
	/*
	 * Kills the job.
	 * 
	 * @return boolean Whether the job was successfully killed.
	 */
	public boolean kill() {

		Process mapredProc = null;
		
		String mapred_exe = HADOOP_INSTALL + "/bin/mapred";
		
		String mapredCmd = mapred_exe + " --config " + CONFIG_BASE_DIR + " job -kill " + this.ID;
		
		System.out.println(mapredCmd);

		String mapredPatternStr = "(.*)(Killed job " + this.ID + ")(.*)";
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
					System.out.println("JOB " + this.ID + " WAS KILLED");
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
		
		System.out.println("JOB " + this.ID + " WAS NOT KILLED");
		return false;
	}
	
	
	/*
	 * Fails the job.
	 * 
	 * @return boolean Whether the job was successfully failed.
	 */
	public boolean fail() {

		String taskID;
		
		String taskIDExtractStr = "(job)(.*)";
		Pattern taskIDPattern = Pattern.compile(taskIDExtractStr);
		
		Matcher taskIDMatcher = taskIDPattern.matcher(this.ID);
		
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
		
		String mapred_exe = HADOOP_INSTALL + "/bin/mapred";
		
		String mapredCmd = mapred_exe + " --config " + CONFIG_BASE_DIR + " job -status " + this.ID;
		
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
					System.out.println("JOB " + this.ID + " WAS FAILED");
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

		System.out.println("JOB " + this.ID + " WAS NOT FAILED");
		return false; // job didn't fail
	}
	
	/*
	 * Waits indefinitely for the job to succeed, and returns true for success.
	 * 
	 * @return boolean whether the job succeeded
	 */
	public boolean waitForSuccess() {
		return this.waitForSuccess(0);
	}
	
	/*
	 * Waits for the specified number of seconds for the job to 
	 * succeed, and returns true for success.
	 * 
	 * @param seconds The number of seconds to wait for the success state.
	 */
	public boolean waitForSuccess(int seconds) {
		// Runs Hadoop to check for the SUCCEEDED state of the job 
		
		// check for job success here
		Process mapredProc = null;
		
		String mapred_exe = HADOOP_INSTALL + "/bin/mapred";
		
		String mapredCmd = mapred_exe + " --config " + CONFIG_BASE_DIR + " job -status " + this.ID;
		
		System.out.println(mapredCmd);

		String mapredPatternStrSuccess = "(.*)(Job state: SUCCEEDED)(.*)";
		Pattern mapredPatternSuccess = Pattern.compile(mapredPatternStrSuccess);
		
		String mapredAppStatusPatternStrSuccess = "(.*)(FinalApplicationStatus=SUCCEEDED)(.*)";
		Pattern mapredAppStatusPatternSuccess = Pattern.compile(mapredAppStatusPatternStrSuccess);
		
		String mapredPatternStrFailed = "(.*)(Job state: FAILED)(.*)";
		Pattern mapredPatternFailed = Pattern.compile(mapredPatternStrFailed);
		
		String mapredPatternStrKilled = "(.*)(Job state: KILLED)(.*)";
		Pattern mapredPatternKilled = Pattern.compile(mapredPatternStrKilled);

		String mapredPatternStrPrep = "(.*)(Job state: PREP)(.*)";
		Pattern mapredPatternPrep = Pattern.compile(mapredPatternStrPrep);

		String mapredPatternStrRunning = "(.*)(Job state: RUNNING)(.*)";
		Pattern mapredPatternRunning = Pattern.compile(mapredPatternStrRunning);

		// Give the sleep job 15 minutes to complete
		for (int i = 0; i <= 150; i++) {
		
			try {
				mapredProc = Runtime.getRuntime().exec(mapredCmd);
				BufferedReader reader=new BufferedReader(new InputStreamReader(mapredProc.getInputStream())); 
				String line=reader.readLine(); 
				while(line!=null) 
				{ 
					System.out.println(line); 

					Matcher mapredMatcherSuccess = mapredPatternSuccess.matcher(line);
					Matcher mapredMatcherAppStatusSuccess = mapredPatternSuccess.matcher(line);
					Matcher mapredMatcherFailed = mapredPatternFailed.matcher(line);
					Matcher mapredMatcherKilled = mapredPatternKilled.matcher(line);
					Matcher mapredMatcherPrep = mapredPatternPrep.matcher(line);
					Matcher mapredMatcherRunning = mapredPatternRunning.matcher(line);

					if (mapredMatcherSuccess.find()) {
						System.out.println("JOB " + this.ID + " SUCCEEDED");
						return true;
					}
					if (mapredMatcherAppStatusSuccess.find()) {
						System.out.println("JOB " + this.ID + " SUCCEEDED");
						return true;
					}
					else if (mapredMatcherFailed.find()) {
						System.out.println("JOB " + this.ID + " FAILED");
						return false;
					}
					else if (mapredMatcherKilled.find()) {
						System.out.println("JOB " + this.ID + " WAS KILLED");
						return false;
					}
					else if (mapredMatcherPrep.find()) {
						System.out.println("JOB " + this.ID + " IS STILL IN PREP STATE");
					}
					else if (mapredMatcherRunning.find()) {
						System.out.println("JOB " + this.ID + " IS STILL RUNNING");
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

		System.out.println("JOB " + this.ID + " didn't SUCCEED within the 5 minute timeout window.");
		return false;
	}

	/*
	 * Fails the task attempt associated with the specified task ID.
	 * 
	 * @param jobID The ID of the job matching the task attempt.
	 * @return boolean Whether the task attempt was killed or not.
	 */
	protected boolean failTaskAttempt(String taskID) {
		
		Process mapredProc = null;
		
		String mapred_exe = HADOOP_INSTALL + "/bin/mapred";
		
		String mapredCmd = mapred_exe + " --config " + CONFIG_BASE_DIR + " job -fail-task " + taskID;
		
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
	 * Get the map task attempt ID associated with the specified job ID.
	 * 
	 * @param jobID The ID of the job to associate with the task attempt.
	 * @return String The ID of the task attempt.
	 */
	public String getMapTaskAttemptID() {
		// Get the task attempt ID given a job ID
		String taskID = "0"; //should get the real taskID here
		
		String taskIDExtractStr = "(job)(.*)";
		Pattern taskIDPattern = Pattern.compile(taskIDExtractStr);
		
		Matcher taskIDMatcher = taskIDPattern.matcher(this.ID);
		
		while (taskIDMatcher.find()) {
			taskID = "attempt" + taskIDMatcher.group(2) + "_m_00000_0";
		}
		
		System.out.println("MAP TASK ID = " + taskID);
		
		return taskID;
	}
	
	/*
	 * Get the reduce task attempt ID associated with the specified job ID.
	 * 
	 * @param jobID The ID of the job to associate with the task attempt.
	 * @return String The ID of the task attempt.
	 */
	public String getReduceTaskAttemptID() {
		// Get the task attempt ID given a job ID
		String taskID = "0"; //should get the real taskID here
		
		String taskIDExtractStr = "(job)(.*)";
		Pattern taskIDPattern = Pattern.compile(taskIDExtractStr);
		
		Matcher taskIDMatcher = taskIDPattern.matcher(this.ID);
		
		while (taskIDMatcher.find()) {
			taskID = "attempt" + taskIDMatcher.group(2) + "_r_00000_0";
		}
		
		System.out.println("REDUCE TASK ID = " + taskID);
		
		return taskID;
	}

	/*
	 * Kills the task attempt associated with the specified task ID.
	 * 
	 * @param jobID The ID of the job matching the task attempt.
	 * @return boolean Whether the task attempt was killed or not.
	 */
	public boolean killTaskAttempt(String taskID) {
		
		Process mapredProc = null;
		
		String mapred_exe = HADOOP_INSTALL + "/bin/mapred";
		
		String mapredCmd = mapred_exe + " --config " + CONFIG_BASE_DIR + " job -kill-task " + taskID;
		
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
}
