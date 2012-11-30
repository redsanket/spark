/*
 * YAHOO!
 */

package hadooptest.cluster.pseudodistributed;

import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.cluster.Job;
import hadooptest.cluster.JobState;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/*
 * A class which represents a base pseudodistributed cluster job.
 */
public abstract class PseudoDistributedJob implements Job {

	public String ID = "0";	// The ID of the job.
	public String USER = ""; // The user for the job.
	public String QUEUE = ""; // The queue for the job.
	public JobState state;

	private final String HADOOP_INSTALL = "/Users/rbernota/workspace/eclipse/branch-0.23.4/hadoop-dist/target/hadoop-0.23.4";
	private final String CONFIG_BASE_DIR = "/Users/rbernota/workspace/hadoop/test/pseudodistributed_configs/test/";
	
	private static TestSession TSM;

	/*
	 * Class Constructor.
	 */
	public PseudoDistributedJob(TestSession testSession) {
		super();
		
		TSM = testSession;
		USER = TSM.conf.getProperty("USER", "");
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
	 * Sets a user for the job other than the default.
	 * 
	 * @param user The user to override the default user with.
	 */
	public void setUser(String user) {
		USER = user;
	}
	
	/*
	 * Sets a queue for the job other than the default.
	 * 
	 * @param queue The queue to override the default queue with.
	 */
	public void setQueue(String queue) {
		QUEUE = queue;
	}
	
	/*
	 * Verifies that the job ID matches the expected format.
	 * 
	 * @return boolean Whether the job ID matches the expected format.
	 */
	public boolean verifyID() {
		if (this.ID == "0") {
			TSM.logger.error("JOB ID DID NOT MATCH FORMAT AND WAS ZERO");
			return false;
		}

		String jobPatternStr = "job_(.*)$";
		Pattern jobPattern = Pattern.compile(jobPatternStr);
		
		Matcher jobMatcher = jobPattern.matcher(this.ID);
		
		if (jobMatcher.find()) {
			TSM.logger.info("JOB ID MATCHED EXPECTED FORMAT");
			TSM.logger.info("JOB ID: " + this.ID);
			return true;
		}
		else {
			TSM.logger.error("JOB ID DID NOT MATCH FORMAT");
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

		TSM.logger.debug(mapredCmd);

		String mapredPatternStr = "(.*)(Killed job " + this.ID + ")(.*)";
		Pattern mapredPattern = Pattern.compile(mapredPatternStr);
		
		try {
			mapredProc = Runtime.getRuntime().exec(mapredCmd);
			BufferedReader reader=new BufferedReader(new InputStreamReader(mapredProc.getInputStream())); 
			String line=reader.readLine(); 
			while(line!=null) 
			{ 
				TSM.logger.debug(line);
				
				Matcher mapredMatcher = mapredPattern.matcher(line);
				
				if (mapredMatcher.find()) {
					TSM.logger.info("JOB " + this.ID + " WAS KILLED");
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
		
		TSM.logger.error("JOB " + this.ID + " WAS NOT KILLED");
		return false;
	}
	
	/*
	 * Fails a job, assuming that a maximum of 1 map task attempts needs to
	 * be failed to fail the job.
	 * 
	 * @return boolean Whether the job was successfully failed.
	 * 
	 * (non-Javadoc)
	 * @see hadooptest.cluster.Job#fail()
	 */
	public boolean fail() {
		return fail(1);
	}
	
	/*
	 * Fails the job.
	 * 
	 * @param max_attempts The maximum number of map task attempts to fail before 
	 * 						assuming that the job should have failed.
	 * 
	 * @return boolean Whether the job was successfully failed.
	 */
	public boolean fail(int max_attempts) {

		String taskID;
		
		String taskIDExtractStr = "(job)(.*)";
		Pattern taskIDPattern = Pattern.compile(taskIDExtractStr);
		
		Matcher taskIDMatcher = taskIDPattern.matcher(this.ID);
		
		while (taskIDMatcher.find()) {
			for (int i = 0; i < max_attempts; i++) {
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
		
		TSM.logger.debug(mapredCmd);

		String mapredPatternStr = "(.*)(Job state: FAILED)(.*)";
		Pattern mapredPattern = Pattern.compile(mapredPatternStr);
		
		// Greps the job status output to see if it failed the job 

		try {
			mapredProc = Runtime.getRuntime().exec(mapredCmd);
			BufferedReader reader=new BufferedReader(new InputStreamReader(mapredProc.getInputStream())); 
			String line=reader.readLine(); 
			while(line!=null) 
			{ 
				TSM.logger.debug(line);
				
				Matcher mapredMatcher = mapredPattern.matcher(line);
				
				if (mapredMatcher.find()) {
					TSM.logger.info("JOB " + this.ID + " WAS FAILED");
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

		TSM.logger.error("JOB " + this.ID + " WAS NOT FAILED");
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
		
		TSM.logger.debug(mapredCmd);

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
					TSM.logger.debug(line);

					Matcher mapredMatcherSuccess = mapredPatternSuccess.matcher(line);
					Matcher mapredMatcherAppStatusSuccess = mapredAppStatusPatternSuccess.matcher(line);
					Matcher mapredMatcherFailed = mapredPatternFailed.matcher(line);
					Matcher mapredMatcherKilled = mapredPatternKilled.matcher(line);
					Matcher mapredMatcherPrep = mapredPatternPrep.matcher(line);
					Matcher mapredMatcherRunning = mapredPatternRunning.matcher(line);

					if (mapredMatcherSuccess.find()) {
						TSM.logger.info("JOB " + this.ID + " SUCCEEDED");
						return true;
					}
					if (mapredMatcherAppStatusSuccess.find()) {
						TSM.logger.info("JOB " + this.ID + " SUCCEEDED");
						return true;
					}
					else if (mapredMatcherFailed.find()) {
						TSM.logger.error("JOB " + this.ID + " FAILED");
						return false;
					}
					else if (mapredMatcherKilled.find()) {
						TSM.logger.error("JOB " + this.ID + " WAS KILLED");
						return false;
					}
					else if (mapredMatcherPrep.find()) {
						TSM.logger.info("JOB " + this.ID + " IS STILL IN PREP STATE");
					}
					else if (mapredMatcherRunning.find()) {
						TSM.logger.info("JOB " + this.ID + " IS STILL RUNNING");
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

			Util.sleep(10);
		}

		TSM.logger.error("JOB " + this.ID + " didn't SUCCEED within the 5 minute timeout window.");
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
		
		TSM.logger.debug(mapredCmd);

		String mapredPatternStr = "(.*)(Killed task " + taskID + " by failing it)(.*)";
		Pattern mapredPattern = Pattern.compile(mapredPatternStr);
		
		try {
			mapredProc = Runtime.getRuntime().exec(mapredCmd);
			BufferedReader reader=new BufferedReader(new InputStreamReader(mapredProc.getInputStream())); 
			String line=reader.readLine(); 
			while(line!=null) 
			{ 
				TSM.logger.debug(line);
				
				Matcher mapredMatcher = mapredPattern.matcher(line);
				
				if (mapredMatcher.find()) {
					TSM.logger.info("TASK ATTEMPT " + taskID + " WAS FAILED");
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
		
		TSM.logger.error("TASK ATTEMPT " + taskID + " WAS NOT FAILED");
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
		
		TSM.logger.info("MAP TASK ID = " + taskID);
		
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
		
		TSM.logger.info("REDUCE TASK ID = " + taskID);
		
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
		
		TSM.logger.debug(mapredCmd);
		
		String mapredPatternStr = "(.*)(Killed task " + taskID + ")(.*)";
		Pattern mapredPattern = Pattern.compile(mapredPatternStr);
		
		try {
			mapredProc = Runtime.getRuntime().exec(mapredCmd);
			BufferedReader reader=new BufferedReader(new InputStreamReader(mapredProc.getInputStream())); 
			String line=reader.readLine(); 
			while(line!=null) 
			{ 
				TSM.logger.debug(line);
				
				Matcher mapredMatcher = mapredPattern.matcher(line);
				
				if (mapredMatcher.find()) {
					TSM.logger.info("TASK ATTEMPT " + taskID + " WAS KILLED");
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

		TSM.logger.error("TASK ATTEMPT " + taskID + " WAS NOT KILLED");
		return false;
	}
	
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
	public boolean findSummaryInfo(String status, String jobName, String user, String queue) throws FileNotFoundException, IOException {
		// Build job summary info template
		String numMaps = "10";
		String numReduces = "10";
		String patternStr = "(.*)"
				+ "jobId=" + this.ID
				+ ",submitTime=[0-9]{13}"
				+ ",launchTime=[0-9]{13}"
				+ ",firstMapTaskLaunchTime=[0-9]{13}"
				+ ",firstReduceTaskLaunchTime=[0-9]{13}"
				+ ",finishTime=[0-9]{13}"
				+ ",resourcesPerMap=[0-9]+"
				+ ",resourcesPerReduce=[0-9]+"
				+ ",numMaps=" + numMaps
				+ ",numReduces=" + numReduces
				+ ",user=" + user
				+ ",queue=" + queue
				+ ",status=" + status
				+ ",mapSlotSeconds=[0-9]+"
				+ ",reduceSlotSeconds=[0-9]+"
				+ ",jobName=" + jobName
				+ "(.*)";
		Pattern infoPattern = Pattern.compile(patternStr);

		TSM.logger.info("Sleeping for 200s to wait for the job summary info log to be updated.");
		//Util.sleep(200);

		String HADOOP_INSTALL = TSM.conf.getProperty("HADOOP_INSTALL", "");
		FileInputStream summaryInfoFile = new FileInputStream(HADOOP_INSTALL + "/logs/hadoop-mapreduce.jobsummary.log");
		DataInputStream in = new DataInputStream(summaryInfoFile);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		String line;
		Matcher infoMatcher;
		Boolean foundSummaryInfo = false;

		while ((line = br.readLine()) != null)   {
			TSM.logger.debug("JOB SUMMARY INFO: " + line);
			infoMatcher = infoPattern.matcher(line);
			if (infoMatcher.find()) {
				foundSummaryInfo = true; 
				TSM.logger.info("Summary info for the job was found.");
				}
		}

		in.close();

		if (!foundSummaryInfo) {
			TSM.logger.error("Job summary info was not found in the log file.");
		}
		
		return foundSummaryInfo;
	}
}
