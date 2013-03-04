/*
 * YAHOO!
 */

package hadooptest.job;

import hadooptest.TestSession;
import hadooptest.Util;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;

/**
 * A class which should represent the base capability of any job
 * submitted to a cluster.
 */
public abstract class Job extends Thread {

	/** The ID of the job. */
	protected String ID = "0";
	
	/** The user the job will run under */
	protected String USER = TestSession.conf.getProperty("USER", System.getProperty("user.name")); // The user for the job.
	
	/** The queue the job will run under */
	protected String QUEUE = "";
	
	/** The process handle for the job when it is run from a system call */
	protected Process process = null;
	
	/** Whether the job should take time to wait for the job ID in the output before progressing */
	protected boolean jobInitSetID = true;
	
	/**
	 * Submit the job to the cluster.
	 */
	protected abstract void submit();
	
	/**
	 * Submit the job to the cluster, but don't wait to assign an ID to this Job.
	 * Should only be intended for cases where you want to saturate a cluster
	 * with Jobs, and don't care about the status or result of the Job.
	 */
	protected abstract void submitNoID();
	
	/**
	 * Get the process handle for a job submitted from a system call.
	 * 
	 * @return Process the handle to the job process.
	 */
	public Process getProcess() {
		return this.process;
	}
	
	/**
	 * Get the job ID.
	 * 
	 * @return String the job ID.
	 */
	public String getID() {
		return this.ID;
	}
	
	/**
	 * Implements Thread.run().
	 * 
	 * (non-Javadoc)
	 * @see java.lang.Thread#run()
	 */
	public void run() {
		if (jobInitSetID) {
			this.submit();
		}
		else {
			this.submitNoID();
		}
	}
	
	/**
	 * Get the status of the Job through the Hadoop API.
	 * 
	 * @return JobState the state of the Job.
	 */
	public JobState getJobStatus() {
		JobState state = JobState.UNKNOWN;
		
		try {			
			state = JobState.getState(this.getHadoopJob().getJobState());
			TestSession.logger.debug("Job Status: " + state.toString());
		}
		catch (IOException ioe) {
			TestSession.logger.error("There was a problem getting the job status.");
			ioe.printStackTrace();
		}
		
		return state;
	}
	
	/**
	 * Get the name of the Job through the Hadoop API.
	 * 
	 * @return String the name of the job.
	 */
	public String getJobName() {
		String name = null;

		name = this.getHadoopJob().getJobName();
		TestSession.logger.debug("Job Name: " + name);
		
		return name;
	}
	
	/**
	 * Get the Hadoop API RunningJob that is represented by this job.
	 * 
	 * @return RunningJob the Hadoop API job represented by this job.
	 */
	public RunningJob getHadoopJob() {
		RunningJob job = null;
		
		try {
			JobClient jobClient = new JobClient(TestSession.cluster.getConf());
			JobID jobID = new JobID();
			jobID = JobID.forName(this.ID);
			job = jobClient.getJob(jobID);
		}
		catch (IOException ioe) {
			TestSession.logger.error("There was a problem getting the Hadoop job.");
			ioe.printStackTrace();
		}
		
		return job;
	}
	
	/**
	 * Sets a user for the job other than the default.
	 * 
	 * @param user The user to override the default user with.
	 */
	public void setUser(String user) {
		this.USER = user;
	}
	
	/**
	 * Sets a queue for the job other than the default.
	 * 
	 * @param queue The queue to override the default queue with.
	 */
	public void setQueue(String queue) {
		this.QUEUE = queue;
	}
	
	/**
	 * Set whether the job should wait for the ID in the output before proceeding.
	 * 
	 * If false, an ID will not be set and many functions of Job will not work 
	 * properly.  This should only be used when submitting many jobs to a cluster
	 * and the resulting state of the job is irrelevant.
	 * 
	 * @param setID whether we should wait for the job to initialize the ID.
	 */
	public void setJobInitSetID(boolean setID) {
		this.jobInitSetID = setID;
	}
	
	/**
	 * Fails a job, assuming that a maximum of 1 map task attempts needs to
	 * be failed to fail the job.
	 * 
	 * @return boolean Whether the job was successfully failed.
	 */
	public boolean fail() {
		return fail(1);
	}
	
	/**
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

		String[] mapredCmd = { 
				TestSession.cluster.getConf().getHadoopProp("MAPRED_BIN"), 
				"--config", TestSession.cluster.getConf().getHadoopConfDirPath(),
				"job", "-status", this.ID };
		
		TestSession.logger.debug(mapredCmd);

		String mapredPatternStr = "(.*)(Job state: FAILED)(.*)";
		Pattern mapredPattern = Pattern.compile(mapredPatternStr);
		
		// Greps the job status output to see if it failed the job 

		try {
			mapredProc = TestSession.exec.runHadoopProcBuilderGetProc(mapredCmd, this.USER);
			BufferedReader reader=new BufferedReader(new InputStreamReader(mapredProc.getInputStream())); 
			String line=reader.readLine(); 
			while(line!=null) 
			{ 
				TestSession.logger.debug(line);
				
				Matcher mapredMatcher = mapredPattern.matcher(line);
				
				if (mapredMatcher.find()) {
					TestSession.logger.info("JOB " + this.ID + " WAS FAILED");
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

		TestSession.logger.error("JOB " + this.ID + " WAS NOT FAILED");
		return false; // job didn't fail
	}
	
	/**
	 * Kills the job.
	 * 
	 * @return boolean Whether the job was successfully killed.
	 */
	public boolean kill() {

		Process mapredProc = null;
		
		String[] mapredCmd = {
				TestSession.cluster.getConf().getHadoopProp("MAPRED_BIN"), 
				"--config", TestSession.cluster.getConf().getHadoopConfDirPath(),
				"job", "-kill", this.ID };

		TestSession.logger.debug(mapredCmd);

		String mapredPatternStr = "(.*)(Killed job " + this.ID + ")(.*)";
		Pattern mapredPattern = Pattern.compile(mapredPatternStr);
		
		try {
			mapredProc = TestSession.exec.runHadoopProcBuilderGetProc(mapredCmd, this.USER);
			BufferedReader reader=new BufferedReader(new InputStreamReader(mapredProc.getInputStream())); 
			String line=reader.readLine(); 
			while(line!=null) 
			{ 
				TestSession.logger.debug(line);
				
				Matcher mapredMatcher = mapredPattern.matcher(line);
				
				if (mapredMatcher.find()) {
					TestSession.logger.info("JOB " + this.ID + " WAS KILLED");
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
		
		TestSession.logger.error("JOB " + this.ID + " WAS NOT KILLED");
		return false;
	}
	
	/**
	 * Verifies that the job ID matches the expected format.
	 * 
	 * @return boolean Whether the job ID matches the expected format.
	 */
	public boolean verifyID() {
		if (this.ID == "0") {
			TestSession.logger.error("JOB ID DID NOT MATCH FORMAT AND WAS ZERO");
			return false;
		}

		String jobPatternStr = "job_(.*)$";
		Pattern jobPattern = Pattern.compile(jobPatternStr);
		
		Matcher jobMatcher = jobPattern.matcher(this.ID);
		
		if (jobMatcher.find()) {
			TestSession.logger.info("JOB ID MATCHED EXPECTED FORMAT");
			TestSession.logger.info("JOB ID: " + this.ID);
			return true;
		}
		else {
			TestSession.logger.error("JOB ID DID NOT MATCH FORMAT");
			return false;
		}
	}
	
	/**
	 * Get the map task attempt ID associated with the specified job ID.
	 * 
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
		
		TestSession.logger.info("MAP TASK ID = " + taskID);
		
		return taskID;
	}
	
	/**
	 * Get the reduce task attempt ID associated with the specified job ID.
	 * 
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
		
		TestSession.logger.info("REDUCE TASK ID = " + taskID);
		
		return taskID;
	}
	
	/**
	 * Sleep while waiting for a job ID.
	 * 
	 * @param seconds the number of seconds to wait for a job ID.
	 * @return boolean whether an ID was found or not within the specified
	 * 					time interval.
	 */
	public boolean waitForID(int seconds) {

		// Give the job time to associate with a job ID
		for (int i = 0; i <= seconds; i++) {
			if (this.ID.equals("0")) {
				Util.sleep(1);
			}
			else {
				return true;
			}
		}
		
		return false;
	}
	
	/**
	 * Waits indefinitely for the job to succeed, and returns true for success.
	 * Uses the Hadoop API to check status of the job.
	 * 
	 * @return boolean whether the job succeeded
	 */
	public boolean waitForSuccess() {
		return this.waitForSuccess(0);
	}
	
	/**
	 * Waits for the specified number of minutes for the job to 
	 * succeed, and returns true for success.
	 * Uses the Hadoop API to check status of the job.
	 * 
	 * @param minutes The number of minutes to wait for the success state.
	 * 
	 * @return boolean true if the job was successful, false if it was not or the waitFor timed out.
	 */
	public boolean waitForSuccess(int minutes) {

		JobState currentState = JobState.UNKNOWN;
		
		// Give the sleep job time to complete
		for (int i = 0; i <= (minutes * 6); i++) {

			currentState = this.getJobStatus();
			if (currentState.equals(JobState.SUCCEEDED)) {
				TestSession.logger.info("JOB " + this.ID + " SUCCEEDED");
				return true;
			}
			else if (currentState.equals(JobState.PREP)) {
				TestSession.logger.error("JOB " + this.ID + " IS STILL IN PREP STATE");
			}
			else if (currentState.equals(JobState.RUNNING)) {
				TestSession.logger.error("JOB " + this.ID + " IS STILL RUNNING");
			}
			else if (currentState.equals(JobState.FAILED)) {
				TestSession.logger.error("JOB " + this.ID + " FAILED");
				return false;
			}
			else if (currentState.equals(JobState.KILLED)) {
				TestSession.logger.error("JOB " + this.ID + " WAS KILLED");
				return false;
			}

			Util.sleep(10);
		}

		TestSession.logger.error("JOB " + this.ID + " didn't SUCCEED within the timeout window.");
		return false;
	}
	
	/**
	 * Waits indefinitely for the job to succeed, and returns true for success.
	 * Uses the Hadoop command line interface to check status of the job.
	 * 
	 * @return boolean whether the job succeeded
	 */
	public boolean waitForSuccessCLI() {
		return this.waitForSuccessCLI(0);
	}
	
	/**
	 * Waits for the specified number of minutes for the job to 
	 * succeed, and returns true for success.
	 * Uses the Hadoop command line interface to check status of the job.
	 * 
	 * @param minutes The number of minutes to wait for the success state.
	 * 
	 * @return boolean true if the job was successful, false if it was not or the waitFor timed out.
	 */
	public boolean waitForSuccessCLI(int minutes) {
		Process mapredProc = null;

		Matcher mapredMatcherSuccess;
		Matcher mapredMatcherAppStatusSuccess;
		Matcher mapredMatcherFailed;
		Matcher mapredMatcherKilled;
		Matcher mapredMatcherPrep;
		Matcher mapredMatcherRunning;
		
		String[] mapredCmd = {
				TestSession.cluster.getConf().getHadoopProp("MAPRED_BIN"), 
				"--config", TestSession.cluster.getConf().getHadoopConfDirPath(),
				"job", "-status", this.ID };
		
		TestSession.logger.debug(mapredCmd);

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

		// Give the sleep job time to complete
		for (int i = 0; i <= (minutes * 6); i++) {
		
			try {
				mapredProc = TestSession.exec.runHadoopProcBuilderGetProc(mapredCmd, this.USER);
				BufferedReader reader=new BufferedReader(new InputStreamReader(mapredProc.getInputStream())); 
				String line=reader.readLine(); 
				while(line!=null) 
				{ 
					TestSession.logger.debug(line);

					mapredMatcherSuccess = mapredPatternSuccess.matcher(line);
					mapredMatcherAppStatusSuccess = mapredAppStatusPatternSuccess.matcher(line);
					mapredMatcherFailed = mapredPatternFailed.matcher(line);
					mapredMatcherKilled = mapredPatternKilled.matcher(line);
					mapredMatcherPrep = mapredPatternPrep.matcher(line);
					mapredMatcherRunning = mapredPatternRunning.matcher(line);

					if (mapredMatcherSuccess.find()) {
						TestSession.logger.info("JOB " + this.ID + " SUCCEEDED");
						return true;
					}
					else if (mapredMatcherAppStatusSuccess.find()) {
						TestSession.logger.info("JOB " + this.ID + " SUCCEEDED");
						return true;
					}
					else if (mapredMatcherFailed.find()) {
						TestSession.logger.error("JOB " + this.ID + " FAILED");
						return false;
					}
					else if (mapredMatcherKilled.find()) {
						TestSession.logger.error("JOB " + this.ID + " WAS KILLED");
						return false;
					}
					else if (mapredMatcherPrep.find()) {
						TestSession.logger.info("JOB " + this.ID + " IS STILL IN PREP STATE");
					}
					else if (mapredMatcherRunning.find()) {
						TestSession.logger.info("JOB " + this.ID + " IS STILL RUNNING");
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

		TestSession.logger.error("JOB " + this.ID + " didn't SUCCEED within the timeout window.");
		return false;
	}
	
	/**
	 * Waits for the specified number of minutes for the job to 
	 * meet a specified state, and returns true for successfully reaching the state.
	 * waitForCLI uses the hadoop/mapred command line interface to check the
	 * status of the job.
	 * 
	 * @param waitForState the job state to wait for
	 * @param seconds The number of minutes to wait for the job state.
	 */
	public boolean waitForCLI(JobState waitForState, int seconds) {
		Process mapredProc = null;
		JobState currentState = null;

		Matcher mapredMatcherSuccess = null;
		Matcher mapredMatcherAppStatusSuccess = null;
		Matcher mapredMatcherFailed = null;
		Matcher mapredMatcherKilled = null;
		Matcher mapredMatcherPrep = null;
		Matcher mapredMatcherRunning = null;
		
		String[] mapredCmd = {
				TestSession.cluster.getConf().getHadoopProp("MAPRED_BIN"), 
				"--config", TestSession.cluster.getConf().getHadoopConfDirPath(),
				"job", "-status", this.ID };
		
		TestSession.logger.debug(mapredCmd);

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
		
		// Give the sleep job time to complete
		for (int i = 0; i <= seconds; i = i + 10) {
		
			try {
				mapredProc = TestSession.exec.runHadoopProcBuilderGetProc(mapredCmd, this.USER);
				BufferedReader reader=new BufferedReader(new InputStreamReader(mapredProc.getInputStream())); 
				String line=reader.readLine(); 
				while(line!=null) 
				{ 
					TestSession.logger.debug(line);

					mapredMatcherSuccess = mapredPatternSuccess.matcher(line);
					mapredMatcherAppStatusSuccess = mapredAppStatusPatternSuccess.matcher(line);
					mapredMatcherFailed = mapredPatternFailed.matcher(line);
					mapredMatcherKilled = mapredPatternKilled.matcher(line);
					mapredMatcherPrep = mapredPatternPrep.matcher(line);
					mapredMatcherRunning = mapredPatternRunning.matcher(line);

					if (mapredMatcherSuccess.find()) {
						currentState = JobState.SUCCEEDED;
						break;
					}
					else if (mapredMatcherAppStatusSuccess.find()) {
						currentState = JobState.SUCCEEDED;
						break;
					}
					else if (mapredMatcherFailed.find()) {
						currentState = JobState.FAILED;
						break;
					}
					else if (mapredMatcherKilled.find()) {
						currentState = JobState.KILLED;
						break;
					}
					else if (mapredMatcherPrep.find()) {
						currentState = JobState.PREP;
						break;
					}
					else if (mapredMatcherRunning.find()) {
						currentState = JobState.RUNNING;
						break;
					}
					else {
						currentState = JobState.UNKNOWN;
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

			TestSession.logger.info("Job " + this.ID + " is in state: " + currentState.toString());
			
			if (currentState.equals(waitForState)) {
				TestSession.logger.info("Job state was successfully reached: " + waitForState.toString());
				return true;
			}
			
			Util.sleep(10);
		}

		TestSession.logger.error("JOB " + this.ID + " didn't meet the specified state within the timeout window: " + waitForState.toString());
		return false;
	}
	
	/**
	 * Waits for the specified number of minutes for the job to 
	 * meet a specified state, and returns true for successfully reaching the state.
	 * waitFor uses the Hadoop API to get the status of the job.
	 * 
	 * @param waitForState the job state to wait for
	 * @param seconds The number of minutes to wait for the job state.
	 */
	public boolean waitFor(JobState waitForState, int seconds) {
		JobState currentState = null;
		
		// Give the sleep job time to complete
		for (int i = 0; i <= seconds; i = i + 10) {
		
			currentState = this.getJobStatus();

			TestSession.logger.info("Job " + this.ID + " is in state: " + currentState.toString());
			
			if (currentState.equals(waitForState)) {
				TestSession.logger.info("Job state was successfully reached: " + waitForState.toString());
				return true;
			}
			
			Util.sleep(10);
		}

		TestSession.logger.error("JOB " + this.ID + " didn't meet the specified state within the timeout window: " + waitForState.toString());
		return false;
	}
	
	/**
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

		TestSession.logger.info("Sleeping for 200s to wait for the job summary info log to be updated.");
		//Util.sleep(200);

		String HADOOP_INSTALL = TestSession.cluster.getConf().getHadoopProp("HADOOP_INSTALL");
		FileInputStream summaryInfoFile = new FileInputStream(HADOOP_INSTALL + "/logs/hadoop-mapreduce.jobsummary.log");
		DataInputStream in = new DataInputStream(summaryInfoFile);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		String line;
		Matcher infoMatcher;
		Boolean foundSummaryInfo = false;

		while ((line = br.readLine()) != null)   {
			TestSession.logger.debug("JOB SUMMARY INFO: " + line);
			infoMatcher = infoPattern.matcher(line);
			if (infoMatcher.find()) {
				foundSummaryInfo = true; 
				TestSession.logger.info("Summary info for the job was found.");
				}
		}

		in.close();

		if (!foundSummaryInfo) {
			TestSession.logger.error("Job summary info was not found in the log file.");
		}
		
		return foundSummaryInfo;
	}
	

	/**
	 * Kills the task attempt associated with the specified task ID.
	 * 
	 * @param taskID The ID of the task attempt to kill.
	 * @return boolean Whether the task attempt was killed or not.
	 */
	public boolean killTaskAttempt(String taskID) {
		
		Process mapredProc = null;
		
		String[] mapredCmd = {
				TestSession.cluster.getConf().getHadoopProp("MAPRED_BIN"), 
				"--config", TestSession.cluster.getConf().getHadoopConfDirPath(),
				"job", "-kill-task", taskID };
		
		TestSession.logger.debug(mapredCmd);
		
		String mapredPatternStr = "(.*)(Killed task " + taskID + ")(.*)";
		Pattern mapredPattern = Pattern.compile(mapredPatternStr);
		
		try {
			mapredProc = TestSession.exec.runHadoopProcBuilderGetProc(mapredCmd, this.USER);
			BufferedReader reader=new BufferedReader(new InputStreamReader(mapredProc.getInputStream())); 
			String line=reader.readLine(); 
			while(line!=null) 
			{ 
				TestSession.logger.debug(line);
				
				Matcher mapredMatcher = mapredPattern.matcher(line);
				
				if (mapredMatcher.find()) {
					TestSession.logger.info("TASK ATTEMPT " + taskID + " WAS KILLED");
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

		TestSession.logger.error("TASK ATTEMPT " + taskID + " WAS NOT KILLED");
		return false;
	}
	
	/**
	 * Fails the task attempt associated with the specified task ID.
	 * 
	 * @param taskID The ID of the job matching the task attempt.
	 * @return boolean Whether the task attempt was killed or not.
	 */
	protected boolean failTaskAttempt(String taskID) {
		
		Process mapredProc = null;
		
		String[] mapredCmd = {
				TestSession.cluster.getConf().getHadoopProp("MAPRED_BIN"), 
				"--config", TestSession.cluster.getConf().getHadoopConfDirPath(),
				"job", "-fail-task", taskID };

		TestSession.logger.debug(mapredCmd);
		
		String mapredPatternStr = "(.*)(Killed task " + taskID + " by failing it)(.*)";
		Pattern mapredPattern = Pattern.compile(mapredPatternStr);
		
		try {
			mapredProc = TestSession.exec.runHadoopProcBuilderGetProc(mapredCmd, this.USER);
			BufferedReader reader=new BufferedReader(new InputStreamReader(mapredProc.getInputStream())); 
			String line=reader.readLine(); 
			while(line!=null) 
			{ 
				TestSession.logger.debug(line);
				
				Matcher mapredMatcher = mapredPattern.matcher(line);
				
				if (mapredMatcher.find()) {
					TestSession.logger.info("TASK ATTEMPT " + taskID + " WAS FAILED");
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
		
		TestSession.logger.error("TASK ATTEMPT " + taskID + " WAS NOT FAILED");
		return false;
	}
	
}

