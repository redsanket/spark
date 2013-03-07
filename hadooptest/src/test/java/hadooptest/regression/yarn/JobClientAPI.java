package hadooptest.regression.yarn;

import static org.junit.Assert.assertTrue;
import hadooptest.TestSession;
import hadooptest.job.SleepJob;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Job.JobState;

import java.io.IOException;

public class JobClientAPI extends TestSession {

	/******************* CLASS BEFORE/AFTER ***********************/

	@BeforeClass
	public static void startTestSession() {
		TestSession.start();
	}

	/******************* TESTS ***********************/

	/*
	 * A test which launches a job and then tries to get information about
	 * it through the Hadoop API.
	 */
	@Test
	public void sleepJobInfoHadoopAPI() throws IOException {


		SleepJob sleepJob;

		sleepJob = new SleepJob();
		sleepJob.setNumMappers(5);
		sleepJob.setNumReducers(5);
		sleepJob.setMapDuration(500);
		sleepJob.setReduceDuration(500);
		
		sleepJob.start();
		
		// Validate the job ID
		assertTrue("Sleep job was not assigned an ID within 10 seconds.", 
				sleepJob.waitForID(10));
		assertTrue("Sleep job ID for sleep job is invalid.", 
				sleepJob.verifyID());
		
		String name = sleepJob.getJobName();
		String state = sleepJob.getJobStatus().toString();
		logger.info("API: JOB NAME = " + name);
		logger.info("API: JOB STATUS = " + state);
		
		assertTrue("Job name was not -Sleep job-.", name.equals("Sleep job"));
		assertTrue("Job status was not -PREP-.", state.equals("PREP"));
	}
	
	@Test
	public void submitSleepJobThruAPI() {
		SleepJob sleepJob = new SleepJob();
		sleepJob.setNumMappers(5);
		sleepJob.setNumReducers(5);
		sleepJob.setMapDuration(500);
		sleepJob.setReduceDuration(500);
		
		sleepJob.start();

		// Validate the job ID
		assertTrue("Sleep job was not assigned an ID within 10 seconds.", 
				sleepJob.waitForID(10));
		assertTrue("Sleep job ID for sleep job is invalid.", 
				sleepJob.verifyID());
		
		String name = sleepJob.getJobName();
		String state = sleepJob.getJobStatus().toString();
		logger.info("API: JOB NAME = " + name);
		logger.info("API: JOB STATUS = " + state);
		
		assertTrue("Job name was not -Sleep job-.", name.equals("Sleep job"));
		assertTrue("Job status was not -PREP-.", state.equals("PREP"));
	}
	
	/******************* END TESTS ***********************/
}
