package hadooptest.hadoop.regression.yarn;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import hadooptest.TestSession;
import hadooptest.workflow.hadoop.job.SleepJob;

import org.junit.Test;

import java.io.IOException;

public class JobClientAPI extends TestSession {

	/******************* TESTS ***********************/

	/*
	 * A test which launches a job and then tries to get information about
	 * it through the Hadoop API.
	 */
	@Test
	public void sleepJobInfoHadoopAPI() throws IOException {
		try {
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
		catch (Exception e) {
			TestSession.logger.error("Exception failure.", e);
			fail();
		}
	}
	
	@Test
	public void submitSleepJobThruAPI() {
		try {
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
		catch (Exception e) {
			TestSession.logger.error("Exception failure.", e);
			fail();
		}
	}
	
	/******************* END TESTS ***********************/
}
