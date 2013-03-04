package hadooptest.regression.yarn;

import static org.junit.Assert.assertTrue;
import hadooptest.TestSession;
import hadooptest.job.JobState;
import hadooptest.job.SleepJob;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/*
 * Tests to exercise the Job class waitFor(JobState, int) method.
 */
public class WaitForJobState extends TestSession {
	
	private SleepJob sleepJob;

	@BeforeClass
	public static void startTestSession() {
		TestSession.start();
	}

	/*
	 * Before each test, we much initialize the sleep job and verify that its job ID is valid.
	 */
	@Before
	public void initTestJob() {
		sleepJob = new SleepJob();
		
		sleepJob.setNumMappers(5);
		sleepJob.setNumReducers(5);
		sleepJob.setMapDuration(500);
		sleepJob.setReduceDuration(500);
		
		sleepJob.start();
		assertTrue("Sleep job was not assigned an ID within 5 seconds.", 
				sleepJob.waitForID(5));
		assertTrue("Sleep job ID is invalid.", 
				sleepJob.verifyID());
	}
	
	/*
	 * After each test, we must reset the state of the cluster to a known default state.
	 */
	@After
	public void resetClusterState() {
		if (sleepJob != null) {
			if (sleepJob.getID() != "0" && sleepJob.kill()) {
				logger.info("Cleaned up latent job by killing it: " + sleepJob.getID());
			}
			else {
				logger.info("Sleep job never started, no need to clean up.");
			}
		}
		else {
			logger.info("Job was already killed or never started, no need to clean up.");
		}
	}
	
	@Test
	public void waitForPrepAPI() {
		assertTrue("Job did not meet specified state:" + JobState.PREP.toString(),
				sleepJob.waitFor(JobState.PREP, 60));
	}
	
	@Test
	public void waitForRunningAPI() {
		assertTrue("Job did not meet specified state:" + JobState.RUNNING.toString(),
				sleepJob.waitFor(JobState.RUNNING, 120));
	}
	
	@Test
	public void waitForSucceededAPI() {
		assertTrue("Job did not meet specified state:" + JobState.SUCCEEDED.toString(),
				sleepJob.waitFor(JobState.SUCCEEDED, 180));
	}
	
	@Test
	public void waitForKilledAPI() {
		sleepJob.kill();
		assertTrue("Job did not meet specified state:" + JobState.KILLED.toString(),
				sleepJob.waitFor(JobState.KILLED, 180));
		
	}
	
	@Test
	public void waitForPrepCLI() {
		assertTrue("Job did not meet specified state:" + JobState.PREP.toString(),
				sleepJob.waitForCLI(JobState.PREP, 60));
	}
	
	@Test
	public void waitForRunningCLI() {
		assertTrue("Job did not meet specified state:" + JobState.RUNNING.toString(),
				sleepJob.waitForCLI(JobState.RUNNING, 120));
	}
	
	@Test
	public void waitForSucceededCLI() {
		assertTrue("Job did not meet specified state:" + JobState.SUCCEEDED.toString(),
				sleepJob.waitForCLI(JobState.SUCCEEDED, 180));
	}
	
	@Test
	public void waitForKilledCLI() {
		sleepJob.kill();
		assertTrue("Job did not meet specified state:" + JobState.KILLED.toString(),
				sleepJob.waitForCLI(JobState.KILLED, 180));
		
	}
	
}
