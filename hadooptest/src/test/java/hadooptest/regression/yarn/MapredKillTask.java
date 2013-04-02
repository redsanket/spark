/*
 * YAHOO!
 */

package hadooptest.regression.yarn;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import hadooptest.TestSession;
import hadooptest.config.TestConfiguration;
import hadooptest.job.SleepJob;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

/*
 * A test suite used to exercise the ability to kill task attempts from a MapReduce sleep job.
 */
public class MapredKillTask extends TestSession {
	
	private SleepJob sleepJob;
	
	private static final int MAPREDUCE_MAP_MAXATTEMPTS = 4;
	private static final int MAPREDUCE_REDUCE_MAXATTEMPTS = 4;

	@BeforeClass
	public static void startTestSession() {
		TestSession.start();
		
		TestConfiguration clusterConfig = cluster.getConf();
		
		clusterConfig.set("mapreduce.map.maxattempts", Integer.toString(MAPREDUCE_MAP_MAXATTEMPTS));
		clusterConfig.set("mapreduce.reduce.maxattempts", Integer.toString(MAPREDUCE_REDUCE_MAXATTEMPTS));
		
		cluster.setConf(clusterConfig);
	}

	/*
	 * Before each test, we much initialize the sleep job and verify that its job ID is valid.
	 */
	@Before
	public void initTestJob() {
		try{
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
		catch (Exception e) {
			logger.error("Exception failure.", e);
			fail();
		}
	}
	
	/*
	 * After each test, we must reset the state of the cluster to a known default state.
	 */
	@After
	public void resetClusterState() {
		try {
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
		catch (IOException ioe) {
			logger.error("Exception failure.", ioe);
			fail();
		}
	}
	
	/*
	 * A test which attempts to kill a running task from a sleep job.
	 */
	@Test
	public void killRunningTask() {	
		try{
			this.killTask();
		}
		catch (Exception e) {
			logger.error("Exception failure.", e);
			fail();
		}
	}
	
	/*
	 * A test which attempts to kill a task from a sleep job which has already been killed.
	 * Kills the job using the mapred CLI.
	 */
	@Test
	public void killTaskOfAlreadyKilledJobCLI() {
		try{
			assertTrue("Was not able to kill the job.", 
					sleepJob.killCLI());

			this.killTask();
		}
		catch (Exception e) {
			logger.error("Exception failure.", e);
			fail();
		}
	}

	/*
	 * A test which attempts to kill a task from a sleep job which has already been killed.
	 * Kills the job using the hadoop API.
	 */
	@Test
	public void killTaskOfAlreadyKilledJobAPI() {
		try{
			assertTrue("Was not able to kill the job.", 
					sleepJob.kill());

			this.killTask();
		}
		catch (Exception e) {
			logger.error("Exception failure.", e);
			fail();
		}
	}
	
	/*
	 * A test which attempts to kill a task from a sleep job which has failed.
	 */
	@Test
	public void killTaskOfAlreadyFailedJob() {
		try{
			assertTrue("Was not able to fail the job.", 
					sleepJob.fail(MAPREDUCE_MAP_MAXATTEMPTS));

			this.killTask();
		}
		catch (Exception e) {
			logger.error("Exception failure.", e);
			fail();
		}
	}
	
	/*
	 * A test which attempts to kill a task from a sleep job which has already succeeded,
	 * using the Hadoop CLI to check job state.
	 */
	@Test
	public void killTaskOfAlreadyCompletedJobCLI() {
		try{
			assertTrue("Job did not succeed.",
					sleepJob.waitForSuccessCLI(2));

			String taskID = sleepJob.getMapTaskAttemptID();
			assertFalse("Killed task and we shouldn't have been able to.", sleepJob.killTaskAttempt(taskID));
		}
		catch (Exception e) {
			logger.error("Exception failure.", e);
			fail();
		}
	}

	/*
	 * A test which attempts to kill a task from a sleep job which has already succeeded,
	 * using the Hadoop API to check job state.
	 */
	@Test
	public void killTaskOfAlreadyCompletedJobAPI() {
		try{
			assertTrue("Job did not succeed.",
					sleepJob.waitForSuccess(2));

			String taskID = sleepJob.getMapTaskAttemptID();
			assertFalse("Killed task and we shouldn't have been able to.", sleepJob.killTaskAttempt(taskID));
		}
		catch (Exception e) {
			logger.error("Exception failure.", e);
			fail();
		}
	}

	/*
	 * A helper method to get the map task attempt ID, and kill the task attempt.
	 */
	private void killTask() {
		try{
			String taskID = sleepJob.getMapTaskAttemptID();
			assertTrue("Killed task message doesn't exist, we weren't able to kill the task.", 
					sleepJob.killTaskAttempt(taskID));
		}
		catch (Exception e) {
			logger.error("Exception failure.", e);
			fail();
		}
	}
	
}
