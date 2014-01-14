package hadooptest.hadoop.regression.yarn;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertTrue;
import hadooptest.TestSession;
import hadooptest.workflow.hadoop.job.FailJob;
import hadooptest.workflow.hadoop.job.SleepJob;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

public class JobSummaryInfo extends TestSession {
	
	private SleepJob sleepJob;
	private FailJob failJob;
	
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

			if (failJob != null) {
				if (failJob.getID() != "0" && failJob.kill()) {
					logger.info("Cleaned up latent job by killing it: " + failJob.getID());
				}
				else {
					logger.info("Fail job never started, no need to clean up.");
				}
			}
			else {
				logger.info("Job was already killed or never started, no need to clean up.");
			}
		}
		catch (Exception e) {
			TestSession.logger.error("Exception failure.", e);
			fail();
		}
	}
	
	/*
	 * A test to check the job summary information after successful job completion.
	 * 
	 * Equivalent to JobSummaryInfo10 in the original shell script YARN regression suite.
	 */
	@Ignore("Known not working.")
	@Test
	public void JobSummaryInfoSuccess() {
		try {
			sleepJob = new SleepJob();

			sleepJob.setNumMappers(10);
			sleepJob.setNumReducers(10);
			sleepJob.setMapDuration(500);
			sleepJob.setReduceDuration(500);

			sleepJob.start();

			assertTrue("Sleep job was not assigned an ID within 5 seconds.", 
					sleepJob.waitForID(5));
			assertTrue("Sleep job ID is invalid.", 
					sleepJob.verifyID());

			assertTrue("Job did not succeed.",
					sleepJob.waitForSuccess());

			assertTrue("Did not find job summary info.", sleepJob.findSummaryInfo("SUCCEEDED", "Sleep\\sjob", conf.getProperty("USER"), "default"));
		}
		catch (Exception e) {
			TestSession.logger.error("Exception failure.", e);
			fail();
		}
	}
	
	/*
	 * A test to check the job summary information for High RAM jobs.
	 * 
	 * Equivalent to JobSummaryInfo20 in the original shell script YARN regression suite.
	 */
	@Ignore("Known not working.")
	@Test
	public void JobSummaryInfoHighRAM() {
		try {
			sleepJob = new SleepJob();

			sleepJob.setNumMappers(10);
			sleepJob.setNumReducers(10);
			sleepJob.setMapDuration(500);
			sleepJob.setReduceDuration(500);
			sleepJob.setMapMemory(6144);
			sleepJob.setReduceMemory(8192);

			sleepJob.start();

			assertTrue("Sleep job was not assigned an ID within 5 seconds.", 
					sleepJob.waitForID(5));
			assertTrue("Sleep job ID is invalid.", 
					sleepJob.verifyID());

			assertTrue("Job did not succeed.",
					sleepJob.waitForSuccess());

			assertTrue("Did not find job summary info.", sleepJob.findSummaryInfo("SUCCEEDED", "Sleep\\sjob", conf.getProperty("USER"), "default"));

		}
		catch (Exception e) {
			TestSession.logger.error("Exception failure.", e);
			fail();
		}
	}
	
	/*
	 * A test to check the job summary information for failed jobs where the mappers failed.
	 * 
	 * Equivalent to JobSummaryInfo30 in the original shell script YARN regression suite.
	 */
	@Ignore("Known not working.")
	@Test
	public void JobSummaryInfoMappersFailed() {
		try {
			failJob = new FailJob();

			failJob.setMappersFail(true);
			failJob.setReducersFail(false);

			failJob.start();

			assertTrue("Fail job was not assigned an ID within 5 seconds.", 
					failJob.waitForID(5));
			assertTrue("Fail job ID is invalid.", 
					failJob.verifyID());

			assertFalse("Job did not fail.",
					failJob.waitForSuccess());

			assertTrue("Did not find job summary info.", failJob.findSummaryInfo("FAILED", "Fail\\sjob", conf.getProperty("USER"), "default"));
		}
		catch (Exception e) {
			TestSession.logger.error("Exception failure.", e);
			fail();
		}
	}
	
	/*
	 * A test to check the job summary information for failed jobs where the reducers failed.
	 * 
	 * Equivalent to JobSummaryInfo40 in the original shell script YARN regression suite.
	 */
	@Ignore("Known not working.")
	@Test
	public void JobSummaryInfoReducersFailed() {
		try {
			failJob = new FailJob();

			failJob.setMappersFail(false);
			failJob.setReducersFail(true);

			failJob.start();

			assertTrue("Fail job was not assigned an ID within 5 seconds.", 
					failJob.waitForID(5));
			assertTrue("Fail job ID is invalid.", 
					failJob.verifyID());

			assertFalse("Job did not fail.",
					failJob.waitForSuccess());

			assertTrue("Did not find job summary info.", failJob.findSummaryInfo("FAILED", "Fail\\sjob", conf.getProperty("USER"), "default"));	
		}
		catch (Exception e) {
			TestSession.logger.error("Exception failure.", e);
			fail();
		}
	}
	
	/*
	 * A test to check the job summary information when job is submitted as another user.
	 * 
	 * Equivalent to JobSummaryInfo50 in the original shell script YARN regression suite.
	 */
	@Ignore("Known not working.")
	@Test
	public void JobSummaryInfoDifferentUser() {
		try {
			sleepJob = new SleepJob();

			sleepJob.setUser("testuser");
			sleepJob.setNumMappers(10);
			sleepJob.setNumReducers(10);
			sleepJob.setMapDuration(500);
			sleepJob.setReduceDuration(500);

			sleepJob.start();

			assertTrue("Sleep job was not assigned an ID within 5 seconds.", 
					sleepJob.waitForID(5));
			assertTrue("Sleep job ID is invalid.", 
					sleepJob.verifyID());

			assertTrue("Job did not succeed.",
					sleepJob.waitForSuccess());

			assertTrue("Did not find job summary info.", sleepJob.findSummaryInfo("FAILED", "Sleep\\sjob", "testuser", "default"));	
		}
		catch (Exception e) {
			TestSession.logger.error("Exception failure.", e);
			fail();
		}
	}
	
	/*
	 * A test to check the job summary information when job is submitted to a different queue.
	 * 
	 * Equivalent to JobSummaryInfo60 in the original shell script YARN regression suite.
	 */
	@Ignore("Known not working.")
	@Test
	public void JobSummaryInfoDifferentQueue() throws IOException, FileNotFoundException {
		try {
			// Start sleep job with mapreduce.job.queuename=grideng 
			sleepJob = new SleepJob();

			sleepJob.setQueue("testQueue");
			sleepJob.setNumMappers(10);
			sleepJob.setNumReducers(10);
			sleepJob.setMapDuration(500);
			sleepJob.setReduceDuration(500);

			sleepJob.start();

			assertTrue("Sleep job was not assigned an ID within 5 seconds.", 
					sleepJob.waitForID(5));
			assertTrue("Sleep job ID is invalid.", 
					sleepJob.verifyID());

			assertTrue("Job did not succeed.",
					sleepJob.waitForSuccess());

			assertTrue("Did not find job summary info.", sleepJob.findSummaryInfo("FAILED", "Sleep\\sjob", conf.getProperty("USER"), "testQueue"));	
		}
		catch (Exception e) {
			TestSession.logger.error("Exception failure.", e);
			fail();
		}
	}
	
	/*
	 * A test to check the job summary information for killed jobs.
	 * 
	 * Equivalent to JobSummaryInfo70 in the original shell script YARN regression suite.
	 */
	@Ignore("Known not working.")
	@Test
	public void JobSummaryInfoKilledJob() throws IOException, FileNotFoundException {
		try {
			sleepJob = new SleepJob();

			sleepJob.setNumMappers(10);
			sleepJob.setNumReducers(10);
			sleepJob.setMapDuration(500);
			sleepJob.setReduceDuration(500);

			sleepJob.start();

			assertTrue("Sleep job was not assigned an ID within 5 seconds.", 
					sleepJob.waitForID(5));
			assertTrue("Sleep job ID is invalid.", 
					sleepJob.verifyID());

			assertTrue("Was not able to kill the job.", 
					sleepJob.kill());

			assertTrue("Did not find job summary info.", sleepJob.findSummaryInfo("KILLED", "Sleep\\sjob", conf.getProperty("USER"), "default"));
		}
		catch (Exception e) {
			TestSession.logger.error("Exception failure.", e);
			fail();
		}
	}

}
