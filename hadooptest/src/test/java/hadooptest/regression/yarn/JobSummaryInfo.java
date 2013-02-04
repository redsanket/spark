package hadooptest.regression.yarn;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import hadooptest.TestSession;
import hadooptest.cluster.FailJobFactory;
import hadooptest.cluster.Job;
import hadooptest.cluster.SleepJobFactory;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class JobSummaryInfo extends TestSession {
	
	private Job sleepJob;
	private Job failJob;
	
	/******************* CLASS BEFORE/AFTER ***********************/
	
	@BeforeClass
	public static void startTestSession() throws IOException {
		TestSession.start();
		cluster.start();
	}
	
	/*
	 * Cluster cleanup that should happen after running tests in the class instance.
	 */
	@AfterClass
	public static void stopCluster() throws IOException {
		cluster.stop();
		cluster.getConf().cleanup();
	}
	
	/******************* TEST BEFORE/AFTER ***********************/
	
	/*
	 * Before each test.
	 */
	@Before
	public void initTest() {
	}
	
	/*
	 * After each test, we must reset the state of the cluster to a known default state.
	 */
	@After
	public void resetClusterState() {
		if (sleepJob != null) {
			if (sleepJob.ID != "0" && sleepJob.kill()) {
				logger.info("Cleaned up latent job by killing it: " + sleepJob.ID);
			}
			else {
				logger.info("Sleep job never started, no need to clean up.");
			}
		}
		else {
			logger.info("Job was already killed or never started, no need to clean up.");
		}
		
		if (failJob != null) {
			if (failJob.ID != "0" && failJob.kill()) {
				logger.info("Cleaned up latent job by killing it: " + failJob.ID);
			}
			else {
				logger.info("Fail job never started, no need to clean up.");
			}
		}
		else {
			logger.info("Job was already killed or never started, no need to clean up.");
		}
	}
	
	/******************* TESTS ***********************/	
	
	/*
	 * A test to check the job summary information after successful job completion.
	 * 
	 * Equivalent to JobSummaryInfo10 in the original shell script YARN regression suite.
	 */
	@Ignore("Known not working.")
	@Test
	public void JobSummaryInfoSuccess() throws IOException, FileNotFoundException {
		sleepJob = SleepJobFactory.getSleepJob();
		sleepJob.submit(10, 10, 500, 500, 1, -1, -1);
		assertTrue("Sleep job ID is invalid.", 
				sleepJob.verifyID());
		
		assertTrue("Job did not succeed.",
				sleepJob.waitForSuccess());
		
		assertTrue("Did not find job summary info.", sleepJob.findSummaryInfo("SUCCEEDED", "Sleep\\sjob", conf.getProperty("USER"), "default"));
	}
	
	/*
	 * A test to check the job summary information for High RAM jobs.
	 * 
	 * Equivalent to JobSummaryInfo20 in the original shell script YARN regression suite.
	 */
	@Ignore("Known not working.")
	@Test
	public void JobSummaryInfoHighRAM() throws IOException, FileNotFoundException  {
		sleepJob = SleepJobFactory.getSleepJob();
		sleepJob.submit(10, 10, 500, 500, 1, 6144, 8192);
		assertTrue("Sleep job ID is invalid.", 
				sleepJob.verifyID());
		
		assertTrue("Job did not succeed.",
				sleepJob.waitForSuccess());
		
		assertTrue("Did not find job summary info.", sleepJob.findSummaryInfo("SUCCEEDED", "Sleep\\sjob", conf.getProperty("USER"), "default"));
	}
	
	/*
	 * A test to check the job summary information for failed jobs where the mappers failed.
	 * 
	 * Equivalent to JobSummaryInfo30 in the original shell script YARN regression suite.
	 */
	@Ignore("Known not working.")
	@Test
	public void JobSummaryInfoMappersFailed() throws IOException, FileNotFoundException {
		failJob = FailJobFactory.getFailJob();
		failJob.submit(true, false);
		assertTrue("Fail job ID is invalid.", 
				failJob.verifyID());
		
		assertFalse("Job did not fail.",
				failJob.waitForSuccess());
		
		assertTrue("Did not find job summary info.", failJob.findSummaryInfo("FAILED", "Fail\\sjob", conf.getProperty("USER"), "default"));
	}
	
	/*
	 * A test to check the job summary information for failed jobs where the reducers failed.
	 * 
	 * Equivalent to JobSummaryInfo40 in the original shell script YARN regression suite.
	 */
	@Ignore("Known not working.")
	@Test
	public void JobSummaryInfoReducersFailed() throws IOException, FileNotFoundException {
		failJob = FailJobFactory.getFailJob();
		failJob.submit(false, true);
		assertTrue("Fail job ID is invalid.", 
				failJob.verifyID());
		
		assertFalse("Job did not fail.",
				failJob.waitForSuccess());
		
		assertTrue("Did not find job summary info.", failJob.findSummaryInfo("FAILED", "Fail\\sjob", conf.getProperty("USER"), "default"));	
	}
	
	/*
	 * A test to check the job summary information when job is submitted as another user.
	 * 
	 * Equivalent to JobSummaryInfo50 in the original shell script YARN regression suite.
	 */
	@Ignore("Known not working.")
	@Test
	public void JobSummaryInfoDifferentUser() throws IOException, FileNotFoundException {
		sleepJob = SleepJobFactory.getSleepJob();
		sleepJob.setUser("testuser");
		sleepJob.submit();
		assertTrue("Sleep job ID is invalid.", 
				sleepJob.verifyID());

		assertTrue("Job did not succeed.",
				sleepJob.waitForSuccess());
		
		assertTrue("Did not find job summary info.", sleepJob.findSummaryInfo("FAILED", "Sleep\\sjob", "testuser", "default"));	
	}
	
	/*
	 * A test to check the job summary information when job is submitted to a different queue.
	 * 
	 * Equivalent to JobSummaryInfo60 in the original shell script YARN regression suite.
	 */
	@Ignore("Known not working.")
	@Test
	public void JobSummaryInfoDifferentQueue() throws IOException, FileNotFoundException {
		// Start sleep job with mapreduce.job.queuename=grideng 
		sleepJob = SleepJobFactory.getSleepJob();
		sleepJob.setQueue("testQueue");
		sleepJob.submit();
		assertTrue("Sleep job ID is invalid.", 
				sleepJob.verifyID());

		assertTrue("Job did not succeed.",
				sleepJob.waitForSuccess());
		
		assertTrue("Did not find job summary info.", sleepJob.findSummaryInfo("FAILED", "Sleep\\sjob", conf.getProperty("USER"), "testQueue"));	
	}
	
	/*
	 * A test to check the job summary information for killed jobs.
	 * 
	 * Equivalent to JobSummaryInfo70 in the original shell script YARN regression suite.
	 */
	@Ignore("Known not working.")
	@Test
	public void JobSummaryInfoKilledJob() throws IOException, FileNotFoundException {
		sleepJob = SleepJobFactory.getSleepJob();
		sleepJob.submit();
		assertTrue("Sleep job ID is invalid.", 
				sleepJob.verifyID());
		
		assertTrue("Was not able to kill the job.", 
				sleepJob.kill());
		
		assertTrue("Did not find job summary info.", sleepJob.findSummaryInfo("KILLED", "Sleep\\sjob", conf.getProperty("USER"), "default"));
	}

	/******************* END TESTS ***********************/	
}
