package hadooptest.regression.yarn;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import hadooptest.TestSession;
import hadooptest.cluster.pseudodistributed.PseudoDistributedCluster;
import hadooptest.cluster.pseudodistributed.SleepJob;
import hadooptest.cluster.pseudodistributed.FailJob;

public class JobSummaryInfo {

	private static TestSession testSession;
	
	private SleepJob sleepJob;
	private FailJob failJob;
	private static PseudoDistributedCluster cluster;
	
	/******************* CLASS BEFORE/AFTER ***********************/
	
	/*
	 * Configuration and cluster setup that should happen before running any of the tests in the class instance.
	 */
	@BeforeClass
	public static void startCluster() throws FileNotFoundException, IOException{
		testSession = new TestSession();
		
		cluster = new PseudoDistributedCluster(testSession);
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
				testSession.logger.info("Cleaned up latent job by killing it: " + sleepJob.ID);
			}
			else {
				testSession.logger.info("Sleep job never started, no need to clean up.");
			}
		}
		else {
			testSession.logger.info("Job was already killed or never started, no need to clean up.");
		}
		
		if (failJob != null) {
			if (failJob.ID != "0" && failJob.kill()) {
				testSession.logger.info("Cleaned up latent job by killing it: " + failJob.ID);
			}
			else {
				testSession.logger.info("Fail job never started, no need to clean up.");
			}
		}
		else {
			testSession.logger.info("Job was already killed or never started, no need to clean up.");
		}
	}
	
	/******************* TESTS ***********************/	
	
	/*
	 * A test to check the job summary information after successful job completion.
	 * 
	 * Equivalent to JobSummaryInfo10 in the original shell script YARN regression suite.
	 */
	@Test
	public void JobSummaryInfoSuccess() throws IOException, FileNotFoundException {
		sleepJob = new SleepJob(testSession);
		sleepJob.submit(10, 10, 500, 500, 1, -1, -1);
		assertTrue("Sleep job ID is invalid.", 
				sleepJob.verifyID());
		
		assertTrue("Job did not succeed.",
				sleepJob.waitForSuccess());
		
		assertTrue("Did not find job summary info.", sleepJob.findSummaryInfo("SUCCEEDED", "Sleep\\sjob", testSession.conf.getProperty("USER"), "default"));
	}
	
	/*
	 * A test to check the job summary information for High RAM jobs.
	 * 
	 * Equivalent to JobSummaryInfo20 in the original shell script YARN regression suite.
	 */
	@Test
	public void JobSummaryInfoHighRAM() throws IOException, FileNotFoundException  {
		sleepJob = new SleepJob(testSession);
		sleepJob.submit(10, 10, 500, 500, 1, 6144, 8192);
		assertTrue("Sleep job ID is invalid.", 
				sleepJob.verifyID());
		
		assertTrue("Job did not succeed.",
				sleepJob.waitForSuccess());
		
		assertTrue("Did not find job summary info.", sleepJob.findSummaryInfo("SUCCEEDED", "Sleep\\sjob", testSession.conf.getProperty("USER"), "default"));
	}
	
	/*
	 * A test to check the job summary information for failed jobs where the mappers failed.
	 * 
	 * Equivalent to JobSummaryInfo30 in the original shell script YARN regression suite.
	 */
	@Test
	public void JobSummaryInfoMappersFailed() throws IOException, FileNotFoundException {
		failJob = new FailJob(testSession);
		failJob.submit(true, false);
		assertTrue("Fail job ID is invalid.", 
				failJob.verifyID());
		
		assertFalse("Job did not fail.",
				failJob.waitForSuccess());
		
		assertTrue("Did not find job summary info.", failJob.findSummaryInfo("FAILED", "Fail\\sjob", testSession.conf.getProperty("USER"), "default"));
	}
	
	/*
	 * A test to check the job summary information for failed jobs where the reducers failed.
	 * 
	 * Equivalent to JobSummaryInfo40 in the original shell script YARN regression suite.
	 */
	@Test
	public void JobSummaryInfoReducersFailed() throws IOException, FileNotFoundException {
		failJob = new FailJob(testSession);
		failJob.submit(false, true);
		assertTrue("Fail job ID is invalid.", 
				sleepJob.verifyID());
		
		assertFalse("Job did not fail.",
				failJob.waitForSuccess());
		
		assertTrue("Did not find job summary info.", failJob.findSummaryInfo("FAILED", "Fail\\sjob", testSession.conf.getProperty("USER"), "default"));	
	}
	
	/*
	 * A test to check the job summary information when job is submitted as another user.
	 * 
	 * Equivalent to JobSummaryInfo50 in the original shell script YARN regression suite.
	 */
	@Test
	public void JobSummaryInfoDifferentUser() throws IOException, FileNotFoundException {
		sleepJob = new SleepJob(testSession);
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
	@Test
	public void JobSummaryInfoDifferentQueue() throws IOException, FileNotFoundException {
		// Start sleep job with mapreduce.job.queuename=grideng 
		sleepJob = new SleepJob(testSession);
		sleepJob.setQueue("testQueue");
		sleepJob.submit();
		assertTrue("Sleep job ID is invalid.", 
				sleepJob.verifyID());

		assertTrue("Job did not succeed.",
				sleepJob.waitForSuccess());
		
		assertTrue("Did not find job summary info.", sleepJob.findSummaryInfo("FAILED", "Sleep\\sjob", testSession.conf.getProperty("USER"), "testQueue"));	
	}
	
	/*
	 * A test to check the job summary information for killed jobs.
	 * 
	 * Equivalent to JobSummaryInfo70 in the original shell script YARN regression suite.
	 */
	@Test
	public void JobSummaryInfoKilledJob() throws IOException, FileNotFoundException {
		sleepJob = new SleepJob(testSession);
		sleepJob.submit();
		assertTrue("Sleep job ID is invalid.", 
				sleepJob.verifyID());
		
		assertTrue("Was not able to kill the job.", 
				sleepJob.kill());
		
		assertTrue("Did not find job summary info.", sleepJob.findSummaryInfo("KILLED", "Sleep\\sjob", testSession.conf.getProperty("USER"), "default"));
	}

	/******************* END TESTS ***********************/	
}
