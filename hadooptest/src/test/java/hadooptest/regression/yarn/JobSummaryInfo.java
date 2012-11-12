package hadooptest.regression.yarn;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import hadooptest.ConfigProperties;
import hadooptest.cluster.pseudodistributed.PseudoDistributedCluster;
import hadooptest.cluster.pseudodistributed.SleepJob;
import hadooptest.config.testconfig.PseudoDistributedConfiguration;

import hadooptest.Util;

//import hadooptest.config.testconfig.StandaloneConfiguration;
//import hadooptest.cluster.standalone.StandaloneCluster;

public class JobSummaryInfo {

	private SleepJob sleepJob;
	private static PseudoDistributedConfiguration conf;
	private static PseudoDistributedCluster cluster;
	
	//private static StandaloneConfiguration conf;
	//private static StandaloneCluster cluster;

	private static ConfigProperties framework_conf;
	
	/******************* CLASS BEFORE/AFTER ***********************/
	
	/*
	 * Configuration and cluster setup that should happen before running any of the tests in the class instance.
	 */
	@BeforeClass
	public static void startCluster() throws FileNotFoundException, IOException{
		
		frameworkInit();
		
		conf = new PseudoDistributedConfiguration();
		conf.write();

		cluster = new PseudoDistributedCluster(conf);
		cluster.start();
		
		//conf = new StandaloneConfiguration();
		
		//cluster = new StandaloneCluster(conf);
		//cluster.start();
	}
	
	/*
	 * Cluster cleanup that should happen after running tests in the class instance.
	 */
	@AfterClass
	public static void stopCluster() throws IOException {
		cluster.stop();
		conf.cleanup();
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
				System.out.println("Cleaned up latent job by killing it: " + sleepJob.ID);
			}
			else {
				System.out.println("Sleep job never started, no need to clean up.");
			}
		}
		else {
			System.out.println("Job was already killed or never started, no need to clean up.");
		}
	}
	
	/******************* TESTS ***********************/	
	
	/*
	 * A test to check the job summary information after successful job completion.
	 * 
	 * Equivalent to JobSummaryInfo10 in the original shell script YARN regression suite.
	 */
	@Test
	public void JobSummaryInfoSuccess() {
		// Start sleep job
		sleepJob = new SleepJob();
		sleepJob.submit();
		assertTrue("Sleep job ID is invalid.", 
				sleepJob.verifyID());
		
		assertTrue("Job did not succeed.",
				sleepJob.waitForSuccess());
		
		// Build job summary info template
		
		// Sleep 200s waiting for logs to be updated
		Util.sleep(200);
		
		// ssh to ResourceManager and grep through summary logs to find job summary info.  If found, pass the test.
	}
	
	/*
	 * A test to check the job summary information for High RAM jobs.
	 * 
	 * Equivalent to JobSummaryInfo20 in the original shell script YARN regression suite.
	 */
	@Test
	public void JobSummaryInfoHighRAM() {
		// Start sleep job with mapred.job.map.memory.mb=6144 
		sleepJob = new SleepJob();
		sleepJob.submit();
		assertTrue("Sleep job ID is invalid.", 
				sleepJob.verifyID());
		
		assertTrue("Job did not succeed.",
				sleepJob.waitForSuccess());
		
		// Build job summary info template
		
		// Sleep 200s waiting for logs to be updated
		Util.sleep(200);
		
		// ssh to ResourceManager and grep through summary logs to find job summary info.  If found, pass the test.
		
	}
	
	/*
	 * A test to check the job summary information for failed jobs where the mappers failed.
	 * 
	 * Equivalent to JobSummaryInfo30 in the original shell script YARN regression suite.
	 */
	@Test
	public void JobSummaryInfoMappersFailed() {
		// Start sleep job with -failMappers 
		sleepJob = new SleepJob();
		sleepJob.submit();
		assertTrue("Sleep job ID is invalid.", 
				sleepJob.verifyID());
		
		assertTrue("Job did not succeed.",
				sleepJob.waitForSuccess());
		
		// Build job summary info template
		
		// Sleep 200s waiting for logs to be updated
		Util.sleep(200);
		
		// ssh to ResourceManager and grep through summary logs to find job summary info.  If found, pass the test.
		
	}
	
	/*
	 * A test to check the job summary information for failed jobs where the reducers failed.
	 * 
	 * Equivalent to JobSummaryInfo40 in the original shell script YARN regression suite.
	 */
	@Test
	public void JobSummaryInfoReducersFailed() {
		// Start sleep job with -failReducers 
		sleepJob = new SleepJob();
		sleepJob.submit();
		assertTrue("Sleep job ID is invalid.", 
				sleepJob.verifyID());
		
		assertTrue("Job did not succeed.",
				sleepJob.waitForSuccess());
		
		// Build job summary info template
		
		// Sleep 200s waiting for logs to be updated
		Util.sleep(200);
		
		// ssh to ResourceManager and grep through summary logs to find job summary info.  If found, pass the test.
		
	}
	
	/*
	 * A test to check the job summary information when job is submitted as another user.
	 * 
	 * Equivalent to JobSummaryInfo50 in the original shell script YARN regression suite.
	 */
	@Test
	public void JobSummaryInfoDifferentUser() {
		 // Gets kerberos ticket for user hadoop1
		 // Sets kerberos ticket for user hadoop1
		
		 // Starts sleep job
		sleepJob = new SleepJob();
		sleepJob.submit();
		assertTrue("Sleep job ID is invalid.", 
				sleepJob.verifyID());

		assertTrue("Job did not succeed.",
				sleepJob.waitForSuccess());
		
		 // Gets kerberos ticket for user hadoopqa
		 // Sets kerberos ticket for user hadoopqa 
		
		// Build job summary info template
		
		// Sleep 200s waiting for logs to be updated
		Util.sleep(200);
		
		// ssh to ResourceManager and grep through summary logs to find job summary info.  If found, pass the test.
		
	}
	
	/*
	 * A test to check the job summary information when job is submitted to a different queue.
	 * 
	 * Equivalent to JobSummaryInfo60 in the original shell script YARN regression suite.
	 */
	@Test
	public void JobSummaryInfoDifferentQueue() {
		// Start sleep job with mapreduce.job.queuename=grideng 
		sleepJob = new SleepJob();
		sleepJob.submit();
		assertTrue("Sleep job ID is invalid.", 
				sleepJob.verifyID());

		assertTrue("Job did not succeed.",
				sleepJob.waitForSuccess());
		
		// Build job summary info template
		
		// Sleep 200s waiting for logs to be updated
		Util.sleep(200);
		
		// ssh to ResourceManager and grep through summary logs to find job summary info.  If found, pass the test.
		
	}
	
	/*
	 * A test to check the job summary information for killed jobs.
	 * 
	 * Equivalent to JobSummaryInfo70 in the original shell script YARN regression suite.
	 */
	@Test
	public void JobSummaryInfoKilledJob() {
		// Start sleep job
		sleepJob = new SleepJob();
		sleepJob.submit();
		assertTrue("Sleep job ID is invalid.", 
				sleepJob.verifyID());
		
		// Kill the job
		assertTrue("Was not able to kill the job.", 
				sleepJob.kill());
		
		// Build job summary info template
		
		// Sleep 200s waiting for logs to be updated
		Util.sleep(200);
		
		// ssh to ResourceManager and grep through summary logs to find job summary info.  If found, pass the test.
		
	}

	/******************* END TESTS ***********************/
	private static void frameworkInit() throws IOException {
		framework_conf = new ConfigProperties();
		File conf_location = new File("/Users/rbernota/workspace/hadoop/test/pseudodistributed_configs/hadooptest.conf");
		framework_conf.load(conf_location);
		System.out.println("Hadooptest conf property USER = " + framework_conf.getProperty("USER"));
	}
	
}
