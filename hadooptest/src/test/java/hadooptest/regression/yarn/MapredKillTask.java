/*
 * YAHOO!
 */

package hadooptest.regression.yarn;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import hadooptest.cluster.pseudodistributed.PseudoDistributedCluster;
import hadooptest.cluster.pseudodistributed.SleepJob;
import hadooptest.config.testconfig.PseudoDistributedConfiguration;
import hadooptest.ConfigProperties;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.File;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/*
 * A test suite used to exercise the ability to kill task attempts from a MapReduce sleep job.
 */
public class MapredKillTask {

	private SleepJob sleepJob;
	private static PseudoDistributedConfiguration conf;
	private static PseudoDistributedCluster cluster;
	
	private static final int MAPREDUCE_MAP_MAXATTEMPTS = 4;
	private static final int MAPREDUCE_REDUCE_MAXATTEMPTS = 4;

	private static ConfigProperties framework_conf;
	
	/******************* CLASS BEFORE/AFTER ***********************/
	
	/*
	 * Configuration and cluster setup that should happen before running any of the tests in the class instance.
	 */
	@BeforeClass
	public static void startCluster() throws FileNotFoundException, IOException{
		
		frameworkInit();
		
		conf = new PseudoDistributedConfiguration();
		conf.set("mapreduce.map.maxattempts", Integer.toString(MAPREDUCE_MAP_MAXATTEMPTS));
		conf.set("mapreduce.reduce.maxattempts", Integer.toString(MAPREDUCE_REDUCE_MAXATTEMPTS));
		conf.write();

		cluster = new PseudoDistributedCluster(conf);
		cluster.start();
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
	 * Before each test, we much initialize the sleep job and verify that its job ID is valid.
	 */
	@Before
	public void initTestJob() {
		sleepJob = new SleepJob();
		sleepJob.submit();
		assertTrue("Sleep job ID is invalid.", 
				sleepJob.verifyID());
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
	 * A test which attempts to kill a running task from a sleep job.
	 */
	@Test
	public void killRunningTask() {	
		this.killTask();
	}
	
	/*
	 * A test which attempts to kill a task from a sleep job which has already been killed.
	 */
	@Test
	public void killTaskOfAlreadyKilledJob() {
		
		assertTrue("Was not able to kill the job.", 
				sleepJob.kill());
		
		this.killTask();
	}
	
	/*
	 * A test which attempts to kill a task from a sleep job which has failed.
	 */
	@Test
	public void killTaskOfAlreadyFailedJob() {
		
		assertTrue("Was not able to fail the job.", 
				sleepJob.fail(MAPREDUCE_MAP_MAXATTEMPTS));

		this.killTask();
	}
	
	/*
	 * A test which attempts to kill a task from a sleep job which has already succeeded.
	 */
	@Test
	public void killTaskOfAlreadyCompletedJob() {
		
		assertTrue("Job did not succeed.",
				sleepJob.waitForSuccess());
		
		String taskID = sleepJob.getMapTaskAttemptID();
		assertFalse("Killed task and we shouldn't have been able to.", 
				sleepJob.killTaskAttempt(taskID));
	}
	
	/******************* END TESTS ***********************/

	/*
	 * A helper method to get the map task attempt ID, and kill the task attempt.
	 */
	private void killTask() {
		String taskID = sleepJob.getMapTaskAttemptID();
		assertTrue("Killed task message doesn't exist, we weren't able to kill the task.", 
				sleepJob.killTaskAttempt(taskID));
	}
	
	private static void frameworkInit() throws IOException {
		framework_conf = new ConfigProperties();
		File conf_location = new File("/Users/rbernota/workspace/hadoop/test/pseudodistributed_configs/hadooptest.conf");
		framework_conf.load(conf_location);
		System.out.println("Hadooptest conf property USER = " + framework_conf.getProperty("USER"));
	}
	
}
