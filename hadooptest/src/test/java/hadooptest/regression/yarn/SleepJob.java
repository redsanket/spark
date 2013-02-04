package hadooptest.regression.yarn;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import hadooptest.TestSession;
import hadooptest.cluster.fullydistributed.FullyDistributedSleepJob;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class SleepJob extends TestSession {

	// private static FullyDistributedCluster cluster;
	
	/******************* CLASS BEFORE/AFTER ***********************/
	
	/*
	 * Configuration and cluster setup that should happen before running any of the tests in the class instance.
	 */
	@BeforeClass
	public static void startCluster() throws FileNotFoundException, IOException{
		TestSession.start();
		// cluster.start();
	}
	
	/*
	 * Cluster cleanup that should happen after running tests in the class instance.
	 */
	@AfterClass
	public static void stopCluster() throws IOException {
		// cluster.stop();
		// cluster.getConf().cleanup();
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
	}
	
	/******************* TESTS ***********************/	
	
	/*
	 * A test for running a sleep job
	 * 
	 * Equivalent to JobSummaryInfo10 in the original shell script YARN regression suite.
	 */
	@Test
	public void runSleepTest() throws IOException, FileNotFoundException {
		FullyDistributedSleepJob job = new FullyDistributedSleepJob();
		job.runSleepJob();
		job.runSleepJob("hadoop1");
		
		Properties jobProps = new Properties();
		jobProps.setProperty("user", "hadoop2");
		job.runSleepJob(jobProps);

		job.runSleepJob( new Properties() {{ this.setProperty("user", "hadoop3"); }} );
		
		String[] output = job.listJobs();
		TestSession.logger.info("Job List:"+output[1]);
	}
	
	/******************* END TESTS ***********************/	
}
