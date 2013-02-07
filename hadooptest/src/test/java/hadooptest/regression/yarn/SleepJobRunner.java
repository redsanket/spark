package hadooptest.regression.yarn;

import static org.junit.Assert.assertTrue;
import hadooptest.TestSession;
import hadooptest.job.SleepJob;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class SleepJobRunner extends TestSession {
	
	/******************* CLASS BEFORE/AFTER ***********************/
	
	/*
	 * Configuration and cluster setup that should happen before running any of the tests in the class instance.
	 */
	@BeforeClass
	public static void startCluster() {
		TestSession.start();
		cluster.start();
	}
	
	/*
	 * Cluster cleanup that should happen after running tests in the class instance.
	 */
	@AfterClass
	public static void stopCluster() {
		cluster.stop();
		cluster.getConf().cleanup();
	}
	
	/******************* TESTS ***********************/	
	
	/*
	 * A test for running a sleep job
	 * 
	 * Equivalent to JobSummaryInfo10 in the original shell script YARN regression suite.
	 */
	@Test
	public void runSleepTest() {
		SleepJob jobUserDefault = new SleepJob();
		SleepJob jobUser1 = new SleepJob();
		SleepJob jobUser2 = new SleepJob();
		SleepJob jobUser3 = new SleepJob();
		
		jobUserDefault.setNumMappers(1);
		jobUserDefault.setNumReducers(1);
		jobUserDefault.setMapDuration(100);
		jobUserDefault.setReduceDuration(100);
		
		jobUserDefault.start();
		
		jobUser1.setNumMappers(1);
		jobUser1.setNumReducers(1);
		jobUser1.setMapDuration(100);
		jobUser1.setReduceDuration(100);
		jobUser1.setUser("hadoop1");
		
		jobUser1.start();
		
		jobUser2.setNumMappers(1);
		jobUser2.setNumReducers(1);
		jobUser2.setMapDuration(100);
		jobUser2.setReduceDuration(100);
		jobUser2.setUser("hadoop2");
		
		jobUser2.start();
		
		jobUser3.setNumMappers(1);
		jobUser3.setNumReducers(1);
		jobUser3.setMapDuration(100);
		jobUser3.setReduceDuration(100);
		jobUser3.setUser("hadoop3");
		
		jobUser3.start();
		
		assertTrue("Sleep job (default user) was not assigned an ID within 10 seconds.", 
				jobUserDefault.waitForID(10));
		assertTrue("Sleep job ID for sleep job (default user) is invalid.", 
				jobUserDefault.verifyID());

		assertTrue("Sleep job (user 1) was not assigned an ID within 10 seconds.", 
				jobUser1.waitForID(10));
		assertTrue("Sleep job ID for sleep job (user 1) is invalid.", 
				jobUser1.verifyID());

		assertTrue("Sleep job (user 2) was not assigned an ID within 10 seconds.", 
				jobUser2.waitForID(10));
		assertTrue("Sleep job ID for sleep job (user 2) is invalid.", 
				jobUser2.verifyID());

		assertTrue("Sleep job (user 3) was not assigned an ID within 10 seconds.", 
				jobUser3.waitForID(10));
		assertTrue("Sleep job ID for sleep job (user 3) is invalid.", 
				jobUser3.verifyID());
		
		assertTrue("Job (default user) did not succeed.", jobUserDefault.waitForSuccess(1));
		assertTrue("Job (user 1) did not succeed.", jobUser1.waitForSuccess(1));
		assertTrue("Job (user 2) did not succeed.", jobUser2.waitForSuccess(1));
		assertTrue("Job (user 3) did not succeed.", jobUser3.waitForSuccess(1));
	}
	
	/******************* END TESTS ***********************/	
}
