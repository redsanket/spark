/*
 * YAHOO!
 */

package hadooptest.regression.yarn;

import static org.junit.Assert.assertTrue;
import hadooptest.TestSession;
import hadooptest.job.SleepJob;

import java.util.Vector;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/*
 * A set of tests which exercise the threaded nature of hadooptest.cluster.Job
 */
public class ManySleepJob extends TestSession {
	
	private Vector<SleepJob> sleepJobVector;

	/******************* CLASS BEFORE/AFTER ***********************/
	
	/*
	 * Setup a cluster for the test.
	 */
	@BeforeClass
	public static void startTestSession() {
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
	
	/******************* TEST BEFORE/AFTER ***********************/
	
	/*
	 * Before each test, we must initialize the sleep job Vector.
	 */
	@Before
	public void initTestJob() {
		this.sleepJobVector = new Vector<SleepJob>();
	}
	
	/******************* TESTS ***********************/
	
	/*
	 * A test which launches a number of threaded jobs in rapid succession,
	 * and tests to see if the jobs were assigned an ID in a reasonable
	 * amount of time.
	 */
	@Test
	public void runManySleepJobs() {
		int i;
		SleepJob sleepJob;
		
		// Configure the jobs
		for (i = 0; i < 10; i++) {
			sleepJob = new SleepJob();
			
			sleepJob.setNumMappers(5);
			sleepJob.setNumReducers(5);
			sleepJob.setMapDuration(500);
			sleepJob.setReduceDuration(500);
			
			sleepJobVector.add(sleepJob);
		}
		
		// Rapidly launch the jobs
		for (i = 0; i < sleepJobVector.size(); i++) {
			sleepJobVector.get(i).start();
		}
		
		// Validate the job IDs
		for (i = 0; i < sleepJobVector.size(); i++) {
			assertTrue("Sleep job " + i + " was not assigned an ID within 10 seconds.", 
					sleepJobVector.get(i).waitForID(10));
			assertTrue("Sleep job ID for sleep job " + i + " is invalid.", 
					sleepJobVector.get(i).verifyID());
		}
	}
	
	/******************* END TESTS ***********************/
	
}
