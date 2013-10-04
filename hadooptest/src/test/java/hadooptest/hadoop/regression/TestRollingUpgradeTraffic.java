package hadooptest.hadoop.regression;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import hadooptest.TestSession;
import hadooptest.workflow.hadoop.job.SleepJob;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import coretest.SerialTests;

import java.io.File;
import java.io.IOException;

@Category(SerialTests.class)
public class TestRollingUpgradeTraffic extends TestSession {

	@BeforeClass
	public static void startTestSession() throws IOException {
		TestSession.start();
	}
	
    @Test
    public void testTraffic() throws Exception {
        // Initialize max duration, default is "5" minutes.
        int maxDurationMin =
                Integer.parseInt(System.getProperty("DURATION", "5"));
        TestSession.logger.info("Run for '" + maxDurationMin + "' minutes.");
        long maxDurationMs = maxDurationMin*60*1000; 
        long startTime = System.currentTimeMillis();
        long currentTime = System.currentTimeMillis();
        long elapsedTime = currentTime - startTime;
        int elapsedTimeMin = (int) (elapsedTime / 1000) / 60;
        String filePath = "/tmp/foo";
        File file = new File(filePath);
        while ((!file.exists()) && (elapsedTime < maxDurationMs))  {
            // Do Something...
            generateTraffic();
            
            currentTime = System.currentTimeMillis();
            elapsedTime = currentTime - startTime;
            elapsedTimeMin = (int) (elapsedTime / 1000) / 60;
            TestSession.logger.info("Elapsed time is: '" + elapsedTimeMin + 
                    "' minutes.");
            
            currentTime = System.currentTimeMillis();
            elapsedTime = currentTime - startTime;
            elapsedTimeMin = (int) (elapsedTime / 1000) / 60;
            TestSession.logger.info("Elapsed time is: '" + elapsedTimeMin + 
                    "' minutes.");

            if (file.exists()){
                TestSession.logger.info("Found stop file.");
            }
            // file.createNewFile();
        }
        if (file.exists()) {
            file.delete();            
        }        
    }
                
	public void generateTraffic() throws Exception {
	    TestSession.logger.info("---> Generate traffic: ");
	    
	    // TODO: Run just sleep jobs for now ...	    
        int numJobs =
                Integer.parseInt(System.getProperty("NUM_JOBS", "4"));
	    runSleepJobsTest(numJobs);
	}	
	
    /*
     * A test for running a sleep job
     * 
     * Equivalent to JobSummaryInfo10 in the original shell script YARN regression suite.
     */
    public void runSleepJobsTest(int numJobs) {
        TestSession.logger.info("---> Run '" + numJobs + "' sleep jobs:");
        try {
            SleepJob[] jobs = new SleepJob[numJobs];
            int index = 0;
            while (index < numJobs) {
                jobs[index] = new SleepJob();
                jobs[index].setNumMappers(1);
                jobs[index].setNumReducers(1);
                jobs[index].setMapDuration(100);
                jobs[index].setReduceDuration(100);
                jobs[index].setUser("hadoop" + ((index%20)+1));
                jobs[index].start();                
                index++;
            }
            waitForJobsID(jobs);
            waitForJobsSuccess(jobs);
        }
        catch (Exception e) {
            TestSession.logger.error("Exception failure.", e);
            fail();
        }
    }

    public void waitForJobsID(SleepJob[] jobs) throws Exception {
        int index = 0;
        int waitTime = 10;
        while (index < jobs.length) {
            assertTrue("Sleep job (" + index + ") was not assigned an ID " +
                    "within " + waitTime + " seconds.", 
                    jobs[index].waitForID(waitTime));
            assertTrue("Sleep job ID for sleep job (" + index + ") is invalid.", 
                    jobs[index].verifyID());
            index++;
        }
    }

    public void waitForJobsSuccess(SleepJob[] jobs) throws Exception {
        int index = 0;
        int waitTime = 2;
        while (index < jobs.length) {
            assertTrue("Job (" + index + ") did not succeed within " +
                    waitTime + " seconds.",
                    jobs[index].waitForSuccess(waitTime));
            index++;
        }
    }

}