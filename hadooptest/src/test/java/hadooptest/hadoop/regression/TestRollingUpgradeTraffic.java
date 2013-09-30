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
import java.util.Random;

@Category(SerialTests.class)
public class TestRollingUpgradeTraffic extends TestSession {

	@BeforeClass
	public static void startTestSession() throws IOException {
		TestSession.start();
	}
	
    @Test
    public void testTraffic() throws Exception {
        long maxDurationMin = 30;
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
	    Random r = new Random();
	    int maxSec = 180;
	    int waitSec;
	    
	    // Sleep Jobs
	    runSleepTest();
	    
	    /*
	    // Sleep a random number of seconds in the range of 0 to maxSec
	    waitSec = r.nextInt(maxSec+1);
	    TestSession.logger.info("Sleep for '" + waitSec + "' seconds.");
	    Thread.sleep(waitSec*1000);	          
	    */  
	}	
	
	   /*
     * A test for running a sleep job
     * 
     * Equivalent to JobSummaryInfo10 in the original shell script YARN regression suite.
     */
    public void runSleepTest() {
        try {
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

            int waitTime = 2;
            assertTrue("Job (default user) did not succeed.",
                jobUserDefault.waitForSuccess(waitTime));
            assertTrue("Job (user 1) did not succeed.",
                jobUser1.waitForSuccess(waitTime));
            assertTrue("Job (user 2) did not succeed.",
                jobUser2.waitForSuccess(waitTime));
            assertTrue("Job (user 3) did not succeed.",
                jobUser3.waitForSuccess(waitTime));
        }
        catch (Exception e) {
            TestSession.logger.error("Exception failure.", e);
            fail();
        }
    }
	
}