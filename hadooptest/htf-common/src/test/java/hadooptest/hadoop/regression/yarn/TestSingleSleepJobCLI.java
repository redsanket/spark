package hadooptest.hadoop.regression.yarn;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import hadooptest.TestSession;
import hadooptest.workflow.hadoop.job.SleepJob;

import org.junit.Test;

public class TestSingleSleepJobCLI extends TestSession {
	
    /*
     * A test for running a sleep job
     * 
     * Equivalent to JobSummaryInfo10 in the original shell script YARN regression suite.
     */
    @Test
    public void runSingleSleepTest() {
        try {
            SleepJob jobUserDefault = new SleepJob();
            jobUserDefault.setNumMappers(1);
            jobUserDefault.setNumReducers(1);
            jobUserDefault.setMapDuration(100);
            jobUserDefault.setReduceDuration(100);
            jobUserDefault.start();
            assertTrue("Sleep job (default user) was not assigned an ID within 10 seconds.", 
                    jobUserDefault.waitForID(10));
            assertTrue("Sleep job ID for sleep job (default user) is invalid.", 
                    jobUserDefault.verifyID());
            int waitTime = 2;
            assertTrue("Job (default user) did not succeed.",
                jobUserDefault.waitForSuccess(waitTime));
        }
        catch (Exception e) {
            TestSession.logger.error("Exception failure.", e);
            fail();
        }
    }	
}
