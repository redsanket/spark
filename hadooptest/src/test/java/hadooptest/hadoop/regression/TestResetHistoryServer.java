package hadooptest.hadoop.regression;

import static org.junit.Assert.assertTrue;
import hadooptest.TestSession;
import hadooptest.cluster.hadoop.HadoopCluster;
import hadooptest.cluster.hadoop.HadoopCluster.Action;

import org.junit.Test;

import coretest.Util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestResetHistoryServer extends TestSession {

	@Test
	public void testResetHistoryServer() throws Exception {
	    // Get the current cluster state
        TestSession.cluster.getState();

        // 1. Stop the History Server
	    TestSession.logger.info("--> Stop the History Server:");
		TestSession.cluster.hadoopDaemon(
		        Action.STOP, HadoopCluster.HISTORYSERVER);    

        // 2. Assert the History Server is down
        assertTrue("History Server failed to stop!!!",
                TestSession.cluster.isComponentFullyDown(
                        HadoopCluster.HISTORYSERVER));

		// Get the current cluster state
		TestSession.cluster.getState();
		
		// 3. Start the History Server
		TestSession.logger.info("--> Start the History Server:");
		// Get the start time
		long startTime = System.currentTimeMillis();
		// Start the HS
		TestSession.cluster.hadoopDaemon(
                Action.START, HadoopCluster.HISTORYSERVER);

		// Get the end time
		// long endTime = System.currentTimeMillis();

		// 4. For the the History Server to fully come back up
        // E.g. num=`curl --ignore http://gsbl90261.blue.ygrid.yahoo.com:19888/jobhistory|grep "/jobhistory/job/job"|wc -l`;echo "num = $num"

        int limitMin = 10;
        int limitSec = limitMin*60;
        int waitInterval = 10;
        int maxWait = (limitSec/waitInterval);

        String rmNode = TestSession.cluster.getNode(
                HadoopCluster.RESOURCE_MANAGER).getHostname();
		String hsUrl = "http://" + rmNode + ":19888/jobhistory";
        int expectedRecords = 20000;
        String jobRecPattern = "/jobhistory/job/job";
		
        int count = 1;
        String[] cmd = new String[] {
                "/usr/bin/curl", "--ignore", hsUrl};
        while (count <= maxWait) {
            String[] output = TestSession.exec.runProcBuilder(
                    cmd, null, false, true);
            Pattern pattern = Pattern.compile(jobRecPattern);
            Matcher  matcher = pattern.matcher(output[1]);
            int actualRecords = 0;
            while (matcher.find()) {
                actualRecords++;                
            }
            if (actualRecords == expectedRecords) {
                TestSession.logger.info(
                        "Job History Server is back online with '" +
                        expectedRecords + "' job records.");
                break;
            }
            else {
                TestSession.logger.debug(
                        "Job History Server is not fully back online: " +
                        "number of job records = '" + actualRecords + "'.");
            }
            Util.sleep(waitInterval);
            count++;
        }
        // Get the end time
        long endTime = System.currentTimeMillis();

        
        // 5. Assert duration is less than limit
        long diffTime = endTime - startTime;
        int diffSec = (int) (diffTime / 1000);
        TestSession.logger.trace("Elapsed time: '" + diffTime + "' ms, '" +
                diffSec + "' sec.");
        assertTrue("History Server took longer than " + limitMin +
                " minutes to restart!!!",
                (diffSec <= limitSec));
        TestSession.logger.info("Verified elapsed time of '" + diffSec + 
                "' seconds is less than the required time of '" + limitSec +
                "' seconds (or '" + limitMin + "' minutes).");
        
        // Get the current cluster state
        TestSession.cluster.getState();
		
        // 6. Assert the History Server is up
        assertTrue("History Server failed to start!!!",
                TestSession.cluster.isComponentFullyUp(
                        HadoopCluster.HISTORYSERVER));
	}	
	
}