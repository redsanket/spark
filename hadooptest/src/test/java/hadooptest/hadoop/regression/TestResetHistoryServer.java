package hadooptest.hadoop.regression;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import hadooptest.TestSession;
import hadooptest.cluster.hadoop.HadoopCluster;
import hadooptest.cluster.hadoop.HadoopCluster.Action;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;

import org.apache.hadoop.mapreduce.Cluster;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class TestResetHistoryServer extends TestSession {

	@BeforeClass
	public static void startTestSession() throws IOException {
		TestSession.start();
	}
	
	@Test
	public void testCluster() throws Exception {
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
        long endTime = System.currentTimeMillis();

        // 4. Assert duration is less than limit
        long diffTime = endTime - startTime;
        TestSession.logger.info("Elapsed milliseconds: " + diffTime);        
        int diffSec = (int) (diffTime / 1000);
        TestSession.logger.info("Elapsed seconds: " + diffSec);
        int limitMin = 15;
        int limitSec = 15*60;
        assertTrue("History Server took longer than " + limitMin +
                " minutes to restart!!!",
                (diffSec <= limitSec));
        
        // Get the current cluster state
        TestSession.cluster.getState();
		
        // 5. Assert the History Server is up
        assertTrue("History Server failed to start!!!",
                TestSession.cluster.isComponentFullyUp(
                        HadoopCluster.HISTORYSERVER));
	}	
	
}