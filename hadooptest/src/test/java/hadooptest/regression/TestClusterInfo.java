package hadooptest.regression;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import hadooptest.TestSession;

import org.apache.hadoop.mapreduce.Cluster;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class TestClusterInfo extends TestSession {

	@BeforeClass
	public static void startTestSession() throws IOException {
		TestSession.start();
	}
	
	@Test
	public void testCluster() throws Exception {
		Cluster clusterInfo = TestSession.cluster.getClusterInfo();
		int ttCount = clusterInfo.getClusterStatus().getTaskTrackerCount();
		TestSession.logger.info("tasktracker count = " +
                Integer.toString(ttCount));
		assertTrue("TaskTracker count is not greater than 0!!!",
				(ttCount > 0));
	}	
	
}