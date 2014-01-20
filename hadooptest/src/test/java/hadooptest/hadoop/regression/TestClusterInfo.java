package hadooptest.hadoop.regression;

import static org.junit.Assert.assertTrue;
import hadooptest.TestSession;

import org.apache.hadoop.mapreduce.Cluster;
import org.junit.Test;

public class TestClusterInfo extends TestSession {

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