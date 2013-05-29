package hadooptest.hadoop.regression;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import hadooptest.TestSession;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import coretest.SerialTests;
import coretest.cluster.ClusterState;

/**
 * Checks to make sure that a cluster is running.
 */
@Category(SerialTests.class)
public class TestGetClusterState extends TestSession {

	@Test
	public void checkClusterState() {
		try {
			TestSession.start();

			ClusterState cluster_state = cluster.getState();

			assertTrue("The cluster isn't fully up.", cluster_state.equals(ClusterState.UP));
		}
		catch (Exception e) {
			TestSession.logger.error("Exception failure.", e);
			fail();
		}
	}	
}
