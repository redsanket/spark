package hadooptest.regression;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.cluster.ClusterState;

import org.junit.Test;
import org.junit.experimental.categories.Category;

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
