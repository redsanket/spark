package hadooptest.hadoop.regression;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import hadooptest.TestSession;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import coretest.SerialTests;
import coretest.cluster.ClusterState;

/**
 * Checks to make sure that a cluster is running.
 */
@Category(SerialTests.class)
public class TestGetClusterState extends TestSession {

	@BeforeClass
	public static void startTestSession() throws IOException {
		TestSession.start();
	}
	
	@Test
	public void checkClusterState() {
		try {
			ClusterState cluster_state = cluster.getState();

			assertTrue("The cluster isn't fully up.", cluster_state.equals(ClusterState.UP));
		}
		catch (Exception e) {
			TestSession.logger.error("Exception failure.", e);
			fail();
		}
	}	
}
