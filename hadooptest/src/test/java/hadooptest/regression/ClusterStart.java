package hadooptest.regression;

import hadooptest.TestSession;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

import hadooptest.SerialTests;
import org.junit.experimental.categories.Category;

/**
 * Starts a cluster.
 */
@Category(SerialTests.class)
public class ClusterStart extends TestSession {

	@Test
	public void startCluster() {
		TestSession.start();
		
		assertTrue("The cluster did not successfully start.", cluster.start());
	}	
}
