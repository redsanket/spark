package hadooptest.regression;

import hadooptest.TestSession;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

import hadooptest.SerialTests;
import org.junit.experimental.categories.Category;

/**
 * Resets a cluster.
 */
@Category(SerialTests.class)
public class ClusterReset extends TestSession {

	@Test
	public void resetCluster() {
		TestSession.start();
		
		assertTrue("The cluster did not successfully reset.", cluster.reset());
	}
	
}
