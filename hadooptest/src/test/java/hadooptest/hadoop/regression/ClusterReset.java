package hadooptest.hadoop.regression;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import hadooptest.TestSession;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import hadooptest.SerialTests;

/**
 * Resets a cluster.
 */
@Category(SerialTests.class)
public class ClusterReset extends TestSession {

	@Test
	public void resetCluster() {
		try {
			assertTrue("The cluster did not successfully reset.", cluster.reset());
		}
		catch (Exception e) {
			TestSession.logger.error("Exception failure.", e);
			fail();
		}
	}
	
}
