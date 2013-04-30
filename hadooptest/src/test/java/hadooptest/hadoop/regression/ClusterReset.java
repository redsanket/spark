package hadooptest.hadoop.regression;

import hadooptest.TestSession;
import org.junit.Test;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import hadooptest.SerialTests;
import org.junit.experimental.categories.Category;

/**
 * Resets a cluster.
 */
@Category(SerialTests.class)
public class ClusterReset extends TestSession {

	@Test
	public void resetCluster() {
		try {
			TestSession.start();

			assertTrue("The cluster did not successfully reset.", cluster.reset());
		}
		catch (Exception e) {
			TestSession.logger.error("Exception failure.", e);
			fail();
		}
	}
	
}
