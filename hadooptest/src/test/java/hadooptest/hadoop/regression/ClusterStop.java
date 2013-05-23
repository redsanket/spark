package hadooptest.hadoop.regression;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import hadooptest.TestSession;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import coretest.SerialTests;

/**
 * Stops a cluster.
 */
@Category(SerialTests.class)
public class ClusterStop extends TestSession {

	@Test
	public void stopCluster() {
		try {
			TestSession.start();

			assertTrue("The cluster did not successfully stop.", cluster.stop());
			cluster.getConf().cleanup();
		}
		catch (Exception e) {
			TestSession.logger.error("Exception failure.", e);
			fail();
		}
	}

}
