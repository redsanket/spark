package hadooptest.regression;

import hadooptest.TestSession;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

public class ClusterStop extends TestSession {

	@Test
	public void stopCluster() {
		TestSession.start();
		
		assertTrue("The cluster did not successfully stop.", cluster.stop());
		cluster.getConf().cleanup();
	}
	
}
