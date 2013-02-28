package hadooptest.regression;

import hadooptest.TestSession;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

public class ClusterReset extends TestSession {

	@Test
	public void resetCluster() {
		TestSession.start();
		
		assertTrue("The cluster did not successfully reset.", cluster.reset());
	}
	
}
