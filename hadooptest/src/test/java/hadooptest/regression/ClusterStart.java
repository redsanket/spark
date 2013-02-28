package hadooptest.regression;

import hadooptest.TestSession;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

public class ClusterStart extends TestSession {

	@Test
	public void startCluster() {
		TestSession.start();
		
		assertTrue("The cluster did not successfully start.", cluster.start());
	}
	
}
