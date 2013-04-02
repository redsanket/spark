package hadooptest.regression;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import hadooptest.TestSession;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class TestClusterReset extends TestSession {

	@BeforeClass
	public static void startTestSession() throws IOException {
		TestSession.start();
	}
	
	@Test
	public void restartCluster() {
		try {
			assertTrue("Cluster reset failed", TestSession.cluster.reset());
			assertTrue("Cluster is not off of safemode after cluster reset", TestSession.cluster.waitForSafemodeOff());
			assertTrue("Cluster is not fully up after cluster reset", TestSession.cluster.isFullyUp());	
		}
		catch (Exception e) {
			TestSession.logger.error("Exception failure.", e);
			fail();
		}	
	}	
	
}