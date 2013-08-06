package hadooptest;

import org.junit.BeforeClass;
import org.junit.Test;

import coretest.Util;
import hadooptest.TestSession;

/**
 * A test that will allow for running the multi cluster client on a
 * remote gateway for a different cluster, and has a configurable timeout.
 */
public class TestMultiClusterClientConnection extends TestSession {

	@BeforeClass
	public static void startTestSession() {
		TestSession.start();
	}

	/**
	 * Wait for the duration of the specified timeout before exiting the 
	 * test.  This should be an amount of time necessary to complete the 
	 * test that uses the multi cluster client.
	 */
	@Test
	public void connectToServerAndWait() throws Exception {		

		String strTimeout = TestSession.conf.getProperty(
				"MULTI_CLUSTER_CLIENT_SESSION_TIMEOUT");
		
		Util.sleep(Integer.parseInt(strTimeout));
	}
	
}
