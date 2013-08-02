package hadooptest;

import org.junit.BeforeClass;
import org.junit.Test;

import coretest.Util;
import hadooptest.TestSession;

public class TestMultiClusterClientConnection extends TestSession {

	@BeforeClass
	public static void startTestSession() {
		TestSession.start();
	}

	@Test
	public void connectToServerAndWait() throws Exception {		

		String strTimeout = TestSession.conf.getProperty(
				"MULTI_CLUSTER_CLIENT_SESSION_TIMEOUT");
		
		Util.sleep(Integer.parseInt(strTimeout));
	}
	
}
