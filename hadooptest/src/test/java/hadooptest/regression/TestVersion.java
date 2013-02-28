package hadooptest.regression;

import static org.junit.Assert.assertTrue;
import hadooptest.TestSession;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class TestVersion extends TestSession {

	@BeforeClass
	public static void startTestSession() throws IOException {
		TestSession.start();
	}
	
	@Test
	public void assertHadoopVersion() {

		String clusterVersion = TestSession.cluster.getVersion();
		String testConfVersionAPI = TestSession.cluster.getConf().getVersion();
		String testConfVersionCLI = TestSession.cluster.getConf().getVersionViaCLI();
		
		TestSession.logger.info("Cluster Obj: Hadoop Version = '" + clusterVersion + "'");		
		TestSession.logger.info("Cluster Conf Obj: Hadoop Version (API) = '" + testConfVersionAPI + "'");		
		TestSession.logger.info("Cluster Conf Obj: Hadoop Version (CLI) = '" + testConfVersionCLI + "'");		

		assertTrue("Version has invalid format!!!", testConfVersionAPI.matches("\\d+[.\\d+]+"));
		assertTrue("API and CLI versions do not match!!!", testConfVersionAPI.equals(testConfVersionCLI));
		assertTrue("Cluster Object version and Cluster Conf Object version do not match!!!", clusterVersion.equals(testConfVersionAPI));
	}

	
}