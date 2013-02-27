package hadooptest.regression;

import static org.junit.Assert.assertTrue;
import hadooptest.TestSession;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class CheckVersion extends TestSession {

	/*
	 * Configuration and cluster setup that should happen before running any of the tests in the class instance.
	 */
	@BeforeClass
	public static void startCluster() throws IOException {
		TestSession.start();
		// cluster.start();
	}
		
	/*
	 * Cluster cleanup that should happen after running tests in the class instance.
	 */
	@AfterClass
	public static void stopCluster() throws IOException {
		// cluster.stop();
		// cluster.getConf().cleanup();
	}
	
	
	/*
	 * Before each test, we much initialize any jobs.
	 */
	@Before
	public void initTestJob() {

	}
	
	/*
	 * After each test, we must reset the state of the cluster to a known default state.
	 */
	@After
	public void resetClusterState() {

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