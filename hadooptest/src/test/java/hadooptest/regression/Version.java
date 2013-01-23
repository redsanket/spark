package hadooptest.regression;

import hadooptest.TestSession;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class Version {

	private static TestSession testSession;
	protected static TestSession TSM;

	/*
	 * Configuration and cluster setup that should happen before running any of the tests in the class instance.
	 */
	@BeforeClass
	public static void startCluster() throws IOException {
		
		testSession = new TestSession();
		TSM = testSession;
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
	public void printHadoopVersion() {
		String version = TSM.cluster.getVersion();
		System.out.println("Hadoop Version = <" + version + ">");
	}
	
	
}