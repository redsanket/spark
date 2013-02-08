package hadooptest.regression;

import hadooptest.TestSession;
import hadooptest.cluster.fullydistributed.FullyDistributedCluster;

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

public class RestartCluster extends TestSession {

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
	public void restartCluster() {

		// TestSession.cluster.stopCluster();
		// TestSession.cluster.startCluster();
		TestSession.cluster.reset();
		
		FullyDistributedCluster cluster = (FullyDistributedCluster) TestSession.cluster;
		cluster.waitForSafemodeOff();
		cluster.isClusterFullyUp();		
	}
	
	
}