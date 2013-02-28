package hadooptest.regression;

import hadooptest.TestSession;
import hadooptest.SerialTests;
import hadooptest.cluster.fullydistributed.FullyDistributedCluster;
import hadooptest.config.TestConfiguration;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;

@Category(SerialTests.class)
public class ChangeConf extends TestSession {

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
	public void backupHadoopConf() {
		FullyDistributedCluster cluster = (FullyDistributedCluster) TestSession.cluster;
		String component = TestConfiguration.RESOURCE_MANAGER;

		// Backup the default configuration directory on the Resource Manager
		// component host.
		cluster.getConf().backupConfDir(component);		
	}

	@Test
	public void copyFilesToHadoopConf() {
		FullyDistributedCluster cluster = (FullyDistributedCluster) TestSession.cluster;
		String component = TestConfiguration.RESOURCE_MANAGER;

		// Copy files to the custom configuration directory on the
		// Resource Manager component host.
		String sourceDir = "/homes/philips/svn/HadoopQEAutomation/branch-23/tests/Regression/YARN/CapacitySchedulerLimits/config/baseline/";
		cluster.getConf().copyFilesToConfDir(component, sourceDir);
	}

	@Test
	public void modifyHadoopConf() {
		FullyDistributedCluster cluster = (FullyDistributedCluster) TestSession.cluster;
		String component = TestConfiguration.RESOURCE_MANAGER;
		String confFile = TestConfiguration.HADOOP_CONF_YARN;

		// Insert a property to the yarn-site.xml configuration file on the
		// Resource Manager component host.
		cluster.getConf().setHadoopConfFileProp (
				"yarn.admin.acl3",
				"gridadmin,hadoop,hadoopqa,philips,foo",
				component, confFile);
	}

	@Test
	public void resetHadoopConf() {
		// Restart the cluster
		FullyDistributedCluster cluster = (FullyDistributedCluster) TestSession.cluster;
		TestSession.cluster.reset();
		cluster.waitForSafemodeOff();
		cluster.isClusterFullyUp();
	}

	@Test
	public void getHadoopConfProperty() {

		FullyDistributedCluster cluster = (FullyDistributedCluster) TestSession.cluster;
		String component = TestConfiguration.RESOURCE_MANAGER;
		String propName = "fs.trash.interval";
		String confFile = TestConfiguration.HADOOP_CONF_CORE;
		
		String propValue = cluster.getConf().getHadoopConfFileProp(
				propName, confFile, component);
		TestSession.logger.info("Prop name '" + propName + "' = " + propValue);
	}
	
}