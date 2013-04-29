package hadooptest.hadoop.regression;

import static org.junit.Assert.fail;
import hadooptest.TestSession;
import hadooptest.SerialTests;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.config.hadoop.HadoopConfiguration;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;

@Category(SerialTests.class)
public class TestConf extends TestSession {

	@BeforeClass
	public static void startTestSession() throws IOException {
		TestSession.start();
	}
	
	@Test
	public void backupHadoopConf() {
		try {
			FullyDistributedCluster cluster = (FullyDistributedCluster) TestSession.cluster;
			String component = HadoopConfiguration.RESOURCE_MANAGER;

			// Backup the default configuration directory on the Resource Manager
			// component host.
			cluster.getConf().backupConfDir(component);		

		}
		catch (Exception e) {
			TestSession.logger.error("Exception failure.", e);
			fail();
		}
	}

	@Test
	public void copyFilesToHadoopConf() {
		try {
			FullyDistributedCluster cluster = (FullyDistributedCluster) TestSession.cluster;
			String component = HadoopConfiguration.RESOURCE_MANAGER;

			// Copy files to the custom configuration directory on the
			// Resource Manager component host.
			// String sourceDir = "/homes/philips/svn/HadoopQEAutomation/branch-23/tests/Regression/YARN/CapacitySchedulerLimits/config/baseline/";
			String sourceDir = "./conf/TestConf/";
			cluster.getConf().copyFilesToConfDir(component, sourceDir);
		}
		catch (Exception e) {
			TestSession.logger.error("Exception failure.", e);
			fail();
		}
	}

	@Test
	public void modifyHadoopConf() {
		try {
			FullyDistributedCluster cluster = (FullyDistributedCluster) TestSession.cluster;
			String component = HadoopConfiguration.RESOURCE_MANAGER;
			String confFile = HadoopConfiguration.HADOOP_CONF_YARN;

			// Insert a property to the yarn-site.xml configuration file on the
			// Resource Manager component host.
			cluster.getConf().setHadoopConfFileProp (
					"yarn.admin.acl3",
					"gridadmin,hadoop,hadoopqa,philips,foo",
					component, confFile);
		}
		catch (Exception e) {
			TestSession.logger.error("Exception failure.", e);
			fail();
		}
	}

	@Test
	public void resetHadoopConf() {
		try {
			// Restart the cluster
			FullyDistributedCluster cluster = (FullyDistributedCluster) TestSession.cluster;
			TestSession.cluster.reset();
			cluster.waitForSafemodeOff();
			cluster.isFullyUp();
		}
		catch (Exception e) {
			TestSession.logger.error("Exception failure.", e);
			fail();
		}
	}

	@Test
	public void getHadoopConfProperty() {
		try {
			FullyDistributedCluster cluster = (FullyDistributedCluster) TestSession.cluster;
			String component = HadoopConfiguration.RESOURCE_MANAGER;
			String propName = "fs.trash.interval";
			String confFile = HadoopConfiguration.HADOOP_CONF_CORE;

			String propValue = cluster.getConf().getResourceProp(
					propName, confFile, component);
			TestSession.logger.info("Prop name '" + propName + "' = " + propValue);
		}
		catch (Exception e) {
			TestSession.logger.error("Exception failure.", e);
			fail();
		}
	}
	
}