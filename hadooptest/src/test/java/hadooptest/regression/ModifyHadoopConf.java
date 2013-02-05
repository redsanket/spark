package hadooptest.regression;

import hadooptest.TestSession;
import hadooptest.cluster.fullydistributed.FullyDistributedCluster;
import hadooptest.config.testconfig.FullyDistributedConfiguration;

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

public class ModifyHadoopConf extends TestSession {

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

		String component = "resourcemanager";
		FullyDistributedConfiguration conf = cluster.getConf();
		conf.backupConfDir(component);
		
		TestSession.cluster.reset();
		cluster.waitForSafemodeOff();
		cluster.getClusterStatus();

		/*
		String confType = "HADOOP_CONF_CAPACITY_SCHEDULER";
		String propName = "yarn.scheduler.capacity.root.capacity";
		String propValue = conf.getHadoopConfFileProp(confType, propName);
		TestSession.logger.info("Yarn scheduler root capacity = " + propValue);
		*/
		
		String confType = "HADOOP_CONF_CORE_SN";
		String propName = "fs.trash.interval";
		// String propValue = conf.getHadoopConfFileProp(propName, confType,
		// "resourcemanager");
		String propValue = conf.getHadoopConfFileProp(propName,
				FullyDistributedConfiguration.HADOOP_CONF_CORE,
				FullyDistributedConfiguration.RESOURCE_MANAGER);
		TestSession.logger.info("Prop name '" + propName + "' = " + propValue);
	}

	/*

	@Test
	public void modifyHadoopConf() {

		
		FullyDistributedCluster cluster = (FullyDistributedCluster) TestSession.cluster;

		String component = "resourcemanager";
		String confType = "HADOOP_CONF_CAPACITY_SCHEDULER";
		FullyDistributedConfiguration conf = cluster.getConf();
		String propName = "";
		String propValue = "";
		conf.setHadoopConfProp(propName, propValue, component, confType, 
				conf.getHadoopBackupConf(component));
		
		TestSession.cluster.reset();
		cluster.waitForSafemodeOff();
		cluster.getClusterStatus();
				
	}
	
	@Test
	public void replaceHadoopConf() {
		
		FullyDistributedCluster cluster = (FullyDistributedCluster) TestSession.cluster;

		String component = "resourcemanager";
		FullyDistributedConfiguration conf = cluster.getConf();
		conf.backupConfDir(component);
		String sourceDir = "";
		conf.copyToConfDir(component, sourceDir);
		
		TestSession.cluster.reset();
		cluster.waitForSafemodeOff();
		cluster.getClusterStatus();
				
	}
	
	*/
	
}