package hadooptest.dfs.regression;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.cluster.hadoop.HadoopCluster.Action;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.dfs.regression.DfsBaseClass.PrintTopology;
import hadooptest.dfs.regression.DfsBaseClass.Report;
import hadooptest.dfs.regression.DfsCliCommands.GenericCliResponseBO;

import java.util.Arrays;
import java.util.Collection;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
@Category(SerialTests.class)
public class TestMetrics extends DfsBaseClass {
	String protocol;

	public TestMetrics(String protocol) {
		this.protocol = protocol;
		logger.info("Test invoked for protocol/schema:" + protocol);
	}

	static Logger logger = Logger.getLogger(TestMetrics.class);

	private static String TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_DATA_DIR = "/user/hadoopqa/FilesInGetListingOps/";
	private static String TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_DATA_DIR00 = "/user/hadoopqa/FilesInGetListingOps/dir00/";
	private static String TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_DATA_DIR01 = "/user/hadoopqa/FilesInGetListingOps/dir01/";
	private static String TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_DATA_DIR_EMPTY = "/user/hadoopqa/FilesInGetListingOps/dir_empty/";
	private static String PROPERTY = "FilesInGetListingOps";
	private static String SERVICE = "Hadoop:name=NameNodeActivity,service=NameNode";
	private static String METRICS_PROP_FILE = HadooptestConstants.Location.Conf.DIRECTORY
			+ "hadoop-metrics2.properties";
	private int PROP_REFRESH_TIME = 0;
	private int CURRENT_VALUE = 0;
	private static String METRICS_PROP = "'*.period'";

	private String namenodeHostname;
	private static String localCluster = System.getProperty("CLUSTER_NAME");

	@Parameters
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][] {
				// Schemas
				{ HadooptestConstants.Schema.WEBHDFS }, { "" },
				{ HadooptestConstants.Schema.HDFS }, });
	}

	@Before
	public void beforeEachTest() throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;

		readInNamenodeHostnameIntoMemory();

		// Stop the History server
		FullyDistributedCluster cluster = (FullyDistributedCluster) TestSession.cluster;
		cluster.hadoopDaemon(Action.STOP,
				HadooptestConstants.NodeTypes.HISTORY_SERVER);

		// Delete dir if already exists
		genericCliResponse = dfsCliCommands.rm(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				Recursive.YES, Force.YES, SkipTrash.YES,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_DATA_DIR);

		// Create the dir
		genericCliResponse = dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_DATA_DIR);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		createTestData();

		StringBuilder sb = new StringBuilder();
		sb.append("/bin/cat ");
		sb.append(METRICS_PROP_FILE);
		sb.append(" | ");
		sb.append("/bin/grep ");
		sb.append(METRICS_PROP);
		sb.append(" | ");
		sb.append("/bin/cut ");
		sb.append("-d");
		sb.append(" ");
		sb.append("'='");
		sb.append(" ");
		sb.append("-f2");

		String response = doJavaSSHClientExec("hdfsqa", "localhost",
				sb.toString(), HADOOPQA_AS_HDFSQA_IDENTITY_FILE);
		PROP_REFRESH_TIME = Integer.parseInt(response.trim());

		waitForRefresh();

		resetInfo();

	}

	void readInNamenodeHostnameIntoMemory() {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		namenodeHostname = dfsCliCommands.getNNUrlForHdfs(System
				.getProperty("CLUSTER_NAME"));
		namenodeHostname = namenodeHostname.replace("hdfs://", "");
		namenodeHostname = namenodeHostname.replace(":8020", "");
	}

	void waitForRefresh() {
		try {
			Thread.sleep(3 * PROP_REFRESH_TIME * 1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	void createTestData() throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;

		// dir00
		genericCliResponse = dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_DATA_DIR00);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
		// dir01
		genericCliResponse = dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_DATA_DIR01);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
		// dir_empty
		genericCliResponse = dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_DATA_DIR_EMPTY);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		// Create files, under DATA_DIR
		genericCliResponse = dfsCliCommands.touchz(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_DATA_DIR + "file00.txt");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
		genericCliResponse = dfsCliCommands.touchz(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_DATA_DIR + "file01.txt");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
		genericCliResponse = dfsCliCommands.touchz(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_DATA_DIR + "file02.txt");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		// Create files, under DATA_DIR00
		genericCliResponse = dfsCliCommands.touchz(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_DATA_DIR00 + "file10.txt");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
		genericCliResponse = dfsCliCommands.touchz(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_DATA_DIR00 + "file11.txt");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
		genericCliResponse = dfsCliCommands.touchz(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_DATA_DIR00 + "file12.txt");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
		genericCliResponse = dfsCliCommands.touchz(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_DATA_DIR00 + "file13.txt");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
		genericCliResponse = dfsCliCommands.touchz(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_DATA_DIR00 + "file14.txt");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
		genericCliResponse = dfsCliCommands.touchz(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_DATA_DIR00 + "file15.txt");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
		genericCliResponse = dfsCliCommands.touchz(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_DATA_DIR00 + "file16.txt");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
		genericCliResponse = dfsCliCommands.touchz(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_DATA_DIR00 + "file17.txt");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
		genericCliResponse = dfsCliCommands.touchz(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_DATA_DIR00 + "file18.txt");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
		genericCliResponse = dfsCliCommands.touchz(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_DATA_DIR00 + "file19.txt");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		// Create 5 files, under DATA_DIR01
		genericCliResponse = dfsCliCommands.touchz(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_DATA_DIR01 + "file10.txt");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
		genericCliResponse = dfsCliCommands.touchz(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_DATA_DIR01 + "file11.txt");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
		genericCliResponse = dfsCliCommands.touchz(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_DATA_DIR01 + "file12.txt");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
		genericCliResponse = dfsCliCommands.touchz(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_DATA_DIR01 + "file13.txt");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
		genericCliResponse = dfsCliCommands.touchz(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_DATA_DIR01 + "file14.txt");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
		// Create 10 dirs under dir01
		genericCliResponse = dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_DATA_DIR01 + "dir10/");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
		genericCliResponse = dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_DATA_DIR01 + "dir11/");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
		genericCliResponse = dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_DATA_DIR01 + "dir12/");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
		genericCliResponse = dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_DATA_DIR01 + "dir13/");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
		genericCliResponse = dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_DATA_DIR01 + "dir14/");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
		genericCliResponse = dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_DATA_DIR01 + "dir15/");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
		genericCliResponse = dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_DATA_DIR01 + "dir16/");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
		genericCliResponse = dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_DATA_DIR01 + "dir17/");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
		genericCliResponse = dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_DATA_DIR01 + "dir18/");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
		genericCliResponse = dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_DATA_DIR01 + "dir19/");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

	}

	void resetInfo() {
		StringBuilder sb = new StringBuilder();
		String pid = getNamenodePID();

		CURRENT_VALUE = getJMXPropertyValue(pid);
		TestSession.logger.info("Current value read from JMX:" + CURRENT_VALUE
				+ " , read off of Namenode:" + namenodeHostname);

	}

	String getNamenodePID() {
		// Get the PID
		StringBuilder sb = new StringBuilder();
		sb.append("ps aux");
		sb.append(" | ");
		sb.append("grep ");
		sb.append("namenode");
		sb.append(" | ");
		sb.append("grep [j]ava ");
		sb.append(" | ");
		sb.append("awk '{print $2}'");
		String response = doJavaSSHClientExec("hdfsqa", namenodeHostname,
				sb.toString(), HADOOPQA_AS_HDFSQA_IDENTITY_FILE);
		TestSession.logger.info("PID of Namenode process:" + response
				+ " , read off of Namenode:" + namenodeHostname);
		return response.trim();

	}

	int getJMXPropertyValue(String pid) {
		StringBuilder sb = new StringBuilder();
		// Get JMX Property value
		sb = new StringBuilder();
		sb.append("ps aux");
		sb.append(" | ");
		sb.append("grep ");
		sb.append("namenode");
		sb.append(" | ");
		sb.append("grep [j]ava ");
		sb.append(" | ");
		sb.append("awk '{print $2}'");
		String response = doJavaSSHClientExec(
				"hdfsqa",
				namenodeHostname,
				"(echo open "
						+ pid
						+ "; echo get -b "
						+ SERVICE
						+ " "
						+ PROPERTY
						+ ")|java -jar /homes/mapred/jmxterm-1.0-SNAPSHOT-uber.jar |grep "
						+ PROPERTY, HADOOPQA_AS_HDFSQA_IDENTITY_FILE);
		TestSession.logger.info("Read back from JMX[" + response + "]");
		response = response.split("\\s+")[2];
		response = response.replaceAll(";", "");
		return Integer.parseInt(response.trim());

	}

	@Test
	public void test_FilesInGetListingOps1() throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;

		waitForRefresh();
		resetInfo();
		genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_DATA_DIR00, Recursive.NO);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		waitForRefresh();

		String namenodePID = getNamenodePID();
		int newValue = getJMXPropertyValue(namenodePID);
		TestSession.logger.info("newValue:" + newValue);
		int change = newValue - CURRENT_VALUE;
		Assert.assertTrue("Change:" + change + " is not equal t0 10",
				change == 10);
		CURRENT_VALUE = newValue;

	}

	@Test
	public void test_FilesInGetListingOps2() throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;

		waitForRefresh();

		resetInfo();
		genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_DATA_DIR, Recursive.YES);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		waitForRefresh();

		String namenodePID = getNamenodePID();
		int newValue = getJMXPropertyValue(namenodePID);
		TestSession.logger.info("newValue:" + newValue);
		int change = newValue - CURRENT_VALUE;
		Assert.assertTrue("Change:" + change + " is not equal to 31",
				change == 31);
		CURRENT_VALUE = newValue;

	}

	@Test
	public void test_FilesInGetListingOps4() throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;

		waitForRefresh();

		resetInfo();
		genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_DATA_DIR00, Recursive.YES);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		waitForRefresh();

		String namenodePID = getNamenodePID();
		int newValue = getJMXPropertyValue(namenodePID);
		TestSession.logger.info("newValue:" + newValue);
		// int change = newValue - CURRENT_VALUE;
		// Assert.assertTrue("Change:" + change + " is not equal to 31",
		// change == 31);
		// CURRENT_VALUE = newValue;
		// Bounce the Namenode server
		FullyDistributedCluster cluster = (FullyDistributedCluster) TestSession.cluster;
		cluster.hadoopDaemon(Action.STOP,
				HadooptestConstants.NodeTypes.NAMENODE);
		cluster.hadoopDaemon(Action.START,
				HadooptestConstants.NodeTypes.NAMENODE);

		// Get NN out of sademode
		dfsCliCommands.dfsadmin(EMPTY_ENV_HASH_MAP, Report.NO, "get",
				ClearQuota.NO, SetQuota.NO, 0, ClearSpaceQuota.NO,
				SetSpaceQuota.NO, 0, PrintTopology.NO, EMPTY_FS_ENTITY);
		dfsCliCommands.dfsadmin(EMPTY_ENV_HASH_MAP, Report.NO, "leave",
				ClearQuota.NO, SetQuota.NO, 0, ClearSpaceQuota.NO,
				SetSpaceQuota.NO, 0, PrintTopology.NO, EMPTY_FS_ENTITY);

		resetInfo();
		Assert.assertTrue("The current value for property " + PROPERTY
				+ " does not reset to 0 after NN restart. See Bug 4626670",
				CURRENT_VALUE == 0);

	}

	@Test
	public void test_FilesInGetListingOps5() throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;

		waitForRefresh();

		resetInfo();

		// Issue dfs -ls command on DATA_DIR, it has 31 items
		genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_DATA_DIR, Recursive.YES);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
		int numOfItemsListed = genericCliResponse.response.split("\n").length;
		TestSession.logger.info("Read back count of items as:"
				+ numOfItemsListed + " was expecting 31");

		// Issue dfs -ls command on DATA_DIR, it has 6 items
		genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_DATA_DIR, Recursive.NO);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
		numOfItemsListed = genericCliResponse.response.split("\n").length;
		TestSession.logger.info("Read back count of items as:"
				+ numOfItemsListed + " was expecting 6");

		// Issue dfs -ls command on dir01, it has 15 items
		genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_DATA_DIR01, Recursive.NO);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
		numOfItemsListed = genericCliResponse.response.split("\n").length;
		TestSession.logger.info("Read back count of items as:"
				+ numOfItemsListed + " was expecting 15");

		// Issue dfs -ls command on DATA_DIR, it has 31 items
		genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_DATA_DIR, Recursive.YES);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);
		numOfItemsListed = genericCliResponse.response.split("\n").length;
		TestSession.logger.info("Read back count of items as:"
				+ numOfItemsListed + " was expecting 31");

		waitForRefresh();

		String namenodePID = getNamenodePID();
		int newValue = getJMXPropertyValue(namenodePID);
		TestSession.logger.info("newValue:" + newValue);
		int change = newValue - CURRENT_VALUE;
		Assert.assertTrue("Change:" + change + " is not equal to 83",
				change == 83);
		CURRENT_VALUE = newValue;

	}

	@Test
	public void test_FilesInGetListingOps6() throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;

		waitForRefresh();

		resetInfo();

		genericCliResponse = dfsCliCommands
				.ls(EMPTY_ENV_HASH_MAP, HadooptestConstants.UserNames.HADOOPQA,
						protocol, localCluster,
						TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_DATA_DIR_EMPTY,
						Recursive.NO);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		waitForRefresh();

		String namenodePID = getNamenodePID();
		int newValue = getJMXPropertyValue(namenodePID);
		TestSession.logger.info("newValue:" + newValue);

		int change = newValue - CURRENT_VALUE;
		Assert.assertTrue(
				"Listed an empty_directory,  so the change should've been 0, got "
						+ change + " instead", change == 0);

	}

	@After
	public void afterTest() throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse = null;

		genericCliResponse = dfsCliCommands.rm(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				Recursive.YES, Force.YES, SkipTrash.YES,
				TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_DATA_DIR);
		Assert.assertTrue("Not able to rm "
				+ TEST_FOLDER_ON_HDFS_REFERRED_TO_AS_DATA_DIR,
				genericCliResponse.process.exitValue() == 0);
		FullyDistributedCluster cluster = (FullyDistributedCluster) TestSession.cluster;
		cluster.hadoopDaemon(Action.START,
				HadooptestConstants.NodeTypes.HISTORY_SERVER);

	}

	@Override
	@After
	public void logTaskReportSummary() {
		// Override to hide the Test Session logs
	}

}
