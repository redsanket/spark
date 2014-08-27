package hadooptest.hadoop.regression.dfs;

import static org.junit.Assert.fail;
import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.cluster.hadoop.HadoopCluster;
import hadooptest.cluster.hadoop.HadoopCluster.Action;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;
import hadooptest.hadoop.regression.dfs.DfsadminReportBO.DatanodeBO;
import hadooptest.node.hadoop.HadoopNode;
import hadooptest.node.hadoop.fullydistributed.FullyDistributedNode;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SerialTests.class)
public class TestAdminReport extends DfsTestsBaseClass {
	private static final int MAX_WAIT = 60;
	private static final int POLL_INTERVAL = 5;
	private static final int NUM_NODES_TO_KILL = 2;
	private static String TEMPORARY_DFS_EXCLUDE_PATH = "/tmp/adminReport_conf/";
	private static String TEMPORARY_DFS_EXCLUDE_FILE = "dfs.exclude";

	ArrayList<String> killedOrExcludedNodes;


	@Before
	public void beforeTest() {
		killedOrExcludedNodes = new ArrayList<String>();
	}

	/**
	 * Port of: checkDeadNodesReported, checkLiveNodesReported, checkRestart.
	 * 
	 * Essentially here is what this test does 1. Check live/dead datanodes
	 * before/after stopping datanodes 2. Bounce the namenode, to ensure that
	 * the counts remain consistent Revide the deadnodes, and check that they
	 * re-appear
	 * 
	 * @throws Exception
	 */
	@Test
	public void testCheckDeadNodesReported() throws Exception {

		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;
		// Dfsadmin
		genericCliResponse = dfsCliCommands.dfsadmin(EMPTY_ENV_HASH_MAP,
				Report.YES, null, ClearQuota.NO, SetQuota.NO, 0,
				ClearSpaceQuota.NO, SetSpaceQuota.NO, 0, PrintTopology.NO,
				EMPTY_FS_ENTITY);
		DfsadminReportBO dfsadminReportBO = new DfsadminReportBO(
				genericCliResponse.response);

		if (dfsadminReportBO.liveDatanodes.size() < NUM_NODES_TO_KILL) {
			fail("Just " + dfsadminReportBO.liveDatanodes.size()
					+ " datanodes are alive. Need greater than "
					+ NUM_NODES_TO_KILL);
		}
		TestSession.logger
				.info("Number of livenodes, before stopping datanodes:"
						+ dfsadminReportBO.liveDatanodes.size());

		FullyDistributedCluster cluster = (FullyDistributedCluster) TestSession.cluster;

		// Stop datanodes
		for (int killCount = 0; killCount < NUM_NODES_TO_KILL; killCount++) {
			DatanodeBO aDatanodeBO = dfsadminReportBO.liveDatanodes
					.get(killCount);
			cluster.hadoopDaemon(Action.STOP,
					HadooptestConstants.NodeTypes.DATANODE,
					new String[] { aDatanodeBO.hostname });
			killedOrExcludedNodes.add(aDatanodeBO.hostname);
			TestSession.logger.info("Killed datanode:" + aDatanodeBO.hostname);

		}
		// Dfsadmin again
		genericCliResponse = dfsCliCommands.dfsadmin(EMPTY_ENV_HASH_MAP,
				Report.YES, null, ClearQuota.NO, SetQuota.NO, 0,
				ClearSpaceQuota.NO, SetSpaceQuota.NO, 0, PrintTopology.NO,
				EMPTY_FS_ENTITY);
		dfsadminReportBO = new DfsadminReportBO(genericCliResponse.response);

		TestSession.logger.info("Number of livenodes, after stopping:"
				+ dfsadminReportBO.liveDatanodes.size());

		int numberOfDeadDatanodesBeforeBouncingNamenode = dfsadminReportBO.deadDatanodes
				.size();

		/**
		 * Bounce the namenode (checkRestart) to check that the counts of downed
		 * nodes are consistent, across restarts
		 */
		TestSession.logger
				.info("Bouncing namenode....to check that dead host counts are "
						+ "consistent across namenode restarts. "
						+ "Test case-3 (check restart)");

		cluster.hadoopDaemon(Action.STOP,
				HadooptestConstants.NodeTypes.NAMENODE);
		cluster.hadoopDaemon(Action.START,
				HadooptestConstants.NodeTypes.NAMENODE);

		// Run the dfsadmin command again, to read in the dead nodes
		genericCliResponse = dfsCliCommands.dfsadmin(EMPTY_ENV_HASH_MAP,
				Report.YES, null, ClearQuota.NO, SetQuota.NO, 0,
				ClearSpaceQuota.NO, SetSpaceQuota.NO, 0, PrintTopology.NO,
				EMPTY_FS_ENTITY);
		dfsadminReportBO = new DfsadminReportBO(genericCliResponse.response);
		int numberOfDeadDatanodesAfterBouncingNamenode = dfsadminReportBO.deadDatanodes
				.size();

		Assert.assertTrue(
				"Count of dead datanodes before bouncing namenode="
						+ numberOfDeadDatanodesBeforeBouncingNamenode
						+ " did not match, count of dead datanodes after bouncing namenode="
						+ numberOfDeadDatanodesAfterBouncingNamenode,
				numberOfDeadDatanodesAfterBouncingNamenode == numberOfDeadDatanodesBeforeBouncingNamenode);

		// Revive nodes (checkLiveNodesReported)
		for (String aNodeToRevive : killedOrExcludedNodes) {
			cluster.hadoopDaemon(Action.START,
					HadooptestConstants.NodeTypes.DATANODE,
					new String[] { aNodeToRevive });
		}

		for (int revivalLoop = 0; revivalLoop < (MAX_WAIT / POLL_INTERVAL); revivalLoop++) {
			genericCliResponse = dfsCliCommands.dfsadmin(EMPTY_ENV_HASH_MAP,
					Report.YES, null, ClearQuota.NO, SetQuota.NO, 0,
					ClearSpaceQuota.NO, SetSpaceQuota.NO, 0, PrintTopology.NO,
					EMPTY_FS_ENTITY);
			dfsadminReportBO = new DfsadminReportBO(genericCliResponse.response);
			if (dfsadminReportBO.deadDatanodes.size() == 0)
				break;
		}

		Assert.assertTrue(dfsadminReportBO.deadDatanodes.size()
				+ " datanodes could not be revived, even after waiting for "
				+ MAX_WAIT + " seconds!",
				dfsadminReportBO.deadDatanodes.size() == 0);

	}

	/**
	 * Port of: checkRevivalOfExcludedNodes Essentially the test checks these:
	 * 1. Move the config dir to a new one 2. Edit the hdfs-site.xml to point to
	 * an empty dfs.exclude 3. Bounce the namenodes, to read in the new config
	 * 4. Assert that the excluded nodes are left out.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testCheckRevivalOfExcludedNodes() throws Exception {

		int NUM_NODES_TO_EXCLUDE = NUM_NODES_TO_KILL;
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;

		genericCliResponse = dfsCliCommands.dfsadmin(EMPTY_ENV_HASH_MAP,
				Report.YES, null, ClearQuota.NO, SetQuota.NO, 0,
				ClearSpaceQuota.NO, SetSpaceQuota.NO, 0, PrintTopology.NO,
				EMPTY_FS_ENTITY);
		DfsadminReportBO dfsadminReportBO = new DfsadminReportBO(
				genericCliResponse.response);

		if (dfsadminReportBO.liveDatanodes.size() < NUM_NODES_TO_EXCLUDE) {
			fail("Just " + dfsadminReportBO.liveDatanodes.size()
					+ " datanodes are alive. Need greater than "
					+ NUM_NODES_TO_EXCLUDE);
		}
		int countOfLiveNodesBeforeExcludingDataNodes = dfsadminReportBO.liveDatanodes
				.size();
		TestSession.logger.info("Number of livenodes, before excluding:"
				+ dfsadminReportBO.liveDatanodes.size());

		// Exclude datanodes
		for (int excludeCount = 0; excludeCount < NUM_NODES_TO_EXCLUDE; excludeCount++) {

			DatanodeBO aDatanodeBO = dfsadminReportBO.liveDatanodes
					.get(excludeCount);
			cluster.hadoopDaemon(Action.STOP,
					HadooptestConstants.NodeTypes.DATANODE,
					new String[] { aDatanodeBO.hostname });
			killedOrExcludedNodes.add(aDatanodeBO.hostname);
			TestSession.logger.info("stopped datanode, for exclusion:"
					+ aDatanodeBO.hostname);

		}
		Hashtable<String, HadoopNode> namenodeHash = TestSession.cluster
				.getNodes(HadooptestConstants.NodeTypes.NAMENODE);

		// Get the namenodes
		Enumeration<HadoopNode> namenodes = namenodeHash.elements();
		while (namenodes.hasMoreElements()) {
			// Backup the conf dir
			FullyDistributedCluster cluster = (FullyDistributedCluster) TestSession.cluster;
			FullyDistributedNode namenode = (FullyDistributedNode) namenodes
					.nextElement();
			namenode.getConf().backupConfDir();
			namenode.getConf().getHadoopConfDir();

			StringBuilder sb = new StringBuilder();
			sb.append("mkdir ");
			sb.append(TEMPORARY_DFS_EXCLUDE_PATH);

			String sshClientResponse = doJavaSSHClientExec("hdfsqa",
					namenode.getHostname(), sb.toString(),
					HADOOPQA_AS_HDFSQA_IDENTITY_FILE);

			// Chmod 755 on the temp folder
			sb = new StringBuilder();
			sb.append("chmod 755 ");
			sb.append(TEMPORARY_DFS_EXCLUDE_PATH);

			sshClientResponse = doJavaSSHClientExec("hdfsqa",
					namenode.getHostname(), sb.toString(),
					HADOOPQA_AS_HDFSQA_IDENTITY_FILE);

			// Write the excluded data nodes in the temp file
			for (String aDatanode : killedOrExcludedNodes) {
				sb = new StringBuilder();
				sb.append("echo " + aDatanode + " >>");
				sb.append(TEMPORARY_DFS_EXCLUDE_PATH
						+ TEMPORARY_DFS_EXCLUDE_FILE);
				sshClientResponse = doJavaSSHClientExec("hdfsqa",
						namenode.getHostname(), sb.toString(),
						HADOOPQA_AS_HDFSQA_IDENTITY_FILE);
			}
			// Read back the contents of the exclude file
			sb = new StringBuilder();
			sb.append("cat ");
			sb.append(TEMPORARY_DFS_EXCLUDE_PATH + TEMPORARY_DFS_EXCLUDE_FILE);
			sshClientResponse = doJavaSSHClientExec("hdfsqa",
					namenode.getHostname(), sb.toString(),
					HADOOPQA_AS_HDFSQA_IDENTITY_FILE);

			TestSession.logger.info("Read back file contents as ["
					+ sshClientResponse + "]");

			// Point the namenode conf to read that temp file
			String hdfsSiteXml = HadooptestConstants.ConfFileNames.HDFS_SITE_XML;

			cluster.getConf(HadooptestConstants.NodeTypes.NAMENODE)
					.setHadoopConfFileProp(
							"dfs.hosts.exclude",
							TEMPORARY_DFS_EXCLUDE_PATH
									+ TEMPORARY_DFS_EXCLUDE_FILE, hdfsSiteXml);

			// Bounce the namenode
			cluster.hadoopDaemon(Action.STOP,
					HadooptestConstants.NodeTypes.NAMENODE);
			cluster.hadoopDaemon(Action.START,
					HadooptestConstants.NodeTypes.NAMENODE);

			// Run the adminreport again
			genericCliResponse = dfsCliCommands.dfsadmin(EMPTY_ENV_HASH_MAP,
					Report.YES, null, ClearQuota.NO, SetQuota.NO, 0,
					ClearSpaceQuota.NO, SetSpaceQuota.NO, 0, PrintTopology.NO,
					EMPTY_FS_ENTITY);
			dfsadminReportBO = new DfsadminReportBO(genericCliResponse.response);
			int countOfLiveNodesAfterExcludingDataNodes = dfsadminReportBO.liveDatanodes
					.size();

			Assert.assertTrue(
					"Expected count of downed data nodes:"
							+ killedOrExcludedNodes.size()
							+ " did not match actual count of downed nodes:"
							+ (countOfLiveNodesBeforeExcludingDataNodes - countOfLiveNodesAfterExcludingDataNodes),
					(countOfLiveNodesBeforeExcludingDataNodes - countOfLiveNodesAfterExcludingDataNodes) == 2);

			sb = new StringBuilder();
			sb.append("rm " + TEMPORARY_DFS_EXCLUDE_PATH
					+ TEMPORARY_DFS_EXCLUDE_FILE);
			sshClientResponse = doJavaSSHClientExec("hdfsqa",
					namenode.getHostname(), sb.toString(),
					HADOOPQA_AS_HDFSQA_IDENTITY_FILE);

			// Start the datanodes
			for (String aNodeToRevive : killedOrExcludedNodes) {
				cluster.hadoopDaemon(Action.START,
						HadooptestConstants.NodeTypes.DATANODE,
						new String[] { aNodeToRevive });
			}

			// Reset the configurations, to the default
			namenode.getConf().resetHadoopConfDir();

			// Bounce the namenode
			FullyDistributedCluster fullyDistributedCluster = (FullyDistributedCluster) TestSession
					.getCluster();

			cluster.hadoopDaemon(Action.STOP,
					HadooptestConstants.NodeTypes.NAMENODE);
			fullyDistributedCluster.hadoopDaemon(Action.START,
					HadooptestConstants.NodeTypes.NAMENODE,
					TestSession.cluster.getNodeNames(HadoopCluster.NAMENODE),
					TestSession.conf.getProperty("HADOOP_INSTALL_CONF_DIR"));

		}

		// Every thing should be back to normal from this point onwards
		genericCliResponse = dfsCliCommands.dfsadmin(EMPTY_ENV_HASH_MAP,
				Report.YES, null, ClearQuota.NO, SetQuota.NO, 0,
				ClearSpaceQuota.NO, SetSpaceQuota.NO, 0, PrintTopology.NO,
				EMPTY_FS_ENTITY);
		dfsadminReportBO = new DfsadminReportBO(genericCliResponse.response);

	}

	/**
	 * Port of: checkZeroDataNodes Essentially this test checks: 1. Backup the
	 * config directory 2. Delete the 'slaves' file from the config directory 3.
	 * Bounce the cluster 4. None of the datanodes should show up in the admin
	 * report.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testCheckZeroDataNodes() throws Exception {

		int NUM_NODES_TO_EXCLUDE = NUM_NODES_TO_KILL;
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;

		genericCliResponse = dfsCliCommands.dfsadmin(EMPTY_ENV_HASH_MAP,
				Report.YES, null, ClearQuota.NO, SetQuota.NO, 0,
				ClearSpaceQuota.NO, SetSpaceQuota.NO, 0, PrintTopology.NO,
				EMPTY_FS_ENTITY);
		DfsadminReportBO dfsadminReportBO = new DfsadminReportBO(
				genericCliResponse.response);

		if (dfsadminReportBO.liveDatanodes.size() < NUM_NODES_TO_EXCLUDE) {
			fail("Just " + dfsadminReportBO.liveDatanodes.size()
					+ " datanodes are alive. Need greater than "
					+ NUM_NODES_TO_EXCLUDE);
		}
		int countOfLiveNodesBeforeTestBegan = dfsadminReportBO.liveDatanodes
				.size();
		TestSession.logger.info("Number of livenodes, before excluding:"
				+ dfsadminReportBO.liveDatanodes.size());

		Hashtable<String, HadoopNode> namenodeHash = TestSession.cluster
				.getNodes(HadooptestConstants.NodeTypes.NAMENODE);

		// Get the namenodes
		Enumeration<HadoopNode> namenodes = namenodeHash.elements();
		while (namenodes.hasMoreElements()) {
			// Backup the conf dir
			FullyDistributedCluster cluster = (FullyDistributedCluster) TestSession.cluster;
			FullyDistributedNode namenode = (FullyDistributedNode) namenodes
					.nextElement();
			namenode.getConf().backupConfDir();
			String newConfDirLocation = namenode.getConf().getHadoopConfDir();

			// Remove the slaves file in the new location.
			StringBuilder sb = new StringBuilder();
			sb.append("rm -f ");
			sb.append(newConfDirLocation + "/slaves");

			String sshClientResponse = doJavaSSHClientExec("hadoopqa",
					namenode.getHostname(), sb.toString(), HADOOPQA_BLUE_DSA);

			// Bounce the cluster
			cluster.stop();
			// cluster.start();
			cluster.hadoopDaemon(Action.START,
					HadooptestConstants.NodeTypes.NAMENODE);

			// Run the adminreport again
			genericCliResponse = dfsCliCommands.dfsadmin(EMPTY_ENV_HASH_MAP,
					Report.YES, null, ClearQuota.NO, SetQuota.NO, 0,
					ClearSpaceQuota.NO, SetSpaceQuota.NO, 0, PrintTopology.NO,
					EMPTY_FS_ENTITY);
			dfsadminReportBO = new DfsadminReportBO(genericCliResponse.response);

			Assert.assertTrue(
					"None of the datanodes should have shown up as live, since I've removed the slaves file",
					dfsadminReportBO.liveDatanodes.size() == 0);
			Assert.assertTrue(
					"None of the datanodes should have shown up as dead, since I've removed the slaves file",
					dfsadminReportBO.deadDatanodes.size() == 0);

			// Reset the configurations, to the default
			namenode.getConf().resetHadoopConfDir();

			cluster.start();

		}

		// Every thing should be back to normal from this point onwards
		genericCliResponse = dfsCliCommands.dfsadmin(EMPTY_ENV_HASH_MAP,
				Report.YES, null, ClearQuota.NO, SetQuota.NO, 0,
				ClearSpaceQuota.NO, SetSpaceQuota.NO, 0, PrintTopology.NO,
				EMPTY_FS_ENTITY);
		dfsadminReportBO = new DfsadminReportBO(genericCliResponse.response);

		int countOfLiveNodesAfterTestFinished = dfsadminReportBO.liveDatanodes
				.size();

		Assert.assertTrue(
				"Count of live nodes mismatched. Count live datanodes before test ran="
						+ countOfLiveNodesBeforeTestBegan
						+ " Count of live datanodes after test ran="
						+ countOfLiveNodesAfterTestFinished,
				countOfLiveNodesBeforeTestBegan == countOfLiveNodesAfterTestFinished);

	}

	@After
	public void afterTest() throws Exception {
		for (String aNodeToRevive : killedOrExcludedNodes) {
			cluster.hadoopDaemon(Action.START,
					HadooptestConstants.NodeTypes.DATANODE,
					new String[] { aNodeToRevive });
		}

	}

}
