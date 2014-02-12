package hadooptest.dfs.regression;

import static org.junit.Assert.*;
import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.cluster.hadoop.HadoopCluster.Action;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.dfs.regression.DfsBaseClass.ClearQuota;
import hadooptest.dfs.regression.DfsBaseClass.ClearSpaceQuota;
import hadooptest.dfs.regression.DfsBaseClass.Report;
import hadooptest.dfs.regression.DfsBaseClass.SetQuota;
import hadooptest.dfs.regression.DfsBaseClass.SetSpaceQuota;
import hadooptest.dfs.regression.DfsCliCommands.GenericCliResponseBO;
import hadooptest.dfs.regression.DfsadminReportBO.DatanodeBO;
import hadooptest.node.hadoop.HadoopNode;
import hadooptest.node.hadoop.fullydistributed.FullyDistributedNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@Category(SerialTests.class)
public class TestAdminReport extends DfsBaseClass {
	private static final int MAX_WAIT = 60;
	private static final int POLL_INTERVAL = 5;
	private static final int NUM_NODES_TO_KILL = 2;
	private static String TEMPORARY_DFS_EXCLUDE_PATH = "/tmp/adminReport_conf/";
	private static String TEMPORARY_DFS_EXCLUDE_FILE = "dfs.exclude";

	static Logger logger = Logger.getLogger(TestAdminReport.class);

	/**
	 * While porting, I've combined these 3 tests into a single test case.
	 * checkDeadNodesReported checkLiveNodesReported checkRestart
	 * 
	 * @throws Exception
	 */
	@Test
	public void testCheckDeadNodesReported() throws Exception {
		ArrayList<String> killedNodes = new ArrayList<String>();
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;
		genericCliResponse = dfsCliCommands.dfsadmin(EMPTY_ENV_HASH_MAP,
				Report.YES, null, ClearQuota.NO, SetQuota.NO, 0,
				ClearSpaceQuota.NO, SetSpaceQuota.NO, 0, null);
		DfsadminReportBO dfsadminReportBO = new DfsadminReportBO(
				genericCliResponse.response);
		if (dfsadminReportBO.liveDatanodes.size() < NUM_NODES_TO_KILL) {
			fail("Just " + dfsadminReportBO.liveDatanodes.size()
					+ " datanodes are alive. Need greater than "
					+ NUM_NODES_TO_KILL);
		}
		TestSession.logger.info("Number of livenodes, before killing:"
				+ dfsadminReportBO.liveDatanodes.size());

		FullyDistributedCluster cluster = (FullyDistributedCluster) TestSession.cluster;

		// Kill datanodes
		for (int killCount = 0; killCount < NUM_NODES_TO_KILL; killCount++) {

			DatanodeBO aDatanodeBO = dfsadminReportBO.liveDatanodes
					.get(killCount);
			cluster.hadoopDaemon(Action.STOP,
					HadooptestConstants.NodeTypes.DATANODE,
					new String[] { aDatanodeBO.hostname });
			killedNodes.add(aDatanodeBO.hostname);
			TestSession.logger.info("Killed datanode:" + aDatanodeBO.hostname);

		}
		genericCliResponse = dfsCliCommands.dfsadmin(EMPTY_ENV_HASH_MAP,
				Report.YES, null, ClearQuota.NO, SetQuota.NO, 0,
				ClearSpaceQuota.NO, SetSpaceQuota.NO, 0, null);
		dfsadminReportBO = new DfsadminReportBO(genericCliResponse.response);

		TestSession.logger.info("Number of livenodes, after killing:"
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
				ClearSpaceQuota.NO, SetSpaceQuota.NO, 0, null);
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
		for (String aNodeToRevive : killedNodes) {
			cluster.hadoopDaemon(Action.START,
					HadooptestConstants.NodeTypes.DATANODE,
					new String[] { aNodeToRevive });
		}

		for (int revivalLoop = 0; revivalLoop < (MAX_WAIT / POLL_INTERVAL); revivalLoop++) {
			genericCliResponse = dfsCliCommands.dfsadmin(EMPTY_ENV_HASH_MAP,
					Report.YES, null, ClearQuota.NO, SetQuota.NO, 0,
					ClearSpaceQuota.NO, SetSpaceQuota.NO, 0, null);
			dfsadminReportBO = new DfsadminReportBO(genericCliResponse.response);
			if (dfsadminReportBO.deadDatanodes.size() == 0)
				break;
		}
		Assert.assertTrue(dfsadminReportBO.deadDatanodes.size()
				+ " datanodes could not be revived, even after waiting for "
				+ MAX_WAIT + " seconds!",
				dfsadminReportBO.deadDatanodes.size() == 0);

	}

	@Test
	public void testCheckRevivalOfExcludedNodes() throws Exception {
		ArrayList<String> excludedNodes = new ArrayList<String>();
		int NUM_NODES_TO_EXCLUDE = NUM_NODES_TO_KILL;
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;

		genericCliResponse = dfsCliCommands.dfsadmin(EMPTY_ENV_HASH_MAP,
				Report.YES, null, ClearQuota.NO, SetQuota.NO, 0,
				ClearSpaceQuota.NO, SetSpaceQuota.NO, 0, null);
		DfsadminReportBO dfsadminReportBO = new DfsadminReportBO(
				genericCliResponse.response);

		if (dfsadminReportBO.liveDatanodes.size() < NUM_NODES_TO_EXCLUDE) {
			fail("Just " + dfsadminReportBO.liveDatanodes.size()
					+ " datanodes are alive. Need greater than "
					+ NUM_NODES_TO_EXCLUDE);
		}
		
		TestSession.logger.info("Number of livenodes, before excluding:"
				+ dfsadminReportBO.liveDatanodes.size());

		// Exclude datanodes
		for (int excludeCount = 0; excludeCount < NUM_NODES_TO_EXCLUDE; excludeCount++) {

			DatanodeBO aDatanodeBO = dfsadminReportBO.liveDatanodes
					.get(excludeCount);
			cluster.hadoopDaemon(Action.STOP,
					HadooptestConstants.NodeTypes.DATANODE,
					new String[] { aDatanodeBO.hostname });
			excludedNodes.add(aDatanodeBO.hostname);
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
			sb.append("chmod 755");
			sb.append(TEMPORARY_DFS_EXCLUDE_PATH);

			sshClientResponse = doJavaSSHClientExec("hdfsqa",
					namenode.getHostname(), sb.toString(),
					HADOOPQA_AS_HDFSQA_IDENTITY_FILE);

			// Write the excluded data nodes in the temp file
			for (String aDatanode : excludedNodes) {
				sb = new StringBuilder();
				sb.append("echo " + "\"" + aDatanode + "\"" + " >>");
				sb.append(TEMPORARY_DFS_EXCLUDE_PATH
						+ TEMPORARY_DFS_EXCLUDE_FILE);
				sshClientResponse = doJavaSSHClientExec("hdfsqa",
						namenode.getHostname(), sb.toString(),
						HADOOPQA_AS_HDFSQA_IDENTITY_FILE);
			}
			
			// Point the namenode conf to read that temp file
			String hdfsSiteXml = HadooptestConstants.ConfFileNames.HDFS_SITE_XML;
			cluster.getConf(HadooptestConstants.NodeTypes.NAMENODE)
					.setHadoopConfFileProp(
							"dfs.hosts.exclude",
							TEMPORARY_DFS_EXCLUDE_PATH
									+ TEMPORARY_DFS_EXCLUDE_FILE, hdfsSiteXml);
			
			//Bounce the namenode
			cluster.hadoopDaemon(Action.STOP, HadooptestConstants.NodeTypes.NAMENODE);
			cluster.hadoopDaemon(Action.START, HadooptestConstants.NodeTypes.NAMENODE);

		}

	}

	@Override
	@After
	public void logTaskReportSummary() {
		// Override to hide the Test Session logs
	}

}
