package hadooptest.hadoop.regression.yarn;

import hadooptest.Util;
import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.cluster.hadoop.HadoopCluster;
import hadooptest.cluster.hadoop.HadoopCluster.Action;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedExecutor;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.ClearQuota;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.ClearSpaceQuota;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.PrintTopology;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Report;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.SetQuota;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.SetSpaceQuota;
import hadooptest.hadoop.regression.dfs.DfsadminReportBO;
import hadooptest.hadoop.regression.yarn.MapredCliCommands.GenericMapredCliResponseBO;
import hadooptest.hadoop.regression.yarn.YarnCliCommands.GenericYarnCliResponseBO;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

@Category(SerialTests.class)
public class TestExcludeNodeCheck extends YarnTestsBaseClass {
	private static String TT_HOSTS_FILE = "TT_hosts";
	private static String EMPTY_FILE = "empty.file";
	private static String customConfDir = "/homes/hadoopqa/tmp/CustomConfDir";
	private static String SSH_OPTS = " -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "; 

	@Rule
	public TemporaryFolder testFolder = new TemporaryFolder();

	@After
	public void restoreState() throws Exception {
		FullyDistributedCluster fullyDistributedCluster = (FullyDistributedCluster) TestSession
				.getCluster();
		// Bounce the RM
		Thread.sleep(10000);
		fullyDistributedCluster.hadoopDaemon(Action.STOP,
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
		fullyDistributedCluster.getConf(
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER)
				.resetHadoopConfDir();
		TestSession.logger.info("Reset the config file location for ResourceManager");
		Thread.sleep(5000);
		fullyDistributedCluster.hadoopDaemon(Action.START,
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER,
				TestSession.cluster
						.getNodeNames(HadoopCluster.RESOURCE_MANAGER),
				TestSession.conf.getProperty("HADOOP_INSTALL_CONF_DIR"));
		// Bounce the NM
		Thread.sleep(10000);
		fullyDistributedCluster.hadoopDaemon(Action.STOP,
				HadooptestConstants.NodeTypes.NODE_MANAGER);
		fullyDistributedCluster.getConf(
				HadooptestConstants.NodeTypes.NODE_MANAGER)
				.resetHadoopConfDir();
		TestSession.logger.info("Reset the config file location for NodeManager");
		Thread.sleep(5000);
		fullyDistributedCluster.hadoopDaemon(Action.START,
				HadooptestConstants.NodeTypes.NODE_MANAGER,
				TestSession.cluster
						.getNodeNames(HadoopCluster.NODEMANAGER),
				TestSession.conf.getProperty("HADOOP_INSTALL_CONF_DIR"));

		Thread.sleep(10000);

	}

	/**
	 * This test verifies that RM can read the exclude file on a restart and
	 * subsequently move indicated datanodes into the exclude list.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testExcludeActiveNodes() throws Exception {
		FullyDistributedCluster fullyDistributedCluster = (FullyDistributedCluster) TestSession
				.getCluster();

		File fileContainingHostsToIgnore = testFolder.newFile(TT_HOSTS_FILE);
		File empty_include_file = testFolder.newFile(EMPTY_FILE);

		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;
		/**
		 * Read in the initial node list
		 */
		genericCliResponse = dfsCliCommands.dfsadmin(EMPTY_ENV_HASH_MAP,
				Report.YES, null, ClearQuota.NO, SetQuota.NO, 0,
				ClearSpaceQuota.NO, SetSpaceQuota.NO, 0, PrintTopology.NO,
				DfsTestsBaseClass.EMPTY_FS_ENTITY);
		DfsadminReportBO dfsadminReportBO = new DfsadminReportBO(
				genericCliResponse.response);

		/**
		 * Get the initial count of active trackers. I have to run a different
		 * command here, because the dfsadmin -report doesnot include the
		 * excluded nodes in its response.
		 */
		int initialCountOfActiveTrackers = getCountOfActiveTrackers();
		TestSession.logger.info("Initial count of active trackers:" + initialCountOfActiveTrackers);

		/**
		 * Populate the hosts to ignore
		 */
		populateIgnoreFile(fileContainingHostsToIgnore, dfsadminReportBO);

		/**
		 * Copy over the file (with hosts to ignore) over to RM
		 */

		copyTheFileToIgnoreOverToRM(fileContainingHostsToIgnore);
		/**
		 * Bounce the RM and NM to read in the new configs
		 */
		updateRMConfigAndNMConfigAndBounceThem(true, false, null);

		int laterCountOfActiveTrackers = getCountOfActiveTrackers();

		TestSession.logger.info("Later count of active trackers:"
				+ laterCountOfActiveTrackers);

		Assert.assertTrue("Initial count of trackers:"
				+ initialCountOfActiveTrackers + " later count of trackers:"
				+ laterCountOfActiveTrackers,
				initialCountOfActiveTrackers > laterCountOfActiveTrackers);

	}

	/**
	 * This test verifies that the exclusion of nodes, is effected by a 'yarn
	 * rmadmin -refreshNodes' command. The test initially copies an empty file
	 * (for exclude list) and bounces the RM/NM. Then it replaces that file,
	 * onthe fly, and effects data node exclusion via 'refreshNodes' command.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testExcludeActiveNodesUsingNodeRefresh() throws Exception {
		FullyDistributedCluster fullyDistributedCluster = (FullyDistributedCluster) TestSession
				.getCluster();

		File fileContainingHostsToIgnore = testFolder.newFile(TT_HOSTS_FILE);
		File empty_include_file = testFolder.newFile(EMPTY_FILE);

		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;
		/**
		 * Read in the initial node list
		 */
		genericCliResponse = dfsCliCommands.dfsadmin(EMPTY_ENV_HASH_MAP,
				Report.YES, null, ClearQuota.NO, SetQuota.NO, 0,
				ClearSpaceQuota.NO, SetSpaceQuota.NO, 0, PrintTopology.NO,
				DfsTestsBaseClass.EMPTY_FS_ENTITY);
		DfsadminReportBO dfsadminReportBO = new DfsadminReportBO(
				genericCliResponse.response);

		/**
		 * Get the initial count of active trackers. I have to run a different
		 * command here, because the dfsadmin -report doesnot include the
		 * excluded nodes in its response.
		 */
		int initialCountOfActiveTrackers = getCountOfActiveTrackers();
		TestSession.logger.info("Initial count of active trackers:" + initialCountOfActiveTrackers);

		/**
		 * Populate the hosts to ignore
		 */
		populateIgnoreFile(fileContainingHostsToIgnore, dfsadminReportBO);

		/**
		 * Copy over an empty file to RM
		 */
		copyAnEmptyExcludeFileToRM(empty_include_file);

		/**
		 * Bounce the RM and NM to read in the new configs
		 */
		updateRMConfigAndNMConfigAndBounceThem(true, true, null);

		/**
		 * Now, Copy over the file (with hosts to ignore) over to RM
		 */
		copyTheFileToIgnoreOverToRM(fileContainingHostsToIgnore);

		int intermediateCountOfActiveTrackers = getCountOfActiveTrackers();

		TestSession.logger.info("Intermediate count of task trackers:"
				+ intermediateCountOfActiveTrackers);

		/**
		 * Do a 'yarn rmadmin -refreshNodes'
		 */
		GenericYarnCliResponseBO genericYarnCliResponse;
		YarnCliCommands yarnCliCommands = new YarnCliCommands();
		genericYarnCliResponse = yarnCliCommands.rmadmin(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA,
				HadooptestConstants.Schema.NONE, localCluster,
				YarnAdminSubCommand.REFRESH_NODES, null);
		
		Assert.assertEquals(genericYarnCliResponse.process.exitValue(), 0);

		Util.sleep(30);
		
		int laterCountOfActiveTrackers = getCountOfActiveTrackers();

		TestSession.logger.info("Later count of active trackers:"
				+ laterCountOfActiveTrackers);

		Assert.assertTrue("Initial count of trackers:"
				+ initialCountOfActiveTrackers + " later count of trackers:"
				+ laterCountOfActiveTrackers,
				initialCountOfActiveTrackers > laterCountOfActiveTrackers);

	}

	/**
	 * This test verifies that once the admin ACL has been set, then only the
	 * specified user can effect datanode exclusion, via a 'yarn rmadmin
	 * -refreshNodes' command.
	 * 
	 * @throws Exception
	 */
	@Test
	@Ignore
	public void testNonPrivilegedUserExcludedNodes() throws Exception {
		FullyDistributedCluster fullyDistributedCluster = (FullyDistributedCluster) TestSession
				.getCluster();

		File fileContainingHostsToIgnore = testFolder.newFile(TT_HOSTS_FILE);
		File empty_include_file = testFolder.newFile(EMPTY_FILE);

		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;
		/**
		 * Read in the initial node list
		 */
		genericCliResponse = dfsCliCommands.dfsadmin(EMPTY_ENV_HASH_MAP,
				Report.YES, null, ClearQuota.NO, SetQuota.NO, 0,
				ClearSpaceQuota.NO, SetSpaceQuota.NO, 0, PrintTopology.NO,
				DfsTestsBaseClass.EMPTY_FS_ENTITY);
		DfsadminReportBO dfsadminReportBO = new DfsadminReportBO(
				genericCliResponse.response);

		/**
		 * Get the initial count of active trackers. I have to run a different
		 * command here, because the dfsadmin -report doesnot include the
		 * excluded nodes in its response.
		 */
		int initialCountOfActiveTrackers = getCountOfActiveTrackers();
		TestSession.logger.info("Initial count of active trackers:" + initialCountOfActiveTrackers);

		/**
		 * Populate the hosts to ignore
		 */
		populateIgnoreFile(fileContainingHostsToIgnore, dfsadminReportBO);

		/**
		 * Copy over an empty file to RM
		 */
		copyAnEmptyExcludeFileToRM(empty_include_file);

		/**
		 * Bounce the RM and NM to read in the new configs
		 */
		updateRMConfigAndNMConfigAndBounceThem(true, true,
				HadooptestConstants.UserNames.HADOOP1);

		/**
		 * Now, Copy over the file (with hosts to ignore) over to RM
		 */
		copyTheFileToIgnoreOverToRM(fileContainingHostsToIgnore);

		/**
		 * Do a 'yarn rmadmin -refreshNodes' This command would fail (expected)
		 * because the ACL has been set to 'hadoop1' and this command is being
		 * executed as 'hadoopqa'
		 */
		GenericYarnCliResponseBO genericYarnCliResponse;
		YarnCliCommands yarnCliCommands = new YarnCliCommands();
		genericYarnCliResponse = yarnCliCommands.rmadmin(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA,
				HadooptestConstants.Schema.NONE, localCluster,
				YarnAdminSubCommand.REFRESH_NODES, null);

		int laterCountOfActiveTrackers = getCountOfActiveTrackers();

		TestSession.logger.info("Later count of active trackers (as hadoopqa):"
				+ laterCountOfActiveTrackers);

		Assert.assertTrue("Initial count of trackers:"
				+ initialCountOfActiveTrackers + " later count of trackers:"
				+ laterCountOfActiveTrackers,
				initialCountOfActiveTrackers == laterCountOfActiveTrackers);


		/**
		 * Try the same command with a user with 'admin' priviliges.
		 */

		genericYarnCliResponse = yarnCliCommands.rmadmin(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.MAPREDQA,
				HadooptestConstants.Schema.NONE, localCluster,
				YarnAdminSubCommand.REFRESH_NODES, null);

		laterCountOfActiveTrackers = getCountOfActiveTrackers();

		TestSession.logger.info("Later count of active trackers (as mapredqa):"
				+ laterCountOfActiveTrackers);

		Assert.assertTrue("Initial count of trackers:"
				+ initialCountOfActiveTrackers + " later count of trackers:"
				+ laterCountOfActiveTrackers,
				initialCountOfActiveTrackers == laterCountOfActiveTrackers);

		/**
		 * Try the same command with the user specified in the admin ACL.
		 */

		genericYarnCliResponse = yarnCliCommands.rmadmin(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOP1,
				HadooptestConstants.Schema.NONE, localCluster,
				YarnAdminSubCommand.REFRESH_NODES, null);

		laterCountOfActiveTrackers = getCountOfActiveTrackers();

		TestSession.logger.info("Later count of active trackers (as hadoop1):"
				+ laterCountOfActiveTrackers);

		Assert.assertTrue(initialCountOfActiveTrackers > laterCountOfActiveTrackers);

	}

	@Test
	@Ignore("This test is similar to testNonPrivilegedUserExcludedNodes, hence ignoring.")
	public void testMapredQaUserExcludedNodes() throws Exception {
	}

	@Test
	@Ignore("# This test should be removed due to 4634445")
	public void toremove_test_AllotmentOfTasksToRecentlyIncludedNodes() {
	}

	/**
	 * Support functions
	 */
	private void populateIgnoreFile(File fileContainingHostsToIgnore,
			DfsadminReportBO dfsadminReportBO) throws IOException {
		FileWriter fileWriter = new FileWriter(
				fileContainingHostsToIgnore.getAbsoluteFile());
		BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

		// Exclude 1 data node
		bufferedWriter.write(dfsadminReportBO.liveDatanodes.get(0).hostname);
		bufferedWriter.write("\n");

		bufferedWriter.close();

		FileReader fileReader = new FileReader(
				fileContainingHostsToIgnore.getAbsoluteFile());
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		String line;
		while ((line = bufferedReader.readLine()) != null) {
			TestSession.logger.info("A host to ignore:" + line);
		}
		bufferedReader.close();
		TestSession.logger.info("The JUnit absolute file is:"
				+ fileContainingHostsToIgnore.getAbsolutePath());

	}

	private void copyTheFileToIgnoreOverToRM(File fileContainingHostsToIgnore)
			throws Exception {
		String[] rmHostnames = TestSession.cluster
				.getNodeNames(HadoopCluster.RESOURCE_MANAGER);
		FullyDistributedExecutor fde = new FullyDistributedExecutor();
		StringBuilder sb = new StringBuilder();
		sb.append("scp " + SSH_OPTS + fileContainingHostsToIgnore.getAbsolutePath()
				+ " hadoopqa@" + rmHostnames[0] + ":" + customConfDir
				+ "/mapred.exclude");
		String command = sb.toString();
		fde.runProcBuilder(command.split("\\s+"));

	}

	private void copyAnEmptyExcludeFileToRM(File anEmptyFile) throws Exception {
		String[] rmHostnames = TestSession.cluster
				.getNodeNames(HadoopCluster.RESOURCE_MANAGER);
		FullyDistributedExecutor fde = new FullyDistributedExecutor();
		StringBuilder sb = new StringBuilder();
		sb.append("scp " + SSH_OPTS + anEmptyFile.getAbsolutePath() + " hadoopqa@"
				+ rmHostnames[0] + ":" + customConfDir + "/mapred.exclude");
		String command = sb.toString();
		fde.runProcBuilder(command.split("\\s+"));

	}

	/**
	 * In this method, you would find calls interpresed with sleep times. This 
	 * is needed, because the yinst sessions (for starting/stopping daemons)
	 * tend to step on each other and one gets erros like "another instance of
	 * yinst is updating ....."
	 * 
	 * @param doRM
	 * @param doNM
	 * @param aclUser
	 * @throws Exception
	 */
	void updateRMConfigAndNMConfigAndBounceThem(boolean doRM, boolean doNM,
			String aclUser) throws Exception {
		FullyDistributedCluster fullyDistributedCluster = (FullyDistributedCluster) TestSession
				.getCluster();

		if (doRM) {
			// Backup config and replace file, for Resource Manager
			fullyDistributedCluster.getConf(
					HadooptestConstants.NodeTypes.RESOURCE_MANAGER)
					.backupConfDir();

			String dirWhereRMConfHasBeenCopiedOnTheRemoteMachine = fullyDistributedCluster
					.getConf(HadooptestConstants.NodeTypes.RESOURCE_MANAGER)
					.getHadoopConfDir();

			TestSession.logger.info("Backed up RM conf dir in :"
					+ dirWhereRMConfHasBeenCopiedOnTheRemoteMachine);

			// Since setting the HadoopConf file also backs up the config dir,
			// wait for a minute before this command.
			Thread.sleep(60000);
			fullyDistributedCluster.getConf(
					HadooptestConstants.NodeTypes.RESOURCE_MANAGER)
					.setHadoopConfFileProp(
							"yarn.resourcemanager.nodes.exclude-path",
							customConfDir + "/mapred.exclude", "yarn-site.xml",
							null);
			
			// Since setting the HadoopConf file also backs up the config dir,
			// wait for a minute before this command.
			Thread.sleep(60000);
			fullyDistributedCluster.getConf(
					HadooptestConstants.NodeTypes.RESOURCE_MANAGER)
					.removeHadoopConfFileProp(
							"yarn.resourcemanager.nodes.include-path",
							"yarn-site.xml", null);

			if (aclUser != null) {
				if (!aclUser.isEmpty()) {
					fullyDistributedCluster.getConf(
							HadooptestConstants.NodeTypes.RESOURCE_MANAGER)
							.setHadoopConfFileProp("yarn.admin.acl", aclUser,
									"yarn-site.xml", null);

				}
			}

			// Bounce RM
			fullyDistributedCluster.hadoopDaemon(Action.STOP,
					HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
			fullyDistributedCluster.hadoopDaemon(Action.START,
					HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
		}

		// If NM also needs to be bounced.
		if (doNM) {
			// Backup config and replace file, for Node Manager
			fullyDistributedCluster.getConf(
					HadooptestConstants.NodeTypes.NODE_MANAGER).backupConfDir();

			String dirWhereRMConfHasBeenCopiedOnTheRemoteMachine = fullyDistributedCluster
					.getConf(HadooptestConstants.NodeTypes.NODE_MANAGER)
					.getHadoopConfDir();

			TestSession.logger.info("Backed up NM conf dir in :"
					+ dirWhereRMConfHasBeenCopiedOnTheRemoteMachine);
			// Since setting the HadoopConf file also backs up the config dir,
			// wait for a minute before this command.
			Thread.sleep(60000);

			fullyDistributedCluster.getConf(
					HadooptestConstants.NodeTypes.NODE_MANAGER)
					.setHadoopConfFileProp(
							"yarn.resourcemanager.nodes.exclude-path",
							customConfDir + "/mapred.exclude", "yarn-site.xml",
							null);
			// Since setting the HadoopConf file also backs up the config dir,
			// wait for a minute before this command.
			Thread.sleep(60000);

			if (aclUser != null) {
				if (!aclUser.isEmpty()) {
					fullyDistributedCluster.getConf(
							HadooptestConstants.NodeTypes.NODE_MANAGER)
							.setHadoopConfFileProp("yarn.admin.acl", aclUser,
									"yarn-site.xml", null);

				}
			}

			// Bounce NM
			fullyDistributedCluster.hadoopDaemon(Action.STOP,
					HadooptestConstants.NodeTypes.NODE_MANAGER);
			fullyDistributedCluster.hadoopDaemon(Action.START,
					HadooptestConstants.NodeTypes.NODE_MANAGER);
		}

		Thread.sleep(5000);
		// Leave safe-mode
//		DfsCliCommands dfsCliCommands = new DfsCliCommands();
//		GenericCliResponseBO genericCliResponse;
//		genericCliResponse = dfsCliCommands.dfsadmin(
//				DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
//				DfsTestsBaseClass.Report.NO, "get",
//				DfsTestsBaseClass.ClearQuota.NO, DfsTestsBaseClass.SetQuota.NO,
//				0, DfsTestsBaseClass.ClearSpaceQuota.NO,
//				DfsTestsBaseClass.SetSpaceQuota.NO, 0,
//				DfsTestsBaseClass.PrintTopology.NO, null);
//
//		genericCliResponse = dfsCliCommands.dfsadmin(
//				DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
//				DfsTestsBaseClass.Report.NO, "leave",
//				DfsTestsBaseClass.ClearQuota.NO, DfsTestsBaseClass.SetQuota.NO,
//				0, DfsTestsBaseClass.ClearSpaceQuota.NO,
//				DfsTestsBaseClass.SetSpaceQuota.NO, 0,
//				DfsTestsBaseClass.PrintTopology.NO, null);
//
//		Configuration conf = fullyDistributedCluster
//				.getConf(HadooptestConstants.NodeTypes.RESOURCE_MANAGER);

	}

//	int getCountOfActiveTrackers() throws IOException, InterruptedException {
//		Cluster clusterInfo = TestSession.cluster.getClusterInfo();
//
//		return clusterInfo.getClusterStatus().getTaskTrackerCount();
//	}
	int getCountOfActiveTrackers() {
		int countOfActiveTrackers = 0;
		MapredCliCommands mapredCliCommands = new MapredCliCommands();
		GenericMapredCliResponseBO genericMapredCliResponseBO = null;

		try {
			genericMapredCliResponseBO = mapredCliCommands.listActiveTrackers(
					YarnTestsBaseClass.EMPTY_ENV_HASH_MAP,
					HadooptestConstants.UserNames.HADOOPQA);

			Assert.assertEquals(genericMapredCliResponseBO.process.exitValue(),
					0);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("Not able to run command 'mapred job -list-active-trackers'");
		}

		String response = genericMapredCliResponseBO.response;

		for (String aResponseFrag : response.split("\n")) {
			if (aResponseFrag.contains("tracker_"))
				countOfActiveTrackers++;
		}
		return countOfActiveTrackers;
	}

}
