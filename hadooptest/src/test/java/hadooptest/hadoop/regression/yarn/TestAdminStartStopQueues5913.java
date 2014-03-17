package hadooptest.hadoop.regression.yarn;

import java.io.IOException;
import java.util.HashMap;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.cluster.hadoop.HadoopCluster.Action;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass;
import hadooptest.hadoop.regression.yarn.YarnCliCommands.GenericYarnCliResponseBO;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobQueueInfo;
import org.junit.After;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class TestAdminStartStopQueues5913 extends YarnTestsBaseClass {
	public static String CAPACITY_SCHEDULER_XML = "capacity-scheduler.xml";

	void copyConfigAndRestartNodes(String replacementConfigFile)
			throws Exception {
		FullyDistributedCluster fullyDistributedCluster = (FullyDistributedCluster) TestSession
				.getCluster();

		// Backup config and replace file, for Resource Manager
		fullyDistributedCluster.getConf(
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER).backupConfDir();
		fullyDistributedCluster.getConf(
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER)
				.copyFileToConfDir(replacementConfigFile,
						CAPACITY_SCHEDULER_XML);

		// Backup config and replace file, for Namenode
		fullyDistributedCluster.getConf(
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER).backupConfDir();
		fullyDistributedCluster.getConf(HadooptestConstants.NodeTypes.NAMENODE)
				.copyFileToConfDir(replacementConfigFile,
						CAPACITY_SCHEDULER_XML);
		// Bounce nodes
		fullyDistributedCluster.hadoopDaemon(Action.STOP,
				HadooptestConstants.NodeTypes.NAMENODE);
		fullyDistributedCluster.hadoopDaemon(Action.START,
				HadooptestConstants.NodeTypes.NAMENODE);

		fullyDistributedCluster.hadoopDaemon(Action.STOP,
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
		fullyDistributedCluster.hadoopDaemon(Action.START,
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER);

		Thread.sleep(20000);
		// Leave safe-mode
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;
		genericCliResponse = dfsCliCommands.dfsadmin(
				DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
				DfsTestsBaseClass.Report.NO, "get",
				DfsTestsBaseClass.ClearQuota.NO, DfsTestsBaseClass.SetQuota.NO,
				0, DfsTestsBaseClass.ClearSpaceQuota.NO,
				DfsTestsBaseClass.SetSpaceQuota.NO, 0,
				DfsTestsBaseClass.PrintTopology.NO, null);
		genericCliResponse = dfsCliCommands.dfsadmin(
				DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
				DfsTestsBaseClass.Report.NO, "leave",
				DfsTestsBaseClass.ClearQuota.NO, DfsTestsBaseClass.SetQuota.NO,
				0, DfsTestsBaseClass.ClearSpaceQuota.NO,
				DfsTestsBaseClass.SetSpaceQuota.NO, 0,
				DfsTestsBaseClass.PrintTopology.NO, null);

	}

	@After
	public void restoreTheConfigFile() throws Exception {
		FullyDistributedCluster fullyDistributedCluster = (FullyDistributedCluster) TestSession
				.getCluster();
		fullyDistributedCluster.getConf(
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER)
				.resetHadoopConfDir();
		fullyDistributedCluster.getConf(HadooptestConstants.NodeTypes.NAMENODE)
				.resetHadoopConfDir();
		// Bounce nodes
		fullyDistributedCluster.hadoopDaemon(Action.STOP,
				HadooptestConstants.NodeTypes.NAMENODE);
		fullyDistributedCluster.hadoopDaemon(Action.START,
				HadooptestConstants.NodeTypes.NAMENODE);

		fullyDistributedCluster.hadoopDaemon(Action.STOP,
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
		fullyDistributedCluster.hadoopDaemon(Action.START,
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER);

		Thread.sleep(20000);

		// Leave safe-mode
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;
		genericCliResponse = dfsCliCommands.dfsadmin(
				DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
				DfsTestsBaseClass.Report.NO, "get",
				DfsTestsBaseClass.ClearQuota.NO, DfsTestsBaseClass.SetQuota.NO,
				0, DfsTestsBaseClass.ClearSpaceQuota.NO,
				DfsTestsBaseClass.SetSpaceQuota.NO, 0,
				DfsTestsBaseClass.PrintTopology.NO, null);
		genericCliResponse = dfsCliCommands.dfsadmin(
				DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
				DfsTestsBaseClass.Report.NO, "leave",
				DfsTestsBaseClass.ClearQuota.NO, DfsTestsBaseClass.SetQuota.NO,
				0, DfsTestsBaseClass.ClearSpaceQuota.NO,
				DfsTestsBaseClass.SetSpaceQuota.NO, 0,
				DfsTestsBaseClass.PrintTopology.NO, null);

	}

	// @Test
	public void testJobSubmissionToQueueWhichIsStoppedLeafNode()
			throws Exception {
		String stoppedQueue = "c2";
		copyConfigAndRestartNodes(TestSession.conf.getProperty("WORKSPACE")
				+ "/resources/hadooptest/hadoop/regression/yarn/adminStartStopQueues/capacity-scheduler_c2QueueStopped.xml");

		JobClient jobClient = new JobClient(
				((FullyDistributedCluster) TestSession.cluster)
						.getConf(HadooptestConstants.NodeTypes.RESOURCE_MANAGER));
		for (JobQueueInfo jobQueueInfo : jobClient.getQueues()) {
			TestSession.logger.info("Q name:" + jobQueueInfo.getQueueName()
					+ " Q state:" + jobQueueInfo.getState());

		}

		HashMap<String, String> sleepJobParams = new HashMap<String, String>();
		sleepJobParams.put("mapreduce.job.queuename", stoppedQueue);
		sleepJobParams.put("mapreduce.job.user.name",
				HadooptestConstants.UserNames.HADOOPQA);
		String[] sleepJobArgs = new String[] { "-m 1 -r 1 -mt 1 -rt 1" };
		try {
			runStdSleepJob(sleepJobParams, sleepJobArgs);
		} catch (Exception e) {
			TestSession.logger.info("CAUSE:" + e.getCause());
			TestSession.logger.info("MESSAGE:" + e.getMessage());
			TestSession.logger.info("LOCALIZED MESSAGE:"
					+ e.getLocalizedMessage());
			Assert.assertTrue(
					"",
					e.getMessage()
							.contains(
									stoppedQueue
											+ " is STOPPED. Cannot accept submission of application"));
		}

	}

	@Test
	@Ignore("Does not seem to be a valid test, because with an incorrect state the RM does not come up")
	public void testJobSubmissionToQueueWhichHasInvalidQueueState()
			throws Exception {
		String invalidStateQueue = "c2";
		copyConfigAndRestartNodes(TestSession.conf.getProperty("WORKSPACE")
				+ "/resources/hadooptest/hadoop/regression/yarn/adminStartStopQueues/capacity-scheduler_c2InvalidState.xml");

		JobClient jobClient = new JobClient(
				((FullyDistributedCluster) TestSession.cluster)
						.getConf(HadooptestConstants.NodeTypes.RESOURCE_MANAGER));
		for (JobQueueInfo jobQueueInfo : jobClient.getQueues()) {
			TestSession.logger.info("Q name:" + jobQueueInfo.getQueueName()
					+ " Q state:" + jobQueueInfo.getState());

		}
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		HashMap<String, String> sleepJobParams = new HashMap<String, String>();
		sleepJobParams.put("mapreduce.job.queuename", invalidStateQueue);
		sleepJobParams.put("mapreduce.job.user.name",
				HadooptestConstants.UserNames.HADOOPQA);
		String[] sleepJobArgs = new String[] { "-m 1 -r 1 -mt 1 -rt 1" };
		try {
			runStdSleepJob(sleepJobParams, sleepJobArgs);
		} catch (Exception e) {
			TestSession.logger.info("CAUSE:" + e.getCause());
			TestSession.logger.info("MESSAGE:" + e.getMessage());
			TestSession.logger.info("LOCALIZED MESSAGE:"
					+ e.getLocalizedMessage());
		}

	}

	// @Test
	public void testJobSubmissionToQueueWhichIsNotLeafNode() throws Exception {
		String notLeafQueue = "a";
		copyConfigAndRestartNodes(TestSession.conf.getProperty("WORKSPACE")
				+ "/resources/hadooptest/hadoop/regression/yarn/adminStartStopQueues/capacity-scheduler_c2QueueStopped.xml");

		JobClient jobClient = new JobClient(
				((FullyDistributedCluster) TestSession.cluster)
						.getConf(HadooptestConstants.NodeTypes.RESOURCE_MANAGER));
		for (JobQueueInfo jobQueueInfo : jobClient.getQueues()) {
			TestSession.logger.info("Q name:" + jobQueueInfo.getQueueName()
					+ " Q state:" + jobQueueInfo.getState());

		}

		HashMap<String, String> sleepJobParams = new HashMap<String, String>();
		sleepJobParams.put("mapreduce.job.queuename", notLeafQueue);
		sleepJobParams.put("mapreduce.job.user.name",
				HadooptestConstants.UserNames.HADOOPQA);
		String[] sleepJobArgs = new String[] { "-m 1 -r 1 -mt 1 -rt 1" };
		try {
			runStdSleepJob(sleepJobParams, sleepJobArgs);
		} catch (Exception e) {
			TestSession.logger.info("CAUSE:" + e.getCause());
			TestSession.logger.info("MESSAGE:" + e.getMessage());
			TestSession.logger.info("LOCALIZED MESSAGE:"
					+ e.getLocalizedMessage());
			Assert.assertTrue(e.getMessage()
					+ " [DID NOT CONTAIN EXPECTED ERROR MESSAGE]", e
					.getMessage()
					.contains("to non-leaf queue: " + notLeafQueue));
		}

	}

	// @Test
	public void testJobSubmissionToQueueWhichIsNotDefined() throws Exception {
		String undefinedQueue = "d";
		copyConfigAndRestartNodes(TestSession.conf.getProperty("WORKSPACE")
				+ "/resources/hadooptest/hadoop/regression/yarn/adminStartStopQueues/capacity-scheduler_c2QueueStopped.xml");

		JobClient jobClient = new JobClient(
				((FullyDistributedCluster) TestSession.cluster)
						.getConf(HadooptestConstants.NodeTypes.RESOURCE_MANAGER));
		for (JobQueueInfo jobQueueInfo : jobClient.getQueues()) {
			TestSession.logger.info("Q name:" + jobQueueInfo.getQueueName()
					+ " Q state:" + jobQueueInfo.getState());

		}

		HashMap<String, String> sleepJobParams = new HashMap<String, String>();
		sleepJobParams.put("mapreduce.job.queuename", undefinedQueue);
		sleepJobParams.put("mapreduce.job.user.name",
				HadooptestConstants.UserNames.HADOOPQA);
		String[] sleepJobArgs = new String[] { "-m 1 -r 1 -mt 1 -rt 1" };
		try {
			runStdSleepJob(sleepJobParams, sleepJobArgs);
		} catch (Exception e) {
			TestSession.logger.info("CAUSE:" + e.getCause());
			TestSession.logger.info("MESSAGE:" + e.getMessage());
			TestSession.logger.info("LOCALIZED MESSAGE:"
					+ e.getLocalizedMessage());
			Assert.assertTrue(
					"to non-leaf queue: a",
					e.getMessage().contains(
							"to unknown queue: " + undefinedQueue));
		}

	}

	// @Test
	public void testJobSubmissionToQueueWhoseParentNodeIsStopped()
			throws Exception {
		String stoppedParentQueue = "a";
		String childQueueWhoseParentIsStopped = "a1";

		copyConfigAndRestartNodes(TestSession.conf.getProperty("WORKSPACE")
				+ "/resources/hadooptest/hadoop/regression/yarn/adminStartStopQueues/capacity-scheduler_modified.xml");

		JobClient jobClient = new JobClient(
				((FullyDistributedCluster) TestSession.cluster)
						.getConf(HadooptestConstants.NodeTypes.RESOURCE_MANAGER));
		for (JobQueueInfo jobQueueInfo : jobClient.getQueues()) {
			TestSession.logger.info("Q name:" + jobQueueInfo.getQueueName()
					+ " Q state:" + jobQueueInfo.getState());

		}
		Thread.sleep(20000);
		HashMap<String, String> sleepJobParams = new HashMap<String, String>();
		sleepJobParams.put("mapreduce.job.queuename",
				childQueueWhoseParentIsStopped);
		sleepJobParams.put("mapreduce.job.user.name",
				HadooptestConstants.UserNames.HADOOPQA);
		String[] sleepJobArgs = new String[] { "-m 1 -r 1 -mt 1 -rt 1" };
		try {
			runStdSleepJob(sleepJobParams, sleepJobArgs);
		} catch (Exception e) {
			TestSession.logger.info("CAUSE:" + e.getCause());
			TestSession.logger.info("MESSAGE:" + e.getMessage());
			TestSession.logger.info("LOCALIZED MESSAGE:"
					+ e.getLocalizedMessage());

			Assert.assertTrue(
					"",
					e.getMessage().contains(
							"Cannot accept submission of application"));

		}

	}

	// @Test
	public void testJobSubmissionToRunningLeafQueue() throws Exception {
		String runningQueue = "c1";

		copyConfigAndRestartNodes(TestSession.conf.getProperty("WORKSPACE")
				+ "/resources/hadooptest/hadoop/regression/yarn/adminStartStopQueues/capacity-scheduler_modified.xml");

		JobClient jobClient = new JobClient(
				((FullyDistributedCluster) TestSession.cluster)
						.getConf(HadooptestConstants.NodeTypes.RESOURCE_MANAGER));
		for (JobQueueInfo jobQueueInfo : jobClient.getQueues()) {
			TestSession.logger.info("Q name:" + jobQueueInfo.getQueueName()
					+ " Q state:" + jobQueueInfo.getState());

		}
		Thread.sleep(20000);
		HashMap<String, String> sleepJobParams = new HashMap<String, String>();
		sleepJobParams.put("mapreduce.job.queuename", runningQueue);
		sleepJobParams.put("mapreduce.job.user.name",
				HadooptestConstants.UserNames.HADOOPQA);
		String[] sleepJobArgs = new String[] { "-m 1 -r 1 -mt 1 -rt 1" };

		runStdSleepJob(sleepJobParams, sleepJobArgs);

	}

	@Test
	public void testQueueStatusChangeAndRefreshQueues() throws Exception {
		GenericYarnCliResponseBO genericYarnCliResponse;
		YarnCliCommands yarnCliCommands = new YarnCliCommands();
		String stoppedQueue = "b";

		copyConfigAndRestartNodes(TestSession.conf.getProperty("WORKSPACE")
				+ "/resources/hadooptest/hadoop/regression/yarn/adminStartStopQueues/capacity-scheduler_modified.xml");

		JobClient jobClient = new JobClient(
				((FullyDistributedCluster) TestSession.cluster)
						.getConf(HadooptestConstants.NodeTypes.RESOURCE_MANAGER));
		for (JobQueueInfo jobQueueInfo : jobClient.getQueues()) {
			TestSession.logger.info("Q name:" + jobQueueInfo.getQueueName()
					+ " Q state:" + jobQueueInfo.getState());

		}
		FullyDistributedCluster fullyDistributedCluster = (FullyDistributedCluster) TestSession
				.getCluster();

		// Copy over different queue states, than before.
		fullyDistributedCluster
				.getConf(HadooptestConstants.NodeTypes.RESOURCE_MANAGER)
				.copyFileToConfDir(
						TestSession.conf.getProperty("WORKSPACE")
								+ "/resources/hadooptest/hadoop/regression"
								+ "/yarn/adminStartStopQueues/capacity-scheduler_refreshQueues.xml",
						CAPACITY_SCHEDULER_XML);
		
		genericYarnCliResponse = yarnCliCommands.rmadmin(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA,
				HadooptestConstants.Schema.NONE, localCluster,
				YarnAdminSubCommand.REFRESH_QUEUES, null);
		Assert.assertTrue("Yarn CLI exited with non-zero exit code",
				genericYarnCliResponse.process.exitValue() == 0);

		HashMap<String, String> sleepJobParams = new HashMap<String, String>();
		sleepJobParams.put("mapreduce.job.queuename", stoppedQueue);
		sleepJobParams.put("mapreduce.job.user.name",
				HadooptestConstants.UserNames.HADOOPQA);
		String[] sleepJobArgs = new String[] { "-m 1 -r 1 -mt 1 -rt 1" };
		try {
			runStdSleepJob(sleepJobParams, sleepJobArgs);
		} catch (Exception e) {
			TestSession.logger.info("CAUSE:" + e.getCause());
			TestSession.logger.info("MESSAGE:" + e.getMessage());
			TestSession.logger.info("LOCALIZED MESSAGE:"
					+ e.getLocalizedMessage());
			Assert.assertTrue("", e.getMessage().contains(stoppedQueue + " is STOPPED. Cannot accept submission of application"));
		}

	}

}
