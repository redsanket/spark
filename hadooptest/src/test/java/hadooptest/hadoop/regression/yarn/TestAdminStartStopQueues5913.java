package hadooptest.hadoop.regression.yarn;

import java.util.HashMap;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.cluster.hadoop.HadoopCluster.Action;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobQueueInfo;
import org.junit.Test;

public class TestAdminStartStopQueues5913 extends YarnTestsBaseClass {
	public static String CAPACITY_SCHEDULER_XML = "capacity-scheduler.xml";

	void copyConfigAndRestartNodes(String replacementConfigFile)
			throws Exception {
		FullyDistributedCluster fullyDistributedCluster = (FullyDistributedCluster) TestSession
				.getCluster();
		// Resource Manager
		fullyDistributedCluster.getConf(
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER).backupConfDir();
		fullyDistributedCluster.getConf(
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER)
				.copyFileToConfDir(replacementConfigFile,
						CAPACITY_SCHEDULER_XML);
		// Namenode
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

	}

	@Test
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
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;
		genericCliResponse = dfsCliCommands.dfsadmin(DfsTestsBaseClass.EMPTY_ENV_HASH_MAP, DfsTestsBaseClass.Report.NO, "get", DfsTestsBaseClass.ClearQuota.NO, DfsTestsBaseClass.SetQuota.NO, 0, DfsTestsBaseClass.ClearSpaceQuota.NO, DfsTestsBaseClass.SetSpaceQuota.NO, 0, DfsTestsBaseClass.PrintTopology.NO, null);
		genericCliResponse = dfsCliCommands.dfsadmin(DfsTestsBaseClass.EMPTY_ENV_HASH_MAP, DfsTestsBaseClass.Report.NO, "leave", DfsTestsBaseClass.ClearQuota.NO, DfsTestsBaseClass.SetQuota.NO, 0, DfsTestsBaseClass.ClearSpaceQuota.NO, DfsTestsBaseClass.SetSpaceQuota.NO, 0, DfsTestsBaseClass.PrintTopology.NO, null);

		HashMap<String, String> sleepJobParams = new HashMap<String, String>();
		sleepJobParams.put("mapreduce.job.queuename", stoppedQueue);
		sleepJobParams.put("mapreduce.job.user.name",
				HadooptestConstants.UserNames.HADOOPQA);
		String[] sleepJobArgs = new String[] { "-m 1 -r 1 -mt 1 -rt 1" };
		try {
			runStdSleepJob(sleepJobParams, sleepJobArgs);
		} catch (Exception e) {
			TestSession.logger.info("CAUSE:" +e.getCause());
			TestSession.logger.info("MESSAGE:" +e.getMessage());
			TestSession.logger.info("LOCALIZED MESSAGE:" +e.getLocalizedMessage());
		}

	}
}
