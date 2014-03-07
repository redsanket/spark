package hadooptest.hadoop.regression.yarn;

import java.util.HashMap;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.cluster.hadoop.HadoopCluster.Action;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobQueueInfo;
import org.junit.Test;

public class TestAdminStartStopQueues5913 extends YarnTestsBaseClass{
	public static String CAPACITY_SCHEDULER_XML = "capacity-scheduler.xml";

	void copyConfigAndRestartNodes(String replacementConfigFile)
			throws Exception {
		FullyDistributedCluster fullyDistributedCluster = (FullyDistributedCluster) TestSession
				.getCluster();
		fullyDistributedCluster.getConf(
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER).backupConfDir();
		fullyDistributedCluster.getConf(
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER)
				.copyFileToConfDir(replacementConfigFile,
						CAPACITY_SCHEDULER_XML);

		fullyDistributedCluster.getConf(HadooptestConstants.NodeTypes.NAMENODE)
				.copyFileToConfDir(replacementConfigFile,
						CAPACITY_SCHEDULER_XML);
		fullyDistributedCluster.hadoopDaemon(Action.STOP,
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
		fullyDistributedCluster.hadoopDaemon(Action.STOP,
				HadooptestConstants.NodeTypes.NAMENODE);

	}

	@Test
	public void testJobSubmissionToQueueWhichIsStoppedLeafNode() throws Exception {
		String stoppedQueue = "c2";
		copyConfigAndRestartNodes(TestSession.conf.getProperty("WORKSPACE")
				+ "/conf/capacity-scheduler_c2QueueStopped.xml");

		JobClient jobClient = new JobClient(
				((FullyDistributedCluster) TestSession.cluster)
						.getConf(HadooptestConstants.NodeTypes.RESOURCE_MANAGER));
		for (JobQueueInfo jobQueueInfo : jobClient.getQueues()) {
			TestSession.logger.info("Q name:" + jobQueueInfo.getQueueName()
					+ " Q state:" + jobQueueInfo.getState());

		}
		HashMap<String, String> sleepJobParams = new HashMap<String, String>();
		sleepJobParams.put("mapreduce.job.queuename", stoppedQueue);
		sleepJobParams.put("mapreduce.job.user.name", HadooptestConstants.UserNames.HADOOPQA);
		String[] sleepJobArgs = new String[]{"-m 1 -r 1 -mt 1 -rt 1"};
		runStdSleepJob(sleepJobParams, sleepJobArgs);
		

	}
}
