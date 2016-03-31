package hadooptest.hadoop.regression.yarn;

import java.io.IOException;
import java.util.HashMap;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.cluster.hadoop.HadoopCluster;
import hadooptest.cluster.hadoop.HadoopCluster.Action;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.config.hadoop.fullydistributed.FullyDistributedConfiguration;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass;
import hadooptest.hadoop.regression.yarn.YarnCliCommands.GenericYarnCliResponseBO;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobQueueInfo;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.QueueInfo;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SerialTests.class)
public class TestRunningAdminQueues extends YarnTestsBaseClass {
	public static String CAPACITY_SCHEDULER_XML = "capacity-scheduler.xml";
	protected static boolean restoredConfig = false;

	@Before
	public void copyConfigAndRestartNodes() throws Exception {
		if (restoredConfig)
			return;
		restoredConfig = true;
		String replacementConfigFile = TestSession.conf
				.getProperty("WORKSPACE")
				+ "/htf-common/resources/hadooptest/hadoop/regression/yarn/adminStartStopQueues/capacity-scheduler_modified.xml";

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
		
        Assert.assertTrue("Did not leave safe mode within timeout.", 
                fullyDistributedCluster.waitForSafemodeOff(120000, null));
        
		fullyDistributedCluster.hadoopDaemon(Action.STOP,
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
		fullyDistributedCluster.hadoopDaemon(Action.START,
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER);

		Thread.sleep(60000);
		
        Assert.assertTrue("Did not leave safe mode within timeout.", 
                fullyDistributedCluster.waitForSafemodeOff(120000, null));
	}

	@AfterClass
	public static void restoreTheConfigFile() throws Exception {
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
				HadooptestConstants.NodeTypes.NAMENODE,
				TestSession.cluster.getNodeNames(HadoopCluster.NAMENODE),
				TestSession.conf.getProperty("HADOOP_INSTALL_CONF_DIR"));

        Assert.assertTrue("Did not leave safe mode within timeout.", 
                fullyDistributedCluster.waitForSafemodeOff(120000, null));
		
		fullyDistributedCluster.hadoopDaemon(Action.STOP,
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
		fullyDistributedCluster.hadoopDaemon(Action.START,
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER,
				TestSession.cluster
						.getNodeNames(HadoopCluster.RESOURCE_MANAGER),
				TestSession.conf.getProperty("HADOOP_INSTALL_CONF_DIR"));

		Thread.sleep(20000);

        Assert.assertTrue("Did not leave safe mode within timeout.", 
                fullyDistributedCluster.waitForSafemodeOff(120000, null));
	}

	@Test
	public void testJobSubmissionToQueueWhoseParentNodeIsStopped()
			throws Exception {
		String stoppedParentQueue = "a";
		String childQueueWhoseParentIsStopped = "a1";

		FullyDistributedConfiguration fdc = ((FullyDistributedCluster) TestSession.cluster)
				.getConf(HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
		Cluster cluster = new Cluster(fdc);

		for (QueueInfo qi : cluster.getQueues()) {
			TestSession.logger.info("Q name:" + qi.getQueueName() + " Q state:"
					+ qi.getState());

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

	@Test
	public void testJobSubmissionToRunningLeafQueue() throws Exception {
		String runningQueue = "c1";

		FullyDistributedConfiguration fdc = ((FullyDistributedCluster) TestSession.cluster)
				.getConf(HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
		Cluster cluster = new Cluster(fdc);

		for (QueueInfo qi : cluster.getQueues()) {
			TestSession.logger.info("Q name:" + qi.getQueueName() + " Q state:"
					+ qi.getState());

		}

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

		FullyDistributedConfiguration fdc = ((FullyDistributedCluster) TestSession.cluster)
				.getConf(HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
		Cluster cluster = new Cluster(fdc);

		for (QueueInfo qi : cluster.getQueues()) {
			TestSession.logger.info("Q name:" + qi.getQueueName() + " Q state:"
					+ qi.getState());

		}

		FullyDistributedCluster fullyDistributedCluster = (FullyDistributedCluster) TestSession
				.getCluster();

		// Copy over different queue states, than before.
		fullyDistributedCluster
				.getConf(HadooptestConstants.NodeTypes.RESOURCE_MANAGER)
				.copyFileToConfDir(
						TestSession.conf.getProperty("WORKSPACE")
								+ "/htf-common/resources/hadooptest/hadoop/regression"
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
			Assert.assertTrue(
					"",
					e.getMessage()
							.contains(
									stoppedQueue
											+ " is STOPPED. Cannot accept submission of application"));
		}

	}

}
