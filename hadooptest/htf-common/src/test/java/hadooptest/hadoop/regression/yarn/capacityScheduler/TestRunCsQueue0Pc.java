package hadooptest.hadoop.regression.yarn.capacityScheduler;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;

import java.util.HashMap;

import org.junit.Test;

public class TestRunCsQueue0Pc extends CapacitySchedulerBaseClass {

	/**
	 * "1. Set user Limit for queue default equals to 0%"; note
	 * "2. Get number of nodes on the cluster, and calculate cluster capacity and queue capacity"
	 * "3. Submit six normal jobs by 6 different users which have number of map and reduce tasks equal 2 times queue capacity"
	 * "4. Verify task slots are distributed equally among first 5 and the last one get the rest"
	 * "5. Verify all jobs ran sucessfully";
	 * 
	 * @throws Exception
	 */
	@Test
	public void testCsSingleQueue0Percent1() throws Exception {
		copyResMgrConfigAndRestartNodes(TestSession.conf.getProperty("WORKSPACE")
				+ "/resources/hadooptest/hadoop/regression/yarn/capacityScheduler/capacity-scheduler0.xml");
		FullyDistributedCluster fullyDistributedCluster = (FullyDistributedCluster) TestSession
				.getCluster();
		String dirWhereRMConfHasBeenCopied = fullyDistributedCluster.getConf(
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER)
				.getHadoopConfDir();
		String resourceMgrConfigFilesCopiedBackHereOnGw = fullyDistributedCluster
				.getConf(HadooptestConstants.NodeTypes.RESOURCE_MANAGER)
				.copyRemoteConfDirToLocal(dirWhereRMConfHasBeenCopied,
						HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
		TestSession.logger.info("Copied back files from RM in "
				+ resourceMgrConfigFilesCopiedBackHereOnGw);
		TestSession.logger
				.info("Read back (locally); after copying from RM [yarn.scheduler.capacity.root.default.capacity] ="
						+ lookupValueInBackCopiedCapacitySchedulerXmlFile(
								resourceMgrConfigFilesCopiedBackHereOnGw
										+ "/capacity-scheduler.xml",
								"yarn.scheduler.capacity.root.default.capacity"));

		CalculatedCapacityLimitsBO capacityBO = new CalculatedCapacityLimitsBO(
				resourceMgrConfigFilesCopiedBackHereOnGw
						+ "/capacity-scheduler.xml");
		for (QueueCapacityDetail aQueueDetail : capacityBO.queueCapacityDetails) {
			TestSession.logger.info("Q name:" + aQueueDetail.name);
			TestSession.logger.info("Q max capacity:"
					+ aQueueDetail.maxCapacityInTermsOfTotalClusterMemory);
			TestSession.logger.info("Q max user limit percent:"
					+ aQueueDetail.minimumUserLimitPercent);
			TestSession.logger.info("Q capacity:" + aQueueDetail.capacityInTermsOfTotalClusterMemory);
			TestSession.logger.info("Q user limit factor:"
					+ aQueueDetail.userLimitFactor);
		}

		HashMap<String, String> sleepJobParams = new HashMap<String, String>();
		sleepJobParams.put("mapreduce.job.queuename", "default");
		sleepJobParams.put("mapreduce.job.user.name",
				HadooptestConstants.UserNames.HADOOPQA);
		String[] sleepJobArgs = new String[] { "-m 1 -r 1 -mt 1 -rt 1" };
		try {
			runStdSleepJob(sleepJobParams, sleepJobArgs);
		} catch (Exception e) {
			/**
			 * Job is expected to fail, because
			 * yarn.scheduler.capacity.root.default.minimum-user-limit-percent
			 * has been set to 0 in the passed capacity-scheduler.xml file
			 */
			TestSession.logger.info("CAUSE:" + e.getCause());
			TestSession.logger.info("MESSAGE:" + e.getMessage());
			TestSession.logger.info("LOCALIZED MESSAGE:"
					+ e.getLocalizedMessage());
		}

	}

}
