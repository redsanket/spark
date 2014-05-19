package hadooptest.hadoop.regression.yarn.capacityScheduler;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.config.hadoop.fullydistributed.FullyDistributedConfiguration;
import hadooptest.hadoop.regression.yarn.MapredCliCommands;
import hadooptest.hadoop.regression.yarn.MapredCliCommands.GenericMapredCliResponseBO;
import hadooptest.hadoop.regression.yarn.YarnTestsBaseClass;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobQueueInfo;
import org.junit.Assert;

/**
 * The Capacity Scheduler Business Object
 * Docs to discerning the collected data:
 * http://twiki.corp.yahoo.com/view/GridDocumentation/GridDocScheduler
 * http://hadoop.apache.org/docs/r2.3.0/hadoop-yarn/hadoop-yarn-site/CapacityScheduler.html
 * 
 * @author tiwari
 * 
 */
public class CalculatedCapacityLimitsBO {

	int totalClusterMemory;
	public ArrayList<QueueCapacityDetail> queueCapacityDetails;
	FullyDistributedConfiguration fullyDistributedConfRM;

	public CalculatedCapacityLimitsBO(String dirWhereConfFilesWereCopiedFromRM) {
		totalClusterMemory = getTotalClusterMemory();
		queueCapacityDetails = new ArrayList<QueueCapacityDetail>();
		FullyDistributedCluster fullyDistributedCluster = (FullyDistributedCluster) TestSession
				.getCluster();
		fullyDistributedConfRM = fullyDistributedCluster
				.getConf(HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
		fullyDistributedConfRM.addResource(new Path(
				dirWhereConfFilesWereCopiedFromRM));

		try {
			for (JobQueueInfo aJobQueueInfo : getQueues()) {
				QueueCapacityDetail queueDetail = new QueueCapacityDetail();
				queueDetail.name = aJobQueueInfo.getQueueName();

				queueDetail.minCapacityInTermsOfPercentage = getQueueCapacityInTermsOfPercentage(aJobQueueInfo
						.getQueueName());
				queueDetail.minCapacityForQueueInTermsOfTotalClusterMemoryInGB = getQueueCapacityInTermsOfTotalClusterMemory(aJobQueueInfo
						.getQueueName());
				queueDetail.maxCapacityForQueueInTermsOfTotalClusterMemoryInGB = getMaxQueueCapacityInTermsOfTotalClusterMemory(aJobQueueInfo
						.getQueueName());
				queueDetail.userLimitFactor = getUserLimitFactor(aJobQueueInfo
						.getQueueName());
				queueDetail.minimumUserLimitPercent = getMinUserLimitPercent(aJobQueueInfo
						.getQueueName());
				queueDetail.maximumAmResourcePercent = getMaxAmResourcePercent(aJobQueueInfo
						.getQueueName());
				queueDetail.maxApplications = getMaxApplications(aJobQueueInfo
						.getQueueName());

				queueDetail.maxApplicationsPerUser = getMaxApplicationsPerUser(aJobQueueInfo
						.getQueueName());
				queueDetail.maxActiveApplications = getMaxActiveApplications(aJobQueueInfo
						.getQueueName());
				queueDetail.maxActiveApplicationsPerUser = getMaxActiveApplicationsPerUser(aJobQueueInfo
						.getQueueName());

				queueCapacityDetails.add(queueDetail);

			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public double getMinUserLimitPercent(String queueName) {
		FullyDistributedCluster fullyDistributedCluster = (FullyDistributedCluster) TestSession
				.getCluster();
		FullyDistributedConfiguration fullyDistributedConfRM = fullyDistributedCluster
				.getConf(HadooptestConstants.NodeTypes.RESOURCE_MANAGER);

		String minUserLimitPercentAsString = "";
		minUserLimitPercentAsString = fullyDistributedConfRM
				.get("yarn.scheduler.capacity.root." + queueName
						+ ".minimum-user-limit-percent");
		if (minUserLimitPercentAsString == null
				|| minUserLimitPercentAsString.isEmpty()) {
			minUserLimitPercentAsString = "1.0"; // Default
		}
		TestSession.logger.info("Retrieved min user limit percent for queue '"
				+ "yarn.scheduler.capacity.root." + queueName
				+ ".minimum-user-limit-percent" + "' as:["
				+ minUserLimitPercentAsString + "]");

		return Double.parseDouble(minUserLimitPercentAsString);

	}

	public double getUserLimitFactor(String queueName) {
		FullyDistributedCluster fullyDistributedCluster = (FullyDistributedCluster) TestSession
				.getCluster();
		FullyDistributedConfiguration fullyDistributedConfRM = fullyDistributedCluster
				.getConf(HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
		String userLimitFactor = "";
		userLimitFactor = fullyDistributedConfRM
				.get("yarn.scheduler.capacity.root." + queueName
						+ ".user-limit-factor");
		if (userLimitFactor == null || userLimitFactor.isEmpty()) {
			userLimitFactor = "1"; // default
		}
		double returnValue = Double.parseDouble(userLimitFactor);
		TestSession.logger.info("Retrieved user-limit-factor for queue '"
				+ "yarn.scheduler.capacity.root." + queueName
				+ ".user-limit-factor" + "' as:[" + returnValue + "]");

		return returnValue;

	}

	/**
	 * http://hadoop.apache.org/docs/r2.3.0/hadoop-yarn/hadoop-yarn-site/
	 * CapacityScheduler.html
	 * yarn.scheduler.capacity.maximum-am-resource-percent /
	 * yarn.scheduler.capacity.<queue-path>.maximum-am-resource-percent Maximum
	 * percent of resources in the cluster which can be used to run application
	 * masters - controls number of concurrent active applications. Limits on
	 * each queue are directly proportional to their queue capacities and user
	 * limits. Specified as a float - ie 0.5 = 50%. Default is 10%. This can be
	 * set for all queues with
	 * yarn.scheduler.capacity.maximum-am-resource-percent and can also be
	 * overridden on a per queue basis by setting
	 * yarn.scheduler.capacity.<queue-path>.maximum-am-resource-percent
	 * 
	 * @param queueName
	 * @return
	 */
	public double getMaxAmResourcePercent(String queueName) {
		FullyDistributedCluster fullyDistributedCluster = (FullyDistributedCluster) TestSession
				.getCluster();
		FullyDistributedConfiguration fullyDistributedConfRM = fullyDistributedCluster
				.getConf(HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
		String maxAmResourcePercent = "";

		maxAmResourcePercent = fullyDistributedConfRM
				.get("yarn.scheduler.capacity.maximum-am-resource-percent");

		if (maxAmResourcePercent == null || maxAmResourcePercent.isEmpty()) {
			// Is there an override for a queue? If yes, then run with that.
			maxAmResourcePercent = fullyDistributedConfRM
					.get("yarn.scheduler.capacity." + queueName
							+ ".maximum-am-resource-percent");
		}

		if (maxAmResourcePercent == null || maxAmResourcePercent.isEmpty()) {
			maxAmResourcePercent = "0.1"; // Default is 10%
		}
		TestSession.logger.info("Retrieved yarn.scheduler.capacity."
				+ queueName + ".maximum-am-resource-percent" + "' as:["
				+ maxAmResourcePercent + "]");

		return Double.parseDouble(maxAmResourcePercent);

	}

	public double getMaxCapacityPercent(String queueName) {
		FullyDistributedCluster fullyDistributedCluster = (FullyDistributedCluster) TestSession
				.getCluster();
		FullyDistributedConfiguration fullyDistributedConfRM = fullyDistributedCluster
				.getConf(HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
		String maxCapacityPercent = "";
		maxCapacityPercent = fullyDistributedConfRM
				.get("yarn.scheduler.capacity.root." + queueName
						+ ".maximum-capacity");
		double returnValue;
		if (maxCapacityPercent == null || maxCapacityPercent.isEmpty()) {
			returnValue = 100.0;
		} else {
			returnValue = (Double.parseDouble(maxCapacityPercent));
		}
		TestSession.logger.info("Retrieved yarn.scheduler.capacity.root."
				+ queueName + ".maximum-capacity" + "' as:[" + returnValue
				+ "]");
		return returnValue;
	}

	public double getAbsCapacityPercent(String queueName) {
		FullyDistributedCluster fullyDistributedCluster = (FullyDistributedCluster) TestSession
				.getCluster();
		FullyDistributedConfiguration fullyDistributedConfRM = fullyDistributedCluster
				.getConf(HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
		String absCapacityPercent = "";
		absCapacityPercent = fullyDistributedConfRM
				.get("yarn.scheduler.capacity.root." + queueName + ".capacity");
		double returnValue;
		if (absCapacityPercent == null || absCapacityPercent.isEmpty()) {
			returnValue = 100.0;
		} else {
			returnValue = (Double.parseDouble(absCapacityPercent));
		}
		TestSession.logger.info("Retrieved yarn.scheduler.capacity.root."
				+ queueName + ".capacity" + "' as:[" + returnValue + "]");
		return returnValue;
	}

	/**
	 * yarn.scheduler.capacity.maximum-applications /
	 * yarn.scheduler.capacity.<queue-path>.maximum-applications Maximum number
	 * of applications in the system which can be concurrently active both
	 * running and pending. Limits on each queue are directly proportional to
	 * their queue capacities and user limits. This is a hard limit and any
	 * applications submitted when this limit is reached will be rejected.
	 * Default is 10000. This can be set for all queues with
	 * yarn.scheduler.capacity.maximum-applications and can also be overridden
	 * on a per queue basis by setting
	 * yarn.scheduler.capacity.<queue-path>.maximum-applications. Integer value
	 * expected.
	 * 
	 * @param queueName
	 * @return
	 */
	public double getMaxApplications(String queueName) {
		FullyDistributedCluster fullyDistributedCluster = (FullyDistributedCluster) TestSession
				.getCluster();
		FullyDistributedConfiguration fullyDistributedConfRM = fullyDistributedCluster
				.getConf(HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
		String maxApplications = "";
		maxApplications = fullyDistributedConfRM.get("yarn.scheduler.capacity."
				+ queueName + ".maximum-applications");
		if (maxApplications == null) {
			TestSession.logger.info("Read back " + "yarn.scheduler.capacity."
					+ queueName + ".maximum-applications as null");
		} else if (maxApplications != null || !maxApplications.isEmpty()) {
			return Double.parseDouble(maxApplications);
		}

		// If not defined per queue, derive it
		maxApplications = fullyDistributedConfRM
				.get("yarn.scheduler.capacity.maximum-applications");
		if (maxApplications == null || maxApplications.isEmpty()) {
			maxApplications = "10000";
		}
		double absCapacityPercent = getAbsCapacityPercent(queueName);
		double returnValue = Double.parseDouble(maxApplications)
				* absCapacityPercent / 100;
		TestSession.logger.info("Retrieved yarn.scheduler.capacity."
				+ queueName + ".maximum-applications" + "' as:[" + returnValue
				+ "]");
		return returnValue;
	}

	public double getMaxApplicationsPerUser(String queueName) {
		double maxApplications = getMaxApplications(queueName);
		double minUserLimitPercent = getMinUserLimitPercent(queueName);
		double userLimitFactor = getUserLimitFactor(queueName);
		double maxApplicationsPerUser = (maxApplications * minUserLimitPercent / 100)
				* userLimitFactor;
		TestSession.logger.info("Calculated maxApplicationsPerUser as:"
				+ maxApplicationsPerUser);
		return maxApplicationsPerUser;
	}

	public double getMaxActiveApplications(String queueName) {
		FullyDistributedCluster fullyDistributedCluster = (FullyDistributedCluster) TestSession
				.getCluster();
		FullyDistributedConfiguration fullyDistributedConfRM = fullyDistributedCluster
				.getConf(HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
		String minAllocationMbString = fullyDistributedConfRM
				.get("yarn.scheduler.minimum-allocation-mb");
		double minAllocationMb = 1536;
		if (minAllocationMbString == null || minAllocationMbString.isEmpty()) {
			minAllocationMb = 1536;
		} else {
			minAllocationMb = Double.parseDouble(minAllocationMbString);
		}
		double maxAmResourcePercent = getMaxAmResourcePercent(queueName);
		double maxQueueCapacityPercent = getMaxCapacityPercent(queueName);
		double a = (int) Math
				.ceil((totalClusterMemory * 1024 / minAllocationMb)
						* maxAmResourcePercent * maxQueueCapacityPercent / 100);
		double maxActiveApplications = Math.max(a, 1);
		TestSession.logger.info("Calculated  getMaxActiveApplications as:"
				+ maxActiveApplications);
		return maxActiveApplications;

	}

	public double getMaxActiveApplicationsPerUser(String queueName) {
		FullyDistributedCluster fullyDistributedCluster = (FullyDistributedCluster) TestSession
				.getCluster();
		FullyDistributedConfiguration fullyDistributedConfRM = fullyDistributedCluster
				.getConf(HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
		String minAllocationMbString = fullyDistributedConfRM
				.get("yarn.scheduler.minimum-allocation-mb");
		double minAllocationMb = 1536;
		if (minAllocationMbString == null || minAllocationMbString.isEmpty()) {
			minAllocationMb = 1536;
		} else {
			minAllocationMb = Double.parseDouble(minAllocationMbString);
		}
		double maxAmResourcePercent = getMaxAmResourcePercent(queueName);
		double absCapacityPercent = getAbsCapacityPercent(queueName);
		double a = (int) Math
				.ceil((totalClusterMemory * 1024 / minAllocationMb)
						* maxAmResourcePercent * absCapacityPercent / 100);
		double maxActiveApplicationsUsingAbsCap = Math.max(a, 1);
		double userLimitFactor = getUserLimitFactor(queueName);
		double minUserLimitPercent = getMinUserLimitPercent(queueName);

		double a2 = Math.ceil(maxActiveApplicationsUsingAbsCap
				* minUserLimitPercent / 100 * userLimitFactor);
		double maxActiveApplicationsPerUser = Math.max(a2, 1.0);
		TestSession.logger
				.info("Calculated  getMaxActiveApplicationsPerUser as:"
						+ maxActiveApplicationsPerUser);
		return maxActiveApplicationsPerUser;

	}

	public double getMaxQueueCapacityInTermsOfTotalClusterMemory(
			String queueName) {
		FullyDistributedCluster fullyDistributedCluster = (FullyDistributedCluster) TestSession
				.getCluster();
		FullyDistributedConfiguration fullyDistributedConfRM = fullyDistributedCluster
				.getConf(HadooptestConstants.NodeTypes.RESOURCE_MANAGER);

		String maxQueueCapacityPercentageAsString = "";
		maxQueueCapacityPercentageAsString = fullyDistributedConfRM
				.get("yarn.scheduler.capacity.root." + queueName
						+ ".maximum-capacity");
		if (maxQueueCapacityPercentageAsString == null
				|| maxQueueCapacityPercentageAsString.isEmpty()) {
			maxQueueCapacityPercentageAsString = "100.0";
		}
		TestSession.logger.info("Retrieved max capacity for queue '"
				+ "yarn.scheduler.capacity.root." + queueName
				+ ".maximum-capacity" + "' as:["
				+ maxQueueCapacityPercentageAsString + "]");
		Double maxQueueCapacity = Double
				.parseDouble(maxQueueCapacityPercentageAsString);
		return (maxQueueCapacity / 100) * totalClusterMemory;
	}

	public double getQueueCapacityInTermsOfTotalClusterMemory(String queueName) {
		FullyDistributedCluster fullyDistributedCluster = (FullyDistributedCluster) TestSession
				.getCluster();
		FullyDistributedConfiguration fullyDistributedConfRM = fullyDistributedCluster
				.getConf(HadooptestConstants.NodeTypes.RESOURCE_MANAGER);

		String queueCapacityPercentageAsString = "";
		queueCapacityPercentageAsString = fullyDistributedConfRM
				.get("yarn.scheduler.capacity.root." + queueName + ".capacity");
		if (queueCapacityPercentageAsString == null
				|| queueCapacityPercentageAsString.isEmpty()) {
			queueCapacityPercentageAsString = "100.0";
		}
		TestSession.logger.info("Retrieved capacity for queue '"
				+ "yarn.scheduler.capacity.root." + queueName + ".capacity"
				+ "' as:" + queueCapacityPercentageAsString +"%");
		double queueCapacityInTermsOfTotalClusterMemory = ((Double
				.parseDouble(queueCapacityPercentageAsString) / 100) * totalClusterMemory);
		TestSession.logger
				.info("Calculated the apportioned queue capacity for "
						+ "yarn.scheduler.capacity.root." + queueName
						+ ".capacity" + "' as:["
						+ queueCapacityInTermsOfTotalClusterMemory + "]");
		return queueCapacityInTermsOfTotalClusterMemory;

	}
	public double getQueueCapacityInTermsOfPercentage(String queueName) {
//		FullyDistributedCluster fullyDistributedCluster = (FullyDistributedCluster) TestSession
//				.getCluster();
//		FullyDistributedConfiguration fullyDistributedConfRM = fullyDistributedCluster
//				.getConf(HadooptestConstants.NodeTypes.RESOURCE_MANAGER);

		String queueCapacityPercentageAsString = "";										  
		queueCapacityPercentageAsString = fullyDistributedConfRM
				.get("yarn.scheduler.capacity.root." + queueName + ".capacity");
		if (queueCapacityPercentageAsString == null
				|| queueCapacityPercentageAsString.isEmpty()) {
			queueCapacityPercentageAsString = "100.0";
		}
		TestSession.logger.info("Retrieved capacity for queue (%)'"
				+ "yarn.scheduler.capacity.root." + queueName + ".capacity"
				+ "' as:" + queueCapacityPercentageAsString +"%");
		return Double.parseDouble(queueCapacityPercentageAsString);

	}

	public int getTotalClusterMemory() {
		int totalClusterMemoryCapacity = 0;
		int countOfExpectedNamenodes = 0;
		int countOfActiveNamenodes = 0;
		int MAX_RETRY_ATTEMPTS = 6;
		int retriedCount = 0;

		FullyDistributedCluster fullyDistributedCluster = (FullyDistributedCluster) TestSession
				.getCluster();

		String temp = fullyDistributedCluster.getConf(
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER).get(
				"yarn.nodemanager.resource.memory-mb");

		int nodemanagerResourceMemoryMB = Integer.parseInt(temp);
		TestSession.logger
				.info("read yarn.nodemanager.resource.memory-mb (Cluster Capacity) as:"
						+ nodemanagerResourceMemoryMB +" MB");
		int ramPerHostInGB = nodemanagerResourceMemoryMB / 1024;
		TestSession.logger.info("RAM shown above, now in GB:" + ramPerHostInGB);

		countOfExpectedNamenodes = fullyDistributedCluster
				.getNodeNames(HadooptestConstants.NodeTypes.NODE_MANAGER).length;
		countOfActiveNamenodes = getCountOfActiveTrackers();
		while ((countOfActiveNamenodes != countOfExpectedNamenodes)
				&& (retriedCount++ < MAX_RETRY_ATTEMPTS)) {
			countOfActiveNamenodes = getCountOfActiveTrackers();
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
			}
		}

		totalClusterMemoryCapacity = countOfActiveNamenodes * ramPerHostInGB;
		TestSession.logger.info("Calculated Total cluster memory capacity:"
				+ totalClusterMemoryCapacity +" GB");
		return totalClusterMemoryCapacity;

	}

	int getCountOfActiveTrackers() {
		int countOfActiveTrackers = 0;
		MapredCliCommands mapredCliCommands = new MapredCliCommands();
		GenericMapredCliResponseBO genericMapredCliResponseBO = null;

		try {
			genericMapredCliResponseBO = mapredCliCommands.listActiveTrackers(
					YarnTestsBaseClass.EMPTY_ENV_HASH_MAP,
					HadooptestConstants.UserNames.HADOOPQA);
			Assert.assertTrue(genericMapredCliResponseBO.process.exitValue() == 0);
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

	JobQueueInfo[] getQueues() throws IOException {
		FullyDistributedCluster fullyDistributedCluster = (FullyDistributedCluster) TestSession
				.getCluster();
		FullyDistributedConfiguration fullyDistributedConfRM = fullyDistributedCluster
				.getConf(HadooptestConstants.NodeTypes.RESOURCE_MANAGER);

		JobClient jobClient = new JobClient(fullyDistributedConfRM);
		return jobClient.getQueues();
	}

}