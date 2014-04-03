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
import java.util.Arrays;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobQueueInfo;
import org.junit.Assert;
import org.xml.sax.SAXException;

/**
 * The Capacity Business Object
 * 
 * @author tiwari
 * 
 */
public class CapacityBO {

	int clusterCapacity;
	ArrayList<QueueDetails> queueDetails;
	String localXmlFilename;

	public CapacityBO(String localXmlFilename) {
		this.localXmlFilename = localXmlFilename;
		clusterCapacity = getClusterCapacity();
		queueDetails = new ArrayList<QueueDetails>();

		try {
			for (JobQueueInfo aJobQueueInfo : getQueues()) {
				QueueDetails queueDetail = new QueueDetails();
				queueDetail.queueName = aJobQueueInfo.getQueueName();
				queueDetail.queueCapacity = getQueueCapacity(aJobQueueInfo
						.getQueueName());
				queueDetail.maxQueueCapacity = getMaxQueueCapacity(aJobQueueInfo
						.getQueueName());
				queueDetail.userLimitFactor = getUserLimitFactor(aJobQueueInfo
						.getQueueName());
				queueDetail.minUserLimitPercent = getMinUserLimitPercent(aJobQueueInfo
						.getQueueName());
				queueDetail.queueCapacityPerUser = Math
						.min(Math.ceil(queueDetail.queueCapacity
								* queueDetail.userLimitFactor), clusterCapacity);
				queueDetail.queueCapacityMinUserLimit = Math
						.min(Math
								.ceil((queueDetail.queueCapacity * queueDetail.minUserLimitPercent) / 100),
								queueDetail.queueCapacity);
				queueDetails.add(queueDetail);

			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public double getMinUserLimitPercent(String queueName) {
		String minUserLimitPercent = "";
		try {
			minUserLimitPercent = YarnTestsBaseClass
					.lookupValueInBackCopiedCapacitySchedulerXmlFile(
							localXmlFilename, "yarn.scheduler.capacity.root."
									+ queueName + ".minimum-user-limit-percent");
		} catch (TransformerConfigurationException e) {
			e.printStackTrace();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {

			e.printStackTrace();
		} catch (TransformerException e) {
			e.printStackTrace();
		}
		TestSession.logger.info("Retrieved max capacity for queue '"
				+ "yarn.scheduler.capacity.root." + queueName
				+ ".minimum-user-limit-percent" + "' as:["
				+ minUserLimitPercent + "]");

		return (minUserLimitPercent.isEmpty()) ? 50 : Double
				.parseDouble(minUserLimitPercent);

	}

	public double getUserLimitFactor(String queueName) {
		FullyDistributedCluster fullyDistributedCluster = (FullyDistributedCluster) TestSession
				.getCluster();
		FullyDistributedConfiguration fullyDistributedConfRM = fullyDistributedCluster
				.getConf(HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
		String userLimitFactor = "";
		try {
			userLimitFactor = YarnTestsBaseClass
					.lookupValueInBackCopiedCapacitySchedulerXmlFile(
							localXmlFilename, "yarn.scheduler.capacity.root."
									+ queueName + ".user-limit-factor");
		} catch (TransformerConfigurationException e) {
			e.printStackTrace();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (TransformerException e) {
			e.printStackTrace();
		}
		TestSession.logger.info("Retrieved max capacity for queue '"
				+ "yarn.scheduler.capacity.root." + queueName
				+ ".user-limit-factor" + "' as:[" + userLimitFactor + "]");

		return (userLimitFactor.isEmpty()) ? 1.0 : Double
				.parseDouble(userLimitFactor);

	}

	public double getMaxQueueCapacity(String queueName) {

		String maxQueueCapacityInPercentage = "";
		try {
			maxQueueCapacityInPercentage = YarnTestsBaseClass
					.lookupValueInBackCopiedCapacitySchedulerXmlFile(
							localXmlFilename, "yarn.scheduler.capacity.root."
									+ queueName + ".maximum-capacity");
		} catch (TransformerConfigurationException e) {
			e.printStackTrace();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (TransformerException e) {
			e.printStackTrace();
		}
		TestSession.logger.info("Retrieved max capacity for queue '"
				+ "yarn.scheduler.capacity.root." + queueName
				+ ".maximum-capacity" + "' as:[" + maxQueueCapacityInPercentage
				+ "]");
		Double maxQueueCapacity = maxQueueCapacityInPercentage.isEmpty() ? 100
				: Double.parseDouble(maxQueueCapacityInPercentage);
		return maxQueueCapacity * clusterCapacity;
	}

	public double getQueueCapacity(String queueName) {

		String queueCapacityInPercentage = "";
		try {
			queueCapacityInPercentage = YarnTestsBaseClass
					.lookupValueInBackCopiedCapacitySchedulerXmlFile(
							localXmlFilename, "yarn.scheduler.capacity.root."
									+ queueName + ".capacity");
		} catch (TransformerConfigurationException e) {
			e.printStackTrace();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (TransformerException e) {
			e.printStackTrace();
		}
		TestSession.logger.info("Retrieved capacity for queue '"
				+ "yarn.scheduler.capacity.root." + queueName + ".capacity"
				+ "' as:" + queueCapacityInPercentage);
		double queueCapacity = (Double.parseDouble(queueCapacityInPercentage) * clusterCapacity) / 100;
		TestSession.logger
				.info("Calculated the apportioned queue capacity for "
						+ "yarn.scheduler.capacity.root." + queueName
						+ ".capacity" + "' as:[" + queueCapacity + "]");
		return queueCapacity;

	}

	public int getClusterCapacity() {
		int totalClusterCapacity = 0;
		int countOfExpectedTrackers = 0;
		int countOfActiveTrackers = 0;
		int MAX_RETRY_ATTEMPTS = 6;
		int retriedCount = 0;

		FullyDistributedCluster fullyDistributedCluster = (FullyDistributedCluster) TestSession
				.getCluster();

		// Backup config and replace file, for Resource Manager
		String temp = fullyDistributedCluster.getConf(
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER).get(
				"yarn.nodemanager.resource.memory-mb");

		int nodemanagerMB = Integer.parseInt(temp);
		TestSession.logger.info("read yarn.nodemanager.resource.memory-mb as:"
				+ nodemanagerMB);
		int ramPerHostInGB = nodemanagerMB / 1024;
		TestSession.logger.info("GB RAM :" + ramPerHostInGB);

		countOfExpectedTrackers = fullyDistributedCluster
				.getNodeNames(HadooptestConstants.NodeTypes.NODE_MANAGER).length;
		countOfActiveTrackers = getCountOfActiveTrackers();
		while ((countOfActiveTrackers != countOfExpectedTrackers)
				&& (retriedCount++ < MAX_RETRY_ATTEMPTS)) {
			countOfActiveTrackers = getCountOfActiveTrackers();
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
			}
		}

		totalClusterCapacity = countOfActiveTrackers * ramPerHostInGB;
		TestSession.logger.info("Calculated Total capacity:"
				+ totalClusterCapacity);
		return totalClusterCapacity;

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

class QueueDetails {
	String queueName;
	double queueCapacity;
	double maxQueueCapacity;
	double userLimitFactor;
	double minUserLimitPercent;
	double queueCapacityPerUser;
	double queueCapacityMinUserLimit;

}
