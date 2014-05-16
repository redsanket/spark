package hadooptest.hadoop.regression.yarn.capacityScheduler;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.hadoop.regression.yarn.capacityScheduler.SchedulerRESTStatsSnapshot.LeafQueue;

import java.util.ArrayList;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.TIPStatus;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.mapreduce.QueueInfo;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskType;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SerialTests.class)
public class TestDominantResourceCalculator extends CapacitySchedulerBaseClass {
	private static int THOUSAND_MILLISECONDS = 1000;
	private static int DEFAULT_NUM_OF_REDUCE_CONTAINERS = 1;

	/**
	 * These are great document for cgroup(s):
	 * http://riccomini.name/posts/hadoop/2013-06-14-yarn-with-cgroups/
	 * https://access
	 * .redhat.com/site/documentation/en-US/Red_Hat_Enterprise_Linux
	 * /6/html/Resource_Management_Guide/ch01.html
	 * 
	 * 
	 * @throws Exception
	 */

	@Test
	public void testDominantResourceCalculatorFixedCapacity() throws Exception {
		String testCode = "DominantResourceCalculatorWithOnlyMappers";
		int numberOfVcoresPerCPUThatAreAvailableForNodeManagerForSpawningContainers = 2;
		int numberOfVcoresToBeUsedByASingleMapTask = 1;
		TestSession.logger.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ "
				+ "Starting testDominantResourceCalculator for queue [default]"
				+ " ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

		setDominantResourceParametersEverywhere(
				TestSession.conf.getProperty("WORKSPACE")
						+ "/htf-common/resources/hadooptest/hadoop/"
						+ "regression/yarn/capacityScheduler/"
						+ "capacity-scheduler-DominantResourceCalculator-default-85.xml",
				numberOfVcoresPerCPUThatAreAvailableForNodeManagerForSpawningContainers,
				numberOfVcoresToBeUsedByASingleMapTask);

		CalculatedCapacityLimitsBO calculatedCapacityBO = selfCalculateCapacityLimits();
		printSelfCalculatedStats(calculatedCapacityBO);
		FullyDistributedCluster fullyDistributedCluster = (FullyDistributedCluster) TestSession
				.getCluster();

		Configuration fullyDistRMConf = fullyDistributedCluster
				.getConf(HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
		Cluster cluster = new Cluster(fullyDistRMConf);

		for (QueueInfo queueInfo : cluster.getQueues()) {
			for (QueueCapacityDetail aQueueCapaityDetail : calculatedCapacityBO.queueCapacityDetails) {
				if (aQueueCapaityDetail.name.equals(queueInfo.getQueueName())
						&& (aQueueCapaityDetail.name
								.equalsIgnoreCase("default"))) {

					CallableWordCountMapperOnlyJob aJobComprisingOnlyMappers = new CallableWordCountMapperOnlyJob(
							testCode, queueInfo.getQueueName(),
							HadooptestConstants.UserNames.HADOOPQA,
							numberOfVcoresToBeUsedByASingleMapTask);
					ArrayList<CallableWordCountMapperOnlyJob> listOfCallableWordCountMapperOnlyJobs = new ArrayList<CallableWordCountMapperOnlyJob>();
					ArrayList<Future<Job>> handlesToTheFuture = null;
					handlesToTheFuture = submitWordCountMapperOnlyJobsToThreadPool(
							listOfCallableWordCountMapperOnlyJobs, 0);
					RuntimeRESTStatsBO runtimeRESTStatsBO = startCollectingRestStats(1 * THOUSAND_MILLISECONDS);
					waitFor(60 * THOUSAND_MILLISECONDS);
					stopCollectingRuntimeStatsAcrossQueues(runtimeRESTStatsBO);
					printValuesReceivedOverRest(runtimeRESTStatsBO);

					for (SchedulerRESTStatsSnapshot aSchedulerRESTStatsSnapshot : runtimeRESTStatsBO.listOfRESTSnapshotsAcrossAllLeafQueues) {
						for (LeafQueue aLeafQueue : aSchedulerRESTStatsSnapshot.allLeafQueues) {
							TestSession.logger
									.info("Number of containers read back from REST ["
											+ aLeafQueue.queueName
											+ "] "
											+ aLeafQueue.numContainers);
						}
					}

					Cluster mapReduceCluster = new Cluster(
							TestSession.cluster.getConf());
					Job aRunningJob = null;
					for (JobStatus aJobStatus : mapReduceCluster
							.getAllJobStatuses()) {
						if (aJobStatus.getState() == State.RUNNING) {
							aRunningJob = mapReduceCluster.getJob(aJobStatus
									.getJobID());
							TestSession.logger
									.info("Found a running job called:"
											+ aRunningJob.getJobName());
							break;
						}
					}
					int actualCountOfMapTasks = 0;
					for (TaskReport aMapTaskReport : mapReduceCluster.getJob(
							aRunningJob.getJobID())
							.getTaskReports(TaskType.MAP)) {
						if (aMapTaskReport.getCurrentStatus() == TIPStatus.RUNNING) {
							actualCountOfMapTasks++;
							TestSession.logger
									.info("incremented the count of a map task, new count:"
											+ actualCountOfMapTasks);
						}

					}

					for (Future<Job> aFutureHandle : handlesToTheFuture) {
						aFutureHandle.get().killJob();
					}
				}
			}
		}
	}

	@Test
	public void testDominantResourceCalculatorCapacityRange() throws Exception {
		String testCode = "DominantResourceCalculatorWithOnlyMappers";
		int numberOfVcoresPerCPUThatAreAvailableForNodeManagerForSpawningContainers = 2;
		int numberOfVcoresToBeUsedByASingleMapTask = 1;
		TestSession.logger.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ "
				+ "Starting testDominantResourceCalculator for queue [default]"
				+ " ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

		setDominantResourceParametersEverywhere(
				TestSession.conf.getProperty("WORKSPACE")
						+ "/htf-common/resources/hadooptest/hadoop/"
						+ "regression/yarn/capacityScheduler/"
						+ "capacity-scheduler-DominantResourceCalculator-default-50.xml",
				numberOfVcoresPerCPUThatAreAvailableForNodeManagerForSpawningContainers,
				numberOfVcoresToBeUsedByASingleMapTask);

		CalculatedCapacityLimitsBO calculatedCapacityBO = selfCalculateCapacityLimits();
		printSelfCalculatedStats(calculatedCapacityBO);
		FullyDistributedCluster fullyDistributedCluster = (FullyDistributedCluster) TestSession
				.getCluster();

		Configuration fullyDistRMConf = fullyDistributedCluster
				.getConf(HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
		Cluster cluster = new Cluster(fullyDistRMConf);

		for (QueueInfo queueInfo : cluster.getQueues()) {
			for (QueueCapacityDetail aQueueCapaityDetail : calculatedCapacityBO.queueCapacityDetails) {
				if (aQueueCapaityDetail.name.equals(queueInfo.getQueueName())) {

					CallableWordCountMapperOnlyJob aJobComprisingOnlyMappers = new CallableWordCountMapperOnlyJob(
							testCode, queueInfo.getQueueName(),
							HadooptestConstants.UserNames.HADOOPQA,
							numberOfVcoresToBeUsedByASingleMapTask);
					ArrayList<CallableWordCountMapperOnlyJob> listOfCallableWordCountMapperOnlyJobs = new ArrayList<CallableWordCountMapperOnlyJob>();
					ArrayList<Future<Job>> handlesToTheFuture = null;
					handlesToTheFuture = submitWordCountMapperOnlyJobsToThreadPool(
							listOfCallableWordCountMapperOnlyJobs, 0);
					RuntimeRESTStatsBO runtimeRESTStatsBO = startCollectingRestStats(1 * THOUSAND_MILLISECONDS);
					waitFor(60 * THOUSAND_MILLISECONDS);
					stopCollectingRuntimeStatsAcrossQueues(runtimeRESTStatsBO);
					printValuesReceivedOverRest(runtimeRESTStatsBO);

					for (SchedulerRESTStatsSnapshot aSchedulerRESTStatsSnapshot : runtimeRESTStatsBO.listOfRESTSnapshotsAcrossAllLeafQueues) {
						for (LeafQueue aLeafQueue : aSchedulerRESTStatsSnapshot.allLeafQueues) {
							TestSession.logger
									.info("Number of containers read back from REST ["
											+ aLeafQueue.queueName
											+ "] "
											+ aLeafQueue.numContainers);
						}
					}

					Cluster mapReduceCluster = new Cluster(
							TestSession.cluster.getConf());
					Job aRunningJob = null;
					for (JobStatus aJobStatus : mapReduceCluster
							.getAllJobStatuses()) {
						if (aJobStatus.getState() == State.RUNNING) {
							aRunningJob = mapReduceCluster.getJob(aJobStatus
									.getJobID());
							TestSession.logger
									.info("Found a running job called:"
											+ aRunningJob.getJobName());
							break;
						}
					}
					int actualCountOfMapTasks = 0;
					for (TaskReport aMapTaskReport : mapReduceCluster.getJob(
							aRunningJob.getJobID())
							.getTaskReports(TaskType.MAP)) {
						if (aMapTaskReport.getCurrentStatus() == TIPStatus.RUNNING) {
							actualCountOfMapTasks++;
							TestSession.logger
									.info("incremented the count of a map task, new count:"
											+ actualCountOfMapTasks);
						}

					}

					for (Future<Job> aFutureHandle : handlesToTheFuture) {
						aFutureHandle.get().killJob();
					}
				}
			}
		}
	}

	// @After
	// public void restoreTheConfigFile() throws Exception {
	// /**
	// * Do not restore the config file
	// * Hence overriding.
	// */
	// }
}
