package hadooptest.hadoop.regression.yarn.capacityScheduler;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.config.hadoop.fullydistributed.FullyDistributedConfiguration;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass;
import hadooptest.hadoop.regression.yarn.capacityScheduler.SchedulerRESTStatsSnapshot.LeafQueue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.TIPStatus;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.mapreduce.QueueInfo;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskType;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
@Category(SerialTests.class)
public class TestDominantResourceCalculatorHierarchicalQueues extends
		CapacitySchedulerBaseClass {
	public String queueUnderTest;

	@Parameters
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][] { { "a1" }, { "a2" }, { "b" }, });
	}

	public TestDominantResourceCalculatorHierarchicalQueues(
			String queueUnderTest) {
		super();
		this.queueUnderTest = queueUnderTest;
	}

	private static int THOUSAND_MILLISECONDS = 1000;
	private static int DEFAULT_NUM_OF_REDUCE_CONTAINERS = 1;
	private static boolean isFileSetEverywhere = false;

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
	public void testDominantResourceCalculatorHierarchicalQueues()
			throws Exception {
		DfsTestsBaseClass.ensureDataPresenceInCluster();

		String testCode = "testDominantResourceCalculatorHierarchicalQueues";
		int numberOfVcoresPerCPUThatAreAvailableForNodeManagerForSpawningContainers = 2;
		int numberOfVcoresToBeUsedByASingleMapTask = 1;
		TestSession.logger
				.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ "
						+ "Starting testDominantResourceCalculatorHierarchicalQueues for queue ["
						+ queueUnderTest + "]"
						+ " ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
		String fileUsedInTest = System.getProperty("WORKSPACE")
				+ "/htf-common/resources/hadooptest/hadoop/"
				+ "regression/yarn/capacityScheduler/"
				+ "capacity-schedulerDominantResourceSchedulerQueueHierarchy.xml";
		if (!isFileSetEverywhere) {
			setDominantResourceParametersEverywhere(
					fileUsedInTest,
					numberOfVcoresPerCPUThatAreAvailableForNodeManagerForSpawningContainers,
					false);
			isFileSetEverywhere = true;
		}

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
						&& (aQueueCapaityDetail.name.equals(queueUnderTest))) {

					CallableWordCountMapperOnlyJob aJobComprisingOnlyMappers = new CallableWordCountMapperOnlyJob(
							testCode, queueInfo.getQueueName(),
							HadooptestConstants.UserNames.HADOOPQA,
							numberOfVcoresToBeUsedByASingleMapTask);
					ArrayList<CallableWordCountMapperOnlyJob> listOfCallableWordCountMapperOnlyJobs = new ArrayList<CallableWordCountMapperOnlyJob>();
					listOfCallableWordCountMapperOnlyJobs
							.add(aJobComprisingOnlyMappers);
					ArrayList<Future<Job>> handlesToTheFuture = null;
					handlesToTheFuture = submitWordCountMapperOnlyJobsToThreadPool(
							listOfCallableWordCountMapperOnlyJobs, 0);
					TestSession.logger.info("Handle size read back:"
							+ handlesToTheFuture.size());
					RuntimeRESTStatsBO runtimeRESTStatsBO = startCollectingRestStats(1 * THOUSAND_MILLISECONDS);
					waitFor(30 * THOUSAND_MILLISECONDS);
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
					TestSession.logger.info("Handle size before loop:"
							+ handlesToTheFuture.size());

					for (Future<Job> aFutureHandle : handlesToTheFuture) {
						TestSession.logger
								.info("Inside a future job loop for job:"
										+ aFutureHandle.get().getJobName());
						for (TaskReport aTaskReport : aFutureHandle.get()
								.getTaskReports(TaskType.MAP)) {
							TestSession.logger.info("Inside aTaskReport(Map);");
							for (TaskAttemptID aTaskAttemptId : aTaskReport
									.getRunningTaskAttemptIds()) {
								TestSession.logger
										.info("Inside aTaskattemptId(Map)");
								TestSession.logger
										.info("Culling MAP task attempt id:"
												+ aTaskAttemptId);
								aFutureHandle.get().killTask(aTaskAttemptId);

							}
						}
						for (TaskReport aTaskReport : aFutureHandle.get()
								.getTaskReports(TaskType.REDUCE)) {
							TestSession.logger
									.info("Inside aTaskReport(Reduce);");
							for (TaskAttemptID aTaskAttemptId : aTaskReport
									.getRunningTaskAttemptIds()) {
								TestSession.logger
										.info("Culling REDUCE task attempt id:"
												+ aTaskAttemptId);
								aFutureHandle.get().killTask(aTaskAttemptId);

							}
						}

						TestSession.logger.info("Proceeding to kill job:"
								+ aFutureHandle.get().getJobName());
						aFutureHandle.get().killJob();
					}
					double minQCapacityPercent = getMinPercentageForLeafQueue(queueUnderTest);

					int minCountOfContainers = getExpectedNumberOfContainers(
							numberOfVcoresPerCPUThatAreAvailableForNodeManagerForSpawningContainers,
							numberOfVcoresToBeUsedByASingleMapTask,
							DEFAULT_NUM_OF_REDUCE_CONTAINERS,
							minQCapacityPercent);
					TestSession.logger.info("Min Count of containers:"
							+ minCountOfContainers);

					double maxQCapacityPercent = getMaxPercentageForLeafQueue(queueUnderTest);

					int maxCountOfContainers = getExpectedNumberOfContainers(
							numberOfVcoresPerCPUThatAreAvailableForNodeManagerForSpawningContainers,
							numberOfVcoresToBeUsedByASingleMapTask,
							DEFAULT_NUM_OF_REDUCE_CONTAINERS,
							maxQCapacityPercent);

					TestSession.logger.info("Max Count of containers:"
							+ maxCountOfContainers);

					Assert.assertTrue(
							"minCountOfContainers:" + minCountOfContainers
									+ " actualCountOfMapContainers="
									+ actualCountOfMapTasks
									+ " maxCountOfMapContainers="
									+ maxCountOfContainers,
							(minCountOfContainers <= actualCountOfMapTasks)
									&& (actualCountOfMapTasks <= maxCountOfContainers));
					TestSession.logger.info("YAY, for queue["
							+ aQueueCapaityDetail.name + "] and file["
							+ fileUsedInTest + "], minCountOfContainers:"
							+ minCountOfContainers + " actualCountOfMapTasks="
							+ actualCountOfMapTasks + " maxCountOfContainers="
							+ maxCountOfContainers);

				}
			}
		}
	}

	float getMinPercentageForLeafQueue(String leafQueue) {
		float capacity = 0;
		float minPercentCapOfChild = 0;
		float minPercentCapOfParent = 0;
		FullyDistributedCluster fullyDistributedCluster = (FullyDistributedCluster) TestSession
				.getCluster();
		FullyDistributedConfiguration fullyDistributedConfRM = fullyDistributedCluster
				.getConf(HadooptestConstants.NodeTypes.RESOURCE_MANAGER);

		if (leafQueue.equalsIgnoreCase("a1")
				|| (leafQueue.equalsIgnoreCase("a2"))) {
			String propName = "yarn.scheduler.capacity.root.a." + leafQueue
					+ ".capacity";
			minPercentCapOfChild = fullyDistributedConfRM.getFloat(propName, 0);
			TestSession.logger.info("Read " + "yarn.scheduler.capacity.root.a."
					+ leafQueue + ".capacity as:" + minPercentCapOfChild);

			propName = "yarn.scheduler.capacity.root.a.capacity";
			minPercentCapOfParent = fullyDistributedConfRM
					.getFloat(propName, 0);
			TestSession.logger.info("Read " + propName + " as:"
					+ minPercentCapOfParent);

			capacity = (minPercentCapOfChild * minPercentCapOfParent) / 100;
			TestSession.logger.info("Calc final Cap as:" + capacity);

		} else {
			// b
			String propName = "yarn.scheduler.capacity.root." + leafQueue
					+ ".capacity";
			capacity = fullyDistributedConfRM.getFloat(propName, 0);
			TestSession.logger.info("Read " + propName + " as:" + capacity);
		}

		return capacity;

	}

	float getMaxPercentageForLeafQueue(String leafQueue) {
		float capacity = 0;
		float maxPercentCapOfChild = 0;
		float maxPercentCapOfParent = 0;
		FullyDistributedCluster fullyDistributedCluster = (FullyDistributedCluster) TestSession
				.getCluster();
		FullyDistributedConfiguration fullyDistributedConfRM = fullyDistributedCluster
				.getConf(HadooptestConstants.NodeTypes.RESOURCE_MANAGER);

		if (leafQueue.equalsIgnoreCase("a1")
				|| (leafQueue.equalsIgnoreCase("a2"))) {
			String propName = "yarn.scheduler.capacity.root.a." + leafQueue
					+ ".maximum-capacity";
			maxPercentCapOfChild = fullyDistributedConfRM
					.getFloat(propName, 0F);
			TestSession.logger.info("Read " + propName + " as:"
					+ maxPercentCapOfChild);
			propName = "yarn.scheduler.capacity.root.a.maximum-capacity";
			maxPercentCapOfParent = fullyDistributedConfRM
					.getFloat(propName, 0);
			TestSession.logger.info("Read " + propName + " as:"
					+ maxPercentCapOfParent);
			capacity = (maxPercentCapOfChild * maxPercentCapOfParent) / 100;
			TestSession.logger.info("Calc final Max Cap as:" + capacity);

		} else {
			// b
			String propName = "yarn.scheduler.capacity.root." + leafQueue
					+ ".maximum-capacity";
			capacity = fullyDistributedConfRM.getFloat(propName, 0);
			TestSession.logger.info("Read " + propName + " as:"
					+ maxPercentCapOfParent);
			TestSession.logger.info("Calc final Max Cap as:" + capacity);
		}

		return capacity;

	}

	// @After
	// public void restoreTheConfigFile() throws Exception {
	// /**
	// * Do not restore the config file
	// * Hence overriding.
	// */
	// }
}
