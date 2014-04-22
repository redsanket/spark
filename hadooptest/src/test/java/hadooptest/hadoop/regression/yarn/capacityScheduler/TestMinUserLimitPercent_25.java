package hadooptest.hadoop.regression.yarn.capacityScheduler;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.hadoop.regression.yarn.capacityScheduler.RuntimeStatsBO.JobStats;

import java.util.ArrayList;
import java.util.concurrent.Future;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobQueueInfo;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SerialTests.class)
public class TestMinUserLimitPercent_25 extends CapacitySchedulerBaseClass {
	private static int THOUSAND_MILLISECONDS = 1000;
	private static int QUEUE_CAPACITY = 1;

	void printSelfCalculatedStats(
			CalculatedCapacityLimitsBO calculatedCapacityBO) {
		for (QueueCapacityDetail aCalculatedQueueDetail : calculatedCapacityBO.queueCapacityDetails) {
			TestSession.logger.info("Q name****:" + aCalculatedQueueDetail.name
					+ " *****");
			TestSession.logger
					.info("Q capacity in terms of cluster memory:"
							+ aCalculatedQueueDetail.capacityInTermsOfTotalClusterMemory
							+ " MB");
			TestSession.logger.info("Q max apps:"
					+ aCalculatedQueueDetail.maxApplications);
			TestSession.logger.info("Q max apps per user:"
					+ aCalculatedQueueDetail.maxApplicationsPerUser);
			TestSession.logger.info("Q max active apps per user:"
					+ aCalculatedQueueDetail.maxActiveApplicationsPerUser);
			TestSession.logger.info("Q max active apps:"
					+ aCalculatedQueueDetail.maxActiveApplications);
			TestSession.logger.info("Q min user limit percent:"
					+ aCalculatedQueueDetail.minimumUserLimitPercent);
			TestSession.logger.info("Q user limit factor:"
					+ aCalculatedQueueDetail.userLimitFactor);

			TestSession.logger
					.info("----------------------------------------------");
		}

	}

	void printValuesReceivedOverRest(
			ArrayList<QueueCapacityDetail> retrievedCapLimits) {
		TestSession.logger
				.info("{V}{A}{L}{U}{E}{S}{}{}{}{}{}{}{}{R}{E}{C}{E}{I}{V}{E}{D}{}}{}{}{}{}{{O}{V}{E}{R}{}{}{}{}{}{}{}{R}{E}{S}{T}");
		for (QueueCapacityDetail aRetrievedQDetail : retrievedCapLimits) {
			TestSession.logger.info("Q name****:" + aRetrievedQDetail.name
					+ " *****");
			TestSession.logger.info("Q capacity in terms of cluster memory:"
					+ aRetrievedQDetail.capacityInTermsOfTotalClusterMemory
					+ " MB");
			TestSession.logger.info("Q max apps:"
					+ aRetrievedQDetail.maxApplications);
			TestSession.logger.info("Q max apps per user:"
					+ aRetrievedQDetail.maxApplicationsPerUser);
			TestSession.logger.info("Q max active apps per user:"
					+ aRetrievedQDetail.maxActiveApplicationsPerUser);
			TestSession.logger.info("Q max active apps:"
					+ aRetrievedQDetail.maxActiveApplications);
			TestSession.logger.info("Q min user limit percent:"
					+ aRetrievedQDetail.minimumUserLimitPercent);
			TestSession.logger.info("Q user limit factor:"
					+ aRetrievedQDetail.userLimitFactor);
			TestSession.logger.info("Pending jobs:"
					+ aRetrievedQDetail.numPendingApplications);

			TestSession.logger
					.info("wwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwww");

		}

	}

	/**
	 * For each queue, get the maximum schedulable jobs limit and submit jobs to
	 * test that liit. Any jobs beyond that limit should get queued
	 * 
	 * @throws Exception
	 */
	@Test
	public void testMinUserLimitPercent() throws Exception {
		String testCode = "t1MinUsrLimit";
		String queueNameForThisTest = "grideng";
		copyResMgrConfigAndRestartNodes(TestSession.conf
				.getProperty("WORKSPACE")
				+ "/resources/hadooptest/hadoop/regression/yarn/capacityScheduler/capacity-scheduler25.xml");

		CalculatedCapacityLimitsBO calculatedCapacityBO = selfCalculateCapacityLimits();
		printSelfCalculatedStats(calculatedCapacityBO);
		ArrayList<Future<Job>> futureCallableSleepJobs;
		JobClient jobClient = new JobClient(TestSession.cluster.getConf());
		for (JobQueueInfo jobQueueInfo : jobClient.getQueues()) {
			for (QueueCapacityDetail aQueueCapaityDetail : calculatedCapacityBO.queueCapacityDetails) {
				if (aQueueCapaityDetail.name
						.equals(jobQueueInfo.getQueueName())
						&& aQueueCapaityDetail.name
								.equals(queueNameForThisTest)) {
					TestSession.logger
							.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Starting testMinUserLimitPercent for queue "
									+ jobQueueInfo.getQueueName()
									+ " ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
					double maxSchedulableApps = aQueueCapaityDetail.maxActiveApplications;
					int numSleepJobsToLaunch = (int) maxSchedulableApps;
					ArrayList<SleepJobParams> sleepJobParamsList = new ArrayList<SleepJobParams>();
					for (int jobCount = 1; jobCount <= numSleepJobsToLaunch; jobCount++) {
						SleepJobParams sleepJobParams = new SleepJobParams(
								calculatedCapacityBO,
								getDefaultSleepJobProps(jobQueueInfo
										.getQueueName()),
								jobQueueInfo.getQueueName(), "hadoop"
										+ jobCount, 4 * QUEUE_CAPACITY,
								10 * THOUSAND_MILLISECONDS, testCode + "-job-"
										+ jobCount + "-launched-by-user-"
										+ "hadoop" + jobCount + "-on-queue-"
										+ jobQueueInfo.getQueueName());
						sleepJobParamsList.add(sleepJobParams);

					}

					futureCallableSleepJobs = submitJobsToAThreadPoolAndRunThemInParallel(
							sleepJobParamsList, 500);

					// Wait until all jobs have reached RUNNING state
					BarrierUntilAllThreadsRunning barrierUntilAllThreadsRunning = new BarrierUntilAllThreadsRunning(
							futureCallableSleepJobs, SLEEP_JOB_DURATION_IN_SECS
									* numSleepJobsToLaunch);
					TestSession.logger
							.info("barrier met....  all jobs've reached runnable");
					RuntimeStatsBO runtimeStatsBO = collateRuntimeStatsForJobs(futureCallableSleepJobs);
					Assert.assertTrue(
							"Jobs submitted to multiple queues. Shold have been just "
									+ queueNameForThisTest,
							runtimeStatsBO.unique_queue_set.size() == 1);
					Assert.assertTrue("Expected count of jobs:"
							+ runtimeStatsBO.unique_user_set.size()
							+ ", but was  " + futureCallableSleepJobs.size()
							+ " in reality", runtimeStatsBO.unique_user_set
							.size() == futureCallableSleepJobs.size());
					double calculatedMinUserLimitPercent = calculatedCapacityBO
					.getMinUserLimitPercent(queueNameForThisTest);
					for (JobStats aJobStats : runtimeStatsBO.jobStatsSet) {
						for (int memoryConsumed : aJobStats.memoryConsumed) {
							Assert.assertTrue(
									"JobId: "
											+ aJobStats.job.getJobName()
											+ " consumed "
											+ memoryConsumed
											+ " MB memory, while min-user-limit-percent is:"
											+ calculatedCapacityBO
													.getMinUserLimitPercent(queueNameForThisTest),
									memoryConsumed < calculatedMinUserLimitPercent);
						}
					}

					ArrayList<QueueCapacityDetail> retrievedCapLimits = getCapacityLimitsViaRestCallIntoRM();
					printValuesReceivedOverRest(retrievedCapLimits);
					// In preparation for the next loop, kill the started jobs
					killStartedJobs(futureCallableSleepJobs);
				}
			}
		}

	}

}