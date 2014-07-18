package hadooptest.hadoop.regression.yarn.capacityScheduler;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.Future;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobQueueInfo;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SerialTests.class)
public class TestMinUserLimitPercentAndHighRam extends
		CapacitySchedulerBaseClass {
	private static int THOUSAND_MILLISECONDS = 1000;
	private static int QUEUE_CAPACITY = 1;

	/**
	 * Submit jobs at really bloated leaf-queue capacities, with the intent that
	 * the memory utilization would drop for each user and come down to a
	 * minimum guarantee of minimum-user-limit-percent and not go beyond that.
	 * 
	 * 
	 * @throws Exception
	 */
	@Ignore @Test
	public void testMinUserLimitPercent() throws Exception {
		String testCode = "t1MinUsrLimit";
		String queueNameForThisTest = "grideng";
		resetTheMaxQueueCapacity();
		copyResMgrConfigAndRestartNodes(TestSession.conf
				.getProperty("WORKSPACE")
				+ "/htf-common/resources/hadooptest/hadoop/regression/yarn/capacityScheduler/capacity-scheduler25.xml");
		JobClient jobClient = new JobClient(TestSession.cluster.getConf());

		ArrayList<String> concurrentUsers = new ArrayList<String>();
		CalculatedCapacityLimitsBO calculatedCapacityBO = selfCalculateCapacityLimits();
		printSelfCalculatedStats(calculatedCapacityBO);
		ArrayList<Future<Job>> futureCallableSleepJobs;
		for (JobQueueInfo jobQueueInfo : jobClient.getQueues()) {
			for (QueueCapacityDetail aQueueCapaityDetail : calculatedCapacityBO.queueCapacityDetails) {
				if (aQueueCapaityDetail.name
						.equals(jobQueueInfo.getQueueName())
						&& aQueueCapaityDetail.name
								.equals(queueNameForThisTest)) {
					concurrentUsers.clear();
					TestSession.logger
							.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Starting testMinUserLimitPercent for queue "
									+ jobQueueInfo.getQueueName()
									+ " ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
					double maxSchedulableApps = aQueueCapaityDetail.maxActiveApplications;
					int numSleepJobsToLaunch = (int) maxSchedulableApps;
					ArrayList<SleepJobParams> sleepJobParamsList = new ArrayList<SleepJobParams>();
					for (int jobCount = 1; jobCount <= numSleepJobsToLaunch; jobCount++) {
						concurrentUsers.add("hadoop" + jobCount);
						SleepJobParams sleepJobParams = new SleepJobParams(
								calculatedCapacityBO,
								getDefaultSleepJobProps(jobQueueInfo
										.getQueueName()),
								jobQueueInfo.getQueueName(), "hadoop"
										+ jobCount, 4 * QUEUE_CAPACITY,
								600 * THOUSAND_MILLISECONDS, testCode + "-job-"
										+ jobCount + "-launched-by-user-"
										+ "hadoop" + jobCount + "-on-queue-"
										+ jobQueueInfo.getQueueName());
						sleepJobParamsList.add(sleepJobParams);

					}

					futureCallableSleepJobs = submitSleepJobsToAThreadPoolAndRunThemInParallel(
							sleepJobParamsList, 1000);

					// Wait until all jobs have reached RUNNING state
					BarrierUntilAllThreadsRunning barrierUntilAllThreadsRunning = new BarrierUntilAllThreadsRunning(
							futureCallableSleepJobs, SLEEP_JOB_DURATION_IN_SECS
									* numSleepJobsToLaunch);
					TestSession.logger
							.info("barrier met....  all jobs've reached runnable");
					// Let the new job to settle down
					waitFor(5 * THOUSAND_MILLISECONDS);

					RuntimeRESTStatsBO runtimeRESTStatsBO = startCollectingRestStats(2 * THOUSAND_MILLISECONDS);
					waitFor(60 * THOUSAND_MILLISECONDS);
					stopCollectingRuntimeStatsAcrossQueues(runtimeRESTStatsBO);

					printValuesReceivedOverRest(runtimeRESTStatsBO);

					assertMinimumUserLimitPercent(testCode,
							aQueueCapaityDetail, jobQueueInfo.getQueueName(),
							runtimeRESTStatsBO);

					// In preparation for the next loop, kill the started jobs
					killStartedJobs(futureCallableSleepJobs);
				}
			}
		}

	}

	@Ignore @Test
	public void testHighRam() throws Exception {
		String testCode = "tHighRam";
		String queueNameForThisTest = "default";
		resetTheMaxQueueCapacity();
		copyResMgrConfigAndRestartNodes(TestSession.conf
				.getProperty("WORKSPACE")
				+ "/htf-common/resources/hadooptest/hadoop/regression/yarn/capacityScheduler/capacity-scheduler100.xml");
		JobClient jobClient = new JobClient(TestSession.cluster.getConf());

		/**
		 * The following delay is needed because one needs to be aware that if
		 * jobs are submitted simultaneously then at least some of them would be
		 * assigned 2048 MB (the size of AM container). So if the
		 * min-user-limit-percent is set to 100% (as is in the case of
		 * capacity-scheduler100.xml"), then since a fraction has been already
		 * assigned to other concurrent users, our asserts would start failing.
		 */

		final int SUFFICIENT_DELAY_BETWEEN_SPAWNING_JOBS = 60 * THOUSAND_MILLISECONDS;

		CalculatedCapacityLimitsBO calculatedCapacityBO = selfCalculateCapacityLimits();
		printSelfCalculatedStats(calculatedCapacityBO);
		ArrayList<Future<Job>> futureCallableSleepJobs;

		for (JobQueueInfo jobQueueInfo : jobClient.getQueues()) {
			for (QueueCapacityDetail aQueueCapaityDetail : calculatedCapacityBO.queueCapacityDetails) {
				if (aQueueCapaityDetail.name
						.equals(jobQueueInfo.getQueueName())
						&& aQueueCapaityDetail.name
								.equals(queueNameForThisTest)) {
					TestSession.logger
							.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Starting testHighRam for queue "
									+ jobQueueInfo.getQueueName()
									+ " ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
					double maxSchedulableApps = aQueueCapaityDetail.maxActiveApplications;
					int numSleepJobsToLaunch = (int) maxSchedulableApps;
					ArrayList<SleepJobParams> sleepJobParamsList = new ArrayList<SleepJobParams>();
					for (int jobCount = 1; jobCount <= numSleepJobsToLaunch; jobCount++) {
						HashMap<String, String> sleepJobConstraints = getDefaultSleepJobProps(jobQueueInfo
								.getQueueName());
						sleepJobConstraints.put("mapreduce.map.memory.mb",
								"10240");
						sleepJobConstraints.put("mapreduce.reduce.memory.mb",
								"2048");
						sleepJobConstraints.put(
								"yarn.app.mapreduce.am.resource.mb", "2048");
						SleepJobParams sleepJobParams = new SleepJobParams(
								calculatedCapacityBO, sleepJobConstraints,
								jobQueueInfo.getQueueName(), "hadoop"
										+ jobCount, 1 * QUEUE_CAPACITY,
								600 * THOUSAND_MILLISECONDS, testCode + "-job-"
										+ jobCount + "-launched-by-user-"
										+ "hadoop" + jobCount + "-on-queue-"
										+ queueNameForThisTest);
						sleepJobParamsList.add(sleepJobParams);

					}

					futureCallableSleepJobs = submitSleepJobsToAThreadPoolAndRunThemInParallel(
							sleepJobParamsList,
							SUFFICIENT_DELAY_BETWEEN_SPAWNING_JOBS);

					// Wait until all jobs have reached RUNNING state
					BarrierUntilAllThreadsRunning barrierUntilAllThreadsRunning = new BarrierUntilAllThreadsRunning(
							futureCallableSleepJobs, SLEEP_JOB_DURATION_IN_SECS
									* numSleepJobsToLaunch);
					// With all jobs running, any additional jobs submitted
					// should wait
					Future<Job> overageJobHandle = submitSingleSleepJobAndGetHandle(
							jobQueueInfo.getQueueName(),
							HadooptestConstants.UserNames.HADOOPQA,
							ExpectToBomb.NO);

					waitFor(5 * THOUSAND_MILLISECONDS);

					RuntimeRESTStatsBO runtimeRESTStatsBO = startCollectingRestStats(1 * THOUSAND_MILLISECONDS);
					waitFor(10 * THOUSAND_MILLISECONDS);
					stopCollectingRuntimeStatsAcrossQueues(runtimeRESTStatsBO);

					printValuesReceivedOverRest(runtimeRESTStatsBO);

					assertMinimumUserLimitPercent(testCode,
							aQueueCapaityDetail, jobQueueInfo.getQueueName(),
							runtimeRESTStatsBO);

					// In preparation for the next loop, kill the started jobs
					killStartedJobs(futureCallableSleepJobs);
				}
			}
		}
	}
}