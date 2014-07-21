package hadooptest.hadoop.regression.yarn.capacityScheduler;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.hadoop.regression.yarn.capacityScheduler.SchedulerRESTStatsSnapshot.LeafQueue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Future;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobQueueInfo;
import org.apache.hadoop.mapreduce.Job;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Ignore;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
@Category(SerialTests.class)
public class TestCapacitySchedulerLimits extends CapacitySchedulerBaseClass {
	public String queueUnderTest;

	@Parameters
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][] { { "a1" },
		 { "a2" },
		 { "b" },
				});
	}

	@BeforeClass
	public static void testSessionStart() throws Exception {
		TestSession.start();
	}

	public TestCapacitySchedulerLimits(String queueUnderTest) {
		super();
		this.queueUnderTest = queueUnderTest;
	}

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
	@Test
	@Ignore
	public void testCapSchedLimits() throws Exception {
		String testCode = "t1CapSchedLimit";
		copyResMgrConfigAndRestartNodes(TestSession.conf
				.getProperty("WORKSPACE")
				+ "/htf-common/resources/hadooptest/hadoop/regression/yarn/capacityScheduler/capacity-schedulerLimits.xml");
		/**
		 * Reset the max capacity as I think values exported, tend to persist across tests.
		 */
		JobClient jobClient = new JobClient(TestSession.cluster.getConf());
		resetTheMaxQueueCapacity();
		ArrayList<String> concurrentUsers = new ArrayList<String>();
		CalculatedCapacityLimitsBO calculatedCapacityBO = selfCalculateCapacityLimits();
		printSelfCalculatedStats(calculatedCapacityBO);
		ArrayList<Future<Job>> futureCallableSleepJobs;
		for (JobQueueInfo jobQueueInfo : jobClient.getQueues()) {
			for (QueueCapacityDetail aQueueCapaityDetail : calculatedCapacityBO.queueCapacityDetails) {
				if (aQueueCapaityDetail.name
						.equals(jobQueueInfo.getQueueName())
						&& aQueueCapaityDetail.name.equals(queueUnderTest)) {
					concurrentUsers.clear();
					TestSession.logger
							.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Starting testCapSchedLimits for queue "
									+ jobQueueInfo.getQueueName()
									+ " ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
					RuntimeRESTStatsBO runtimeRESTStatsBO = startCollectingRestStats(2 * THOUSAND_MILLISECONDS);
					waitFor(10 * THOUSAND_MILLISECONDS);
					stopCollectingRuntimeStatsAcrossQueues(runtimeRESTStatsBO);
					LeafQueue leafQueueSnapshotWithHighestMemUtil = getLeafSnapshotWithHighestMemUtil(
							queueUnderTest, runtimeRESTStatsBO);
					double maxSchedulableApps = leafQueueSnapshotWithHighestMemUtil.maxActiveApplications;

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
					ExpectToBomb expectOverageJobToBomb = ExpectToBomb.NO;
					if (leafQueueSnapshotWithHighestMemUtil.maxApplications == leafQueueSnapshotWithHighestMemUtil.maxActiveApplications) {
						expectOverageJobToBomb = ExpectToBomb.YES;
					}
					Future<Job> overageJobHandle = submitSingleSleepJobAndGetHandle(
							jobQueueInfo.getQueueName(),
							HadooptestConstants.UserNames.HADOOPQA,
							expectOverageJobToBomb);
					TestSession.logger
							.info("Collect the stats just one more time!");

					runtimeRESTStatsBO = startCollectingRestStats(1 * THOUSAND_MILLISECONDS);
					waitFor(2 * THOUSAND_MILLISECONDS);
					stopCollectingRuntimeStatsAcrossQueues(runtimeRESTStatsBO);
					if (expectOverageJobToBomb == ExpectToBomb.NO) {
						assertThatOverageJobIsInPendingState(
								HadooptestConstants.UserNames.HADOOPQA,
								jobQueueInfo.getQueueName(), runtimeRESTStatsBO);
					}

					// In preparation for the next loop, kill the started jobs
					killStartedJobs(futureCallableSleepJobs);
					if (expectOverageJobToBomb == ExpectToBomb.NO) {
						ArrayList<Future<Job>> overageList = new ArrayList<Future<Job>>();
						overageList.add(overageJobHandle);
						killStartedJobs(overageList);
					}
				}
			}
		}

	}

	 @Test
	public void testCapSchedLimitsMaxSchedBySingleUser() throws Exception {
		String testCode = "t2CapSchedLimitsSingleUsr";

		String SINGLE_USER_NAME = "hadoop2";
		copyResMgrConfigAndRestartNodes(TestSession.conf
				.getProperty("WORKSPACE")
				+ "/resources/hadooptest/hadoop/regression/yarn/capacityScheduler/capacity-schedulerLimits.xml");
		/**
		 * Reset the max capacity as I think values exported, tend to persist across tests.
		 */
		JobClient jobClient = new JobClient(TestSession.cluster.getConf());
		resetTheMaxQueueCapacity();
		CalculatedCapacityLimitsBO calculatedCapacityBO = selfCalculateCapacityLimits();
		printSelfCalculatedStats(calculatedCapacityBO);
		ArrayList<Future<Job>> futureCallableSleepJobs;
		for (JobQueueInfo jobQueueInfo : jobClient.getQueues()) {
			for (QueueCapacityDetail aQueueCapaityDetail : calculatedCapacityBO.queueCapacityDetails) {
				if (aQueueCapaityDetail.name
						.equals(jobQueueInfo.getQueueName())
						&& jobQueueInfo.getQueueName().equals("a2")) {
					TestSession.logger
							.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Starting testCapSchedLimitsMaxSchedBySingleUser for queue "
									+ jobQueueInfo.getQueueName()
									+ " ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
					RuntimeRESTStatsBO runtimeRESTStatsBO = startCollectingRestStats(2 * THOUSAND_MILLISECONDS);
					waitFor(10 * THOUSAND_MILLISECONDS);
					stopCollectingRuntimeStatsAcrossQueues(runtimeRESTStatsBO);
					LeafQueue leafQueueSnapshotWithHighestMemUtil = getLeafSnapshotWithHighestMemUtil(
							jobQueueInfo.getQueueName(), runtimeRESTStatsBO);

					double maxSchedulableAppsPerUser = leafQueueSnapshotWithHighestMemUtil.maxActiveApplicationsPerUser;
					double maxSchedulableApps = leafQueueSnapshotWithHighestMemUtil.maxActiveApplications;
					/**
					 * Here was the scenario. As an example:For the search queue
					 * the values looked like this:
					 * <p>
					 * Max Schedulable Applications: 4
					 * <p>
					 * MaxSchedulable Applications Per User: 13
					 * <p>
					 * This does not mean that a single user would be able to
					 * submit 13 applications in parallel. When I submit 13 apps
					 * in parallel, even for a single user (as was with multiple
					 * users - each submitting 1 job) just 4 jobs ran in
					 * parallel, while the rest were queued. Tom says Max
					 * Schedulable Applications Per User: 13 is just an
					 * artifact. One has to take a min of the two to determine
					 * the concurrent limit.
					 */
					double minOfTheTwo = (maxSchedulableApps < maxSchedulableAppsPerUser) ? maxSchedulableApps
							: maxSchedulableAppsPerUser;
					int numSleepJobsToLaunch = (int) minOfTheTwo;
					TestSession.logger.info("Num sleep jobs to launch:"
							+ numSleepJobsToLaunch);
					ArrayList<SleepJobParams> sleepJobParamsList = new ArrayList<SleepJobParams>();
					for (int jobCount = 1; jobCount <= numSleepJobsToLaunch; jobCount++) {
						SleepJobParams sleepJobParams = new SleepJobParams(
								calculatedCapacityBO,
								getDefaultSleepJobProps(jobQueueInfo
										.getQueueName()),
								jobQueueInfo.getQueueName(), SINGLE_USER_NAME,
								1 * QUEUE_CAPACITY,
								600 * THOUSAND_MILLISECONDS, testCode + "-job-"
										+ jobCount + "-launched-by-user-"
										+ SINGLE_USER_NAME + "-on-queue-"
										+ jobQueueInfo.getQueueName());
						sleepJobParamsList.add(sleepJobParams);

					}

					futureCallableSleepJobs = submitSleepJobsToAThreadPoolAndRunThemInParallel(
							sleepJobParamsList, 0);

					// Wait until all jobs have reached RUNNING state
					BarrierUntilAllThreadsRunning barrierUntilAllThreadsRunning = new BarrierUntilAllThreadsRunning(
							futureCallableSleepJobs, SLEEP_JOB_DURATION_IN_SECS
									* numSleepJobsToLaunch);
					TestSession.logger
							.info("barrier met....  all jobs've reached runnable");

					// With all jobs running, any additional jobs submitted
					// should wait
					Future<Job> overageJobHandle = submitSingleSleepJobAndGetHandle(
							jobQueueInfo.getQueueName(), SINGLE_USER_NAME,
							ExpectToBomb.NO);

					// Let the new job to settle down
					waitFor(5 * THOUSAND_MILLISECONDS);

					runtimeRESTStatsBO = startCollectingRestStats(1 * THOUSAND_MILLISECONDS);
					waitFor(2 * THOUSAND_MILLISECONDS);
					stopCollectingRuntimeStatsAcrossQueues(runtimeRESTStatsBO);

					printValuesReceivedOverRest(runtimeRESTStatsBO);

					assertThatOverageJobIsInPendingState(SINGLE_USER_NAME,
							jobQueueInfo.getQueueName(), runtimeRESTStatsBO);

					// In preparation for the next loop, kill the started jobs
					killStartedJobs(futureCallableSleepJobs);
					ArrayList<Future<Job>> overageList = new ArrayList<Future<Job>>();
					overageList.add(overageJobHandle);
					killStartedJobs(overageList);

				}
			}
		}

	}

}
