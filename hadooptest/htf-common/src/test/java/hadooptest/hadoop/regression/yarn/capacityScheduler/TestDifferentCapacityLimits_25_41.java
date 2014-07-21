package hadooptest.hadoop.regression.yarn.capacityScheduler;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Future;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobQueueInfo;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
@Category(SerialTests.class)
public class TestDifferentCapacityLimits_25_41 extends
		CapacitySchedulerBaseClass {
	private static int THOUSAND_MILLISECONDS = 1000;
	private static int QUEUE_CAPACITY = 1;
	private String capSchedConfFile;

	@Parameters
	public static Collection<Object[]> data() {
		TestSession.start();
		return Arrays
				.asList(new Object[][] {
//						{ TestSession.conf.getProperty("WORKSPACE")
//								+ "/htf-common/resources/hadooptest/hadoop/regression/yarn/capacityScheduler/capacity-scheduler25.xml" },
						{ TestSession.conf.getProperty("WORKSPACE")
								+ "/htf-common/resources/hadooptest/hadoop/regression/yarn/capacityScheduler/capacity-scheduler41.xml" }

				});
	}

	@BeforeClass
	public static void testSessionStart() throws Exception {
		TestSession.start();
	}

	public TestDifferentCapacityLimits_25_41(String capSchedConfFile) {
		super();
		this.capSchedConfFile = capSchedConfFile;
	}
	

	/**
	 * For each queue, get the maximum schedulable jobs limit and submit jobs to
	 * test that liit. Any jobs beyond that limit should get queued
	 * 
	 * @throws Exception
	 */
	@Ignore
	@Test
	public void testMaxConcurrentlySchedulableApplicationsByMultipleUsers()
			throws Exception {

		String testCode = "t1pc";
		testCode += capSchedConfFile.contains("25") ? "25" : "41";
		resetTheMaxQueueCapacity();
		copyResMgrConfigAndRestartNodes(capSchedConfFile);

		JobClient jobClient = new JobClient(TestSession.cluster.getConf());
		
		CalculatedCapacityLimitsBO calculatedCapacityBO = selfCalculateCapacityLimits();
		printSelfCalculatedStats(calculatedCapacityBO);
		ArrayList<Future<Job>> futureCallableSleepJobs;
		for (JobQueueInfo jobQueueInfo : jobClient.getQueues()) {
			for (QueueCapacityDetail aQueueCapaityDetail : calculatedCapacityBO.queueCapacityDetails) {
				if (aQueueCapaityDetail.name
						.equals(jobQueueInfo.getQueueName())) {
					TestSession.logger
							.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Starting testMaxConcurrentlySchedulableApplicationsByMultipleUsers for queue "
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
										+ jobCount, 1 * QUEUE_CAPACITY,
								600 * THOUSAND_MILLISECONDS, testCode + "-job-"
										+ jobCount + "-launched-by-user-"
										+ "hadoop" + jobCount + "-on-queue-"
										+ jobQueueInfo.getQueueName());
						sleepJobParamsList.add(sleepJobParams);

					}

					futureCallableSleepJobs = submitSleepJobsToAThreadPoolAndRunThemInParallel(
							sleepJobParamsList, 1 * THOUSAND_MILLISECONDS);

					// Wait until all jobs have reached RUNNING state
					BarrierUntilAllThreadsRunning barrierUntilAllThreadsRunning = new BarrierUntilAllThreadsRunning(
							futureCallableSleepJobs, SLEEP_JOB_DURATION_IN_SECS
									* numSleepJobsToLaunch);
					TestSession.logger
							.info("Either barrier met....  or waited long enough...so continue");

					// With all jobs running, any additional jobs submitted
					// should wait
					Future<Job> overageJobHandle = submitSingleSleepJobAndGetHandle(
							jobQueueInfo.getQueueName(),
							HadooptestConstants.UserNames.HADOOPQA,
							ExpectToBomb.NO);

					waitFor(10 * THOUSAND_MILLISECONDS);
					RuntimeRESTStatsBO runtimeRESTStatsBO = startCollectingRestStats(1 * THOUSAND_MILLISECONDS);
					waitFor(60 * THOUSAND_MILLISECONDS);
					stopCollectingRuntimeStatsAcrossQueues(runtimeRESTStatsBO);

					printValuesReceivedOverRest(runtimeRESTStatsBO);

					assertThatOverageJobIsInPendingState(
							HadooptestConstants.UserNames.HADOOPQA,
							jobQueueInfo.getQueueName(), runtimeRESTStatsBO);
					
					assertMinimumUserLimitPercent(testCode, aQueueCapaityDetail, jobQueueInfo.getQueueName(), runtimeRESTStatsBO);

					// In preparation for the next loop, kill the started jobs
					killStartedJobs(futureCallableSleepJobs);
					ArrayList<Future<Job>> overageList = new ArrayList<Future<Job>>();
					overageList.add(overageJobHandle);
					killStartedJobs(overageList);

				}
			}
		}

	}

	 @Test
	public void testMaxConcurrentlySchedulableApplicationsByASingleUser()
			throws Exception {
		String testCode = "t2pc";
		testCode += capSchedConfFile.contains("25") ? "25" : "41";

		String SINGLE_USER_NAME = "hadoop2";
		
		copyResMgrConfigAndRestartNodes(capSchedConfFile);
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
						.equals(jobQueueInfo.getQueueName())) {
					TestSession.logger
							.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Starting testMaxConcurrentlySchedulableApplicationsByASingleUser for queue "
									+ jobQueueInfo.getQueueName()
									+ " ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

					double maxSchedulableAppsPerUser = aQueueCapaityDetail.maxActiveApplicationsPerUser;
					double maxSchedulableApps = aQueueCapaityDetail.maxActiveApplications;
					/**
					 * Here was the scenario. As an example:For the search queue the values
					 * looked like this:
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
					ArrayList<SleepJobParams> sleepJobParamsList = new ArrayList<SleepJobParams>();
					for (int jobCount = 1; jobCount <= numSleepJobsToLaunch; jobCount++) {
						SleepJobParams sleepJobParams = new SleepJobParams(
								calculatedCapacityBO,
								getDefaultSleepJobProps(jobQueueInfo
										.getQueueName()),
								jobQueueInfo.getQueueName(), SINGLE_USER_NAME,
								1 * QUEUE_CAPACITY, 600 * THOUSAND_MILLISECONDS,
								testCode + "-job-" + jobCount
										+ "-launched-by-user-"
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

					RuntimeRESTStatsBO runtimeRESTStatsBO = startCollectingRestStats(2 * THOUSAND_MILLISECONDS);
					waitFor(60 * THOUSAND_MILLISECONDS);
					stopCollectingRuntimeStatsAcrossQueues(runtimeRESTStatsBO);

					printValuesReceivedOverRest(runtimeRESTStatsBO);

					assertThatOverageJobIsInPendingState(
							SINGLE_USER_NAME,
							jobQueueInfo.getQueueName(), runtimeRESTStatsBO);
					
					assertMinimumUserLimitPercent(testCode, aQueueCapaityDetail, jobQueueInfo.getQueueName(), runtimeRESTStatsBO);
					
					// In preparation for the next loop, kill the started jobs
					killStartedJobs(futureCallableSleepJobs);
					ArrayList<Future<Job>> overageList = new ArrayList<Future<Job>>();
					overageList.add(overageJobHandle);
					killStartedJobs(overageList);

				}
			}
		}

	}

	 @Test
	public void testHittingMaxApplicationsLimitWithASingleUser()
			throws Exception {
		String testCode = "t3pc";
		testCode += capSchedConfFile.contains("25") ? "25" : "41";

		String fixedQueueForThisTest = "search";
		String SINGLE_USER_NAME = HadooptestConstants.UserNames.HADOOPQA;
		copyResMgrConfigAndRestartNodes(capSchedConfFile);
		/**
		 * Reset the max capacity as I think values exported, tend to persist across tests.
		 */
		JobClient jobClient = new JobClient(TestSession.cluster.getConf());
		resetTheMaxQueueCapacity();
		CalculatedCapacityLimitsBO calculatedCapacityBO = selfCalculateCapacityLimits();
		printSelfCalculatedStats(calculatedCapacityBO);
		ArrayList<Future<Job>> futureCallableSleepJobs = new ArrayList<Future<Job>>();
		for (JobQueueInfo jobQueueInfo : jobClient.getQueues()) {
			for (QueueCapacityDetail aQueueCapaityDetail : calculatedCapacityBO.queueCapacityDetails) {
				// Just limit the test run to to fixedQueueForThisTest queue.
				if (aQueueCapaityDetail.name
						.equals(jobQueueInfo.getQueueName())
						&& (jobQueueInfo.getQueueName()
								.equals(fixedQueueForThisTest))) {
					TestSession.logger
							.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Starting testHittingMaxApplicationsLimitWithASingleUser for queue "
									+ jobQueueInfo.getQueueName()
									+ " ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
					double maxApplications = aQueueCapaityDetail.maxApplications;
					double maxAppsPerUser = aQueueCapaityDetail.maxApplicationsPerUser;
					Assume.assumeTrue(maxApplications < maxAppsPerUser);

					ArrayList<SleepJobParams> sleepJobParamsList = new ArrayList<SleepJobParams>();
					if (maxApplications < maxAppsPerUser) {
						// This means a single user can hit the ceiling.
						// go for it Mr. 'hadoopqa'
						TestSession.logger
								.info("00000000000000 Reach max applications limit via single users");
						int numSleepJobsToLaunch = (int) maxApplications;
						for (int jobCount = 1; jobCount <= numSleepJobsToLaunch; jobCount++) {
							SleepJobParams sleepJobParams = new SleepJobParams(
									calculatedCapacityBO,
									getDefaultSleepJobProps(jobQueueInfo
											.getQueueName()),
									jobQueueInfo.getQueueName(),
									SINGLE_USER_NAME, 1 * QUEUE_CAPACITY,
									500 * THOUSAND_MILLISECONDS, testCode
											+ "-job-" + jobCount
											+ "-launched-by-user-"
											+ SINGLE_USER_NAME + "-on-queue-"
											+ jobQueueInfo.getQueueName());
							sleepJobParamsList.add(sleepJobParams);

						}
						futureCallableSleepJobs = submitSleepJobsToAThreadPoolAndRunThemInParallel(
								sleepJobParamsList, 0);

						/**
						 * With all jobs submitted, any additional jobs
						 * submitted should get rejected. Sleep for a while, so
						 * that this job that is expected to fail does not mix
						 * with the other jobs.
						 */
						Thread.sleep(30 * THOUSAND_MILLISECONDS);
						Future<Job> overageJobHandle = submitSingleSleepJobAndGetHandle(
								jobQueueInfo.getQueueName(), SINGLE_USER_NAME,
								ExpectToBomb.YES);
						Thread.sleep(20 * THOUSAND_MILLISECONDS);
						killStartedJobs(futureCallableSleepJobs);

					} else {

						Assert.fail("Test not setup right. You want to test that a single user can hit the 'Max Applications' limit, but"
								+ " since 'Max Applications Per User' < 'Max Applications', for the queue named  "
								+ fixedQueueForThisTest
								+ " this test cannot be run");
					}
				}
			}
		}

	}

	 @Test
	public void testHittingMaxApplicationsLimitWithMultipleUsers()
			throws Exception {
		String testCode = "t4pc";
		testCode += capSchedConfFile.contains("25") ? "25" : "41";

		String queueRelevantForThisTest = "grideng";
		copyResMgrConfigAndRestartNodes(capSchedConfFile);
		/**
		 * Reset the max capacity as I think values exported, tend to persist across tests.
		 */
		JobClient jobClient = new JobClient(TestSession.cluster.getConf());
		resetTheMaxQueueCapacity();
		CalculatedCapacityLimitsBO calculatedCapacityBO = selfCalculateCapacityLimits();
		printSelfCalculatedStats(calculatedCapacityBO);
		ArrayList<Future<Job>> futureCallableSleepJobs = new ArrayList<Future<Job>>();

		for (JobQueueInfo jobQueueInfo : jobClient.getQueues()) {
			for (QueueCapacityDetail aQueueCapaityDetail : calculatedCapacityBO.queueCapacityDetails) {
				// Just limit the test run to to "grideng" queue.
				if (aQueueCapaityDetail.name
						.equals(jobQueueInfo.getQueueName())
						&& jobQueueInfo.getQueueName().equals(
								queueRelevantForThisTest)) {
					TestSession.logger
							.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Starting testHittingMaxApplicationsLimitWithMultipleUsers for queue "
									+ jobQueueInfo.getQueueName()
									+ " ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
					double maxApplications = aQueueCapaityDetail.maxApplications;
					double maxAppsPerUser = aQueueCapaityDetail.maxApplicationsPerUser;
					ArrayList<SleepJobParams> sleepJobParamsList = new ArrayList<SleepJobParams>();
					TestSession.logger
							.info("00000000000000 Reach max applications limit via multiple users 00000000000");
					boolean limitReached = false;
					// Need multiple users to hit the limit
					int numSleepJobsToLaunch = (int) maxApplications;
					int appsLaunchedSoFar = 0;
					for (int userId = 1; userId <= 10; userId++) {
						sleepJobParamsList = new ArrayList<SleepJobParams>();
						for (int jobCount = 1; (jobCount <= maxAppsPerUser); jobCount++) {

							SleepJobParams sleepJobParams = new SleepJobParams(
									calculatedCapacityBO,
									getDefaultSleepJobProps(jobQueueInfo
											.getQueueName()),
									jobQueueInfo.getQueueName(), "hadoop"
											+ userId, 1 * QUEUE_CAPACITY,
									500 * THOUSAND_MILLISECONDS, testCode
											+ "-job-" + jobCount
											+ "-launched-by-user-" + "hadoop"
											+ userId + "-on-queue-"
											+ jobQueueInfo.getQueueName());
							sleepJobParamsList.add(sleepJobParams);
							appsLaunchedSoFar++;
							if (appsLaunchedSoFar >= numSleepJobsToLaunch) {
								limitReached = true;
								break;
							}

						}
						futureCallableSleepJobs
								.addAll(submitSleepJobsToAThreadPoolAndRunThemInParallel(
										sleepJobParamsList, 0));

						// Sleep for a while before launching the job for
						// another user
						Thread.sleep(20 * THOUSAND_MILLISECONDS);
						if (limitReached) {
							break;
						}
					}

					// With all jobs submitted, any additional jobs
					// submitted
					// should get rejected. Sleep for a while, so that
					// this job
					// that is expected to fail does not mix with the
					// other jobs.
					Thread.sleep(30 * THOUSAND_MILLISECONDS);
					Future<Job> overageJobHandle = submitSingleSleepJobAndGetHandle(
							jobQueueInfo.getQueueName(),
							HadooptestConstants.UserNames.HADOOPQA,
							ExpectToBomb.YES);
					Thread.sleep(20 * THOUSAND_MILLISECONDS);
					// In preparation for the next loop, kill the
					// started jobs
					killStartedJobs(futureCallableSleepJobs);

				}
			}
		}

	}

}