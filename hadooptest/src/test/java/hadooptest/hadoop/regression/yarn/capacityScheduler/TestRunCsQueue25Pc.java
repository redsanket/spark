package hadooptest.hadoop.regression.yarn.capacityScheduler;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;

import java.util.ArrayList;
import java.util.concurrent.Future;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobQueueInfo;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
@Category(SerialTests.class)
public class TestRunCsQueue25Pc extends CapacitySchedulerBaseClass {
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
	public void testMaxSchedulableApplicationsMultiUser() throws Exception {
		String testCode = "t1";
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
						.equals(jobQueueInfo.getQueueName())) {
					TestSession.logger
							.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Starting testMaxSchedulableApplicationsMultiUser for queue "
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
								10 * THOUSAND_MILLISECONDS, testCode + "-job-" + jobCount
										+ "-launched-by-user-" + "hadoop"
										+ jobCount + "-on-queue-"
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

					// With all jobs running, any additional jobs submitted
					// should wait
					Future<Job> overageJobHandle = submitSingleSleepJobAndGetHandle(
							jobQueueInfo.getQueueName(),
							HadooptestConstants.UserNames.HADOOPQA, false);
					Thread.sleep(10000);
					ArrayList<QueueCapacityDetail> retrievedCapLimits = getCapacityLimitsViaRestCallIntoRM();
					printValuesReceivedOverRest(retrievedCapLimits);
					boolean confirmedQueueSubmissions = false;
					for (QueueCapacityDetail aQueueCapacityDetailOverREST : retrievedCapLimits) {
						if (aQueueCapacityDetailOverREST.name
								.equals(jobQueueInfo.getQueueName())) {
							Assert.assertTrue(
									"Could not find overage job in pending state jobId:"
											+ overageJobHandle.get().getJobID()
											+ " is in state:"
											+ overageJobHandle.get()
													.getJobState(),
									aQueueCapacityDetailOverREST.numPendingApplications == 1);
							confirmedQueueSubmissions = true;
						}
					}
					Assert.assertTrue(confirmedQueueSubmissions);
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
	public void testMaxSchedulableApplicationsSingleUser() throws Exception {
		String testCode = "t2";
		String SINGLE_USER_NAME = "hadoop2";
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
						.equals(jobQueueInfo.getQueueName())) {
					TestSession.logger
							.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Starting testMaxSchedulableApplicationsSingleUser for queue "
									+ jobQueueInfo.getQueueName()
									+ " ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
					double maxSchedulableAppsPerUser = aQueueCapaityDetail.maxActiveApplicationsPerUser;
					double maxSchedulableApps = aQueueCapaityDetail.maxActiveApplications;
					/**
					 * Here was the scenario. For the search queue the values
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
								1 * QUEUE_CAPACITY, 10 * THOUSAND_MILLISECONDS,
								testCode + "-job-" + jobCount + "-launched-by-user-"
										+ SINGLE_USER_NAME + "-on-queue-"
										+ jobQueueInfo.getQueueName());
						sleepJobParamsList.add(sleepJobParams);

					}

					futureCallableSleepJobs = submitJobsToAThreadPoolAndRunThemInParallel(
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
							false);
					Thread.sleep(3000);
					ArrayList<QueueCapacityDetail> retrievedCapLimits = getCapacityLimitsViaRestCallIntoRM();
					printValuesReceivedOverRest(retrievedCapLimits);
					boolean confirmedQueueSubmissions = false;
					for (QueueCapacityDetail aQueueCapacityDetailOverREST : retrievedCapLimits) {
						if (aQueueCapacityDetailOverREST.name
								.equals(jobQueueInfo.getQueueName())) {
							Assert.assertTrue(
									"Could not find overage job in pending state jobId:"
											+ overageJobHandle.get().getJobID()
											+ " is in state:"
											+ overageJobHandle.get()
													.getJobState(),
									aQueueCapacityDetailOverREST.numPendingApplications == 1);
							confirmedQueueSubmissions = true;
						}
					}
					Assert.assertTrue(confirmedQueueSubmissions);
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
	public void testMaxApplicationsLimitViaSingleUser() throws Exception {
		String testCode = "t3";
		String SINGLE_USER_NAME = HadooptestConstants.UserNames.HADOOPQA;
		copyResMgrConfigAndRestartNodes(TestSession.conf
				.getProperty("WORKSPACE")
				+ "/resources/hadooptest/hadoop/regression/yarn/capacityScheduler/capacity-scheduler25.xml");

		CalculatedCapacityLimitsBO calculatedCapacityBO = selfCalculateCapacityLimits();
		printSelfCalculatedStats(calculatedCapacityBO);
		ArrayList<Future<Job>> futureCallableSleepJobs = new ArrayList<Future<Job>>();
		JobClient jobClient = new JobClient(TestSession.cluster.getConf());
		for (JobQueueInfo jobQueueInfo : jobClient.getQueues()) {
			for (QueueCapacityDetail aQueueCapaityDetail : calculatedCapacityBO.queueCapacityDetails) {
				// Just limit the test run to to "search" queue.
				if (aQueueCapaityDetail.name
						.equals(jobQueueInfo.getQueueName())
						&& (jobQueueInfo.getQueueName().equals("search"))) {
					TestSession.logger
							.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Starting testMaxApplicationsLimitViaSingleUser for queue "
									+ jobQueueInfo.getQueueName()
									+ " ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
					double maxApplications = aQueueCapaityDetail.maxApplications;
					double maxAppsPerUser = aQueueCapaityDetail.maxApplicationsPerUser;
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
									500 * THOUSAND_MILLISECONDS, testCode + "-job-"
											+ jobCount + "-launched-by-user-"
											+ SINGLE_USER_NAME + "-on-queue-"
											+ jobQueueInfo.getQueueName());
							sleepJobParamsList.add(sleepJobParams);

						}
						futureCallableSleepJobs = submitJobsToAThreadPoolAndRunThemInParallel(
								sleepJobParamsList, 0);

						// With all jobs submitted, any additional jobs
						// submitted
						// should get rejected. Sleep for a while, so that this
						// job
						// that is expected to fail does not mix with the other
						// jobs.
						Thread.sleep(30000);
						Future<Job> overageJobHandle = submitSingleSleepJobAndGetHandle(
								jobQueueInfo.getQueueName(), SINGLE_USER_NAME,
								true);
						ArrayList<QueueCapacityDetail> retrievedCapLimits = getCapacityLimitsViaRestCallIntoRM();
						printValuesReceivedOverRest(retrievedCapLimits);
						Thread.sleep(20000);
						// In preparation for the next loop, kill the started
						// jobs
						killStartedJobs(futureCallableSleepJobs);

					} else {
						Assert.fail("Test not setup right");
					}
				}
			}
		}

	}

	@Test
	public void testMaxApplicationsLimitViaMultipleUsers() throws Exception {
		String testCode ="t4";
		String SINGLE_USER_NAME = HadooptestConstants.UserNames.HADOOPQA;
		copyResMgrConfigAndRestartNodes(TestSession.conf
				.getProperty("WORKSPACE")
				+ "/resources/hadooptest/hadoop/regression/yarn/capacityScheduler/capacity-scheduler25.xml");

		CalculatedCapacityLimitsBO calculatedCapacityBO = selfCalculateCapacityLimits();
		printSelfCalculatedStats(calculatedCapacityBO);
		ArrayList<Future<Job>> futureCallableSleepJobs = new ArrayList<Future<Job>>();
		JobClient jobClient = new JobClient(TestSession.cluster.getConf());
		for (JobQueueInfo jobQueueInfo : jobClient.getQueues()) {
			for (QueueCapacityDetail aQueueCapaityDetail : calculatedCapacityBO.queueCapacityDetails) {
				// Just limit the test run to to "search" queue.
				if (aQueueCapaityDetail.name
						.equals(jobQueueInfo.getQueueName())
						&& jobQueueInfo.getQueueName().equals("grideng")) {
					TestSession.logger
							.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Starting testMaxApplicationsLimitViaMultipleUsers for queue "
									+ jobQueueInfo.getQueueName()
									+ " ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
					double maxApplications = aQueueCapaityDetail.maxApplications;
					double maxAppsPerUser = aQueueCapaityDetail.maxApplicationsPerUser;
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
									500 * THOUSAND_MILLISECONDS, testCode + "-job-"
											+ jobCount + "-launched-by-user-"
											+ SINGLE_USER_NAME + "-on-queue-"
											+ jobQueueInfo.getQueueName());
							sleepJobParamsList.add(sleepJobParams);

							futureCallableSleepJobs = submitJobsToAThreadPoolAndRunThemInParallel(
									sleepJobParamsList, 0);

						}

						// With all jobs submitted, any additional jobs
						// submitted
						// should get rejected. Sleep for a while, so that this
						// job
						// that is expected to fail does not mix with the other
						// jobs.
						Thread.sleep(30000);
						Future<Job> overageJobHandle = submitSingleSleepJobAndGetHandle(
								jobQueueInfo.getQueueName(), SINGLE_USER_NAME,
								true);
						ArrayList<QueueCapacityDetail> retrievedCapLimits = getCapacityLimitsViaRestCallIntoRM();
						printValuesReceivedOverRest(retrievedCapLimits);
						Thread.sleep(20000);
						// In preparation for the next loop, kill the started
						// jobs
						killStartedJobs(futureCallableSleepJobs);

					} else {
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
										500 * THOUSAND_MILLISECONDS, testCode + "-job-"
												+ jobCount
												+ "-launched-by-user-"
												+ "hadoop" + userId
												+ "-on-queue-"
												+ jobQueueInfo.getQueueName());
								sleepJobParamsList.add(sleepJobParams);
								appsLaunchedSoFar++;
								if (appsLaunchedSoFar >= numSleepJobsToLaunch) {
									limitReached = true;
									break;
								}

							}
							futureCallableSleepJobs
									.addAll(submitJobsToAThreadPoolAndRunThemInParallel(
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
						Thread.sleep(30000);
						Future<Job> overageJobHandle = submitSingleSleepJobAndGetHandle(
								jobQueueInfo.getQueueName(),
								HadooptestConstants.UserNames.HADOOPQA, true);
						ArrayList<QueueCapacityDetail> retrievedCapLimits = getCapacityLimitsViaRestCallIntoRM();
						printValuesReceivedOverRest(retrievedCapLimits);
						Thread.sleep(20000);
						// In preparation for the next loop, kill the
						// started jobs
						killStartedJobs(futureCallableSleepJobs);

					}

				}
			}
		}

	}

}