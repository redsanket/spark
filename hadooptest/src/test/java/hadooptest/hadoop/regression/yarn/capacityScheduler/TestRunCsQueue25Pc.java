package hadooptest.hadoop.regression.yarn.capacityScheduler;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.hadoop.regression.yarn.capacityScheduler.CapacitySchedulerBaseClass.CallableSleepJob;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobQueueInfo;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.junit.Assert;
import org.junit.Test;

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
				.info("{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}");
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
							.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Starting testMaxSchedulableApplications for queue "
									+ jobQueueInfo.getQueueName()
									+ " ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
					double maxSchedulableApps = aQueueCapaityDetail.maxActiveApplications;
					int numSleepJobsToLaunch = (int) maxSchedulableApps;
					ArrayList<SleepJobParams> sleepJobParamsList = new ArrayList<SleepJobParams>();
					for (int jobCount = 1; jobCount <= numSleepJobsToLaunch; jobCount++) {
						SleepJobParams sleepJobParams = new SleepJobParams(
								calculatedCapacityBO,
								getDefaultSleepJobProps(jobQueueInfo.getQueueName()), jobQueueInfo.getQueueName(), "hadoop"
										+ jobCount,
								1 * QUEUE_CAPACITY,
								10 * THOUSAND_MILLISECONDS);
						sleepJobParamsList.add(sleepJobParams);

					}

					futureCallableSleepJobs = submitJobsToAThreadPoolAndRunThemInParallel(sleepJobParamsList);

					// Wait until all jobs have reached RUNNING state
					BarrierUntilAllThreadsRunning barrierUntilAllThreadsRunning = new BarrierUntilAllThreadsRunning(
							futureCallableSleepJobs, SLEEP_JOB_DURATION_IN_SECS
									* numSleepJobsToLaunch);
					TestSession.logger
							.info("barrier met....  all jobs've reached runnable");

					// With all jobs running, any additional jobs submitted
					// should wait
					Future<Job> overageJobHandle = submitSingleSleepJobAndGetHandle(jobQueueInfo.getQueueName());
					Thread.sleep(10000);
					ArrayList<QueueCapacityDetail> retrievedCapLimits = getCapacityLimitsViaRestCallIntoRM();
					printValuesReceivedOverRest(retrievedCapLimits);
					boolean confirmedQueueSubmissions = false;
					for (QueueCapacityDetail aQueueCapacityDetailOverREST : retrievedCapLimits) {
						if (aQueueCapacityDetailOverREST.name.equals(jobQueueInfo.getQueueName())) {
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

}
