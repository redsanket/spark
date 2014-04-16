package hadooptest.hadoop.regression.yarn.capacityScheduler;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.hadoop.regression.yarn.capacityScheduler.CapacitySchedulerBaseClass.CallableSleepJob;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.Future;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.junit.Test;

public class TestRunCsQueue25Pc extends CapacitySchedulerBaseClass {

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

	void printValuesReceivedOverRest(ArrayList<QueueCapacityDetail> retrievedCapLimits){
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
	 * "1. Get number of nodes on the cluster, and calculate cluster capacity and queue capacity"
	 * "2. Submit a normal job which has a number of map and reduce tasks equal 2 times queue capacity"
	 * "3. Verify the number of tasks do not exceed queue capacity limit * user limit factor for a single user"
	 * "4. Verify the normal job can run sucessfully";
	 * 
	 * @throws Exception
	 */
	@Test
	public void testSpawnJobsMultiUser() throws Exception {
		copyResMgrConfigAndRestartNodes(TestSession.conf
				.getProperty("WORKSPACE")
				+ "/resources/hadooptest/hadoop/regression/yarn/capacityScheduler/capacity-scheduler25.xml");

		CalculatedCapacityLimitsBO calculatedCapacityBO = selfCalculateCapacityLimits();
		printSelfCalculatedStats(calculatedCapacityBO);

		int numSleepJobsToLaunch = 4;
		ArrayList<SleepJobParams> sleepJobParamsList = new ArrayList<SleepJobParams>();
		for (int jobCount = 1; jobCount < numSleepJobsToLaunch; jobCount++) {
			SleepJobParams sleepJobParams = new SleepJobParams(
					calculatedCapacityBO, getDefaultSleepJobProps(), "default",
					"hadoop" + jobCount, 0, 1, 20000);
			sleepJobParamsList.add(sleepJobParams);

		}
		ArrayList<Future<Job>> futureCallableSleepJobs = submitJobsToAThreadPoolAndRunThem(sleepJobParamsList);
		BarrierUntilAllThreadsRunning barrierUntilAllThreadsRunning = new BarrierUntilAllThreadsRunning(
				futureCallableSleepJobs, SLEEP_JOB_DURATION_IN_SECS
						* numSleepJobsToLaunch);
		TestSession.logger
				.info("barrier met....  all threads 've reached runnable");
		Future<Job> jobHandle = submitSingleSleepJobAndGetHandle();

		ArrayList<QueueCapacityDetail> retrievedCapLimits = getCapacityLimitsViaRestCallIntoRM();
		printValuesReceivedOverRest(retrievedCapLimits);

	}

}
