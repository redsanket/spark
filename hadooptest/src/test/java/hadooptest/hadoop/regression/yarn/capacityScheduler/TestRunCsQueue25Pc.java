package hadooptest.hadoop.regression.yarn.capacityScheduler;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;

import java.util.ArrayList;
import java.util.concurrent.Future;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.junit.Test;

public class TestRunCsQueue25Pc extends CapacitySchedulerBaseClass {

	/**
	 * "1. Get number of nodes on the cluster, and calculate cluster capacity and queue capacity"
	 * "2. Submit a normal job which has a number of map and reduce tasks equal 2 times queue capacity"
	 * "3. Verify the number of tasks do not exceed queue capacity limit * user limit factor for a single user"
	 * "4. Verify the normal job can run sucessfully";
	 * 
	 * @throws Exception
	 */
	@Test
	public void testCsSingleQueue25Percent1() throws Exception {
		copyResMgrConfigAndRestartNodes(TestSession.conf.getProperty("WORKSPACE")
				+ "/resources/hadooptest/hadoop/regression/yarn/capacityScheduler/capacity-scheduler25.xml");

		
		CalculatedCapacityLimitsBO calculatedCapacityBO = calculateCapacityLimits();

		for (QueueCapacityDetail aCalculatedQueueDetail : calculatedCapacityBO.queueCapacityDetails) {
			TestSession.logger.info("Q name****:" + aCalculatedQueueDetail.name +" *****");
			TestSession.logger.info("Q max capacity:"
					+ aCalculatedQueueDetail.maxCapacityInTermsOfTotalClusterMemory + " MB");
			TestSession.logger.info("Q capacity in terms of cluster memory:" + aCalculatedQueueDetail.capacityInTermsOfTotalClusterMemory + " MB");
			TestSession.logger.info("Q max apps:"					+ aCalculatedQueueDetail.maxApplications);
			TestSession.logger.info("Q max apps per user:" 			+ aCalculatedQueueDetail.maxApplicationsPerUser);			
			TestSession.logger.info("Q max active apps per user:"	+ aCalculatedQueueDetail.maxActiveApplicationsPerUser);
			TestSession.logger.info("Q max active apps:"			+ aCalculatedQueueDetail.maxActiveApplications);
			TestSession.logger.info("Q min user limit percent:"		+ aCalculatedQueueDetail.minimumUserLimitPercent);
			TestSession.logger.info("Q user limit factor:"			+ aCalculatedQueueDetail.userLimitFactor);

			TestSession.logger.info("----------------------------------------------");

		}
		int numSleepJobsToLaunch = 4;
		ArrayList<SleepJobParams> sleepJobParamsList = new ArrayList<SleepJobParams>();
		for (int jobCount = 1; jobCount < numSleepJobsToLaunch; jobCount++) {
			SleepJobParams sleepJobParams = new SleepJobParams(calculatedCapacityBO,
					getDefaultSleepJobProps(), "default", "hadoop" + jobCount,
					0, 1, 20000);
			sleepJobParamsList.add(sleepJobParams);

		}
		ArrayList<Future<Job>> futureCallableSleepJobs = expandJobsAndSubmitThemForExecution(sleepJobParamsList);
		TestSession.logger.info("================================== Before Barrier thread ==================================" );
		BarrierUntilAllThreadsRunning barrierUntilAllThreadsRunning = new BarrierUntilAllThreadsRunning(
				futureCallableSleepJobs, SLEEP_JOB_DURATION_IN_SECS
						* numSleepJobsToLaunch);
		TestSession.logger
				.info("barrier met.............  all threads 've reached runnable");

		RuntimeStatsBO runtimeStatsBO = collateRuntimeStatsForJobs(futureCallableSleepJobs);
		TestSession.logger.info(runtimeStatsBO);

		for (Future<Job> aTetherToACallableSleepJob : futureCallableSleepJobs) {

			TestSession.logger.info("finishtime:"
					+ aTetherToACallableSleepJob.get().getFinishTime());
			TestSession.logger.info("jobfile:"
					+ aTetherToACallableSleepJob.get().getJobFile());
			TestSession.logger.info("jobd:"
					+ aTetherToACallableSleepJob.get().getJobID());
			TestSession.logger.info("jobName:"
					+ aTetherToACallableSleepJob.get().getJobName());
			TestSession.logger.info("jobState:"
					+ aTetherToACallableSleepJob.get().getJobState());
			TestSession.logger.info("getMaxMapAttempts:"
					+ aTetherToACallableSleepJob.get().getMaxMapAttempts());
			TestSession.logger.info("getMaxReduceAttempts:"
					+ aTetherToACallableSleepJob.get().getMaxReduceAttempts());
			TestSession.logger.info("getNumReduceTasks:"
					+ aTetherToACallableSleepJob.get().getNumReduceTasks());
			TestSession.logger.info("getPriority:"
					+ aTetherToACallableSleepJob.get().getPriority());
			TestSession.logger.info("getUser:"
					+ aTetherToACallableSleepJob.get().getUser());

			TestSession.logger.info("The queue:"
					+ aTetherToACallableSleepJob.get().getStatus().getQueue());

		}


	}

}
