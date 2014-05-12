package hadooptest.hadoop.regression.yarn.capacityScheduler;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.hadoop.regression.yarn.capacityScheduler.CapacitySchedulerBaseClass.BarrierUntilAllThreadsRunning;
import hadooptest.hadoop.regression.yarn.capacityScheduler.CapacitySchedulerBaseClass.ExpectToBomb;
import hadooptest.hadoop.regression.yarn.capacityScheduler.CapacitySchedulerBaseClass.SleepJobParams;
import hadooptest.hadoop.regression.yarn.capacityScheduler.SchedulerRESTStatsSnapshot.LeafQueue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobQueueInfo;
import org.apache.hadoop.mapreduce.Job;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@Category(SerialTests.class)
public class TestDominantResourceCalculator extends CapacitySchedulerBaseClass {
	private static int THOUSAND_MILLISECONDS = 1000;
	private static int QUEUE_CAPACITY = 1;

	@Test
	public void testDominantResourceCalculator() throws Exception {
		String testCode="DomResCalc";
		setDominantResourceParametersEverywhere(TestSession.conf.getProperty("WORKSPACE")
				+ "/htf-common/resources/hadooptest/hadoop/regression/yarn/capacityScheduler/capacity-scheduler-DominantResourceCalculator.xml");
		JobClient jobClient = new JobClient(TestSession.cluster.getConf());
		
//		CalculatedCapacityLimitsBO calculatedCapacityBO = selfCalculateCapacityLimits();
//		printSelfCalculatedStats(calculatedCapacityBO);
//		ArrayList<Future<Job>> futureCallableSleepJobs;
//		for (JobQueueInfo jobQueueInfo : jobClient.getQueues()) {
//			for (QueueCapacityDetail aQueueCapaityDetail : calculatedCapacityBO.queueCapacityDetails) {
//				if (aQueueCapaityDetail.name
//						.equals(jobQueueInfo.getQueueName()) && jobQueueInfo.getQueueName().equalsIgnoreCase("default")) {
//					TestSession.logger
//							.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Starting testDominantResourceCalculator for queue "
//									+ jobQueueInfo.getQueueName()
//									+ " ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
//					double maxSchedulableApps = aQueueCapaityDetail.maxActiveApplications;
//					int numSleepJobsToLaunch = (int) maxSchedulableApps;
//					ArrayList<SleepJobParams> sleepJobParamsList = new ArrayList<SleepJobParams>();
//					for (int jobCount = 1; jobCount <= numSleepJobsToLaunch; jobCount++) {
//						SleepJobParams sleepJobParams = new SleepJobParams(
//								calculatedCapacityBO,
//								getDefaultSleepJobProps(jobQueueInfo
//										.getQueueName()),
//								jobQueueInfo.getQueueName(), "hadoop"
//										+ jobCount, 1 * QUEUE_CAPACITY,
//								600 * THOUSAND_MILLISECONDS, testCode + "-job-"
//										+ jobCount + "-launched-by-user-"
//										+ "hadoop" + jobCount + "-on-queue-"
//										+ jobQueueInfo.getQueueName());
//						sleepJobParamsList.add(sleepJobParams);
//
//					}
//
//					futureCallableSleepJobs = submitJobsToAThreadPoolAndRunThemInParallel(
//							sleepJobParamsList, 1 * THOUSAND_MILLISECONDS);
//
//					// Wait until all jobs have reached RUNNING state
//					BarrierUntilAllThreadsRunning barrierUntilAllThreadsRunning = new BarrierUntilAllThreadsRunning(
//							futureCallableSleepJobs, SLEEP_JOB_DURATION_IN_SECS
//									* numSleepJobsToLaunch);
//					TestSession.logger
//							.info("Either barrier met....  or waited long enough...so continue");
//
//					RuntimeRESTStatsBO runtimeRESTStatsBO = startCollectingRestStats(1 * THOUSAND_MILLISECONDS);
//					printValuesReceivedOverRest(runtimeRESTStatsBO);
//
//				}
//			}
//		}

	}
//	@After
//	public void restoreTheConfigFile() throws Exception {
//		/**
//		 * Do not restore the config file
//		 * Hence overriding.
//		 */
//	}
}
