package hadooptest.hadoop.regression.yarn.capacityScheduler;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.config.hadoop.fullydistributed.FullyDistributedConfiguration;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Force;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Recursive;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.SkipTrash;
import hadooptest.hadoop.regression.yarn.capacityScheduler.SchedulerRESTStatsSnapshot.LeafQueue;
import hadooptest.hadoop.regression.yarn.capacityScheduler.SchedulerRESTStatsSnapshot.User;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobQueueInfo;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@Category(SerialTests.class)
public class TestReproduceHeadRoomJira extends CapacitySchedulerBaseClass {
	private static int THOUSAND_MILLISECONDS = 1000;
	private static int QUEUE_CAPACITY = 1;
	private String capSchedConfFile;

	@BeforeClass
	public static void testSessionStart() throws Exception {
		TestSession.start();
	}

	void copyJarToHdfs() throws Exception {
		DfsCliCommands dfsCommonCli = new DfsCliCommands();
		dfsCommonCli.rm(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA,
				HadooptestConstants.Schema.WEBHDFS,
				System.getProperty("CLUSTER_NAME"), Recursive.YES, Force.YES,
				SkipTrash.YES, "/tmp/htf-common-1.0-SNAPSHOT-tests.jar");

		dfsCommonCli
				.copyFromLocal(
						EMPTY_ENV_HASH_MAP,
						HadooptestConstants.UserNames.HDFSQA,
						HadooptestConstants.Schema.WEBHDFS,
						System.getProperty("CLUSTER_NAME"),
						TestSession.conf.getProperty("WORKSPACE")
								+ "/htf-common/target/htf-common-1.0-SNAPSHOT-tests.jar",
						"/tmp");

		dfsCommonCli.chmod(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA,
				HadooptestConstants.Schema.WEBHDFS,
				System.getProperty("CLUSTER_NAME"), "/tmp", "777", Recursive.NO);
		dfsCommonCli.chmod(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA,
				HadooptestConstants.Schema.WEBHDFS,
				System.getProperty("CLUSTER_NAME"),
				"/tmp/htf-common-1.0-SNAPSHOT-tests.jar", "777", Recursive.NO);

	}

	/**
	 * For each queue, get the maximum schedulable jobs limit and submit jobs to
	 * test that liit. Any jobs beyond that limit should get queued
	 * 
	 * @throws Exception
	 */
	 @Test
	public void testReproduceHeadroomJiraTwoUsersTwoQueues() throws Exception {

		String testCode = "tCustomShuffle2Usr2Q";
		copyResMgrConfigAndRestartNodes(TestSession.conf
				.getProperty("WORKSPACE")
				+ "/htf-common/resources/hadooptest/hadoop/regression/yarn/capacityScheduler/customShuffleTwoUsersTwoQueues.xml");

		JobClient jobClient = new JobClient(TestSession.cluster.getConf());
		copyJarToHdfs();
		CalculatedCapacityLimitsBO calculatedCapacityBO = selfCalculateCapacityLimits();
		printSelfCalculatedStats(calculatedCapacityBO);
		ArrayList<Future<Job>> futureCallableSleepJobs = new ArrayList<Future<Job>>();
		String userName = "hadoop";
		for (JobQueueInfo jobQueueInfo : jobClient.getQueues()) {
			for (QueueCapacityDetail aQueueCapaityDetail : calculatedCapacityBO.queueCapacityDetails) {
				if (aQueueCapaityDetail.name
						.equals(jobQueueInfo.getQueueName())) {
					TestSession.logger
							.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Starting testReproduceHeadroomJiraTwoUsersTwoQueues for queue "
									+ jobQueueInfo.getQueueName()
									+ " ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
					if (jobQueueInfo.getQueueName().equalsIgnoreCase("default")) {
						userName = "hadoop1";
					} else if (jobQueueInfo.getQueueName()
							.equalsIgnoreCase("a")) {
						userName = "hadoop2";
					}
					ArrayList<SleepJobParams> sleepJobParamsList = new ArrayList<SleepJobParams>();
					HashMap<String, String> sleepJobProps = getDefaultSleepJobProps(jobQueueInfo
							.getQueueName());

					sleepJobProps
							.put("mapreduce.job.reduce.shuffle.consumer.plugin.class",
									CustomShuffle.class.getCanonicalName());

					SleepJobParams sleepJobParams = new SleepJobParams(
							calculatedCapacityBO, sleepJobProps,
							jobQueueInfo.getQueueName(), userName, 1,
							60 * THOUSAND_MILLISECONDS, testCode + "-job-"
									+ "-launched-by-user-" + userName
									+ "-on-queue-"
									+ jobQueueInfo.getQueueName());
					sleepJobParamsList.add(sleepJobParams);

					futureCallableSleepJobs
							.addAll(submitSleepJobsToAThreadPoolAndRunThemInParallel(
									sleepJobParamsList,
									1 * THOUSAND_MILLISECONDS));

					// Wait until all jobs have reached RUNNING state
					BarrierUntilAllThreadsRunning barrierUntilAllThreadsRunning = new BarrierUntilAllThreadsRunning(
							futureCallableSleepJobs,
							SLEEP_JOB_DURATION_IN_SECS * 2);
					TestSession.logger
							.info("Either barrier met....  or waited long enough...so continue");

					RuntimeRESTStatsBO runtimeRESTStatsBO = startCollectingRestStats(1 * THOUSAND_MILLISECONDS);
					waitFor(5 * THOUSAND_MILLISECONDS);
					stopCollectingRuntimeStatsAcrossQueues(runtimeRESTStatsBO);

					printValuesReceivedOverRest(runtimeRESTStatsBO);

					for (JobStatus aJobStatus : jobClient.getAllJobs()) {
						TestSession.logger
								.info("###############################################################");
						TestSession.logger.info("Failure info:"
								+ aJobStatus.getFailureInfo());
						TestSession.logger.info("Finish time:"
								+ aJobStatus.getFinishTime());
						TestSession.logger.info("History File:"
								+ aJobStatus.getHistoryFile());
						TestSession.logger.info("Job ID:"
								+ aJobStatus.getJobID());
						TestSession.logger.info("Job Name:"
								+ aJobStatus.getJobName());
						TestSession.logger.info("Map progress:"
								+ aJobStatus.getMapProgress());
						TestSession.logger.info("Needed Mem:"
								+ aJobStatus.getNeededMem());
						TestSession.logger.info("Num reserved slot:"
								+ aJobStatus.getNumReservedSlots());
						TestSession.logger.info("Num used slots:"
								+ aJobStatus.getNumUsedSlots());
						TestSession.logger.info("Queue:"
								+ aJobStatus.getQueue());
						TestSession.logger.info("Reduce progress:"
								+ aJobStatus.getReduceProgress());
						TestSession.logger.info("Reserved mem:"
								+ aJobStatus.getReservedMem());
						TestSession.logger.info("Scheduling info:"
								+ aJobStatus.getSchedulingInfo());
						TestSession.logger.info("Setup progress"
								+ aJobStatus.getSetupProgress());
						TestSession.logger.info("Start time: "
								+ aJobStatus.getStartTime());
						TestSession.logger.info("State:"
								+ aJobStatus.getState());
						TestSession.logger.info("Used mem:"
								+ aJobStatus.getUsedMem());
						TestSession.logger.info("User name:"
								+ aJobStatus.getUsername());
					}
				}
			}
		}
		waitTillAllMapTasksComplete(futureCallableSleepJobs);

		HashMap<String, String> stateOfReduceTasks = getMapOfReduceTasksIdAndState(futureCallableSleepJobs);

		String attemptIdOfACompletedMapTaskThatWasKilled = "";
		String expectedAttemptIdOfARespawnedMapTask = "";
		do {
			Thread.sleep(1000);
			TestSession.logger
					.info("Waiting for a MAP task to complete....sleeping for 1 sec");
		} while ((attemptIdOfACompletedMapTaskThatWasKilled = killACompletedMapTaskAndGetCurrentAttemptID(futureCallableSleepJobs)) == null);
		TestSession.logger.info("Got back atempt Id:"
				+ attemptIdOfACompletedMapTaskThatWasKilled);
		TestSession.logger
				.info("************************************************************************************************************");
		TestSession.logger
				.info("********************************************* there, killed["
						+ attemptIdOfACompletedMapTaskThatWasKilled
						+ "] ************************************************");
		TestSession.logger
				.info("************************************************************************************************************");

		int attemptId = attemptIdOfACompletedMapTaskThatWasKilled
				.charAt(attemptIdOfACompletedMapTaskThatWasKilled.length() - 1) - '0';
		attemptId++;
		expectedAttemptIdOfARespawnedMapTask = attemptIdOfACompletedMapTaskThatWasKilled
				.substring(0, attemptIdOfACompletedMapTaskThatWasKilled
						.lastIndexOf('_') + 1)
				+ attemptId;
		assertThatTheKilledMapTaskWasRespawnedAndCompleted(
				futureCallableSleepJobs, expectedAttemptIdOfARespawnedMapTask);
		assertThatAReduceTaskWasKilledToMakeRoom(futureCallableSleepJobs,
				stateOfReduceTasks);

	}

	@Test
	public void testReproduceHeadroomJiraTwoUsersSingleQueue() throws Exception {

		String testCode = "tCustomShuffle2Usr1Q";

		copyResMgrConfigAndRestartNodes(TestSession.conf
				.getProperty("WORKSPACE")
				+ "/htf-common/resources/hadooptest/hadoop/regression/yarn/capacityScheduler/customShuffleTwoUsersSingleQueue.xml");

		JobClient jobClient = new JobClient(TestSession.cluster.getConf());
		copyJarToHdfs();
		CalculatedCapacityLimitsBO calculatedCapacityBO = selfCalculateCapacityLimits();
		printSelfCalculatedStats(calculatedCapacityBO);
		ArrayList<Future<Job>> futureCallableSleepJobs = new ArrayList<Future<Job>>();
		String[] userNames = new String[] { "hadoop1" };
		for (String userName : userNames) {
			for (JobQueueInfo jobQueueInfo : jobClient.getQueues()) {
				for (QueueCapacityDetail aQueueCapaityDetail : calculatedCapacityBO.queueCapacityDetails) {
					if (aQueueCapaityDetail.name.equals(jobQueueInfo
							.getQueueName())) {
						TestSession.logger
								.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Starting testReproduceHeadroomJiraTwoUsersSingleQueue for queue "
										+ jobQueueInfo.getQueueName()
										+ " ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
						ArrayList<SleepJobParams> sleepJobParamsList = new ArrayList<SleepJobParams>();
						HashMap<String, String> sleepJobProps = getDefaultSleepJobProps(jobQueueInfo
								.getQueueName());

						sleepJobProps
								.put("mapreduce.job.reduce.shuffle.consumer.plugin.class",
										CustomShuffle.class.getCanonicalName());

						SleepJobParams sleepJobParams = new SleepJobParams(
								calculatedCapacityBO, sleepJobProps,
								jobQueueInfo.getQueueName(), userName, 1,
								60 * THOUSAND_MILLISECONDS, testCode + "-job-"
										+ "-launched-by-user-" + userName
										+ "-on-queue-"
										+ jobQueueInfo.getQueueName());
						sleepJobParamsList.add(sleepJobParams);

						futureCallableSleepJobs
								.addAll(submitSleepJobsToAThreadPoolAndRunThemInParallel(
										sleepJobParamsList,
										1 * THOUSAND_MILLISECONDS));

					}
				}
			}
		}
		// Wait until all jobs have reached RUNNING state
		BarrierUntilAllThreadsRunning barrierUntilAllThreadsRunning = new BarrierUntilAllThreadsRunning(
				futureCallableSleepJobs, SLEEP_JOB_DURATION_IN_SECS * 2);
		TestSession.logger
				.info("Either barrier met....  or waited long enough...so continue");

		RuntimeRESTStatsBO runtimeRESTStatsBO = startCollectingRestStats(1 * THOUSAND_MILLISECONDS);
		waitFor(5 * THOUSAND_MILLISECONDS);
		stopCollectingRuntimeStatsAcrossQueues(runtimeRESTStatsBO);

		printValuesReceivedOverRest(runtimeRESTStatsBO);

		waitTillAllMapTasksComplete(futureCallableSleepJobs);

		//Start off the 2nd user
		userNames = new String[] { "hadoop2" };
		for (String userName : userNames) {
			for (JobQueueInfo jobQueueInfo : jobClient.getQueues()) {
				for (QueueCapacityDetail aQueueCapaityDetail : calculatedCapacityBO.queueCapacityDetails) {
					if (aQueueCapaityDetail.name.equals(jobQueueInfo
							.getQueueName())) {
						TestSession.logger
								.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Starting testReproduceHeadroomJiraTwoUsersSingleQueue for queue "
										+ jobQueueInfo.getQueueName()
										+ " ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
						ArrayList<SleepJobParams> sleepJobParamsList = new ArrayList<SleepJobParams>();
						HashMap<String, String> sleepJobProps = getDefaultSleepJobProps(jobQueueInfo
								.getQueueName());

						sleepJobProps
								.put("mapreduce.job.reduce.shuffle.consumer.plugin.class",
										CustomShuffle.class.getCanonicalName());

						SleepJobParams sleepJobParams = new SleepJobParams(
								calculatedCapacityBO, sleepJobProps,
								jobQueueInfo.getQueueName(), userName, 1,
								60 * THOUSAND_MILLISECONDS, testCode + "-job-"
										+ "-launched-by-user-" + userName
										+ "-on-queue-"
										+ jobQueueInfo.getQueueName());
						sleepJobParamsList.add(sleepJobParams);

						futureCallableSleepJobs
								.addAll(submitSleepJobsToAThreadPoolAndRunThemInParallel(
										sleepJobParamsList,
										1 * THOUSAND_MILLISECONDS));

					}
				}
			}
		}
		// Wait until all jobs have reached RUNNING state
		barrierUntilAllThreadsRunning = new BarrierUntilAllThreadsRunning(
				futureCallableSleepJobs, SLEEP_JOB_DURATION_IN_SECS * 2);
		TestSession.logger
				.info("Either barrier met....  or waited long enough...so continue");

		runtimeRESTStatsBO = startCollectingRestStats(1 * THOUSAND_MILLISECONDS);
		waitFor(5 * THOUSAND_MILLISECONDS);
		stopCollectingRuntimeStatsAcrossQueues(runtimeRESTStatsBO);

		printValuesReceivedOverRest(runtimeRESTStatsBO);

		waitTillAllMapTasksComplete(futureCallableSleepJobs);
		
		HashMap<String, String> stateOfReduceTasks = getMapOfReduceTasksIdAndState(futureCallableSleepJobs);

		String attemptIdOfACompletedMapTaskThatWasKilled = "";
		String expectedAttemptIdOfARespawnedMapTask = "";
		do {
			Thread.sleep(1000);
			TestSession.logger
					.info("Waiting for a MAP task to complete....sleeping for 1 sec");
		} while ((attemptIdOfACompletedMapTaskThatWasKilled = killACompletedMapTaskAndGetCurrentAttemptID(futureCallableSleepJobs)) == null);
		TestSession.logger.info("Got back atempt Id:"
				+ attemptIdOfACompletedMapTaskThatWasKilled);
		TestSession.logger
				.info("************************************************************************************************************");
		TestSession.logger
				.info("********************************************* there, killed["
						+ attemptIdOfACompletedMapTaskThatWasKilled
						+ "] ************************************************");
		TestSession.logger
				.info("************************************************************************************************************");

		int attemptId = attemptIdOfACompletedMapTaskThatWasKilled
				.charAt(attemptIdOfACompletedMapTaskThatWasKilled.length() - 1) - '0';
		attemptId++;
		expectedAttemptIdOfARespawnedMapTask = attemptIdOfACompletedMapTaskThatWasKilled
				.substring(0, attemptIdOfACompletedMapTaskThatWasKilled
						.lastIndexOf('_') + 1)
				+ attemptId;
		assertThatTheKilledMapTaskWasRespawnedAndCompleted(
				futureCallableSleepJobs, expectedAttemptIdOfARespawnedMapTask);
		assertThatAReduceTaskWasKilledToMakeRoom(futureCallableSleepJobs,
				stateOfReduceTasks);

	}
	
}