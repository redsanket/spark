package hadooptest.hadoop.regression.yarn;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.hadoop.mapred.TIPStatus;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskType;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class TestMapredListAttemptIds extends YarnTestsBaseClass {

	@Test
	@Ignore("Tests incorrect CLI usage, with just list-attempt-ids. Not relevant from API's perspective")
	public void testListAttemptIdsWithOnlyValidJobId()
			throws InterruptedException, ExecutionException, IOException {
	}

	@Test
	@Ignore("Tests incorrect CLI usage, with just jobId and task state. Not relevant from API's perspective")
	public void testListAttemptIdsForValidJobIdTaskTypeTaskState() {
	}

	@Test
	@Ignore("Tests incorrect CLI usage, for missing args. Not relevant from API's perspective")
	public void testListAttemptIdsWithNoArguments() {

	}

	@Test
	@Ignore("Tests incorrect CLI usage, for missing jobID. Not relevant from API's perspective")
	public void testListAttemptIdsWithMissingJobId() {

	}

	@Test
	@Ignore("Tests incorrect CLI usage, with only taskType. Not relevant from API's perspective")
	public void testListAttemptIdsWithOnlyTaskType() {

	}

	@Test
	@Ignore("Tests incorrect CLI usage, with only taskState. Not relevant from API's perspective")
	public void testListAttemptIdsWithOnlyTaskState() {

	}

	@Test
	@Ignore("Tests incorrect CLI usage, with only jobID. Not relevant from API's perspective")
	public void testListAttemptIdsWithOnlyValidJobId2() {

	}

	@Test
	@Ignore("Tests incorrect CLI usage, with only jobID and task type. Not relevant from API's perspective")
	public void testListAttemptIdsWithOnlyValidJobIdAndValidTaskType() {

	}

	@Test
	@Ignore("Tests incorrect CLI usage, with only jobID and invalid task type. Not relevant from API's perspective")
	public void testListAttemptIdsWithOnlyValidJobIdAndInValidTaskType() {

	}

	@Test
	@Ignore("Tests incorrect CLI usage, with only jobID and invalid task state. Not relevant from API's perspective")
	public void test_listAttemptIdsWithOnlyValidJobIdValidTaskTypeAndInvalidTaskState() {

	}

	@Test
	@Ignore("")
	public void testListAttemptIdsWithNoRunningMaps()
			throws InterruptedException, ExecutionException, IOException {
		String testName = "testListAttemptIdsForValidJobIdTaskTypeTaskState";
		String queueToUse = "default";
		String user = HadooptestConstants.UserNames.HADOOPQA;
		/**
		 * Sleep Job args numMapper, numReducer, mapSleepTime,
		 * mapSleepCount,reduceSleepTime, reduceSleepCount
		 */
		Future<Job> handle = submitSingleSleepJobAndGetHandle(queueToUse, user,
				getDefaultSleepJobProps(queueToUse), 1, 10, 1, 1, 50000, 1,
				testName, false);
		Job job = handle.get();
		waitTillJobStartsRunning(job);
		// Sleep for 10 secs, to let the Map complete.
		Thread.sleep(10000);
		int countOfRunningMaps = 0;
		TaskType[] taskTypes = { TaskType.MAP };
		for (TaskType aTaskType : taskTypes) {
			for (TaskReport aTaskReport : job.getTaskReports(aTaskType)) {
				for (TaskAttemptID aTaskAttemptID : aTaskReport
						.getRunningTaskAttemptIds()) {
					countOfRunningMaps++;
					TestSession.logger.info("Task Type:"
							+ aTaskAttemptID.getTaskType() + " job["
							+ aTaskAttemptID.getJobID() + "] taskId["
							+ aTaskAttemptID.getTaskID() + "]");
				}
			}
		}
		Assert.assertTrue("Was expecting countOfRunningMaps to be == 0, it is:"
				+ countOfRunningMaps, countOfRunningMaps == 0);
		job.killJob();

	}

	@Test
	@Ignore("")
	public void testListAttemptIdsWithNoCompletedMapsAndTaskStatusCompleted()
			throws InterruptedException, ExecutionException, IOException {
		String testName = "testListAttemptIdsForValidJobIdTaskTypeTaskState";
		String queueToUse = "default";
		String user = HadooptestConstants.UserNames.HADOOPQA;
		/**
		 * Sleep Job args numMapper, numReducer, mapSleepTime,
		 * mapSleepCount,reduceSleepTime, reduceSleepCount
		 */
		Future<Job> handle = submitSingleSleepJobAndGetHandle(queueToUse, user,
				getDefaultSleepJobProps(queueToUse), 1, 1, 50000, 1, 50000, 1,
				testName, false);
		Job job = handle.get();
		waitTillJobStartsRunning(job);
		waitTillAnyTaskGetsToState(TIPStatus.COMPLETE, job, TaskType.MAP, 60);
		boolean testConditionMet = false;
		TaskType[] taskTypes = { TaskType.MAP };
		for (TaskType aTaskType : taskTypes) {
			for (TaskReport aTaskReport : job.getTaskReports(aTaskType)) {
				if (aTaskReport.getCurrentStatus() == TIPStatus.PENDING
						|| aTaskReport.getCurrentStatus() == TIPStatus.FAILED)
					continue;
				if (aTaskReport.getCurrentStatus() == TIPStatus.COMPLETE) {
					testConditionMet = true;
					break;
				}
				if (aTaskReport.getCurrentStatus() == TIPStatus.KILLED) {
					testConditionMet = false;
					break;
				}
			}
		}

		job.killJob();
		Assert.assertTrue(testConditionMet);

	}

	@Test
	@Ignore("begin from here")
	public void testListAttemptIdsForReduceTaskAndTaskStateAsRunning()
			throws InterruptedException, ExecutionException, IOException {
		String testName = "testListAttemptIdsForReduceTaskAndTaskStateAsRunning";
		String queueToUse = "default";
		String user = HadooptestConstants.UserNames.HADOOPQA;

		/**
		 * Sleep Job args numMapper, numReducer, mapSleepTime,
		 * mapSleepCount,reduceSleepTime, reduceSleepCount
		 */
		Future<Job> handle = submitSingleSleepJobAndGetHandle(queueToUse, user,
				getDefaultSleepJobProps(queueToUse), 1, 50000, 1, 1, 50000, 1,
				testName, false);
		Job job = handle.get();
		waitTillJobStartsRunning(job);
		waitTillAnyTaskGetsToState(TIPStatus.COMPLETE, job, TaskType.MAP, 60);
		boolean testConditionMet = false;
		TaskType[] taskTypes = { TaskType.MAP, TaskType.REDUCE };

		for (TaskType aTaskType : taskTypes) {
			if (aTaskType == TaskType.MAP) {
				for (TaskReport aTaskReport : job.getTaskReports(aTaskType)) {
					Assert.assertTrue(
							"MAP task not completed yet! It is in state:"
									+ aTaskReport.getCurrentStatus(),
							aTaskReport.getCurrentStatus() == TIPStatus.COMPLETE);
				}
			} else {
				for (TaskReport aTaskReport : job.getTaskReports(aTaskType)) {
					if (aTaskReport.getCurrentStatus() == TIPStatus.COMPLETE
							|| aTaskReport.getCurrentStatus() == TIPStatus.KILLED) {
						testConditionMet = false;
						TestSession.logger.info("reduce task :"
								+ aTaskReport.getTaskId() + " is in state:"
								+ aTaskReport.getCurrentStatus());
						break;

					}
					if (aTaskReport.getCurrentStatus() == TIPStatus.RUNNING
							|| aTaskReport.getCurrentStatus() == TIPStatus.PENDING
							|| aTaskReport.getCurrentStatus() == TIPStatus.FAILED) {
						TestSession.logger.info("reduce task :"
								+ aTaskReport.getTaskId() + " is in state:"
								+ aTaskReport.getCurrentStatus());
						testConditionMet = true;
						continue;
					}

				}

			}
		}
		job.killJob();
		Assert.assertTrue(testConditionMet);

	}

	@Test
	@Ignore("works")
	public void testListAttemptIdsForCompletedJobsForReduceTaskAndTaskStateAsCompleted()
			throws InterruptedException, ExecutionException, IOException {
		String testName = "testListAttemptIdsForCompletedJobsForReduceTaskAndTaskStateAsCompleted";
		String queueToUse = "default";
		String user = HadooptestConstants.UserNames.HADOOPQA;

		/**
		 * Sleep Job args numMapper, numReducer, mapSleepTime,
		 * mapSleepCount,reduceSleepTime, reduceSleepCount
		 */
		Future<Job> handle = submitSingleSleepJobAndGetHandle(queueToUse, user,
				getDefaultSleepJobProps(queueToUse), 10, 10, 10000, 1, 700, 1,
				testName, false);
		Job job = handle.get();
		waitTillJobStartsRunning(job);
		waitTillAnyTaskGetsToState(TIPStatus.COMPLETE, job, TaskType.MAP, 20);
		waitTillAnyTaskGetsToState(TIPStatus.COMPLETE, job, TaskType.REDUCE, 20);
		boolean testConditionMet = false;
		TaskType[] taskTypes = { TaskType.MAP, TaskType.REDUCE };
		int countOfCompletedReduceTasks = 0;
		for (TaskType aTaskType : taskTypes) {
			if (aTaskType == TaskType.MAP) {
				for (TaskReport aTaskReport : job.getTaskReports(aTaskType)) {
					Assert.assertTrue(
							"MAP task not completed yet! It is in state:"
									+ aTaskReport.getCurrentStatus(),
							aTaskReport.getCurrentStatus() == TIPStatus.COMPLETE);
				}
			} else {
				for (TaskReport aTaskReport : job.getTaskReports(aTaskType)) {
					if (aTaskReport.getCurrentStatus() == TIPStatus.KILLED) {
						testConditionMet = false;
						TestSession.logger.info("reduce task :"
								+ aTaskReport.getTaskId() + " is in state:"
								+ aTaskReport.getCurrentStatus());
						break;

					}
					if (aTaskReport.getCurrentStatus() == TIPStatus.RUNNING
							|| aTaskReport.getCurrentStatus() == TIPStatus.PENDING
							|| aTaskReport.getCurrentStatus() == TIPStatus.FAILED) {
						TestSession.logger.info("reduce task :"
								+ aTaskReport.getTaskId() + " is in state:"
								+ aTaskReport.getCurrentStatus());

						continue;
					}
					if (aTaskReport.getCurrentStatus() == TIPStatus.COMPLETE) {
						countOfCompletedReduceTasks++;
						testConditionMet = true;
					}
				}

			}
		}
		job.killJob();
		Assert.assertTrue(testConditionMet);
		Assert.assertTrue(countOfCompletedReduceTasks > 0);

	}

	@Test
	@Ignore("works")
	public void testListAttemptIdsForCompletedJobsForReduceTaskAndTaskStateAsRunning()
			throws InterruptedException, ExecutionException, IOException {
		String testName = "testListAttemptIdsForCompletedJobsForReduceTaskAndTaskStateAsRunning";
		String queueToUse = "default";
		String user = HadooptestConstants.UserNames.HADOOPQA;

		/**
		 * Sleep Job args numMapper, numReducer, mapSleepTime,
		 * mapSleepCount,reduceSleepTime, reduceSleepCount
		 */
		Future<Job> handle = submitSingleSleepJobAndGetHandle(queueToUse, user,
				getDefaultSleepJobProps(queueToUse), 1, 10, 10000, 1, 7000, 1,
				testName, false);
		Job job = handle.get();
		waitTillJobStartsRunning(job);
		waitTillAnyTaskGetsToState(TIPStatus.COMPLETE, job, TaskType.MAP, 20);
		waitTillAnyTaskGetsToState(TIPStatus.RUNNING, job, TaskType.REDUCE, 20);
		boolean testConditionMet = false;
		TaskType[] taskTypes = { TaskType.MAP, TaskType.REDUCE };
		int countOfRunningReduceTasks = 0;
		for (TaskType aTaskType : taskTypes) {
			if (aTaskType == TaskType.MAP) {
				for (TaskReport aTaskReport : job.getTaskReports(aTaskType)) {
					Assert.assertTrue(
							"MAP task not completed yet! It is in state:"
									+ aTaskReport.getCurrentStatus(),
							aTaskReport.getCurrentStatus() == TIPStatus.COMPLETE);
				}
			} else {
				for (TaskReport aTaskReport : job.getTaskReports(aTaskType)) {
					if (aTaskReport.getCurrentStatus() == TIPStatus.KILLED) {
						testConditionMet = false;
						TestSession.logger.info("reduce task :"
								+ aTaskReport.getTaskId() + " is in state:"
								+ aTaskReport.getCurrentStatus());
						break;

					}
					if (aTaskReport.getCurrentStatus() == TIPStatus.RUNNING) {
						TestSession.logger.info("reduce task :"
								+ aTaskReport.getTaskId() + " is in state:"
								+ aTaskReport.getCurrentStatus());
						countOfRunningReduceTasks++;
						testConditionMet = true;
					}
					if (aTaskReport.getCurrentStatus() == TIPStatus.PENDING
							|| aTaskReport.getCurrentStatus() == TIPStatus.FAILED
							|| aTaskReport.getCurrentStatus() == TIPStatus.COMPLETE) {
						TestSession.logger.info("reduce task :"
								+ aTaskReport.getTaskId() + " is in state:"
								+ aTaskReport.getCurrentStatus());
						continue;
					}

				}

			}
		}
		job.killJob();
		Assert.assertTrue(testConditionMet);
		Assert.assertTrue(countOfRunningReduceTasks > 0);

	}

	@Test
	@Ignore("repetetive test")
	public void testListAttemptIdsForCompletedJobsForMapTaskAndTaskStateAsRunning()
			throws InterruptedException, ExecutionException, IOException {

	}

	@Test
	@Ignore("repetetive test")
	public void testListAttemptIdsWithNoRunningReduceTasksAndTaskStatusRunning()
			throws InterruptedException, ExecutionException, IOException {

	}

}
