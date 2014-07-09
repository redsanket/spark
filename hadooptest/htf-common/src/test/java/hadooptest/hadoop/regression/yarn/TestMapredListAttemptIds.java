package hadooptest.hadoop.regression.yarn;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.hadoop.mapred.TIPStatus;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class TestMapredListAttemptIds extends YarnTestsBaseClass {

	@Before
	public void ensureNoJobsAreRunning() throws IOException,
			InterruptedException {
		Cluster cluster = new Cluster(TestSession.cluster.getConf());
		for (JobStatus aJobStatus : cluster.getAllJobStatuses()) {
			if ((aJobStatus.getState() == State.RUNNING)) {
				Job runningJob = cluster.getJob(aJobStatus.getJobID());
				TestSession.logger
						.info("RUNNING job name:" + runningJob.getJobName()
								+ " ID:" + runningJob.getJobID()
								+ " ... proceeding to kill it!");
				runningJob.killJob();

			}
		}

	}

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

	/**
	 * Test to request attemptids for map with state as completed when there are
	 * no map tasks that have completed
	 * 
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * @throws IOException
	 */
	@Test
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
		waitTillAnyTaskGetsToState(TIPStatus.RUNNING, job, TaskType.MAP, 20);
		boolean testConditionMet = true;
		int countOfRunningMaps = 0;
		TaskType[] taskTypes = { TaskType.MAP };
		for (TaskType aTaskType : taskTypes) {
			for (TaskReport aTaskReport : job.getTaskReports(aTaskType)) {
				if (aTaskReport.getCurrentStatus() == TIPStatus.RUNNING) {
					testConditionMet = true;
					countOfRunningMaps++;
					continue;
				}
				if (aTaskReport.getCurrentStatus() == TIPStatus.COMPLETE) {
					testConditionMet = false;
					break;
				}
				if (aTaskReport.getCurrentStatus() == TIPStatus.KILLED) {
					testConditionMet = false;
					break;
				}
			}
		}

		job.killJob();
		Assert.assertTrue(countOfRunningMaps > 0);

	}

	/**
	 * Test listAttemptId for reduce tasks that is are currently in running state
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * @throws IOException
	 */
	@Test
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
				getDefaultSleepJobProps(queueToUse), 1, 10, 1, 1, 50000, 1,
				testName, false);
		Job job = handle.get();
		waitTillJobStartsRunning(job);
		waitTillAnyTaskGetsToState(TIPStatus.COMPLETE, job, TaskType.MAP, 60);
		waitTillAnyTaskGetsToState(TIPStatus.RUNNING, job, TaskType.REDUCE, 60);
		boolean testConditionMet = false;
		TaskType[] taskTypes = { TaskType.REDUCE };

		for (TaskType aTaskType : taskTypes) {

			for (TaskReport aTaskReport : job.getTaskReports(aTaskType)) {
				if (aTaskReport.getCurrentStatus() == TIPStatus.COMPLETE
						|| aTaskReport.getCurrentStatus() == TIPStatus.KILLED) {
					testConditionMet = false;
					TestSession.logger.info(aTaskType + " task :"
							+ aTaskReport.getTaskId() + " is in state:"
							+ aTaskReport.getCurrentStatus());
					break;

				}
				if (aTaskReport.getCurrentStatus() == TIPStatus.PENDING) {
					continue;
				}
				if (aTaskReport.getCurrentStatus() == TIPStatus.RUNNING
				|| aTaskReport.getCurrentStatus() == TIPStatus.FAILED) {
					TestSession.logger.info(taskTypes + " task :"
							+ aTaskReport.getTaskId() + " is in state:"
							+ aTaskReport.getCurrentStatus());
					testConditionMet = true;
					continue;
				}

			}
		}
		job.killJob();
		Assert.assertTrue(testConditionMet);

	}

	/**
	 * Test listAttemptId for reduce tasks that is are currently in completed state
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * @throws IOException
	 */
	@Test
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
				getDefaultSleepJobProps(queueToUse), 10, 10, 1, 1, 700, 1,
				testName, false);
		Job job = handle.get();
		waitTillJobSucceeds(job);
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
						TestSession.logger.info(aTaskType + " task :"
								+ aTaskReport.getTaskId() + " is in state:"
								+ aTaskReport.getCurrentStatus());
						break;

					}
					if (aTaskReport.getCurrentStatus() == TIPStatus.RUNNING
							|| aTaskReport.getCurrentStatus() == TIPStatus.PENDING
							|| aTaskReport.getCurrentStatus() == TIPStatus.FAILED) {
						TestSession.logger.info(aTaskType + " task :"
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

	/**
	 * Even though the test says that it is looking for running tasks, it is
	 * actually looking for completed Reduce tasks, after a job finishes. Look
	 * at http://bug.corp.yahoo.com/show_bug.cgi?id=4608365&mark=2#c2
	 * 
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * @throws IOException
	 */
	@Test
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
				getDefaultSleepJobProps(queueToUse), 10, 10, 10000, 1, 7000, 1,
				testName, false);
		Job job = handle.get();
		waitTillJobSucceeds(job);
		boolean testConditionMet = false;
		TaskType[] taskTypes = { TaskType.REDUCE };
		int countOfCompletedReduceTasks = 0;
		for (TaskType aTaskType : taskTypes) {
			for (TaskReport aTaskReport : job.getTaskReports(aTaskType)) {
				if (aTaskReport.getCurrentStatus() == TIPStatus.KILLED) {
					testConditionMet = false;
					TestSession.logger.info(aTaskType + " task :"
							+ aTaskReport.getTaskId() + " is in state:"
							+ aTaskReport.getCurrentStatus());
					break;

				}
				if (aTaskReport.getCurrentStatus() == TIPStatus.RUNNING
						|| aTaskReport.getCurrentStatus() == TIPStatus.PENDING
						|| aTaskReport.getCurrentStatus() == TIPStatus.FAILED) {
					TestSession.logger.info(aTaskType + " task :"
							+ aTaskReport.getTaskId() + " is in state:"
							+ aTaskReport.getCurrentStatus());
					continue;
				}
				if (aTaskReport.getCurrentStatus() == TIPStatus.COMPLETE) {
					TestSession.logger.info(aTaskType + " task :"
							+ aTaskReport.getTaskId() + " is in state:"
							+ aTaskReport.getCurrentStatus());
					countOfCompletedReduceTasks++;
					testConditionMet = true;

				}

			}
		}
		job.killJob();
		Assert.assertTrue(testConditionMet);
		Assert.assertTrue(countOfCompletedReduceTasks > 0);

	}

	/**
	 * Test listAttemptId for reduce tasks that is are currently in completed state
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * @throws IOException
	 */
	@Test
	public void testListAttemptIdsForCompletedJobsForMapTaskAndTaskStateAsRunning()
			throws InterruptedException, ExecutionException, IOException {
		String testName = "testListAttemptIdsForCompletedJobsForMapTaskAndTaskStateAsRunning";
		String queueToUse = "default";
		String user = HadooptestConstants.UserNames.HADOOPQA;

		/**
		 * Sleep Job args numMapper, numReducer, mapSleepTime,
		 * mapSleepCount,reduceSleepTime, reduceSleepCount
		 */
		Future<Job> handle = submitSingleSleepJobAndGetHandle(queueToUse, user,
				getDefaultSleepJobProps(queueToUse), 2, 2, 10000, 1, 5000, 1,
				testName, false);
		Job job = handle.get();
		waitTillJobSucceeds(job);
		boolean testConditionMet = false;
		TaskType[] taskTypes = { TaskType.MAP };
		int countOfRunningMapTasks = 0;
		for (TaskType aTaskType : taskTypes) {
			for (TaskReport aTaskReport : job.getTaskReports(aTaskType)) {
				if (aTaskReport.getCurrentStatus() == TIPStatus.KILLED) {
					testConditionMet = false;
					TestSession.logger.info(aTaskType + " task :"
							+ aTaskReport.getTaskId() + " is in state:"
							+ aTaskReport.getCurrentStatus());
					break;

				}
				if (aTaskReport.getCurrentStatus() == TIPStatus.RUNNING) {
					TestSession.logger.info(aTaskType + " task :"
							+ aTaskReport.getTaskId() + " is in state:"
							+ aTaskReport.getCurrentStatus());
					testConditionMet = false;
					countOfRunningMapTasks++;

				}
				if (aTaskReport.getCurrentStatus() == TIPStatus.COMPLETE) {
					TestSession.logger.info(aTaskType + " task :"
							+ aTaskReport.getTaskId() + " is in state:"
							+ aTaskReport.getCurrentStatus());
					testConditionMet = true;

				}

			}
		}
		job.killJob();
		Assert.assertTrue(testConditionMet);
		Assert.assertTrue("Expected count:0, in reality got:"
				+ countOfRunningMapTasks, countOfRunningMapTasks == 0);

	}

	/**
	 * Test to request attemptids for reduce task with state as running when there are no reduce tasks that are running 
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * @throws IOException
	 */
	@Test
	public void testListAttemptIdsWithNoRunningReduceTasksAndTaskStatusRunning()
			throws InterruptedException, ExecutionException, IOException {
		String testName = "testListAttemptIdsWithNoRunningReduceTasksAndTaskStatusRunning";
		String queueToUse = "default";
		String user = HadooptestConstants.UserNames.HADOOPQA;

		/**
		 * Sleep Job args numMapper, numReducer, mapSleepTime,
		 * mapSleepCount,reduceSleepTime, reduceSleepCount
		 */
		Future<Job> handle = submitSingleSleepJobAndGetHandle(queueToUse, user,
				getDefaultSleepJobProps(queueToUse), 10, 0, 10000, 1, 0, 0,
				testName, false);
		Job job = handle.get();
		waitTillAnyTaskGetsToState(TIPStatus.RUNNING, job, TaskType.REDUCE, 60);
		TaskType[] taskTypes = { TaskType.REDUCE };
		int countOfRunningReduceTasks = 0;
		for (TaskType aTaskType : taskTypes) {
			for (TaskReport aTaskReport : job.getTaskReports(aTaskType)) {
				if (aTaskReport.getCurrentStatus() == TIPStatus.KILLED) {
					TestSession.logger.info(aTaskType + " task :"
							+ aTaskReport.getTaskId() + " is in state:"
							+ aTaskReport.getCurrentStatus());
					break;

				}
				if (aTaskReport.getCurrentStatus() == TIPStatus.RUNNING) {
					TestSession.logger.info(aTaskType + " task :"
							+ aTaskReport.getTaskId() + " is in state:"
							+ aTaskReport.getCurrentStatus());
					countOfRunningReduceTasks++;

				}
				if (aTaskReport.getCurrentStatus() == TIPStatus.COMPLETE) {
					TestSession.logger.info(aTaskType + " task :"
							+ aTaskReport.getTaskId() + " is in state:"
							+ aTaskReport.getCurrentStatus());
				}

			}
		}
		job.killJob();
		Assert.assertTrue(countOfRunningReduceTasks == 0);

	}

}
