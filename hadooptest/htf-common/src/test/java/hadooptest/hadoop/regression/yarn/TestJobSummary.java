package hadooptest.hadoop.regression.yarn;

import hadooptest.ParallelMethodTests;
import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Recursive;

import java.util.concurrent.Future;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskType;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelMethodTests.class)
public class TestJobSummary extends YarnTestsBaseClass {

	@Test
	public void testSuccessJobStatusInJobSummaryLog() throws Exception {
		String testName = "testSummary-1";
		String queueToUse = "default";
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;

		String user = HadooptestConstants.UserNames.HADOOPQA;
		Future<Job> handle = submitSingleSleepJobAndGetHandle(queueToUse, user,
				getDefaultSleepJobProps(queueToUse), 1, 1, 1, 1, 1, 1,
				testName, false);
		Job job = handle.get();
		String jobId = job.getJobID().toString();

		for (int xx = 0; xx < 60; xx++) {
			if (job.getStatus().getState() == State.SUCCEEDED) {
				break;
			}

			Util.sleep(1);
		}

		Util.sleep(10);
		
		// Look in the done folder
		StringBuilder resonsesInDoneAndDoneIntermediateFolder = new StringBuilder();
		genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "", localCluster,
				"/mapred/history/done", Recursive.YES);

		resonsesInDoneAndDoneIntermediateFolder
				.append(genericCliResponse.response);

		// Also look in the done_intermediate folder
		genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "", localCluster,
				"/mapred/history/done_intermediate", Recursive.YES);
		resonsesInDoneAndDoneIntermediateFolder
				.append(genericCliResponse.response);

		Assert.assertTrue(
				"Not able to lookup:(" + jobId + ") "
						+ resonsesInDoneAndDoneIntermediateFolder.toString(),
				resonsesInDoneAndDoneIntermediateFolder.toString().contains(
						jobId));

	}

	@Test
	public void testQueueInfoInJobSummaryLog() throws Exception {
		String testName = "testSummary-2";
		String queueToUse = "grideng";
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;

		String user = HadooptestConstants.UserNames.HADOOPQA;
		Future<Job> handle = submitSingleSleepJobAndGetHandle(queueToUse, user,
				getDefaultSleepJobProps(queueToUse), 1, 1, 1, 1, 1, 1,
				testName, false);
		Job job = handle.get();
		String jobId = job.getJobID().toString();

		for (int xx = 0; xx < 60; xx++) {
			if (job.getStatus().getState() == State.SUCCEEDED) {
				break;
			}
			Thread.sleep(1000);
		}

		// Look in the done folder
		StringBuilder resonsesInDoneAndDoneIntermediateFolder = new StringBuilder();
		genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "", localCluster,
				"/mapred/history/done", Recursive.YES);

		resonsesInDoneAndDoneIntermediateFolder
				.append(genericCliResponse.response);

		// Also look in the done_intermediate folder
		genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "", localCluster,
				"/mapred/history/done_intermediate", Recursive.YES);
		resonsesInDoneAndDoneIntermediateFolder
				.append(genericCliResponse.response);

		Assert.assertTrue(
				"Not able to lookup:(" + jobId + ") "
						+ resonsesInDoneAndDoneIntermediateFolder.toString(),
				resonsesInDoneAndDoneIntermediateFolder.toString().contains(
						jobId));

	}

	@Test
	public void testKilledJobStatusInJobSummaryLog() throws Exception {
		String testName = "testSummary-3";
		String queueToUse = "default";
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;

		String user = HadooptestConstants.UserNames.HADOOPQA;
		Future<Job> handle = submitSingleSleepJobAndGetHandle(queueToUse, user,
				getDefaultSleepJobProps(queueToUse), 10, 10, 50, 2, 50, 2,
				testName, false);
		Job job = handle.get();
		String jobId = job.getJobID().toString();

		job.killJob();

		for (int xx = 0; xx < 60; xx++) {
			if (job.getStatus().getState() == State.KILLED) {
				break;
			}
			Thread.sleep(1000);
		}

		/**
		 * Commenting the steps below, because jobs killed via the API do not
		 * show up under /mapred/history/done
		 */
		//
		// StringBuilder resonsesInDoneAndDoneIntermediateFolder = new
		// StringBuilder();
		// // Check the done folder
		// genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
		// HadooptestConstants.UserNames.HDFSQA, "", localCluster,
		// "/mapred/history/done", Recursive.YES);
		// resonsesInDoneAndDoneIntermediateFolder
		// .append(genericCliResponse.response);
		//
		// // Also check the done_intermediate folder
		// genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
		// HadooptestConstants.UserNames.HDFSQA, "", localCluster,
		// "/mapred/history/done_intermediate", Recursive.YES);
		// resonsesInDoneAndDoneIntermediateFolder
		// .append(genericCliResponse.response);
		//
		// Assert.assertTrue(
		// "Not able to lookup:(" + jobId + ") "
		// + resonsesInDoneAndDoneIntermediateFolder.toString(),
		// resonsesInDoneAndDoneIntermediateFolder.toString().contains(
		// jobId));

	}

	@Test
	public void testFailedJobStatusInJobSummaryLog() throws Exception {
		String testName = "testSummary-4";
		String queueToUse = "default";
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;

		String user = HadooptestConstants.UserNames.HADOOPQA;
		// numMapper, numReducer, mapSleepTime, mapSleepCount, reduceSleepTime,
		// reduceSleepCount
		Future<Job> handle = submitSingleSleepJobAndGetHandle(queueToUse, user,
				getDefaultSleepJobProps(queueToUse), 10, 10, 50, 2, 50, 2,
				testName, false);
		Job job = handle.get();
		String jobId = job.getJobID().toString();

		for (int xx = 0; xx < 120; xx++) {
			if (job.getStatus().getState() == State.RUNNING) {
				break;
			}
			Thread.sleep(1000);
		}

		// Give a chance for the tasks to run
		Thread.sleep(5000);

		// Running Tasks
		for (TaskReport aTaskReport : job.getTaskReports(TaskType.MAP)) {
			for (TaskAttemptID aTaskAttemptId : aTaskReport
					.getRunningTaskAttemptIds()) {
				job.failTask(aTaskAttemptId);

			}
		}
		for (TaskReport aTaskReport : job.getTaskReports(TaskType.REDUCE)) {
			for (TaskAttemptID aTaskAttemptId : aTaskReport
					.getRunningTaskAttemptIds()) {
				job.failTask(aTaskAttemptId);
			}
		}

		for (int xx = 0; xx < 60; xx++) {
			if (job.getStatus().getState() == State.SUCCEEDED) {
				break;
			}
			Thread.sleep(1000);
		}

		StringBuilder resonsesInDoneAndDoneIntermediateFolder = new StringBuilder();
		// First check in the done folder
		genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "", localCluster,
				"/mapred/history/done", Recursive.YES);
		resonsesInDoneAndDoneIntermediateFolder
				.append(genericCliResponse.response);

		// Also append the contents of the done_intermediate folder
		genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "", localCluster,
				"/mapred/history/done_intermediate", Recursive.YES);

		resonsesInDoneAndDoneIntermediateFolder
				.append(genericCliResponse.response);

		TestSession.logger.info(resonsesInDoneAndDoneIntermediateFolder
				.toString());

		// delay to give time for RM to move done files, ideally we would want to use
		// a definitive state that job is completely done including the bookeeping but
		// we don't have this here, job's bookkeeping should be done after 10 secs 
		boolean JobNotFound = true;
		int counter = 0;
		while (JobNotFound && counter <= 10) {
			counter++;
			Thread.sleep(1000);
			JobNotFound = resonsesInDoneAndDoneIntermediateFolder.toString().contains(jobId);
		}

		Assert.assertTrue(
				"Not able to lookup:(" + jobId + ") "
						+ resonsesInDoneAndDoneIntermediateFolder.toString(),
				resonsesInDoneAndDoneIntermediateFolder.toString().contains(jobId));

	}


}
