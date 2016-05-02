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

	// utility method to check JHS for a given jobId, returns true if the job is found
	// else returns false after 10 seconds of checking. This is not ideal, it would be
	// better to check a definitive "done" state for the job where we can be sure the 
 	// job files are transferred to history server, but we don't have this here.
	boolean checkJobInDonePath (String jobid) {
		boolean JobFound = false;
		int counter = 0;
		while ( !JobFound && counter <= 10 ) {
        		counter++;
        		Thread.sleep(1000);
        		JobFound = (responsesInDoneAndDoneIntermediateFolder.toString().contains(jobId));
		}
		
		return JobFound;
	}


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
		StringBuilder responsesInDoneAndDoneIntermediateFolder = new StringBuilder();
		genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "", localCluster,
				"/mapred/history/done", Recursive.YES);

		responsesInDoneAndDoneIntermediateFolder
				.append(genericCliResponse.response);

		// Also look in the done_intermediate folder
		genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "", localCluster,
				"/mapred/history/done_intermediate", Recursive.YES);
		responsesInDoneAndDoneIntermediateFolder
				.append(genericCliResponse.response);

		// race condition when checking JHS, since we don't have a definite job "done"
		// state to use, check JHS for job and break to the Assert if it's there, if not
   		// then we throw the Assert
		if ( !checkJobInDonePath(jobId) ) {
			TestSession.logger.info("JobID " + jobId + " not found in historyserver done paths after 10 seconds!!");
                }
			
		Assert.assertTrue(
				"Not able to lookup:(" + jobId + ") "
						+ responsesInDoneAndDoneIntermediateFolder.toString(),
				responsesInDoneAndDoneIntermediateFolder.toString().contains(jobId));
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
		StringBuilder responsesInDoneAndDoneIntermediateFolder = new StringBuilder();
		genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "", localCluster,
				"/mapred/history/done", Recursive.YES);

		responsesInDoneAndDoneIntermediateFolder
				.append(genericCliResponse.response);

		// Also look in the done_intermediate folder
		genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "", localCluster,
				"/mapred/history/done_intermediate", Recursive.YES);
		responsesInDoneAndDoneIntermediateFolder
				.append(genericCliResponse.response);

                // race condition when checking JHS, since we don't have a definite job "done"
                // state to use, check JHS for job and break to the Assert if it's there, if not
                // then we throw the Assert
                if ( !checkJobInDonePath(jobId) ) {
                        TestSession.logger.info("JobID " + jobId + " not found in historyserver done paths after 10 seconds!!");
                }

		Assert.assertTrue(
				"Not able to lookup:(" + jobId + ") "
						+ responsesInDoneAndDoneIntermediateFolder.toString(),
				responsesInDoneAndDoneIntermediateFolder.toString().contains(jobId));
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
		// StringBuilder responsesInDoneAndDoneIntermediateFolder = new
		// StringBuilder();
		// // Check the done folder
		// genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
		// HadooptestConstants.UserNames.HDFSQA, "", localCluster,
		// "/mapred/history/done", Recursive.YES);
		// responsesInDoneAndDoneIntermediateFolder
		// .append(genericCliResponse.response);
		//
		// // Also check the done_intermediate folder
		// genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
		// HadooptestConstants.UserNames.HDFSQA, "", localCluster,
		// "/mapred/history/done_intermediate", Recursive.YES);
		// responsesInDoneAndDoneIntermediateFolder
		// .append(genericCliResponse.response);
		//
		// Assert.assertTrue(
		// "Not able to lookup:(" + jobId + ") "
		// + responsesInDoneAndDoneIntermediateFolder.toString(),
		// responsesInDoneAndDoneIntermediateFolder.toString().contains(
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

		StringBuilder responsesInDoneAndDoneIntermediateFolder = new StringBuilder();
		// First check in the done folder
		genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "", localCluster,
				"/mapred/history/done", Recursive.YES);
		responsesInDoneAndDoneIntermediateFolder
				.append(genericCliResponse.response);

		// Also append the contents of the done_intermediate folder
		genericCliResponse = dfsCliCommands.ls(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "", localCluster,
				"/mapred/history/done_intermediate", Recursive.YES);

		responsesInDoneAndDoneIntermediateFolder
				.append(genericCliResponse.response);

		TestSession.logger.info(responsesInDoneAndDoneIntermediateFolder
				.toString());

                // race condition when checking JHS, since we don't have a definite job "done"
                // state to use, check JHS for job and break to the Assert if it's there, if not
                // then we throw the Assert
                if ( !checkJobInDonePath(jobId) ) {
                        TestSession.logger.info("JobID " + jobId + " not found in historyserver done paths after 10 seconds!!");
                }

		Assert.assertTrue(
				"Not able to lookup:(" + jobId + ") "
						+ responsesInDoneAndDoneIntermediateFolder.toString(),
				responsesInDoneAndDoneIntermediateFolder.toString().contains(jobId));
	}


}
