package hadooptest.hadoop.regression.yarn;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskType;
import org.junit.Assert;
import org.junit.Test;

public class TestLinuxTaskController extends YarnTestsBaseClass {

	void waitTillJobStartsRunning(Job job) throws IOException,
			InterruptedException {
		boolean objectiveMet = true;
		State jobState = job.getStatus().getState();
		while (jobState != State.RUNNING) {
			if ((jobState == State.FAILED) || (jobState == State.KILLED)) {
				objectiveMet = false;
				break;
			}
			Thread.sleep(1000);
			jobState = job.getStatus().getState();
			TestSession.logger.info(job.getJobName() + " is in state : "
					+ jobState + " hence sleeping for 1 sec");
		}
	}

	@Test
	public void testCheckOwnerOfJobAndTasksMrJob() throws IOException,
			InterruptedException, ExecutionException {
		String queueToUse = "default";
		String user = HadooptestConstants.UserNames.HADOOP3;
		String testName = "testCheckOwnerOfJobAndTasksMrJob";
		// int numMapper, int numReducer, int mapSleepTime, int mapSleepCount,
		// int reduceSleepTime, int reduceSleepCount,

		Future<Job> handle = submitSingleSleepJobAndGetHandle(queueToUse, user,
				getDefaultSleepJobProps(queueToUse), 10, 10, 30, 1, 30,
				1, testName, false);
		Job job = handle.get();
		waitTillJobStartsRunning(job);		
		Assert.assertTrue("Job should have run as " + user
				+ " insead it ran as " + job.getUser(), job.getUser()
				.equalsIgnoreCase(user));
		TaskType[] taskTypes = { TaskType.MAP, TaskType.REDUCE };
		for (TaskType aTaskType : taskTypes) {
			Thread.sleep(10000);
			for (TaskReport aTaskReport : job.getTaskReports(aTaskType)) {
				for (TaskAttemptID aTaskAttemptID : aTaskReport
						.getRunningTaskAttemptIds()) {
					TestSession.logger.info("Task Type:"
							+ aTaskAttemptID.getTaskType() + " job["
							+ aTaskAttemptID.getJobID() + "] taskId["
							+ aTaskAttemptID.getTaskID() + "]");
				}
			}
		}

	}
}
