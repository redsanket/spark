package hadooptest.hadoop.regression.yarn;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskType;
import org.junit.Assert;
import org.junit.Ignore;
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
	@Ignore("FOR NOW")
	public void testCheckOwnerOfJobAndTasksMrJob() throws IOException,
			InterruptedException, ExecutionException {
		String queueToUse = "default";
		String user = HadooptestConstants.UserNames.HADOOP3;
		String testName = "testCheckOwnerOfJobAndTasksMrJob";
		// int numMapper, int numReducer, int mapSleepTime, int mapSleepCount,
		// int reduceSleepTime, int reduceSleepCount,

		Future<Job> handle = submitSingleSleepJobAndGetHandle(queueToUse, user,
				getDefaultSleepJobProps(queueToUse), 10, 10, 30, 1, 30, 1,
				testName, false);
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

	@Test
	public void testCheckOwnerOfJobAndTasksCacheArchiveStreamingJob()
			throws Exception {
		DfsCliCommands dfsCommonCliCommands = new DfsCliCommands();
		String user = HadooptestConstants.UserNames.HADOOP3;
		GenericCliResponseBO genericCliResponse;
		String testName = "testCheckOwnerOfJobAndTasksCacheArchiveStreamingJob";
		String timeStamp = "-" + System.currentTimeMillis();
		String dirInHdfs = "/user/" + HadooptestConstants.UserNames.HADOOPQA
				+ "/" + testName + timeStamp;

		// mkdir
		genericCliResponse = dfsCommonCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA,
				HadooptestConstants.Schema.HDFS,
				TestSession.cluster.getClusterName(), dirInHdfs);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		// File 1 (cachedir.jar)
		String fileToCopy = TestSession.conf.getProperty("WORKSPACE") + "/"
				+ "htf-common/resources/hadooptest/hadoop/regression"
				+ "/yarn/linuxTaskController/cachedir.jar";

		genericCliResponse = dfsCommonCliCommands.put(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA,
				HadooptestConstants.Schema.HDFS,
				TestSession.cluster.getClusterName(), fileToCopy, dirInHdfs
						+ "/cachedir.jar");

		// File 2 (cacheinput.txt)
		fileToCopy = TestSession.conf.getProperty("WORKSPACE") + "/"
				+ "htf-common/resources/hadooptest/hadoop/regression"
				+ "/yarn/linuxTaskController/cacheinput.txt";

		genericCliResponse = dfsCommonCliCommands.put(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA,
				HadooptestConstants.Schema.HDFS,
				TestSession.cluster.getClusterName(), fileToCopy, dirInHdfs
						+ "/cacheinput.txt");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		Map<String, String> streamJobArgs = new HashMap<String, String>();
		streamJobArgs.put("mapreduce.job.maps", "10");
		streamJobArgs.put("mapreduce.job.reduces", "10");
		streamJobArgs.put("mapreduce.job.queuename", "default");
		StringBuilder sb = new StringBuilder();
		sb.append("-input " + dirInHdfs + "/cacheinput.txt");
		sb.append(" -output " + dirInHdfs + "/OutDir");
		sb.append(" -mapper " + "cat");
		sb.append(" -reducer " + "cat");
		sb.append(" -cacheArchive " + dirInHdfs + "/cachedir.jar#testlink");
		for (String key : streamJobArgs.keySet()) {
			sb.append(" -jobconf \"" + key + "=" + streamJobArgs.get(key)
					+ "\"");
		}
		String streamJobCommand = sb.toString();
		streamJobCommand.replaceAll("\\s+", " ");
		TestSession.logger.info(streamJobCommand);

		TestSession.logger.info("-----------------------till here:");
		Job job = submitSingleStreamJobAndGetHandle(user,
				streamJobCommand.split("\\s+"));
		// runStdHadoopStreamingJob(streamJobCommand.split("\\s+"));

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

		genericCliResponse = dfsCommonCliCommands.cat(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA,
				HadooptestConstants.Schema.HDFS,
				TestSession.cluster.getClusterName(), dirInHdfs + "/OutDir*");

		Assert.assertTrue(genericCliResponse.response
				+ " did not contain expected string",
				genericCliResponse.response
						.contains("This is just the cache string"));

	}
}
