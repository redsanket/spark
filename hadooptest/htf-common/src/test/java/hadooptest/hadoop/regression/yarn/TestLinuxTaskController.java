package hadooptest.hadoop.regression.yarn;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;
import hadooptest.hadoop.regression.yarn.MapredCliCommands.GenericMapredCliResponseBO;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
	private static final String HADOOPQA_AS_HDFSQA_IDENTITY_FILE = "/homes/hadoopqa/.ssh/flubber_hadoopqa_as_hdfsqa";

	private List<String> getDataNodes() throws Exception {
		List<String> dataNodesList = new ArrayList<String>();
		MapredCliCommands mapredCliCommands = new MapredCliCommands();
		GenericMapredCliResponseBO genericMapredCliResponseBO = mapredCliCommands
				.listActiveTrackers(YarnTestsBaseClass.EMPTY_ENV_HASH_MAP,
						HadooptestConstants.UserNames.HADOOPQA);
		for (String aLine : genericMapredCliResponseBO.response.split("\n")) {
			String strippedLine;
			String beginPattern = "tracker_";
			String endPattern = ":8041";
			if (aLine.contains("tracker_")) {
				strippedLine = aLine.substring(beginPattern.length() - 1,
						aLine.length() - endPattern.length() - 1);
				dataNodesList.add(strippedLine);
			}
		}

		return dataNodesList;

	}

	void waitTillJobStartsRunning(Job job) throws IOException,
			InterruptedException {
		State jobState = job.getStatus().getState();
		while (jobState != State.RUNNING) {
			if ((jobState == State.FAILED) || (jobState == State.KILLED)) {
				break;
			}
			Thread.sleep(1000);
			jobState = job.getStatus().getState();
			TestSession.logger
					.info(job.getJobName()
							+ " is in state : "
							+ jobState
							+ ", awaiting its state to change to 'RUNNING' hence sleeping for 1 sec");
		}
	}

	void waitTillJobSucceeds(Job job) throws IOException, InterruptedException {

		State jobState = job.getStatus().getState();
		while (jobState != State.SUCCEEDED) {
			if ((jobState == State.FAILED) || (jobState == State.KILLED)) {
				break;
			}
			Thread.sleep(1000);
			jobState = job.getStatus().getState();
			TestSession.logger
					.info(job.getJobName()
							+ " is in state : "
							+ jobState
							+ ", awaiting its state to change to 'SUCCEEDED' hence sleeping for 1 sec");
		}
	}

	@Test
	@Ignore("works")
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
	@Ignore("works")
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

	@Test
	@Ignore("works")
	public void testCheckOwnerOfJobAndTasksCacheFileStreamingJob()
			throws Exception {
		DfsCliCommands dfsCommonCliCommands = new DfsCliCommands();
		String user = HadooptestConstants.UserNames.HADOOP3;
		GenericCliResponseBO genericCliResponse;
		String testName = "testCheckOwnerOfJobAndTasksCacheFileStreamingJob";
		String timeStamp = "-" + System.currentTimeMillis();
		String dirInHdfs = "/user/" + user + "/" + testName + timeStamp;

		// mkdir
		genericCliResponse = dfsCommonCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				user, HadooptestConstants.Schema.HDFS,
				TestSession.cluster.getClusterName(), dirInHdfs);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		// File 1
		String fileToCopy = TestSession.conf.getProperty("WORKSPACE") + "/"
				+ "htf-common/resources/hadooptest/hadoop/regression"
				+ "/yarn/linuxTaskController/input.txt";

		genericCliResponse = dfsCommonCliCommands.put(EMPTY_ENV_HASH_MAP, user,
				HadooptestConstants.Schema.HDFS,
				TestSession.cluster.getClusterName(), fileToCopy, dirInHdfs
						+ "/input.txt");

		// File 2
		fileToCopy = TestSession.conf.getProperty("WORKSPACE") + "/"
				+ "htf-common/resources/hadooptest/hadoop/regression"
				+ "/yarn/linuxTaskController/cache.txt";

		genericCliResponse = dfsCommonCliCommands.put(EMPTY_ENV_HASH_MAP, user,
				HadooptestConstants.Schema.HDFS,
				TestSession.cluster.getClusterName(), fileToCopy, dirInHdfs
						+ "/cache.txt");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		ArrayList<String> commandFrags = new ArrayList<String>();
		commandFrags.add("-input");
		commandFrags.add(dirInHdfs + "/input.txt");
		commandFrags.add("-output");
		commandFrags.add(dirInHdfs + "/OutDir");
		commandFrags.add("-mapper");
		commandFrags.add("xargs cat");
		commandFrags.add("-reducer");
		commandFrags.add("cat");
		commandFrags.add("-cacheArchive");
		commandFrags.add(dirInHdfs + "/cache.txt#testlink");
		commandFrags.add("-jobconf");

		commandFrags.add("\"mapreduce.job.maps=10\"");
		commandFrags.add("-jobconf");
		commandFrags.add("\"mapreduce.job.reduces=10\"");
		commandFrags.add("-jobconf");
		commandFrags.add("\"mapreduce.job.queuename=default\"");
		for (String aCommandFrag : commandFrags) {
			TestSession.logger.info(aCommandFrag + " ");
		}
		TestSession.logger.info("-----------------------till here:");
		Job job = submitSingleStreamJobAndGetHandle(user,
				commandFrags.toArray(new String[0]));

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
		waitTillJobSucceeds(job);
		genericCliResponse = dfsCommonCliCommands.cat(EMPTY_ENV_HASH_MAP, user,
				HadooptestConstants.Schema.HDFS,
				TestSession.cluster.getClusterName(), dirInHdfs + "/OutDir/*");

		Assert.assertTrue(genericCliResponse.response
				+ " did not contain expected string",
				genericCliResponse.response
						.contains("This is just the cache string"));

	}

	@Test
	@Ignore("works")
	public void testCheckOwnerOfJobAndTasksFileStreamingJob() throws Exception {
		DfsCliCommands dfsCommonCliCommands = new DfsCliCommands();
		String user = HadooptestConstants.UserNames.HADOOP3;
		GenericCliResponseBO genericCliResponse;
		String testName = "testCheckOwnerOfJobAndTasksFileStreamingJob";
		String timeStamp = "-" + System.currentTimeMillis();
		String dirInHdfs = "/user/" + user + "/" + testName + timeStamp;

		// mkdir
		genericCliResponse = dfsCommonCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				user, HadooptestConstants.Schema.HDFS,
				TestSession.cluster.getClusterName(), dirInHdfs);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		// File 1
		String fileToCopy = TestSession.conf.getProperty("WORKSPACE") + "/"
				+ "htf-common/resources/hadooptest/hadoop/regression"
				+ "/yarn/linuxTaskController/input.txt";

		genericCliResponse = dfsCommonCliCommands.put(EMPTY_ENV_HASH_MAP, user,
				HadooptestConstants.Schema.HDFS,
				TestSession.cluster.getClusterName(), fileToCopy, dirInHdfs
						+ "/input.txt");
		// File 2
		fileToCopy = TestSession.conf.getProperty("WORKSPACE") + "/"
				+ "htf-common/resources/hadooptest/hadoop/regression"
				+ "/yarn/linuxTaskController/cache.txt";

		genericCliResponse = dfsCommonCliCommands.put(EMPTY_ENV_HASH_MAP, user,
				HadooptestConstants.Schema.HDFS,
				TestSession.cluster.getClusterName(), fileToCopy, dirInHdfs
						+ "/cache.txt");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		ArrayList<String> commandFrags = new ArrayList<String>();
		commandFrags.add("-input");
		commandFrags.add(dirInHdfs + "/input.txt");
		commandFrags.add("-output");
		commandFrags.add(dirInHdfs + "/OutDir");
		commandFrags.add("-mapper");
		commandFrags.add("xargs cat");
		commandFrags.add("-reducer");
		commandFrags.add("cat");
		commandFrags.add("-cacheArchive");
		commandFrags.add(dirInHdfs + "/cache.txt#testlink");
		commandFrags.add("-jobconf");
		commandFrags.add("\"mapreduce.job.maps=10\"");
		commandFrags.add("-jobconf");
		commandFrags.add("\"mapreduce.job.reduces=10\"");
		commandFrags.add("-jobconf");
		commandFrags.add("\"mapreduce.job.queuename=default\"");

		for (String aCommandFrag : commandFrags) {
			TestSession.logger.info(aCommandFrag + " ");
		}
		TestSession.logger.info("-----------------------till here:");
		Job job = submitSingleStreamJobAndGetHandle(user,
				commandFrags.toArray(new String[0]));

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
		waitTillJobSucceeds(job);
		genericCliResponse = dfsCommonCliCommands.cat(EMPTY_ENV_HASH_MAP, user,
				HadooptestConstants.Schema.HDFS,
				TestSession.cluster.getClusterName(), dirInHdfs + "/OutDir/*");

		Assert.assertTrue(genericCliResponse.response
				+ " did not contain expected string",
				genericCliResponse.response
						.contains("This is just the cache string"));

	}

	@Test
	@Ignore("does not work")
	public void testCheckOwnerOfJobAndTasksCacheLibJarStreamingJob()
			throws Exception {
		DfsCliCommands dfsCommonCliCommands = new DfsCliCommands();
		String user = HadooptestConstants.UserNames.HADOOP3;
		GenericCliResponseBO genericCliResponse;
		String testName = "testCheckOwnerOfJobAndTasksCacheLibJarStreamingJob";
		String timeStamp = "-" + System.currentTimeMillis();
		String dirInHdfs = "/user/" + user + "/" + testName + timeStamp;

		// mkdir
		genericCliResponse = dfsCommonCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				user, HadooptestConstants.Schema.HDFS,
				TestSession.cluster.getClusterName(), dirInHdfs);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		// File 1
		String fileToCopy = TestSession.conf.getProperty("WORKSPACE") + "/"
				+ "htf-common/resources/hadooptest/hadoop/regression"
				+ "/yarn/linuxTaskController/cachedir.jar";

		genericCliResponse = dfsCommonCliCommands.put(EMPTY_ENV_HASH_MAP, user,
				HadooptestConstants.Schema.HDFS,
				TestSession.cluster.getClusterName(), fileToCopy, dirInHdfs
						+ "/cachedir.jar");

		// File 2
		fileToCopy = TestSession.conf.getProperty("WORKSPACE") + "/"
				+ "htf-common/resources/hadooptest/hadoop/regression"
				+ "/yarn/linuxTaskController/cacheinput.txt";

		genericCliResponse = dfsCommonCliCommands.put(EMPTY_ENV_HASH_MAP, user,
				HadooptestConstants.Schema.HDFS,
				TestSession.cluster.getClusterName(), fileToCopy, dirInHdfs
						+ "/cacheinput.txt");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		ArrayList<String> commandFrags = new ArrayList<String>();
		commandFrags.add("-input");
		commandFrags.add(dirInHdfs + "/cacheinput.txt");
		commandFrags.add("-output");
		commandFrags.add(dirInHdfs + "/OutDir");
		commandFrags.add("-mapper");
		commandFrags.add("mapper.sh");
		commandFrags.add("-reducer");
		commandFrags.add("cat");
		commandFrags.add("-jobconf");
		commandFrags.add("\"mapreduce.job.maps=10\"");
		commandFrags.add("-jobconf");
		commandFrags.add("\"mapreduce.job.reduces=10\"");
		commandFrags.add("-jobconf");
		commandFrags.add("\"mapreduce.job.queuename=default\"");
		commandFrags.add("-file");
		commandFrags.add(TestSession.conf.getProperty("WORKSPACE") + "/"
				+ "htf-common/resources/hadooptest/hadoop/regression"
				+ "/yarn/linuxTaskController/mapper.sh");
		// There is no libJar in the StreamJob, hence using -cacheFile
		commandFrags.add("-cacheFile");
		commandFrags.add(TestSession.conf.getProperty("WORKSPACE") + "/"
				+ "htf-common/resources/hadooptest/hadoop/regression"
				+ "/yarn/linuxTaskController/cachedir.jar");

		for (String aCommandFrag : commandFrags) {
			TestSession.logger.info(aCommandFrag + " ");
		}
		TestSession.logger.info("-----------------------till here:");
		Job job = submitSingleStreamJobAndGetHandle(user,
				commandFrags.toArray(new String[0]));

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
		waitTillJobSucceeds(job);
		genericCliResponse = dfsCommonCliCommands.cat(EMPTY_ENV_HASH_MAP, user,
				HadooptestConstants.Schema.HDFS,
				TestSession.cluster.getClusterName(), dirInHdfs + "/OutDir/*");

		Assert.assertTrue(genericCliResponse.response
				+ " did not contain expected string",
				genericCliResponse.response.contains("cachedir.jar"));

	}

	@Test
	public void testTaskControllerKillTaskAndCheckChildProcess()
			throws Exception {
		DfsCliCommands dfsCommonCliCommands = new DfsCliCommands();
		String user = HadooptestConstants.UserNames.HADOOP3;
		GenericCliResponseBO genericCliResponse;
		String testName = "testTaskControllerKillTaskAndCheckChildProcess";
		String timeStamp = "-" + System.currentTimeMillis();
		String dirInHdfs = "/user/" + user + "/" + testName + timeStamp;

		// mkdir
		genericCliResponse = dfsCommonCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				user, HadooptestConstants.Schema.HDFS,
				TestSession.cluster.getClusterName(), dirInHdfs);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		// File 1
		String fileToCopy = TestSession.conf.getProperty("WORKSPACE") + "/"
				+ "htf-common/resources/hadooptest/hadoop/regression"
				+ "/yarn/linuxTaskController/input.txt";

		genericCliResponse = dfsCommonCliCommands.put(EMPTY_ENV_HASH_MAP, user,
				HadooptestConstants.Schema.HDFS,
				TestSession.cluster.getClusterName(), fileToCopy, dirInHdfs
						+ "/input.txt");

		// File 2
		fileToCopy = TestSession.conf.getProperty("WORKSPACE") + "/"
				+ "htf-common/resources/hadooptest/hadoop/regression"
				+ "/yarn/linuxTaskController/cache.txt";

		genericCliResponse = dfsCommonCliCommands.put(EMPTY_ENV_HASH_MAP, user,
				HadooptestConstants.Schema.HDFS,
				TestSession.cluster.getClusterName(), fileToCopy, dirInHdfs
						+ "/cache.txt");
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

		ArrayList<String> commandFrags = new ArrayList<String>();
		commandFrags.add("-input");
		commandFrags.add(dirInHdfs + "/input.txt");
		commandFrags.add("-output");
		commandFrags.add(dirInHdfs + "/OutDir");
		commandFrags.add("-mapper");
		commandFrags.add("subshell.sh");
		commandFrags.add("-reducer");
		commandFrags.add("NONE");
		// commandFrags.add("-cacheArchive");
		// commandFrags.add(dirInHdfs + "/cache.txt#testlink");
		commandFrags.add("-jobconf");
		commandFrags.add("\"mapreduce.job.maps=10\"");
		commandFrags.add("-jobconf");
		commandFrags.add("\"mapreduce.job.reduces=10\"");
		commandFrags.add("-jobconf");
		commandFrags.add("\"mapreduce.job.queuename=default\"");
		commandFrags.add("-file");
		commandFrags.add(TestSession.conf.getProperty("WORKSPACE") + "/"
				+ "htf-common/resources/hadooptest/hadoop/regression"
				+ "/yarn/linuxTaskController/subshell.sh");

		for (String aCommandFrag : commandFrags) {
			TestSession.logger.info(aCommandFrag + " ");
		}
		TestSession.logger.info("-----------------------till here:");
		Job job = submitSingleStreamJobAndGetHandle(user,
				commandFrags.toArray(new String[0]));

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
					job.killTask(aTaskAttemptID);

				}
			}
		}
		job.killJob();
		Assert.assertTrue("Expecting job to be in KILLED, but it is in:"
				+ job.getStatus().getState(),
				job.getStatus().getState() == State.KILLED);

	}

}
