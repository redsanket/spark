package hadooptest.hadoop.regression.yarn;

import java.util.ArrayList;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskType;
import org.junit.Assert;
import org.junit.Test;

public class TestBackwardCompatibility extends YarnTestsBaseClass {

	@Test
	public void testBackwardCompatibility10()
			throws Exception {
		DfsCliCommands dfsCommonCliCommands = new DfsCliCommands();
		String user = HadooptestConstants.UserNames.HADOOPQA;
		GenericCliResponseBO genericCliResponse;
		String testName = "testBackwardCompatibility10";
		String timeStamp = "-" + System.currentTimeMillis();
		String dirInHdfs = "/user/" + user + "/" + testName +"-"+ timeStamp;

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

}
