package hadooptest.hadoop.regression.yarn.mapred;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.hadoop.regression.yarn.MapredCliCommands;
import hadooptest.hadoop.regression.yarn.MapredCliCommands.GenericMapredCliResponseBO;
import hadooptest.hadoop.regression.yarn.YarnTestsBaseClass;

import java.util.concurrent.Future;

import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SerialTests.class)
public class TestMapredCli extends YarnTestsBaseClass {
	String uberAppend = "cli-";

	@Test
	public void testGetJobStatusBasedOnJobId() throws Exception {
		String testName = uberAppend + "testGetJobStatusBasedOnJobId";
		String queueToUse = "default";
		String user = HadooptestConstants.UserNames.HADOOPQA;
		Future<Job> handle = submitSingleSleepJobAndGetHandle(queueToUse, user,
				getDefaultSleepJobProps(queueToUse), 1, 1, 10, 1, 10, 1,
				testName, false);
		Job job = handle.get();
		String jobId = job.getJobID().toString();
		MapredCliCommands mapredCliCommands = new MapredCliCommands();
		GenericMapredCliResponseBO genericMapredCliResponseBO = mapredCliCommands
				.getJobStatus(EMPTY_ENV_HASH_MAP, user, jobId);
		Assert.assertTrue(genericMapredCliResponseBO.process.exitValue() == 0);
		TestSession.logger.info("Job status:" + job.getJobState());
	}

	@Test
	public void testGetJobStatusBasedOnCompletedJobId() throws Exception {
		String testName = uberAppend + "testGetJobStatusBasedOnCompletedJobId";
		String queueToUse = "default";
		String user = HadooptestConstants.UserNames.HADOOPQA;
		Future<Job> handle = submitSingleSleepJobAndGetHandle(queueToUse, user,
				getDefaultSleepJobProps(queueToUse), 1, 1, 10, 1, 10, 1,
				testName, false);
		Job job = handle.get();
		String jobId = job.getJobID().toString();
		MapredCliCommands mapredCliCommands = new MapredCliCommands();
		GenericMapredCliResponseBO genericMapredCliResponseBO = mapredCliCommands
				.getJobStatus(EMPTY_ENV_HASH_MAP, user, jobId);
		Assert.assertTrue(genericMapredCliResponseBO.process.exitValue() == 0);
		boolean testConditionMet = false;
		for (int xx = 0; xx < 60; xx++) {
			if (job.getJobState().toString().equalsIgnoreCase("SUCCEEDED")) {
				testConditionMet = true;
                                TestSession.logger.info("Got expected job status after " + xx + " msecs");
				break;
			}
			Thread.sleep(1000);
		}
		Assert.assertTrue(testConditionMet);

                // gridci-1771
                // also need to check after enough time has passed to be sure status is sent
                // from history server, previously status can come right from the RM/AM
                boolean testCondition2Met = false;
                Thread.sleep(30000);
                if (job.getJobState().toString().equalsIgnoreCase("SUCCEEDED")) {
                        testCondition2Met = true;
                        TestSession.logger.info("Still got expected job status after 30 secs");
                }
                Assert.assertTrue(testCondition2Met);

		genericMapredCliResponseBO = mapredCliCommands.getJobStatus(
				EMPTY_ENV_HASH_MAP, user, jobId);
		Assert.assertTrue(genericMapredCliResponseBO.process.exitValue() == 0);
	}

	@Test
	public void testGetJobStatusBasedOnKilledJobId() throws Exception {
		String testName = uberAppend + "testGetJobStatusBasedOnKilledJobId";
		String queueToUse = "default";
		String user = HadooptestConstants.UserNames.HADOOPQA;
		Future<Job> handle = submitSingleSleepJobAndGetHandle(queueToUse, user,
				getDefaultSleepJobProps(queueToUse), 1, 1, 10, 1, 10, 1,
				testName, false);
		Job job = handle.get();
		String jobId = job.getJobID().toString();
		handle.get().killJob();
		MapredCliCommands mapredCliCommands = new MapredCliCommands();
		GenericMapredCliResponseBO genericMapredCliResponseBO = mapredCliCommands
				.getJobStatus(EMPTY_ENV_HASH_MAP, user, jobId);
		Assert.assertTrue(genericMapredCliResponseBO.process.exitValue() == 0);
	}

	@Test
	public void testGetJobStatusBasedOnFailedJobId() throws Exception {
		String testName = uberAppend + "testGetJobStatusBasedOnFailedJobId";
		String queueToUse = "nonExistentQueue";
		String user = HadooptestConstants.UserNames.HADOOPQA;
		Future<Job> handle = submitSingleSleepJobAndGetHandle(queueToUse, user,
				getDefaultSleepJobProps(queueToUse), 1, 1, 10, 1, 10, 1,
				testName, true);
		Job job = handle.get();
		String jobId = job.getJobID().toString();
		MapredCliCommands mapredCliCommands = new MapredCliCommands();
		GenericMapredCliResponseBO genericMapredCliResponseBO = mapredCliCommands
				.getJobStatus(EMPTY_ENV_HASH_MAP, user, jobId);
		Assert.assertTrue(genericMapredCliResponseBO.process.exitValue() == 0);
		genericMapredCliResponseBO.response.contains("Job state: FAILED");
	}

	@Test
	public void testKillJobValidJobIdInRunningState() throws Exception {
		String testName = uberAppend + "testKillJobValidJobIdInRunningState";
		String queueToUse = "default";
		String user = HadooptestConstants.UserNames.HADOOPQA;
		Future<Job> handle = submitSingleSleepJobAndGetHandle(queueToUse, user,
				getDefaultSleepJobProps(queueToUse), 1, 1, 30, 2, 30, 2,
				testName, true);
		Job job = handle.get();
		String jobId = job.getJobID().toString();
		boolean testConditionMet = false;
		for (int xx = 0; xx < 60; xx++) {
			if (job.getStatus().getState() == State.RUNNING) {
				testConditionMet = true;
				break;
			}
			Thread.sleep(1000);
		}
		if (testConditionMet) {
			job.killJob();
		} else {
			// Job was not in the desired state
			Assert.fail("Could not kill the running job");
		}

		MapredCliCommands mapredCliCommands = new MapredCliCommands();
		GenericMapredCliResponseBO genericMapredCliResponseBO = mapredCliCommands
				.getJobStatus(EMPTY_ENV_HASH_MAP, user, jobId);
		Assert.assertTrue(genericMapredCliResponseBO.process.exitValue() == 0);
		genericMapredCliResponseBO.response.contains("Job state: KILLED");
	}

	@Test
	public void testKillJobValidJobIdInKilledState() throws Exception {
		String testName = uberAppend + "testKillJobValidJobIdInKilledState";
		String queueToUse = "default";
		String user = HadooptestConstants.UserNames.HADOOPQA;
		Future<Job> handle = submitSingleSleepJobAndGetHandle(queueToUse, user,
				getDefaultSleepJobProps(queueToUse), 1, 1, 30, 2, 30, 2,
				testName, true);
		Job job = handle.get();
		String jobId = job.getJobID().toString();
		boolean testConditionMet = false;
		for (int xx = 0; xx < 60; xx++) {
			if (job.getStatus().getState() == State.RUNNING) {
				testConditionMet = true;
				break;
			}
			Thread.sleep(1000);
		}
		if (testConditionMet) {
			job.killJob();
		} else {
			// Job was not in the desired state
			Assert.fail("Could not kill the running job");
		}

		MapredCliCommands mapredCliCommands = new MapredCliCommands();
		GenericMapredCliResponseBO genericMapredCliResponseBO = mapredCliCommands
				.killJob(EMPTY_ENV_HASH_MAP, user, jobId);
		Assert.assertTrue(genericMapredCliResponseBO.process.exitValue() == 0);
		genericMapredCliResponseBO.response.contains("Killed job");
	}

	@Test
	public void testKillJobIdAlreadyFailedJob() throws Exception {
		String testName = uberAppend + "testGetJobStatusBasedOnFailedJobId";
		String queueToUse = "nonExistentQueue";
		String user = HadooptestConstants.UserNames.HADOOPQA;
		Future<Job> handle = submitSingleSleepJobAndGetHandle(queueToUse, user,
				getDefaultSleepJobProps(queueToUse), 1, 1, 10, 1, 10, 1,
				testName, true);
		Job job = handle.get();
		String jobId = job.getJobID().toString();
		MapredCliCommands mapredCliCommands = new MapredCliCommands();
		GenericMapredCliResponseBO genericMapredCliResponseBO = mapredCliCommands
				.getJobStatus(EMPTY_ENV_HASH_MAP, user, jobId);
		genericMapredCliResponseBO = mapredCliCommands.killJob(EMPTY_ENV_HASH_MAP,
				user, jobId);
		Assert.assertTrue(genericMapredCliResponseBO.process.exitValue() == 0);
		genericMapredCliResponseBO.response.contains("Killed job");

	}

	@Test
	public void testGetJobListWithNoRunningJobs() throws Exception {
		String testName = uberAppend + "testGetJobListWithNoRunningJobs";
		Cluster cluster = new Cluster(TestSession.cluster.getConf());
		for (JobStatus aJobStatus : cluster.getAllJobStatuses()) {
			cluster.getJob(aJobStatus.getJobID()).killJob();
		}
		MapredCliCommands mapredCliCommands = new MapredCliCommands();
		GenericMapredCliResponseBO genericMapredCliResponseBO = mapredCliCommands
				.jobList(EMPTY_ENV_HASH_MAP,
						HadooptestConstants.UserNames.HADOOPQA, "");
		Assert.assertTrue(genericMapredCliResponseBO.process.exitValue() == 0);
		String[] lines = genericMapredCliResponseBO.response.split("\n");
		int lineCount = lines.length;
		int idx = 0;
		for (String line : lines) {
			TestSession.logger.info("[index:" + idx + "] line[" + line + "]");
		}
		Assert.assertTrue(lines[lineCount - 1]
				.contains("JobId	     State	     StartTime	    UserName	       Queue	  Priority	 UsedContainers	 RsvdContainers	 UsedMem	 RsvdMem	 NeededMem	   AM info"));

	}

	@Test
	public void testGetJobListWithOneRunningJob() throws Exception {
		String testName = uberAppend + "testGetJobListWithOneRunningJob";
		String queueToUse = "default";
		String user = HadooptestConstants.UserNames.HADOOPQA;
		Cluster cluster = new Cluster(TestSession.cluster.getConf());
		for (JobStatus aJobStatus : cluster.getAllJobStatuses()) {
			cluster.getJob(aJobStatus.getJobID()).killJob();
		}

		Future<Job> handle = submitSingleSleepJobAndGetHandle(queueToUse, user,
				getDefaultSleepJobProps(queueToUse), 1, 1, 30, 1, 30, 1,
				testName, false);
		Job job = handle.get();
		String jobId = job.getJobID().toString();

		boolean testConditionMet = false;
		for (int xx = 0; xx < 60; xx++) {
			if (job.getStatus().getState() == State.RUNNING) {
				testConditionMet = true;
				break;
			}
			Thread.sleep(1000);
		}

		MapredCliCommands mapredCliCommands = new MapredCliCommands();
		GenericMapredCliResponseBO genericMapredCliResponseBO = mapredCliCommands
				.jobList(EMPTY_ENV_HASH_MAP,
						HadooptestConstants.UserNames.HADOOPQA, "");
		Assert.assertTrue(genericMapredCliResponseBO.process.exitValue() == 0);
		testConditionMet = false;
		String[] lines = genericMapredCliResponseBO.response.split("\n");

		for (String line : lines) {
			if (line.contains(jobId)) {
				Assert.assertTrue(line.contains("RUNNING"));
				testConditionMet = true;
				break;

			}
		}
		Assert.assertTrue(testConditionMet);
	}

	@Test
	public void testGetJobListWithOneKilledJob() throws Exception {
		String testName = uberAppend + "testGetJobListWithOneKilledJob";
		String queueToUse = "default";
		String user = HadooptestConstants.UserNames.HADOOPQA;
		Cluster cluster = new Cluster(TestSession.cluster.getConf());
		for (JobStatus aJobStatus : cluster.getAllJobStatuses()) {
			cluster.getJob(aJobStatus.getJobID()).killJob();
		}

		Future<Job> handle = submitSingleSleepJobAndGetHandle(queueToUse, user,
				getDefaultSleepJobProps(queueToUse), 1, 1, 10, 1, 10, 1,
				testName, false);
		Job job = handle.get();
		String jobId = job.getJobID().toString();

		boolean testConditionMet = false;
		for (int xx = 0; xx < 60; xx++) {
			if (job.getStatus().getState() == State.RUNNING) {
				testConditionMet = true;
				break;
			}
			Thread.sleep(1000);
		}
		job.killJob();
		MapredCliCommands mapredCliCommands = new MapredCliCommands();
		GenericMapredCliResponseBO genericMapredCliResponseBO = mapredCliCommands
				.jobList(EMPTY_ENV_HASH_MAP,
						HadooptestConstants.UserNames.HADOOPQA, "all");
		Assert.assertTrue(genericMapredCliResponseBO.process.exitValue() == 0);
		testConditionMet = false;
		String[] lines = genericMapredCliResponseBO.response.split("\n");

		for (String line : lines) {
			if (line.contains(jobId)) {
				Assert.assertTrue(line.contains("KILLED"));
				testConditionMet = true;
				break;

			}
		}
		Assert.assertTrue(testConditionMet);
	}

	@Test
	public void testGetJobListWithOneCompletedJob() throws Exception {
		String testName = uberAppend + "testGetJobListWithOneCompletedJob";

		String queueToUse = "default";
		String user = HadooptestConstants.UserNames.HADOOPQA;
		Future<Job> handle = submitSingleSleepJobAndGetHandle(queueToUse, user,
				getDefaultSleepJobProps(queueToUse), 1, 1, 1, 1, 1, 1,
				testName, false);
		Job job = handle.get();
		String jobId = job.getJobID().toString();
		TestSession.logger.info("Got job Id:" + jobId);

		boolean testConditionMet = false;
		waitTillJobSucceeds(job);
		MapredCliCommands mapredCliCommands = new MapredCliCommands();
		GenericMapredCliResponseBO genericMapredCliResponseBO = mapredCliCommands
				.jobList(EMPTY_ENV_HASH_MAP,
						HadooptestConstants.UserNames.HADOOPQA, "all");
		Assert.assertTrue(genericMapredCliResponseBO.process.exitValue() == 0);
		testConditionMet = false;
		String[] lines = genericMapredCliResponseBO.response.split("\n");

		for (String line : lines) {
			if (line.contains(jobId)) {
				Assert.assertTrue(line.contains("SUCCEEDED"));
				testConditionMet = true;
				break;

			}
		}
		Assert.assertTrue(testConditionMet);
	}

	@Test
	public void testGetJobListWithAllCompletedJob() throws Exception {
		String testName = uberAppend + "testGetJobListWithAllCompletedJob";

		String queueToUse = "default";
		String user = HadooptestConstants.UserNames.HADOOPQA;
		Future<Job> handle = submitSingleSleepJobAndGetHandle(queueToUse, user,
				getDefaultSleepJobProps(queueToUse), 1, 1, 10, 1, 10, 1,
				testName, false);
		Job job = handle.get();
		String jobId = job.getJobID().toString();

		boolean testConditionMet = false;
		for (int xx = 0; xx < 60; xx++) {
			if (job.getStatus().getState() == State.SUCCEEDED) {
				testConditionMet = true;
				break;
			}
			Thread.sleep(1000);
		}
		MapredCliCommands mapredCliCommands = new MapredCliCommands();
		GenericMapredCliResponseBO genericMapredCliResponseBO = mapredCliCommands
				.jobList(EMPTY_ENV_HASH_MAP,
						HadooptestConstants.UserNames.HADOOPQA, "all");
		Assert.assertTrue(genericMapredCliResponseBO.process.exitValue() == 0);
		testConditionMet = false;
		String[] lines = genericMapredCliResponseBO.response.split("\n");

		for (String line : lines) {
			if (line.contains(jobId)) {
				Assert.assertTrue(line.contains("SUCCEEDED"));
				testConditionMet = true;
				break;

			}
		}
		Assert.assertTrue(testConditionMet);
	}

}
