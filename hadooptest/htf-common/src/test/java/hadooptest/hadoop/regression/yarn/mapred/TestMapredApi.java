package hadooptest.hadoop.regression.yarn.mapred;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.hadoop.regression.yarn.YarnTestsBaseClass;

import java.util.concurrent.Future;

import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SerialTests.class)
public class TestMapredApi extends YarnTestsBaseClass {
	String uberAppend = "api-";

	@Test
	public void testGetJobStatusBasedOnJobId() throws Exception {
		String testName = uberAppend + "testGetJobStatusBasedOnJobId";
		String queueToUse = "default";
		String user = HadooptestConstants.UserNames.HADOOPQA;
		killAllJobs();
		Future<Job> handle = submitSingleSleepJobAndGetHandle(queueToUse, user,
				getDefaultSleepJobProps(queueToUse), 1, 1, 10, 1, 10, 1,
				testName, false);
		Job job = handle.get();
		TestSession.logger.info("API job state:" + job.getJobState());

	}

	@Test
	public void testGetJobStatusBasedOnCompletedJobId() throws Exception {
		String testName = uberAppend + "testGetJobStatusBasedOnCompletedJobId";
		String queueToUse = "default";
		String user = HadooptestConstants.UserNames.HADOOPQA;
		killAllJobs();
		Future<Job> handle = submitSingleSleepJobAndGetHandle(queueToUse, user,
				getDefaultSleepJobProps(queueToUse), 1, 1, 10, 1, 10, 1,
				testName, false);
		Job job = handle.get();
		TestSession.logger.info("API job state:" + job.getJobState());
		boolean testConditionMet = false;
		for (int xx = 0; xx < 60; xx++) {
			if (job.getStatus().getState() == State.SUCCEEDED) {
				testConditionMet = true;
			}
			Thread.sleep(1000);
		}

	}

	@Test
	public void testGetJobStatusBasedOnKilledJobId() throws Exception {
		String testName = uberAppend + "testGetJobStatusBasedOnKilledJobId";
		String queueToUse = "default";
		String user = HadooptestConstants.UserNames.HADOOPQA;
		killAllJobs();
		Future<Job> handle = submitSingleSleepJobAndGetHandle(queueToUse, user,
				getDefaultSleepJobProps(queueToUse), 1, 1, 10, 1, 10, 1,
				testName, false);
		Job job = handle.get();
		TestSession.logger.info("API job state:" + job.getJobState());
		job.killJob();
		Assert.assertTrue("Expected Job state:" + State.KILLED
				+ " while obtained state:" + job.getStatus().getState(), job
				.getStatus().getState() == State.KILLED);

	}

	@Test
	public void testGetJobStatusBasedOnFailedJobId() throws Exception {
		String testName = uberAppend + "testGetJobStatusBasedOnFailedJobId";
		String queueToUse = "nonExistentQueue";
		String user = HadooptestConstants.UserNames.HADOOPQA;
		killAllJobs();
		Future<Job> handle = submitSingleSleepJobAndGetHandle(queueToUse, user,
				getDefaultSleepJobProps(queueToUse), 1, 1, 10, 1, 10, 1,
				testName, true);
		Job job = handle.get();
		for (int xx=0;xx<60;xx++){
			Thread.sleep(1000);
			if (job.getStatus().getState() == State.FAILED)
				break;
		}
		
		TestSession.logger.info("API job state:" + job.getJobState());
		Assert.assertTrue(job.getStatus().getState() == State.FAILED);

	}

	@Test
	public void testKillJobValidJobIdInRunningState() throws Exception {
		String testName = uberAppend + "testKillJobValidJobIdInRunningState";
		String queueToUse = "default";
		String user = HadooptestConstants.UserNames.HADOOPQA;
		killAllJobs();
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
			Assert.assertTrue(job.getStatus().getState() == State.KILLED);
		} else {
			// Test Condition not met
			Assert.fail("Could not kill a runnig job");
		}

	}

	@Test
	public void testKillJobValidJobIdInKilledState() throws Exception {
		String testName = uberAppend + "testKillJobValidJobIdInKilledState";
		String queueToUse = "default";
		String user = HadooptestConstants.UserNames.HADOOPQA;
		killAllJobs();
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
			Assert.assertTrue(job.getStatus().getState() == State.KILLED);
			// Kill it again
			job.killJob();
			Assert.assertTrue(job.getStatus().getState() == State.KILLED);
		} else {
			// Test Condition not met
			Assert.fail("Could not kill a runnig job");
		}

	}

	@Test
	public void testKillJobIdAlreadyFailedJob() throws Exception {
		String testName = uberAppend + "testKillJobIdAlreadyFailedJob";
		String queueToUse = "nonExistentQueue";
		String user = HadooptestConstants.UserNames.HADOOPQA;
		killAllJobs();
		Future<Job> handle = submitSingleSleepJobAndGetHandle(queueToUse, user,
				getDefaultSleepJobProps(queueToUse), 1, 1, 10, 1, 10, 1,
				testName, true);
		Job job = handle.get();
		for (int xx=0;xx<60;xx++){
			Thread.sleep(1000);
			if (job.getStatus().getState() == State.FAILED)
				break;
		}
		TestSession.logger.info("API job state:" + job.getJobState());
		Assert.assertTrue(job.getStatus().getState() == State.FAILED);
		try {
			job.killJob();
		} catch (Exception e) {
			Assert.fail("Exception not expected while killing a Failed job");
		}
		TestSession.logger.info("Test status, after killing a failed job:"
				+ job.getStatus());

	}

	@Test
	public void testKillJobIdAlreadyCompletedJob() throws Exception {
		String testName = uberAppend + "testKillJobIdAlreadyCompletedJob";
		String queueToUse = "default";
		String user = HadooptestConstants.UserNames.HADOOPQA;
		killAllJobs();
		Future<Job> handle = submitSingleSleepJobAndGetHandle(queueToUse, user,
				getDefaultSleepJobProps(queueToUse), 1, 1, 10, 1, 10, 1,
				testName, false);
		Job job = handle.get();
		TestSession.logger.info("API job state:" + job.getJobState());
		boolean testConditionMet = false;
		for (int xx = 0; xx < 60; xx++) {
			if (job.getStatus().getState() == State.SUCCEEDED) {
				testConditionMet = true;
			}
			Thread.sleep(1000);
		}
		if (testConditionMet) {
			try {
				job.killJob();
			} catch (Exception e) {
				Assert.fail("Exception not expected while killing a completed job");
			}
		}

	}

	@Ignore("Not a good candidate to run, as cannot do 1:1 comparision with its CLI counterpart. cluster.getAllJobStatuses() would return statuses of any job artifacts.")
	@Test
	public void testGetJobListWithNoRunningJobs() throws Exception {
	}

	@Test
	public void testGetJobListWithOneRunningJob() throws Exception {
		String testName = uberAppend + "testGetJobListWithOneRunningJob";
		String queueToUse = "default";
		String user = HadooptestConstants.UserNames.HADOOPQA;
		killAllJobs();
		Cluster cluster = new Cluster(TestSession.cluster.getConf());
		for (JobStatus aJobStatus : cluster.getAllJobStatuses()) {
			cluster.getJob(aJobStatus.getJobID()).killJob();
		}

		Future<Job> handle = submitSingleSleepJobAndGetHandle(queueToUse, user,
				getDefaultSleepJobProps(queueToUse), 1, 1, 30, 1, 30, 1,
				testName, false);
		Job job = handle.get();
		cluster = new Cluster(TestSession.cluster.getConf());
		for (org.apache.hadoop.mapreduce.JobStatus jobStatus : cluster
				.getAllJobStatuses()) {
			if (jobStatus.getJobID() == job.getJobID()) {
				Assert.assertTrue(jobStatus.getState() == State.RUNNING);
			}
		}

	}

	@Test
	public void testGetJobListWithOneKilledJob() throws Exception {
		String testName = uberAppend + "testGetJobListWithOneKilledJob";
		String queueToUse = "default";
		String user = HadooptestConstants.UserNames.HADOOPQA;
		killAllJobs();
		Future<Job> handle = submitSingleSleepJobAndGetHandle(queueToUse, user,
				getDefaultSleepJobProps(queueToUse), 1, 1, 30, 1, 30, 1,
				testName, false);
		Job job = handle.get();
		job.killJob();
		Cluster cluster = new Cluster(TestSession.cluster.getConf());
		for (org.apache.hadoop.mapreduce.JobStatus jobStatus : cluster
				.getAllJobStatuses()) {
			if (jobStatus.getJobID() == job.getJobID()) {
				Assert.assertTrue(jobStatus.getState() == State.KILLED);
			}
		}

	}

	@Test
	public void testGetJobListWithOneCompletedJob() throws Exception {
		String testName = uberAppend + "testGetJobListWithOneCompletedJob";
		String queueToUse = "default";
		String user = HadooptestConstants.UserNames.HADOOPQA;
		killAllJobs();
		Future<Job> handle = submitSingleSleepJobAndGetHandle(queueToUse, user,
				getDefaultSleepJobProps(queueToUse), 1, 1, 30, 1, 30, 1,
				testName, false);
		boolean testConditionMet = false;
		Job job = handle.get();
		for (int xx = 0; xx < 60; xx++) {
			if (job.getStatus().getState() == State.SUCCEEDED) {
				testConditionMet = true;
				break;
			}
			Thread.sleep(1000);
		}
		Assert.assertTrue(testConditionMet);
		Cluster cluster = new Cluster(TestSession.cluster.getConf());
		for (org.apache.hadoop.mapreduce.JobStatus jobStatus : cluster
				.getAllJobStatuses()) {
			if (jobStatus.getJobID() == job.getJobID()) {
				Assert.assertTrue(jobStatus.getState() == State.SUCCEEDED);
			}
		}

	}

}
