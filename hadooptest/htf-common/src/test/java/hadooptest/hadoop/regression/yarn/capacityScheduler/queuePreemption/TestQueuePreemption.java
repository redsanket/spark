package hadooptest.hadoop.regression.yarn.capacityScheduler.queuePreemption;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.hadoop.regression.yarn.capacityScheduler.CalculatedCapacityLimitsBO;
import hadooptest.hadoop.regression.yarn.capacityScheduler.CapacitySchedulerBaseClass;
import hadooptest.hadoop.regression.yarn.capacityScheduler.RuntimeRESTStatsBO;
import hadooptest.hadoop.regression.yarn.capacityScheduler.SchedulerRESTStatsSnapshot.LeafQueue;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SerialTests.class)
public class TestQueuePreemption extends CapacitySchedulerBaseClass {

	// @Test
	public void testQueuePreemption1() throws Exception {
		Queue<Job> submittedJobsQueueA = new ConcurrentLinkedQueue<Job>();
		Queue<Job> submittedJobsQueueB = new ConcurrentLinkedQueue<Job>();

		// copyResMgrConfigAndRestartNodes(TestSession.conf
		// .getProperty("WORKSPACE")
		// + "/htf-common/resources/hadooptest"
		// + "/hadoop/regression/yarn/capacityScheduler"
		// + "/queuePreemptionCapacitySchedulerLimits.xml");
		JobClient jobClient = new JobClient(TestSession.cluster.getConf());
		resetTheMaxQueueCapacity();
		CalculatedCapacityLimitsBO calculatedCapacityBO = selfCalculateCapacityLimits();
		printSelfCalculatedStats(calculatedCapacityBO);
		List<RunnableWordCount> runnableWordCountsA = new ArrayList<RunnableWordCount>();
		List<RunnableWordCount> runnableWordCountsB = new ArrayList<RunnableWordCount>();
		String[] users = new String[] { HadooptestConstants.UserNames.HITUSR_1
		// ,HadooptestConstants.UserNames.HITUSR_2,
		// HadooptestConstants.UserNames.HITUSR_3,HadooptestConstants.UserNames.HITUSR_4
		};

		for (String aUser : users) {
			TestSession.logger.info("Adding job to run for user:" + aUser);
			RunnableWordCount aWordCountToRun = new RunnableWordCount(aUser,
					"a", submittedJobsQueueA, "/tmp/bigfile4.txt");
			runnableWordCountsA.add(aWordCountToRun);
		}

		ExecutorService execServiceA = submitWordCount(runnableWordCountsA);
		TestSession.logger.info("after submitWordCount A");
		Thread.sleep(60000);
		for (String aUser : users) {
			TestSession.logger.info("Adding 2nd job to run for user:" + aUser);
			RunnableWordCount aWordCountToRun = new RunnableWordCount(aUser,
					"b", submittedJobsQueueB, "/tmp/bigfile4.txt");
			runnableWordCountsB.add(aWordCountToRun);
		}
		ExecutorService execServiceB = submitWordCount(runnableWordCountsB);
		Thread.sleep(60000);

		TestSession.logger.info("after submitWordCount B");
		TestSession.logger.info("submittedJobsQueueA size:"
				+ submittedJobsQueueA.size());
		TestSession.logger.info("submittedJobsQueueB size:"
				+ submittedJobsQueueB.size());
		Job aJobThatRan = submittedJobsQueueA.poll();
		TestSession.logger.info("jobId:" + aJobThatRan.getJobID());
		TestSession.logger.info("jobName:" + aJobThatRan.getJobName());
		TaskReport[] mapTaskReports = aJobThatRan.getTaskReports(TaskType.MAP);

		int numMapTasks = aJobThatRan.getTaskReports(TaskType.MAP).length;

		RuntimeRESTStatsBO runtimeRESTStatsBO = startCollectingRestStats(2 * 1000);
		waitFor(10 * 1000);
		stopCollectingRuntimeStatsAcrossQueues(runtimeRESTStatsBO);
		LeafQueue leafQueueSnapshotWithHighestMemUtil = getLeafSnapshotWithHighestMemUtil(
				"a", runtimeRESTStatsBO);
		TestSession.logger.info("Abs capacity:"
				+ leafQueueSnapshotWithHighestMemUtil.absoluteCapacity);
		TestSession.logger.info("Abs absoluteMaxCapacity:"
				+ leafQueueSnapshotWithHighestMemUtil.absoluteMaxCapacity);
		TestSession.logger.info("Abs absoluteUsedCapacity:"
				+ leafQueueSnapshotWithHighestMemUtil.absoluteUsedCapacity);
		TestSession.logger.info("Capacity:"
				+ leafQueueSnapshotWithHighestMemUtil.capacity);
		TestSession.logger.info("Max capacity:"
				+ leafQueueSnapshotWithHighestMemUtil.maxCapacity);
		TestSession.logger.info("QueueName:"
				+ leafQueueSnapshotWithHighestMemUtil.queueName);
		TestSession.logger.info("Used capacity:"
				+ leafQueueSnapshotWithHighestMemUtil.usedCapacity);

		// if (leafQueueSnapshotWithHighestMemUtil.usedCapacity > 100) {
		// int overagePercent = (int)
		// (leafQueueSnapshotWithHighestMemUtil.usedCapacity - 100);
		// int countOfTasksToKill = (overagePercent * numMapTasks) / 100;
		// for (int xx = 0; countOfTasksToKill > 0; countOfTasksToKill--, xx++)
		// {
		//
		// for (TaskAttemptID aTaskAttemptId : mapTaskReports[xx]
		// .getRunningTaskAttemptIds()) {
		// aJobThatRan.killTask(aTaskAttemptId);
		// TestSession.logger
		// .info("Killed map task:" + aTaskAttemptId);
		// }
		//
		// }
		// }

		while (!execServiceA.isTerminated() && !execServiceB.isTerminated()) {
			Thread.sleep(1000);
		}

	}

	@Test
	public void testQueuePreemption() throws Exception {
		Queue<Job> submittedJobsQueueA = new ConcurrentLinkedQueue<Job>();
		Queue<Job> submittedJobsQueueB = new ConcurrentLinkedQueue<Job>();
		Queue<Job> submittedJobsQueueC = new ConcurrentLinkedQueue<Job>();

		// copyResMgrConfigAndRestartNodes(TestSession.conf
		// .getProperty("WORKSPACE")
		// + "/htf-common/resources/hadooptest"
		// + "/hadoop/regression/yarn/capacityScheduler"
		// + "/queuePreemptionCapacitySchedulerLimits.xml");
		JobClient jobClient = new JobClient(TestSession.cluster.getConf());
		resetTheMaxQueueCapacity();
		CalculatedCapacityLimitsBO calculatedCapacityBO = selfCalculateCapacityLimits();
		printSelfCalculatedStats(calculatedCapacityBO);
		List<RunnableWordCount> runnableWordCountsA = new ArrayList<RunnableWordCount>();
		List<RunnableWordCount> runnableWordCountsB = new ArrayList<RunnableWordCount>();
		List<RunnableWordCount> runnableWordCountsC = new ArrayList<RunnableWordCount>();
		
		String user = HadooptestConstants.UserNames.HITUSR_1;

		TestSession.logger.info("Adding 1st job to run for user:" + user);
		RunnableWordCount aWordCountToRun = new RunnableWordCount(user, "a",
				submittedJobsQueueA, "/tmp/bigfile4.txt");
		runnableWordCountsA.add(aWordCountToRun);

		ExecutorService execServiceA = submitWordCount(runnableWordCountsA);
		TestSession.logger.info("after submitWordCount A");
		Thread.sleep(60000);
		
		user = HadooptestConstants.UserNames.HITUSR_2;
		TestSession.logger.info("Adding 2nd job to run for user:" + user);
		aWordCountToRun = new RunnableWordCount(user, "b", submittedJobsQueueB,
				"/tmp/smallFileForQPreemptionTest.txt");
		runnableWordCountsB.add(aWordCountToRun);

		ExecutorService execServiceB = submitWordCount(runnableWordCountsB);
		Thread.sleep(60000);
		
		user = HadooptestConstants.UserNames.HITUSR_3;
		TestSession.logger.info("Adding 3rd job to run for user:" + user);
		aWordCountToRun = new RunnableWordCount(user, "c", submittedJobsQueueC,
				"/tmp/bigfile4.txt");
		runnableWordCountsC.add(aWordCountToRun);

		ExecutorService execServiceC = submitWordCount(runnableWordCountsC);

		while (!execServiceA.isTerminated() || !execServiceB.isTerminated() || !execServiceC.isTerminated()) {
			Thread.sleep(1000);
		}

	}

	public ExecutorService submitWordCount(List<RunnableWordCount> runMe)
			throws ClassNotFoundException, IOException, InterruptedException {

		ExecutorService wordCountThreadPool = Executors.newFixedThreadPool(1);
		for (RunnableWordCount aRunner : runMe) {
			TestSession.logger.info("Submitting job for " + aRunner.user);
			wordCountThreadPool.submit(aRunner);
		}

		wordCountThreadPool.shutdown();
		return wordCountThreadPool;

	}

	class RunnableWordCount implements Runnable {
		String user;
		String queueToSubmitJob;
		Queue<Job> liveJobsQueue;
		String inputFile;

		RunnableWordCount(String user, String queueToSubmitJob,
				Queue<Job> liveJobsQueue, String inputFile) {
			this.user = user;
			this.queueToSubmitJob = queueToSubmitJob;
			this.liveJobsQueue = liveJobsQueue;
			this.inputFile = inputFile;

		}

		public void run() {
			TestSession.logger.info("In run of RunnableWordCount");
			UserGroupInformation ugi = getUgiForUser(user);
			try {
				DoAs doAs = new DoAs(ugi, queueToSubmitJob, liveJobsQueue,
						inputFile);
				doAs.doAction();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	private class DoAs {
		UserGroupInformation ugi;
		String queueToSubmitJob;
		Queue<Job> liveJobsQueue;
		String inputFile;

		DoAs(UserGroupInformation ugi, String queueToSubmitJob,
				Queue<Job> liveJobsQueue, String inputFile) throws IOException {
			this.ugi = ugi;
			this.queueToSubmitJob = queueToSubmitJob;
			this.liveJobsQueue = liveJobsQueue;
			this.inputFile = inputFile;
		}

		public void doAction() throws AccessControlException, IOException,
				InterruptedException {
			PrivilegedExceptionActionImpl privilegedExceptionAction = new PrivilegedExceptionActionImpl(
					ugi, queueToSubmitJob, liveJobsQueue, inputFile);
			TestSession.logger
					.info("About to submit job to privilegedExceptionAction");
			ugi.doAs(privilegedExceptionAction);

		}

	}

	private class PrivilegedExceptionActionImpl implements
			PrivilegedExceptionAction<String> {
		UserGroupInformation ugi;
		String queueToSubmitJob;
		Queue<Job> liveJobsQueue;
		String inputFile;

		PrivilegedExceptionActionImpl(UserGroupInformation ugi,
				String queueToSubmitJob, Queue<Job> liveJobsQueue,
				String inputFile) throws IOException {
			this.ugi = ugi;
			this.queueToSubmitJob = queueToSubmitJob;
			this.liveJobsQueue = liveJobsQueue;
			this.inputFile = inputFile;
		}

		public String run() throws Exception {
			WordCountForQueuePreemmption wordCountForQueuePreemption = new WordCountForQueuePreemmption(
					queueToSubmitJob);
			Configuration conf = new Configuration();
			TestSession.logger
					.info("In privilegedExceptionAction about to run wordCountForQueuePreemption now...");
			wordCountForQueuePreemption.run(
					inputFile,
					"/tmp/qPrempt/" + ugi.getUserName() + "/"
							+ System.currentTimeMillis(), conf);
			TestSession.logger.info("Got job handle:"
					+ wordCountForQueuePreemption.jobHandle);
			Job job = wordCountForQueuePreemption.jobHandle;
			liveJobsQueue.add(job);
			TestSession.logger.info("Added job to queue, size:"
					+ liveJobsQueue.size());
			return "";

		}
	}

	UserGroupInformation getUgiForUser(String aUser) {
		String keyTabLocation = null;
		TestSession.logger.info("Entering UGI....");
		if (aUser.equalsIgnoreCase(HadooptestConstants.UserNames.HITUSR_1)) {
			keyTabLocation = HadooptestConstants.Location.Keytab.HITUSR_1;
		} else if (aUser
				.equalsIgnoreCase(HadooptestConstants.UserNames.HITUSR_2)) {
			keyTabLocation = HadooptestConstants.Location.Keytab.HITUSR_2;
		} else if (aUser
				.equalsIgnoreCase(HadooptestConstants.UserNames.HITUSR_3)) {
			keyTabLocation = HadooptestConstants.Location.Keytab.HITUSR_3;
		} else if (aUser
				.equalsIgnoreCase(HadooptestConstants.UserNames.HITUSR_3)) {
			keyTabLocation = HadooptestConstants.Location.Keytab.HITUSR_4;
		}
		UserGroupInformation ugi;
		try {

			ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(aUser,
					keyTabLocation);

		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		TestSession.logger.info("Got UGI for user:" + ugi.getUserName());
		return ugi;
	}

	@AfterClass
	public static void restoreTheConfigFile() throws Exception {
		// Override
	}
}
