package hadooptest.hadoop.regression.yarn.capacityScheduler;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.cluster.hadoop.HadoopCluster.Action;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass;
import hadooptest.hadoop.regression.yarn.YarnTestsBaseClass;
import hadooptest.hadoop.regression.yarn.capacityScheduler.RuntimeStatsBO.JobStats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.RandomWriter;
import org.apache.hadoop.examples.Sort;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.mapreduce.SleepJob;
import org.junit.After;
import org.junit.Assert;

public class CapacitySchedulerBaseClass extends YarnTestsBaseClass {
	public static String CAPACITY_SCHEDULER_XML = "capacity-scheduler.xml";
	int NUM_THREADS = 10;
	int SLEEP_JOB_DURATION_IN_SECS = 20;

	public enum SingleUser {
		YES, NO
	};

	void copyResMgrConfigAndRestartNodes(String replacementConfigFile)
			throws Exception {
		FullyDistributedCluster fullyDistributedCluster = (FullyDistributedCluster) TestSession
				.getCluster();

		// Backup config and replace file, for Resource Manager
		fullyDistributedCluster.getConf(
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER).backupConfDir();
		fullyDistributedCluster.getConf(
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER)
				.copyFileToConfDir(replacementConfigFile,
						CAPACITY_SCHEDULER_XML);

		// Bounce node
		fullyDistributedCluster.hadoopDaemon(Action.STOP,
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
		fullyDistributedCluster.hadoopDaemon(Action.START,
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER);

		Thread.sleep(20000);
		// Leave safe-mode
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;
		genericCliResponse = dfsCliCommands.dfsadmin(
				DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
				DfsTestsBaseClass.Report.NO, "get",
				DfsTestsBaseClass.ClearQuota.NO, DfsTestsBaseClass.SetQuota.NO,
				0, DfsTestsBaseClass.ClearSpaceQuota.NO,
				DfsTestsBaseClass.SetSpaceQuota.NO, 0,
				DfsTestsBaseClass.PrintTopology.NO, null);
		genericCliResponse = dfsCliCommands.dfsadmin(
				DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
				DfsTestsBaseClass.Report.NO, "leave",
				DfsTestsBaseClass.ClearQuota.NO, DfsTestsBaseClass.SetQuota.NO,
				0, DfsTestsBaseClass.ClearSpaceQuota.NO,
				DfsTestsBaseClass.SetSpaceQuota.NO, 0,
				DfsTestsBaseClass.PrintTopology.NO, null);
		
		Configuration conf = fullyDistributedCluster.getConf(
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
		
		Iterator iter = conf.iterator();
		while (iter.hasNext()){
			Entry<String, String> entry = (Entry<String, String>) iter.next();
			TestSession.logger.info("Key:[" + entry.getKey() + "] Value[" + entry.getValue() +"]");
		}
		

	}

	public RuntimeStatsBO collateRuntimeStatsForJobs(
			ArrayList<Future<Job>> futureCallableSleepJobs)
			throws InterruptedException, ExecutionException {
		RuntimeStatsBO runtimeStats = new RuntimeStatsBO();
		for (Future<Job> aTetherToACallableSleepJob : futureCallableSleepJobs) {
			try{
			runtimeStats.registerJob(aTetherToACallableSleepJob.get());
			} catch (ExecutionException e){
				TestSession.logger.info(e.getCause());
			}
		}
		runtimeStats.startCollectingStats();
		for (JobStats jobStats:runtimeStats.jobStatsSet){
			TestSession.logger.info("Dumping memory for job:" + jobStats.job.getJobID());
			for (Integer mem:jobStats.memoryConsumed){
				TestSession.logger.info("Mem:" + mem);
			}
		}
		return runtimeStats;
	}

	public CalculatedCapacityLimitsBO getCapacityBO() throws Exception {
		FullyDistributedCluster fullyDistributedCluster = (FullyDistributedCluster) TestSession
				.getCluster();
		String dirWhereRMConfHasBeenCopied = fullyDistributedCluster.getConf(
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER)
				.getHadoopConfDir();
		String resourceMgrConfigFilesCopiedBackHereOnGw = fullyDistributedCluster
				.getConf(HadooptestConstants.NodeTypes.RESOURCE_MANAGER)
				.copyRemoteConfDirToLocal(dirWhereRMConfHasBeenCopied,
						HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
		TestSession.logger.info("Copied back files from RM in "
				+ resourceMgrConfigFilesCopiedBackHereOnGw);
		TestSession.logger
				.info("Read back (locally); after copying from RM [yarn.scheduler.capacity.root.default.capacity] ="
						+ lookupValueInBackCopiedCapacitySchedulerXmlFile(
								resourceMgrConfigFilesCopiedBackHereOnGw
										+ "/capacity-scheduler.xml",
								"yarn.scheduler.capacity.root.default.capacity"));
		CalculatedCapacityLimitsBO capacityBO = new CalculatedCapacityLimitsBO(
				resourceMgrConfigFilesCopiedBackHereOnGw
						+ "/capacity-scheduler.xml");
		return capacityBO;

	}

	public HashMap<String, String> getDefaultSleepJobProps() {
		HashMap<String, String> defaultSleepJobProps = new HashMap<String, String>();
		defaultSleepJobProps.put("mapreduce.job.acl-view-job", "*");
		defaultSleepJobProps.put("mapreduce.job.acl-view-job", "2048");
		defaultSleepJobProps.put("mapreduce.map.memory.mb", "1024");
		defaultSleepJobProps.put("mapreduce.reduce.memory.mb", "1024");

		return defaultSleepJobProps;
	}

	public ArrayList<Future<Job>> expandJobsAndSubmitThemForExecution(
			ArrayList<SleepJobParams> sleepJobParamsList) {

		ExecutorService sleepJobThreadPool = Executors
				.newFixedThreadPool(NUM_THREADS);
		ArrayList<Future<Job>> futureCallableSleepJobs = new ArrayList<Future<Job>>();
		int jobNameSuffix = 1;
		for (SleepJobParams aSleepJobParams : sleepJobParamsList) {
			try {
				Thread.sleep(500);
				TestSession.logger
						.info("Staggering launching of jobs, by half sec, else they would step on each others feet");
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			Future<Job> aFutureCallableSleepJob = sleepJobThreadPool
					.submit(new CallableSleepJob(
							aSleepJobParams.configProperties,
							aSleepJobParams.numMapTasks,
							aSleepJobParams.numRedTasks,
							aSleepJobParams.taskSleepDuration,
							aSleepJobParams.numTimesMapWouldSleep,
							aSleepJobParams.taskSleepDuration,
							aSleepJobParams.numTimesRedWouldSleep,
							aSleepJobParams.userName, jobNameSuffix++));
			futureCallableSleepJobs.add(aFutureCallableSleepJob);

		}
		sleepJobThreadPool.shutdown();
		return futureCallableSleepJobs;
	}

//	public double getCapacityLimit(SingleUser singleUser,
//			CalculatedCapacityLimitsBO capacityBO, String queue) {
//		double capacityLimit = 0.0;
//		if (singleUser == SingleUser.YES) {
//			for (QueueCapacityDetail queueCapacityDetail : capacityBO.queueCapacityDetails) {
//				if (!queueCapacityDetail.name.equalsIgnoreCase(queue))
//					continue;
//				capacityLimit = queueCapacityDetail.capacityInTermsOfTotalClusterMemory;
//			}
//		} else {
//			// TODO: Not implemented yet
//		}
//		return capacityLimit;
//	}

	//@After
	public void restoreTheConfigFile() throws Exception {
		FullyDistributedCluster fullyDistributedCluster = (FullyDistributedCluster) TestSession
				.getCluster();
		fullyDistributedCluster.getConf(
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER)
				.resetHadoopConfDir();
		fullyDistributedCluster.getConf(HadooptestConstants.NodeTypes.NAMENODE)
				.resetHadoopConfDir();

		fullyDistributedCluster.hadoopDaemon(Action.STOP,
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
		fullyDistributedCluster.hadoopDaemon(Action.START,
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER);

		Thread.sleep(20000);

		// Leave safe-mode
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;
		genericCliResponse = dfsCliCommands.dfsadmin(
				DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
				DfsTestsBaseClass.Report.NO, "get",
				DfsTestsBaseClass.ClearQuota.NO, DfsTestsBaseClass.SetQuota.NO,
				0, DfsTestsBaseClass.ClearSpaceQuota.NO,
				DfsTestsBaseClass.SetSpaceQuota.NO, 0,
				DfsTestsBaseClass.PrintTopology.NO, null);
		genericCliResponse = dfsCliCommands.dfsadmin(
				DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
				DfsTestsBaseClass.Report.NO, "leave",
				DfsTestsBaseClass.ClearQuota.NO, DfsTestsBaseClass.SetQuota.NO,
				0, DfsTestsBaseClass.ClearSpaceQuota.NO,
				DfsTestsBaseClass.SetSpaceQuota.NO, 0,
				DfsTestsBaseClass.PrintTopology.NO, null);

	}

	class CallableSleepJob implements Callable<Job> {
		HashMap<String, String> jobParamsMap;
		int numMapper;
		int numReducer;
		int mapSleepTime;
		int mapSleepCount;
		int reduceSleepTime;
		int reduceSleepCount;
		String userName;
		int jobName;

		public CallableSleepJob(HashMap<String, String> jobParamsMap,
				int numMapper, int numReducer, int mapSleepTime,
				int mapSleepCount, int reduceSleepTime, int reduceSleepCount,
				String userName, int jobName) {
			this.jobParamsMap = jobParamsMap;
			this.numMapper = numMapper;
			this.numReducer = numReducer;
			this.mapSleepTime = mapSleepTime;
			this.mapSleepCount = mapSleepCount;
			this.reduceSleepTime = reduceSleepTime;
			this.reduceSleepCount = reduceSleepCount;
			this.userName = userName;
			this.jobName = jobName;

		}

		@Override
		public Job call() {
			Job createdSleepJob = null;
			Configuration conf = TestSession.cluster.getConf();
			for (String key : jobParamsMap.keySet()) {
				conf.set(key, jobParamsMap.get(key));
			}

			try {
				TestSession.cluster.setSecurityAPI("keytab-" + userName,
						"user-" + userName);

				SleepJob sleepJob = new SleepJob();
				sleepJob.setConf(TestSession.cluster.getConf());
			
				createdSleepJob = sleepJob.createJob(numMapper, numReducer,
						mapSleepTime, mapSleepCount, reduceSleepTime,
						reduceSleepCount);
				createdSleepJob.setJobName("htf-cap-sched-sleep-job-named-"
						+ jobName + "-started-by-" + userName);
				TestSession.logger
						.info("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%"
								+ "%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%"
								+ "%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%"
								+ "%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");

				createdSleepJob.submit();

			} catch (Exception e) {
				Assert.fail("SleepJob invoked via API barfed...");

			}
			return createdSleepJob;
		}
		
//		@Override
//		public Job call() {
//			Job createdSortJob = null;
//			jobParamsMap.put("mapreduce.randomwriter.bytespermap", "256000");
//			Configuration conf = TestSession.cluster.getConf();
//			for (String key : jobParamsMap.keySet()) {
//				conf.set(key, jobParamsMap.get(key));
//			}
//
//			try {
//				TestSession.cluster.setSecurityAPI("keytab-" + userName,
//						"user-" + userName);
//
//				Sort<Text, Text> sort = new Sort();
//				String DATA_DIR_IN_HDFS = "/HTF/testdata/dfs/";
//				String FSCK_TESTS_DIR_ON_HDFS = DATA_DIR_IN_HDFS
//						+ "fsck_tests/";
//				String RANDOM_WRITER_DATA_DIR = FSCK_TESTS_DIR_ON_HDFS
//						+ "randomWriter/testFsckResultsLeveragingRandomWriterAndSortJobs/";
//				Random rand = new Random();
//				int  n = rand.nextInt(500) + 1;
//				String randomWriterOutputDir  = "randWrtr" + rand;
//				sort.setConf(TestSession.cluster.getConf());
//				String[] args = new String[] {RANDOM_WRITER_DATA_DIR,
//						"/tmp/" + randomWriterOutputDir};
//				RunSortMethod runSortMethod = new RunSortMethod(sort, args);
//				Thread runnerThread = new Thread(runSortMethod);
//				TestSession.logger.info("AAAAAAAAAAAA Just before calling the start AAAAAAAAAAAA");
//				runnerThread.start();
//				TestSession.logger.info("BBBBBBBBBBBB Just after calling the start BBBBBBBBBBBB");
//				Thread.sleep(5000);
//				createdSortJob = sort.getResult();
//				TestSession.logger.info("CCCCCCCCCCCC Just after calling the getResult CCCCCCCCCCCCC");
//				createdSortJob.setJobName("htf-cap-sched-sort-job-named-"
//						+ jobName + "-started-by-" + userName);
//				TestSession.logger
//						.info("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%"
//								+ "%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%"
//								+ "%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%"
//								+ "%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
//
////				createdSortJob.submit();
//
//			} catch (Exception e) {
//				Assert.fail("SleepJob invoked via API barfed...");
//
//			}
//			return createdSortJob;
//		}
//		class RunSortMethod implements Runnable{
//			Sort<Text, Text> sort;
//			String[] args;
//			RunSortMethod(Sort<Text, Text> sort, String[] args){
//				this.sort = sort;
//				this.args = args;
//			}
//			@Override
//			public void run() {
//				try {
//					sort.run(args);
//				} catch (Exception e) {
//					e.printStackTrace();
//				}
//				
//			}
//			
//		};
//
	}

	class SleepJobParams {
		public CalculatedCapacityLimitsBO capacityBO;
		public HashMap<String, String> configProperties;
		public String queue;
		public String userName;
		public int numJobs;
		public int factor;
		public int numTimesMapWouldSleep = 1;
		public int numTimesRedWouldSleep = 1;
		public int taskSleepDuration;
		public Double queueCapacity;
		public int numMapTasks = 0;
		public int numRedTasks = 0;

		public SleepJobParams(CalculatedCapacityLimitsBO capacityBO,
				HashMap<String, String> configProperties, String queue,
				String userName, int numJobs, int factor, int taskSleepDuration) {
			super();
			this.capacityBO = capacityBO;
			this.configProperties = configProperties;
			this.userName = (userName.isEmpty() || userName == null) ? HadooptestConstants.UserNames.HADOOPQA
					: userName;
			this.queue = (queue.isEmpty() || queue == null) ? "default" : queue;
			this.numJobs = numJobs;
			this.factor = factor;
			this.taskSleepDuration = taskSleepDuration;
			QueueCapacityDetail queueDetails = null;
			for (QueueCapacityDetail aQueueDetail : capacityBO.queueCapacityDetails) {
				if (aQueueDetail.name.equals(queue)) {
					queueDetails = aQueueDetail;
					break;
				}
			}
			this.queueCapacity = queueDetails.capacityInTermsOfTotalClusterMemory;
			this.numMapTasks = queueCapacity.intValue() * factor;
			this.numRedTasks = queueCapacity.intValue() * factor;

		}

		String getAppMasterResourceMb() {

			if (configProperties
					.containsKey("yarn.app.mapreduce.am.resource.mb")) {
				return configProperties
						.get("yarn.app.mapreduce.am.resource.mb");
			} else {
				return "";
			}
		}
	}

	class BarrierUntilAllThreadsRunning {
		ArrayList<Future<Job>> futureCallableSleepJobs;
		ArrayList<Thread> spawnedThreads;
		int maxWaitTimeForThread;

		public BarrierUntilAllThreadsRunning(
				ArrayList<Future<Job>> futureCallableSleepJobs,
				int maxWaitTimeForThread) throws InterruptedException,
				ExecutionException, IOException {
			TestSession.logger.info("Instantiating BarrierUntilAllThreadsRunning");

			this.maxWaitTimeForThread = maxWaitTimeForThread;
			this.futureCallableSleepJobs = futureCallableSleepJobs;

			final CyclicBarrier cyclicBarrierToWaitOnThreadStateRunnable = new CyclicBarrier(
					futureCallableSleepJobs.size(), new Runnable() {
						@Override
						public void run() {
							// This task will be executed once all thread
							// reaches barrier
							System.out
									.println("All threads state==RUNNING now.....hence continue!");
						}
					});
			spawnedThreads = new ArrayList<Thread>();
			TestSession.logger.info("Before Looping through futures, size of futures:" + futureCallableSleepJobs.size());
			for (Future<Job> aTetherToACallableSleepJob : futureCallableSleepJobs) {
				try{
				TestSession.logger
						.info("\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\ About to start a blocking thread for "
								+ aTetherToACallableSleepJob.get().getJobID()
								+ "///////////////////////////////");
				ThreadThatWillWaitAtBarrier threadThatWillWaitAtBarrier = new ThreadThatWillWaitAtBarrier(
						cyclicBarrierToWaitOnThreadStateRunnable,
						aTetherToACallableSleepJob, this.maxWaitTimeForThread);
				Thread t = new Thread(threadThatWillWaitAtBarrier);
				spawnedThreads.add(t);
				t.start();
				} catch (ExecutionException e){
					TestSession.logger.info(e.getCause());
				}

			}
			for (Thread aThreadThatHasReachedRunningState : spawnedThreads) {
				aThreadThatHasReachedRunningState.join();
			}

		}

		class ThreadThatWillWaitAtBarrier implements Runnable {
			CyclicBarrier barrierToHoldThreadsTillTheyAreRunnable;
			Future<Job> futureCallableSleepJob;
			int maxWait;

			ThreadThatWillWaitAtBarrier(CyclicBarrier cb,
					Future<Job> futureCallableSleepJob, int maxWait) {
				this.barrierToHoldThreadsTillTheyAreRunnable = cb;
				this.futureCallableSleepJob = futureCallableSleepJob;
				this.maxWait = maxWait;
			}

			@Override
			public void run() {
				try {
					TestSession.logger
							.info("\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\ Started blocking thread for "
									+ futureCallableSleepJob.get().getJobID()
									+ "///////////////////////////////");
				} catch (InterruptedException e1) {

					e1.printStackTrace();
				} catch (ExecutionException e1) {
					e1.printStackTrace();
				}
				waitForTaskState(futureCallableSleepJob,
						JobStatus.State.RUNNING);
				try {
					barrierToHoldThreadsTillTheyAreRunnable.await();
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (BrokenBarrierException e) {
					e.printStackTrace();
				}

			}

			public void waitForTaskState(
					Future<Job> aTetherToACallableSleepJob, State expectedState) {
				int MAX_WAIT_IN_SECONDS = 180;
				int SECONDS_TO_WAIT = 1;
				int SECONDS_WAITED = 0;
				try {
					while (aTetherToACallableSleepJob.get().getJobState() != expectedState) {
						TestSession.logger.info("Current job state["
								+ aTetherToACallableSleepJob.get().getJobName()
								+ "]:"
								+ aTetherToACallableSleepJob.get()
										.getJobState() + " != " + expectedState
								+ " hence, sleeping for " + SECONDS_TO_WAIT
								+ " seconds");
						Thread.sleep(SECONDS_TO_WAIT * 1000);
						SECONDS_WAITED += SECONDS_TO_WAIT;
						if (SECONDS_WAITED == MAX_WAIT_IN_SECONDS)
							break;
					}
				} catch (IOException e) {

					e.printStackTrace();
				} catch (InterruptedException e) {

					e.printStackTrace();
				} catch (ExecutionException e) {

					e.printStackTrace();
				}

			}

		}

	}

}
