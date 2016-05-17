package hadooptest.hadoop.regression.yarn.capacityScheduler;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.automation.utils.http.HTTPHandle;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.hadoop.HadoopCluster.Action;
import hadooptest.cluster.hadoop.HadoopCluster;
import hadooptest.cluster.hadoop.HadoopComponent;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.config.hadoop.fullydistributed.FullyDistributedConfiguration;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass;
import hadooptest.hadoop.regression.yarn.YarnTestsBaseClass;
import hadooptest.hadoop.regression.yarn.capacityScheduler.SchedulerRESTStatsSnapshot.LeafQueue;
import hadooptest.hadoop.regression.yarn.capacityScheduler.SchedulerRESTStatsSnapshot.User;
import hadooptest.hadoop.stress.tokenRenewal.Map;
import hadooptest.hadoop.stress.tokenRenewal.TimedCpuHoggerMap;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.commons.httpclient.HttpMethod;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobQueueInfo;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.mapreduce.SleepJob;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;

public class CapacitySchedulerBaseClass extends YarnTestsBaseClass {
	public static String CAPACITY_SCHEDULER_XML = "capacity-scheduler.xml";
	private static String CAPACITY = "capacity";
	private static String ABSOLUTE_MAX_CAPACITY = "absoluteMaxCapacity";
	private static String MAXCAPACITY = "maxCapacity";
	private static String USERLIMITFACTOR = "userLimitFactor";
	private static String USERLIMIT = "userLimit";
	private static String MAX_APPLICATIONS = "maxApplications";
	private static String MAX_APPLICATIONS_PER_USER = "maxApplicationsPerUser";
	private static String MAX_ACTIVE_APPLICATIONS = "maxActiveApplications";
	private static String MAX_ACTIVE_APPLICATIONS_PER_USER = "maxActiveApplicationsPerUser";
	private static String ABSOLUTE_CAPACITY = "absoluteCapacity";
	private static String NUM_PENDING_APPLICATIONS = "numPendingApplications";

	int NUM_THREADS = 800;
	int SLEEP_JOB_DURATION_IN_SECS = 20;

	public enum SingleUser {
		YES, NO
	};

	public enum ExpectToBomb {
		YES, NO
	};

	/**
	 * Had to take this long winded approach to find the state of the pending
	 * job, because the "Job.getState()" method does not apply to pending jobs.
	 * The State.* does not list PENDING as a state.
	 * 
	 * @param user
	 * @param queue
	 * @param runtimeStatsBO
	 */
	void assertThatOverageJobIsInPendingState(String user, String queue,
			RuntimeRESTStatsBO runtimeStatsBO) {
		boolean assertConditionMet = false;
		for (SchedulerRESTStatsSnapshot aSchedulerRESTStatsSnapshot : runtimeStatsBO.listOfRESTSnapshotsAcrossAllLeafQueues) {
			for (LeafQueue aLeafQueue : aSchedulerRESTStatsSnapshot.allLeafQueues) {
				if (aLeafQueue.queueName.equalsIgnoreCase(queue)) {
					for (User anOverageUser : aLeafQueue.users) {
						if (anOverageUser.userName.equalsIgnoreCase(user)) {
							Assert.assertTrue("User:" + user + "'s job on "
									+ queue + " should have waited in PENDING",
									anOverageUser.numPendingApplications == 1);
							assertConditionMet = true;
						}
					}
				}
			}
		}
		Assert.assertTrue(assertConditionMet);

	}

	void assertMinimumUserLimitPercent(String testCode,
			QueueCapacityDetail aCalculatedQueueCapaityDetail, String queue,
			RuntimeRESTStatsBO runtimeRESTStatsBO) {
		LeafQueue leafQueueSnapshotWithHighestMemUtil = getLeafSnapshotWithHighestMemUtil(
				queue, runtimeRESTStatsBO);
		// Assert.assertTrue(
		// (aCalculatedQueueCapaityDetail.maxCapacityForQueueInTermsOfTotalClusterMemoryInGB
		// * 1024)
		// + " is < than "
		// + leafQueueSnapshotWithHighestMemUtil.resourcesUsed.memory
		// + " for queue " + queue,
		// (aCalculatedQueueCapaityDetail.maxCapacityForQueueInTermsOfTotalClusterMemoryInGB
		// * 1024) >= leafQueueSnapshotWithHighestMemUtil.resourcesUsed.memory);

		Configuration config = TestSession.cluster.getConf();

		TestSession.logger
				.info("Max mem used by the queue, in the duration that the snapshots were taken:"
						+ leafQueueSnapshotWithHighestMemUtil.resourcesUsed.memory);
		int minContainerSize = config.getInt(
				"yarn.scheduler.minimum-allocation-mb", 1024);
		int maxContainerSize = config.getInt(
				"yarn.scheduler.maximum-allocation-mb", 8192);
		boolean isMultiUser = false;
		int multiUserCount = 0;
		for (User aUser : leafQueueSnapshotWithHighestMemUtil.users) {
			if (aUser.resourcesUsed.memory > 0)
				multiUserCount++;
		}
		if (multiUserCount > 1)
			isMultiUser = true;

		if (isMultiUser
		// && (leafQueueSnapshotWithHighestMemUtil.resourcesUsed.memory >=
		// (aCalculatedQueueCapaityDetail.minCapacityForQueueInTermsOfTotalClusterMemoryInGB
		// * 1024))
		) {
			/**
			 * Implies that there is contention and that more memory is being
			 * allocated (because it can grow till the max)
			 */
			double calculatedMinUserLimitMem = (aCalculatedQueueCapaityDetail.minimumUserLimitPercent
					/ 100
					* aCalculatedQueueCapaityDetail.minCapacityForQueueInTermsOfTotalClusterMemoryInGB * 1024);
			boolean discountMemoryUsedByOtherConcurrentUsers = true;
			if (aCalculatedQueueCapaityDetail.minimumUserLimitPercent == 100) {
				discountMemoryUsedByOtherConcurrentUsers = false;
			}
			if (aCalculatedQueueCapaityDetail.minCapacityForQueueInTermsOfTotalClusterMemoryInGB == aCalculatedQueueCapaityDetail.maxCapacityForQueueInTermsOfTotalClusterMemoryInGB) {
				discountMemoryUsedByOtherConcurrentUsers = false;
			}
			int discountedMemory = 0;
			if (!discountMemoryUsedByOtherConcurrentUsers) {
				/**
				 * Since the order of queues having memory consumption 2048 is
				 * uncertain, lap up the 2048's first.
				 */
				for (User aUser : leafQueueSnapshotWithHighestMemUtil.users) {
					if (aUser.resourcesUsed.memory == 2048) {
						discountedMemory += 2048;
					}

				}
			}
			for (User aUser : leafQueueSnapshotWithHighestMemUtil.users) {
				if (aUser.resourcesUsed.memory == 2048) {
					/**
					 * Here are the salient config parameters to consider, for
					 * contaier size. yarn.scheduler.minimum-allocation-mb =
					 * 1024 (in our case) yarn.scheduler.maximum-allocation-mb =
					 * 8192 yarn.app.mapreduce.am.resource.mb == 1536 (But in
					 * reality yarn.app.mapreduce.am.resource.mb==2048). Per
					 * Tom, AM is assigned a multiple of
					 * yarn.scheduler.minimum-allocation-mb.
					 * 
					 * Say you have 48GB in a cluster and that 5 users each
					 * launch a SleepJob, each with 48 mappers and 48 reducers
					 * (each consuming 1024 MB), with the intent of consuming
					 * the entire available memory. With this configuration I've
					 * noticed that ~35 containers get launched.
					 * 
					 * It is likely that the memory was consumed such: 2GB * 5 =
					 * 10 GB went to the AMs respective to each user. The rest
					 * 38GB was distributed across 35 containers [each of which
					 * could have a size between
					 * yarn.scheduler.minimum-allocation-mb and
					 * yarn.scheduler.maximum-allocation-mb MB
					 */
					TestSession.logger.info("User:[" + aUser.userName
							+ "] is using 2048 MB on queue[" + queue + "]");
					continue;
				} else if (aUser.resourcesUsed.memory == 0) {
					/**
					 * This could be the overage job, not assigned any memory,
					 * so skip.
					 */
					continue;
				}
				if (discountMemoryUsedByOtherConcurrentUsers) {
					TestSession.logger.info("User:[" + aUser.userName
							+ "] is using [" + aUser.resourcesUsed.memory
							+ "] MB on while calculatedMinUserLimitMem is["
							+ calculatedMinUserLimitMem + "] on queue[" + queue
							+ "]");
					Assert.assertTrue(
							"User:["
									+ aUser.userName
									+ "] is using ["
									+ aUser.resourcesUsed.memory
									+ "] MB which is lesser than the guarantee of["
									+ calculatedMinUserLimitMem + "] on queue["
									+ queue + "]",
							aUser.resourcesUsed.memory >= calculatedMinUserLimitMem);
				} else {
					TestSession.logger.info("User:[" + aUser.userName
							+ "] is using [" + aUser.resourcesUsed.memory
							+ "] MB on while calculatedMinUserLimitMem is["
							+ calculatedMinUserLimitMem + "] on queue[" + queue
							+ "] and additional memory to be considered is["
							+ discountedMemory + "]");
					Assert.assertTrue("User:[" + aUser.userName
							+ "] is using [" + aUser.resourcesUsed.memory
							+ "] MB which is lesser than the guarantee of["
							+ calculatedMinUserLimitMem + "] on queue[" + queue
							+ "]", aUser.resourcesUsed.memory
							+ discountedMemory >= calculatedMinUserLimitMem);

				}
			}
		} else {
			TestSession.logger
					.info("Not asserted, as no contention. Max memory used in leaf queue["
							+ queue
							+ "] was ["
							+ leafQueueSnapshotWithHighestMemUtil.resourcesUsed.memory
							+ "] While queue capacity (a.k.a min) was ["
							+ (aCalculatedQueueCapaityDetail.minCapacityForQueueInTermsOfTotalClusterMemoryInGB * 1024)
							+ "]");
		}

	}

	/**
	 * Note: This method assumes there are no redue tasks.
	 * 
	 * @param numberOfCoresPerBoxThatAreAvailableForNodeManagerForSpawningContainers
	 * @param numberOfVcoresToBeUsedByASingleMapTask
	 * @param expectedNumberOfReduceContainers
	 * @param queueCapacityPercent
	 * @return
	 */
	int getExpectedNumberOfContainers(
			int numberOfCoresPerBoxThatAreAvailableForNodeManagerForSpawningContainers,
			int numberOfVcoresToBeUsedByASingleMapTask,
			int expectedNumberOfReduceContainers, double queueCapacityPercent) {
		HadoopComponent hadoopComp = TestSession.cluster.getComponents().get(
				HadooptestConstants.NodeTypes.DATANODE);
		int numberOfDatanodes = hadoopComp.getNodes().size();

		int expectedCountOfContainers = numberOfCoresPerBoxThatAreAvailableForNodeManagerForSpawningContainers
				* numberOfDatanodes;
		expectedCountOfContainers = (int) Math.floor(expectedCountOfContainers
				* queueCapacityPercent / 100);
		expectedCountOfContainers--; // Account for AM (App Master)
		if (expectedCountOfContainers < 1)
			expectedCountOfContainers = 1;
		return (expectedCountOfContainers / numberOfVcoresToBeUsedByASingleMapTask);

	}

	protected void resetTheMaxQueueCapacity() throws IOException {
		JobClient jobClient = new JobClient(TestSession.cluster.getConf());
		for (JobQueueInfo aJobQueueInfo : jobClient.getQueues()) {
			String valueToResetAsItTendsToPersistAcrossTests = "yarn.scheduler.capacity.root."
					+ aJobQueueInfo.getQueueName() + ".maximum-capacity";
			FullyDistributedCluster fullyDistributedCluster = (FullyDistributedCluster) TestSession
					.getCluster();
			FullyDistributedConfiguration fullyDistributedConfRM = fullyDistributedCluster
					.getConf(HadooptestConstants.NodeTypes.RESOURCE_MANAGER);

			fullyDistributedConfRM.set(
					valueToResetAsItTendsToPersistAcrossTests, "");
			TestSession.logger.info("Reset the "
					+ valueToResetAsItTendsToPersistAcrossTests + " for queue "
					+ aJobQueueInfo.getQueueName());
		}

	}

	void printValuesReceivedOverRest(RuntimeRESTStatsBO runtimeStatsBO) {
		for (SchedulerRESTStatsSnapshot aSchedulerRESTStatsSnapshot : runtimeStatsBO.listOfRESTSnapshotsAcrossAllLeafQueues) {
			for (LeafQueue aLeafQueue : aSchedulerRESTStatsSnapshot.allLeafQueues) {
				TestSession.logger.info(aLeafQueue);
				TestSession.logger
						.info("LlLlLlLlLlLlLlLlLlLlLlLlLlLlLlLlLlLlLlLlLlLlLlLlLlLlLlLlLlLlLlLlLlLlLlLl");
			}
		}
	}

	protected LeafQueue getLeafSnapshotWithHighestMemUtil(String queue,
			RuntimeRESTStatsBO runtimeStatsBO) {
		LeafQueue snapshotWithHighestMemUtil = null;
		for (SchedulerRESTStatsSnapshot aSchedulerRESTStatsSnapshot : runtimeStatsBO.listOfRESTSnapshotsAcrossAllLeafQueues) {
			for (LeafQueue aLeafQueue : aSchedulerRESTStatsSnapshot.allLeafQueues) {
				if (aLeafQueue.queueName.equalsIgnoreCase(queue)) {
					if (snapshotWithHighestMemUtil == null) {
						snapshotWithHighestMemUtil = aLeafQueue;
					} else {
						if (aLeafQueue.resourcesUsed.memory >= snapshotWithHighestMemUtil.resourcesUsed.memory) {
							snapshotWithHighestMemUtil = aLeafQueue;
							TestSession.logger
									.info(aLeafQueue.resourcesUsed.memory
											+ "is >= "
											+ snapshotWithHighestMemUtil.resourcesUsed.memory
											+ " hence got a new leader");

						}
					}
				}
			}
		}
		return snapshotWithHighestMemUtil;

	}

	protected void printSelfCalculatedStats(
			CalculatedCapacityLimitsBO calculatedCapacityBO) {
		for (QueueCapacityDetail aCalculatedQueueDetail : calculatedCapacityBO.queueCapacityDetails) {
			TestSession.logger.info("Q name****:" + aCalculatedQueueDetail.name
					+ " *****");
			TestSession.logger
					.info("Q capacity in terms of cluster memory:"
							+ aCalculatedQueueDetail.minCapacityForQueueInTermsOfTotalClusterMemoryInGB
							+ " MB");
			TestSession.logger.info("Q max apps:"
					+ aCalculatedQueueDetail.maxApplications);
			TestSession.logger.info("Q max apps per user:"
					+ aCalculatedQueueDetail.maxApplicationsPerUser);
			TestSession.logger.info("Q max active apps per user:"
					+ aCalculatedQueueDetail.maxActiveApplicationsPerUser);
			TestSession.logger.info("Q max active apps:"
					+ aCalculatedQueueDetail.maxActiveApplications);
			TestSession.logger.info("Q min user limit percent:"
					+ aCalculatedQueueDetail.minimumUserLimitPercent);
			TestSession.logger.info("Q user limit factor:"
					+ aCalculatedQueueDetail.userLimitFactor);

			TestSession.logger
					.info("----------------------------------------------");
		}

	}

	protected void waitFor(int timeInMilliseconds) {
		try {
			Thread.sleep(timeInMilliseconds);
		} catch (InterruptedException e) {
		}
	}

	public Future<Job> submitSingleSleepJobAndGetHandle(String queueToSubmit,
			String username, ExpectToBomb expectedToBomb) {
		Future<Job> jobHandle = null;
		CapSchedCallableSleepJob callableSleepJob = new CapSchedCallableSleepJob(
				getDefaultSleepJobProps(queueToSubmit), 1, 1, 1, 1, 1, 1,
				username, "overage-job-launched-by-user-" + username
						+ "-on-queue-" + queueToSubmit, expectedToBomb);

		ExecutorService singleLanePool = Executors.newFixedThreadPool(1);
		jobHandle = singleLanePool.submit(callableSleepJob);

		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return jobHandle;
	}

	public ArrayList<Future<Job>> submitWordCountMapperOnlyJobsToThreadPool(
			ArrayList<CallableWordCountMapperOnlyJob> callableWordCountMapperOnlyJobs,
			int delayInMs) {

		ExecutorService threadPoolOfCallableWordCountMapperOnlyJobs = Executors
				.newFixedThreadPool(NUM_THREADS);
		ArrayList<Future<Job>> listOfFutureWordCountMapperOnlyJobs = new ArrayList<Future<Job>>();
		int jobNameSuffix = 1;
		for (CallableWordCountMapperOnlyJob aCallableWordCountMapperOnlyJob : callableWordCountMapperOnlyJobs) {
			try {
				Thread.sleep(delayInMs);
				TestSession.logger
						.info("Staggering launching of jobs, by "
								+ delayInMs
								+ " millisec, else they would step on each others feet");
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			Future<Job> aFutureCallableWorcCountMapperOnlyJob = threadPoolOfCallableWordCountMapperOnlyJobs
					.submit(aCallableWordCountMapperOnlyJob);
			listOfFutureWordCountMapperOnlyJobs
					.add(aFutureCallableWorcCountMapperOnlyJob);

		}
		// threadPoolOfCallableWordCountMapperOnlyJobs.shutdown();
		TestSession.logger.info("Returning size:"
				+ listOfFutureWordCountMapperOnlyJobs.size());
		return listOfFutureWordCountMapperOnlyJobs;
	}

	/**
	 * The intention of this Hadoop (Map-only job) is to ensure that we spawn
	 * map containers, of sizes that we pre-set in the config. There are no
	 * reducers.
	 * 
	 * @author tiwari
	 * 
	 */
	class CallableWordCountMapperOnlyJob implements Callable<Job> {
		String queue = null;
		String username = "hadoopqa";
		int factor = 1;
		String jobName;
		int numCpusPerMap;

		public CallableWordCountMapperOnlyJob(String jobName,
				String queueToSubmit, String username, int numCpusPerMap) {
			this.queue = queueToSubmit;
			this.username = username;
			this.jobName = jobName;
			this.numCpusPerMap = numCpusPerMap;
		}

		public Job call() {
			Job callableWordCountMapperOnlyJob = null;
			try {
				TestSession.cluster.setSecurityAPI("keytab-" + username,
						"user-" + username);
				Thread.sleep(3000);
				Configuration jobConf = TestSession.cluster.getConf();

				jobConf.set("mapreduce.job.acl-view-job", "*");
				jobConf.set("mapreduce.map.cpu.vcores",
						String.valueOf(numCpusPerMap));
				jobConf.set("mapred.job.queue.name", queue);

				callableWordCountMapperOnlyJob = Job.getInstance(jobConf);
				callableWordCountMapperOnlyJob.setJarByClass(Map.class);
				callableWordCountMapperOnlyJob.setJobName(jobName);
				callableWordCountMapperOnlyJob
						.setMapperClass(TimedCpuHoggerMap.class);
				callableWordCountMapperOnlyJob
						.setInputFormatClass(TextInputFormat.class);
				Path[] inputPaths = new Path[] { new Path(
						DfsTestsBaseClass.DATA_DIR_IN_HDFS
								+ DfsTestsBaseClass.INPUT_TO_WORD_COUNT) };
				TextInputFormat.setInputPaths(callableWordCountMapperOnlyJob,
						inputPaths);
				callableWordCountMapperOnlyJob
						.setOutputFormatClass(TextOutputFormat.class);
				TextOutputFormat.setOutputPath(callableWordCountMapperOnlyJob,
						new Path("/tmp/" + System.currentTimeMillis()));
				callableWordCountMapperOnlyJob.submit();

			} catch (IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				try {
					TestSession.logger
							.info("Got an interrupt !!! proceeding to kill job:"
									+ callableWordCountMapperOnlyJob
											.getJobName());
					callableWordCountMapperOnlyJob.killJob();
					TestSession.logger.info("There killed job:"
							+ callableWordCountMapperOnlyJob.getJobName());

				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}

			return callableWordCountMapperOnlyJob;

		}

	}

	protected void copyResMgrConfigAndRestartNodes(String replacementConfigFile)
			throws Exception {
		TestSession.logger
				.info("Copying over canned cap sched file localted @:"
						+ replacementConfigFile);
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

                // wait up to 5 minutes for NN to be out of safemode
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;
                for (int waitCounter = 0; waitCounter < 30; waitCounter++) {
                  genericCliResponse = dfsCliCommands.dfsadmin(
				DfsTestsBaseClass.EMPTY_ENV_HASH_MAP, 
				DfsTestsBaseClass.Report.NO, 
				"get",
                                DfsTestsBaseClass.ClearQuota.NO, 
				DfsTestsBaseClass.SetQuota.NO, 
				0, 
				DfsTestsBaseClass.ClearSpaceQuota.NO,
                                DfsTestsBaseClass.SetSpaceQuota.NO, 
				0, 
				DfsTestsBaseClass.PrintTopology.NO, 
				null);

                  if (genericCliResponse.response.contains("Safe mode is OFF")) {
                    TestSession.logger.info("NN is out of Safemode after " + (waitCounter*10) + " seconds");
                    break;
                  } else if (waitCounter >= 30) {
                    TestSession.logger.error("NN never left Safemode after " + (waitCounter*10) + " seconds!");
                    Assert.fail();
                  }
                  else
                  {
                    Thread.sleep(10000);
                  }
                }

		Configuration conf = fullyDistributedCluster
				.getConf(HadooptestConstants.NodeTypes.RESOURCE_MANAGER);

		Iterator iter = conf.iterator();
		while (iter.hasNext()) {
			Entry<String, String> entry = (Entry<String, String>) iter.next();
			TestSession.logger.trace("Key:[" + entry.getKey() + "] Value["
					+ entry.getValue() + "]");
		}

	}

	void setDominantResourceParametersEverywhere(
			String replacementConfigFile,
			int numberOfVcoresPerCPUThatAreAvailableForNodeManagerForSpawningContainers,
			boolean moveProcsUnderCgroup) throws Exception {
		TestSession.logger
				.info("Copying over canned cap sched file localted @:"
						+ replacementConfigFile);
		FullyDistributedCluster fullyDistributedCluster = (FullyDistributedCluster) TestSession
				.getCluster();

		// Backup config and replace file, for Resource Manager
		fullyDistributedCluster.getConf(
				HadooptestConstants.NodeTypes.NODE_MANAGER).backupConfDir();
		fullyDistributedCluster.getConf(
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER).backupConfDir();
		fullyDistributedCluster.getConf(HadooptestConstants.NodeTypes.NAMENODE)
				.backupConfDir();
		fullyDistributedCluster.getConf(HadooptestConstants.NodeTypes.DATANODE)
				.backupConfDir();
		fullyDistributedCluster.getConf(
				HadooptestConstants.NodeTypes.HISTORY_SERVER).backupConfDir();

		// Replace the cap sched file
		fullyDistributedCluster.getConf(
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER)
				.copyFileToConfDir(replacementConfigFile,
						CAPACITY_SCHEDULER_XML);

		/**
		 * The config values needed for DominantResouceCalculator to work
		 */
		HashMap<String, String> keyValuesToBeSet = new HashMap<String, String>();
		keyValuesToBeSet
				.put("yarn.nodemanager.linux-container-executor.resources-handler.class",
						"org.apache.hadoop.yarn.server.nodemanager.util.CgroupsLCEResourcesHandler");
		keyValuesToBeSet.put(
				"yarn.nodemanager.linux-container-executor.cgroups.hierarchy",
				"/hadoop-yarn");
		keyValuesToBeSet.put(
				"yarn.nodemanager.linux-container-executor.cgroups.mount",
				"true");
		keyValuesToBeSet.put(
				"yarn.nodemanager.linux-container-executor.cgroups.mount-path",
				"/cgroup");
		keyValuesToBeSet.put("yarn.nodemanager.linux-container-executor.group",
				"hadoop");

		/**
		 * These are not needed for the DominantResouceCalculator to work These
		 * are only test specific.
		 */
		keyValuesToBeSet.put("mapreduce.job.acl-view-job", "*");

		/**
		 * Number of virtual cores cores that can be allocated for containers.
		 * It is usually set to 8 (default) meaning that all the Virtual CPUs
		 * (in a core) are up for grabs by the Node Manager to allocate to a
		 * container. If you set it to, say 2, then it means that out of 8
		 * possible vcpus only 2 are up for grabs.
		 */
		keyValuesToBeSet
				.put("yarn.nodemanager.resource.cpu-vcores",
						String.valueOf(numberOfVcoresPerCPUThatAreAvailableForNodeManagerForSpawningContainers));

		String[] nodesThatWeAreInterestedIn = new String[] {
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER,
				HadooptestConstants.NodeTypes.HISTORY_SERVER,
				HadooptestConstants.NodeTypes.NAMENODE,
				HadooptestConstants.NodeTypes.NODE_MANAGER,
				HadooptestConstants.NodeTypes.DATANODE };

		for (String aNodeThatWeAreInterestedIn : nodesThatWeAreInterestedIn) {
			fullyDistributedCluster.getConf(aNodeThatWeAreInterestedIn)
					.setHadoopConfFileProp(keyValuesToBeSet,
							HadooptestConstants.ConfFileNames.YARN_SITE_XML,
							null);

		}

		// Bounce nodes
		for (String aNodeThatWeAreInterestedIn : nodesThatWeAreInterestedIn) {
			fullyDistributedCluster.hadoopDaemon(Action.STOP,
					aNodeThatWeAreInterestedIn);
		}
		Thread.sleep(20000);
		// Bounce nodes
		for (String aNodeThatWeAreInterestedIn : nodesThatWeAreInterestedIn) {
			fullyDistributedCluster.hadoopDaemon(Action.START,
					aNodeThatWeAreInterestedIn);
		}
		Thread.sleep(20000);
		DfsTestsBaseClass dfsTestsBaseClass = new DfsTestsBaseClass();
		HadoopComponent hadoopComp = TestSession.cluster.getComponents().get(
				HadooptestConstants.NodeTypes.DATANODE);
		HashMap<String, String> nodeManagerHostAndProdId = new HashMap<String, String>();
		for (String aDataNode : hadoopComp.getNodes().keySet()) {

			String prodId = dfsTestsBaseClass.doJavaSSHClientExec(
					HadooptestConstants.UserNames.MAPREDQA, aDataNode,
					"ps auxww |grep nodemanage[r]|  awk '{print $2}'",
					DfsTestsBaseClass.HADOOPQA_AS_MAPREDQA_IDENTITY_FILE);
			nodeManagerHostAndProdId.put(aDataNode, prodId.trim());
		}
		for (String aHostName : nodeManagerHostAndProdId.keySet()) {
			TestSession.logger.info("Nodemanager running on host [" + aHostName
					+ "] had procId ["
					+ nodeManagerHostAndProdId.get(aHostName) + "]");
		}

		if (moveProcsUnderCgroup) {
			for (String aDataNode : hadoopComp.getNodes().keySet()) {
				String command = "cgclassify -g cpu:hadoop-yarn "
						+ nodeManagerHostAndProdId.get(aDataNode);
				TestSession.logger
						.info("Moving process as a cgroup task on host["
								+ aDataNode + "] [" + command + "]");
				dfsTestsBaseClass.doJavaSSHClientExec(
						HadooptestConstants.UserNames.MAPREDQA, aDataNode,
						command,
						DfsTestsBaseClass.HADOOPQA_AS_MAPREDQA_IDENTITY_FILE);
			}
		}

                // wait up to 5 minutes for NN to be out of safemode
                DfsCliCommands dfsCliCommands = new DfsCliCommands();
                GenericCliResponseBO genericCliResponse;
                for (int waitCounter = 0; waitCounter < 30; waitCounter++) {
                  genericCliResponse = dfsCliCommands.dfsadmin(
                                DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
                                DfsTestsBaseClass.Report.NO,
                                "get",
                                DfsTestsBaseClass.ClearQuota.NO,
                                DfsTestsBaseClass.SetQuota.NO,
                                0,
                                DfsTestsBaseClass.ClearSpaceQuota.NO,
                                DfsTestsBaseClass.SetSpaceQuota.NO,
                                0,
                                DfsTestsBaseClass.PrintTopology.NO,
                                null);

                  if (genericCliResponse.response.contains("Safe mode is OFF")) {
                    TestSession.logger.info("NN is out of Safemode after " + (waitCounter*10) + " seconds");
                    break;
                  } else if (waitCounter >= 30) {
                    TestSession.logger.error("NN never left Safemode after " + (waitCounter*10) + " seconds!");
                    Assert.fail();
                  }
                  else
                  {
                    Thread.sleep(10000);
                  }
                }

		Configuration conf = fullyDistributedCluster
				.getConf(HadooptestConstants.NodeTypes.RESOURCE_MANAGER);

		Iterator iter = conf.iterator();
		while (iter.hasNext()) {
			Entry<String, String> entry = (Entry<String, String>) iter.next();
			TestSession.logger.info("Key:[" + entry.getKey() + "] Value["
					+ entry.getValue() + "]");
		}

	}

	/**
	 * This function is unused, because the read back memory stats always come
	 * back as 0. I got with Tom and there is already a Jira open for this.
	 * 
	 * @param futureCallableSleepJobs
	 * @return
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public RuntimeRESTStatsBO startCollectingRestStats(
			int periodicityInMilliseconds) throws InterruptedException,
			ExecutionException {
		RuntimeRESTStatsBO restStats = new RuntimeRESTStatsBO(
				periodicityInMilliseconds);
		restStats.start();
		return restStats;
	}

	public void stopCollectingRuntimeStatsAcrossQueues(
			RuntimeRESTStatsBO runtimeRESTStatsBO) {
		runtimeRESTStatsBO.stop();
	}

	public CalculatedCapacityLimitsBO selfCalculateCapacityLimits()
			throws Exception {
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

	@Deprecated
	ArrayList<QueueCapacityDetail> getCapacityLimitsViaRestCallIntoRM()
			throws InterruptedException {
		ArrayList<QueueCapacityDetail> queueCapacityDetails;
		queueCapacityDetails = new ArrayList<QueueCapacityDetail>();
		Properties crossClusterProperties;
		String workingDir = System
				.getProperty(HadooptestConstants.Miscellaneous.USER_DIR);
		crossClusterProperties = new Properties();
		try {
			crossClusterProperties.load(new FileInputStream(workingDir
					+ "/conf/CrossCluster/Resource.properties"));
		} catch (IOException ex) {
			ex.printStackTrace();
		}

		HashMap<String, HashMap<String, String>> limitsReadViaRest = new HashMap<String, HashMap<String, String>>();
		String resourceManager = TestSession.getResourceManagerURL(
		        System.getProperty("CLUSTER_NAME"));

		String resource = "/ws/v1/cluster/scheduler";
		HTTPHandle httpHandle = new HTTPHandle();
		HttpMethod getMethod = httpHandle.makeGET(resourceManager, resource,
				null);
		Response response = new Response(getMethod);

		TestSession.logger.info(response.toString());
		JSONObject rmResponseJson = response.getJsonObject();
		JSONObject schedulerJson = (JSONObject) rmResponseJson.get("scheduler");
		JSONObject jsonContainingQueuesAndQueue = (JSONObject) schedulerJson
				.get("schedulerInfo");
		recursivelyUpdateHash(queueCapacityDetails,
				jsonContainingQueuesAndQueue);
		TestSession.logger.info("Queue dump:" + limitsReadViaRest);
		return queueCapacityDetails;
	}

	@Deprecated
	void recursivelyUpdateHash(
			ArrayList<QueueCapacityDetail> queueCapacityDetails,
			JSONObject jsonContainingQueuesAndQueue) {
		JSONObject queuesJson = (JSONObject) jsonContainingQueuesAndQueue
				.get("queues");
		JSONArray queueArrayJson = (JSONArray) queuesJson.get("queue");
		for (int xx = 0; xx < queueArrayJson.size(); xx++) {
			if (queueArrayJson.getJSONObject(xx).containsKey("type")) {
				TestSession.logger.info("Queue type, for queue named:"
						+ queueArrayJson.getJSONObject(xx).get("queueName"));
				String queueType = (String) queueArrayJson.getJSONObject(xx)
						.get("type");
				if (queueType.equals("capacitySchedulerLeafQueueInfo")) {

					QueueCapacityDetail queueCapacityDetail = new QueueCapacityDetail();
					// Queue name
					TestSession.logger
							.info("Adding values for queue named:"
									+ queueArrayJson.getJSONObject(xx).get(
											"queueName"));

					String queueName = (String) queueArrayJson
							.getJSONObject(xx).get("queueName");
					queueCapacityDetail.name = (String) queueArrayJson
							.getJSONObject(xx).get("queueName");

					HashMap<String, String> queueDetails = new HashMap<String, String>();

					// Capacity
					TestSession.logger.info("Adding KEY:"
							+ CAPACITY
							+ " value:"
							+ String.valueOf(queueArrayJson.getJSONObject(xx)
									.get(CAPACITY)));
					String temp = String.valueOf(queueArrayJson.getJSONObject(
							xx).get(CAPACITY));
					queueCapacityDetail.minCapacityInTermsOfPercentage = Double
							.parseDouble(temp);
					queueDetails.put(CAPACITY, String.valueOf(queueArrayJson
							.getJSONObject(xx).get(CAPACITY)));

					// Absolute Max Capacity
					TestSession.logger.info("Adding KEY:"
							+ ABSOLUTE_MAX_CAPACITY
							+ " value:"
							+ String.valueOf(queueArrayJson.getJSONObject(xx)
									.get(ABSOLUTE_MAX_CAPACITY)));
					queueDetails.put(ABSOLUTE_MAX_CAPACITY, String
							.valueOf(queueArrayJson.getJSONObject(xx).get(
									ABSOLUTE_MAX_CAPACITY)));

					// numPendingApplications
					TestSession.logger.info("Adding KEY:"
							+ NUM_PENDING_APPLICATIONS
							+ " value:"
							+ String.valueOf(queueArrayJson.getJSONObject(xx)
									.get(NUM_PENDING_APPLICATIONS)));
					queueCapacityDetail.numPendingApplications = Integer
							.parseInt(String.valueOf(queueArrayJson
									.getJSONObject(xx).get(
											NUM_PENDING_APPLICATIONS)));
					queueDetails.put(NUM_PENDING_APPLICATIONS, String
							.valueOf(queueArrayJson.getJSONObject(xx).get(
									NUM_PENDING_APPLICATIONS)));

					// Max Applications
					TestSession.logger.info("Adding KEY:"
							+ MAX_APPLICATIONS
							+ " value:"
							+ String.valueOf(queueArrayJson.getJSONObject(xx)
									.get(MAX_APPLICATIONS)));
					temp = String.valueOf(queueArrayJson.getJSONObject(xx).get(
							MAX_APPLICATIONS));
					queueCapacityDetail.maxApplications = Double
							.parseDouble(temp);
					queueDetails.put(MAX_APPLICATIONS, String
							.valueOf(queueArrayJson.getJSONObject(xx).get(
									MAX_APPLICATIONS)));
					// Max Capacity
					TestSession.logger.info("Adding KEY:"
							+ MAXCAPACITY
							+ " value:"
							+ String.valueOf(queueArrayJson.getJSONObject(xx)
									.get(MAXCAPACITY)));
					queueDetails.put(MAXCAPACITY, (String
							.valueOf(queueArrayJson.getJSONObject(xx).get(
									MAXCAPACITY))));
					// Min User Limit Percent
					TestSession.logger.info("Adding KEY:"
							+ USERLIMIT
							+ " value:"
							+ String.valueOf(queueArrayJson.getJSONObject(xx)
									.get(USERLIMIT)));
					temp = String.valueOf(queueArrayJson.getJSONObject(xx).get(
							USERLIMIT));
					queueCapacityDetail.minimumUserLimitPercent = Double
							.parseDouble(temp);
					queueDetails.put(USERLIMIT, String.valueOf(queueArrayJson
							.getJSONObject(xx).get(USERLIMIT)));

					// User Limit Factor
					TestSession.logger.info("Adding KEY:"
							+ USERLIMITFACTOR
							+ " value:"
							+ String.valueOf(queueArrayJson.getJSONObject(xx)
									.get(USERLIMITFACTOR)));
					temp = String.valueOf(queueArrayJson.getJSONObject(xx).get(
							USERLIMITFACTOR));
					queueCapacityDetail.userLimitFactor = Double
							.parseDouble(temp);
					queueDetails.put(USERLIMITFACTOR, String
							.valueOf(queueArrayJson.getJSONObject(xx).get(
									USERLIMITFACTOR)));

					// Max apps per user
					TestSession.logger.info("Adding KEY:"
							+ MAX_APPLICATIONS_PER_USER
							+ " value:"
							+ String.valueOf(queueArrayJson.getJSONObject(xx)
									.get(MAX_APPLICATIONS_PER_USER)));
					temp = String.valueOf(queueArrayJson.getJSONObject(xx).get(
							MAX_APPLICATIONS_PER_USER));
					queueCapacityDetail.maxApplicationsPerUser = Double
							.parseDouble(temp);
					queueDetails.put(MAX_APPLICATIONS_PER_USER, String
							.valueOf(queueArrayJson.getJSONObject(xx).get(
									MAX_APPLICATIONS_PER_USER)));

					// Max active applications
					TestSession.logger.info("Adding KEY:"
							+ MAX_ACTIVE_APPLICATIONS
							+ " value:"
							+ String.valueOf(queueArrayJson.getJSONObject(xx)
									.get(MAX_ACTIVE_APPLICATIONS)));
					temp = String.valueOf(queueArrayJson.getJSONObject(xx).get(
							MAX_ACTIVE_APPLICATIONS));
					queueCapacityDetail.maxActiveApplications = Double
							.parseDouble(temp);
					queueDetails.put(MAX_ACTIVE_APPLICATIONS, String
							.valueOf(queueArrayJson.getJSONObject(xx).get(
									MAX_ACTIVE_APPLICATIONS)));

					// Max active applications per user
					TestSession.logger.info("Adding KEY:"
							+ MAX_ACTIVE_APPLICATIONS_PER_USER
							+ " value:"
							+ String.valueOf(queueArrayJson.getJSONObject(xx)
									.get(MAX_ACTIVE_APPLICATIONS_PER_USER)));
					temp = String.valueOf(queueArrayJson.getJSONObject(xx).get(
							MAX_ACTIVE_APPLICATIONS_PER_USER));
					queueCapacityDetail.maxActiveApplicationsPerUser = Double
							.parseDouble(temp);
					queueDetails.put(MAX_ACTIVE_APPLICATIONS_PER_USER, String
							.valueOf(queueArrayJson.getJSONObject(xx).get(
									MAX_ACTIVE_APPLICATIONS_PER_USER)));
					// Absolute capacity
					TestSession.logger.info("Adding KEY:"
							+ ABSOLUTE_CAPACITY
							+ " value:"
							+ String.valueOf(queueArrayJson.getJSONObject(xx)
									.get(ABSOLUTE_CAPACITY)));
					queueDetails.put(ABSOLUTE_CAPACITY, String
							.valueOf(queueArrayJson.getJSONObject(xx).get(
									ABSOLUTE_CAPACITY)));

					queueCapacityDetails.add(queueCapacityDetail);
				} else {
					TestSession.logger
							.info("Queue type not leaf! for queue named:"
									+ queueArrayJson.getJSONObject(xx).get(
											"queueName"));
				}
			} else {
				// This is a parent
				TestSession.logger.info("Hit a parent queue named:"
						+ queueArrayJson.getJSONObject(xx).get("queueName"));
				recursivelyUpdateHash(queueCapacityDetails,
						queueArrayJson.getJSONObject(xx));
			}
		}
	}

	public HashMap<String, String> getDefaultSleepJobProps(String queueToSubmit) {
		HashMap<String, String> defaultSleepJobProps = new HashMap<String, String>();
		defaultSleepJobProps.put("mapreduce.job.acl-view-job", "*");
		defaultSleepJobProps.put("mapreduce.map.memory.mb", "1024");
		defaultSleepJobProps.put("mapreduce.reduce.memory.mb", "1024");
		defaultSleepJobProps.put("mapred.job.queue.name", queueToSubmit);

		return defaultSleepJobProps;
	}

	public ArrayList<Future<Job>> submitSleepJobsToAThreadPoolAndRunThemInParallel(
			ArrayList<SleepJobParams> sleepJobParamsList, int delayInMs) {

		ExecutorService sleepJobThreadPool = Executors
				.newFixedThreadPool(NUM_THREADS);
		ArrayList<Future<Job>> futureCallableSleepJobs = new ArrayList<Future<Job>>();
		int jobNameSuffix = 1;
		for (SleepJobParams aSleepJobParams : sleepJobParamsList) {
			try {
				Thread.sleep(delayInMs);
				TestSession.logger
						.info("Staggering launching of jobs, by "
								+ delayInMs
								+ " millisec, else they would step on each others feet");
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			Future<Job> aFutureCallableSleepJob = sleepJobThreadPool
					.submit(new CapSchedCallableSleepJob(
							aSleepJobParams.configProperties,
							aSleepJobParams.numMapTasks,
							aSleepJobParams.numRedTasks,
							aSleepJobParams.taskSleepDuration,
							aSleepJobParams.numTimesMapWouldSleep,
							aSleepJobParams.taskSleepDuration,
							aSleepJobParams.numTimesRedWouldSleep,
							aSleepJobParams.userName, aSleepJobParams.jobName,
							ExpectToBomb.NO));
			futureCallableSleepJobs.add(aFutureCallableSleepJob);

		}
		sleepJobThreadPool.shutdown();
		return futureCallableSleepJobs;
	}

	void killStartedJobs(ArrayList<Future<Job>> futureCallableSleepJobs) {
		CountDownLatch countDownKillLatch = new CountDownLatch(
				futureCallableSleepJobs.size());
		ExecutorService killerPool = Executors
				.newFixedThreadPool(futureCallableSleepJobs.size());
		for (Future<Job> aFutureJob : futureCallableSleepJobs) {
			JobKiller aJobKiller = new JobKiller(countDownKillLatch, aFutureJob);
			killerPool.submit(aJobKiller);
		}
		try {
			countDownKillLatch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		TestSession.logger
				.info("{{{{{{{{{{ Killed all the launched jobs }}}}}}}}}}");
	}

	String killACompletedMapTaskAndGetCurrentAttemptID(
			ArrayList<Future<Job>> futureCallableSleepJobs) throws IOException,
			IllegalArgumentException, InterruptedException, ExecutionException {
		boolean missionAccomplished = false;
		String attemptId = null;
		Cluster aCluster = new Cluster(TestSession.cluster.getConf());
		for (Future<Job> aFutureJob : futureCallableSleepJobs) {
			if (!missionAccomplished) {
				for (TaskReport aTaskReport : aCluster.getJob(
						aFutureJob.get().getJobID()).getTaskReports(
						TaskType.MAP)) {
					if (aTaskReport.getState().equalsIgnoreCase(("SUCCEEDED"))) {
						String userName = aFutureJob.get().getUser();
						TestSession.logger
								.info("Trying to kill MAP task as user:"
										+ userName
										+ " task attempt id:"
										+ aTaskReport
												.getSuccessfulTaskAttemptId());
						TestSession.cluster.setSecurityAPI(
								"keytab-" + userName, "user-" + userName);
						Thread.sleep(3000);
						aCluster = new Cluster(TestSession.cluster.getConf());
						aCluster.getJob(aFutureJob.get().getJobID()).killTask(
								aTaskReport.getSuccessfulTaskAttemptId());
						missionAccomplished = true;
						TestSession.logger
								.info("Succeeded in killing MAP task as user:"
										+ userName
										+ " task attempt id:"
										+ aTaskReport
												.getSuccessfulTaskAttemptId());
						attemptId = aTaskReport.getSuccessfulTaskAttemptId()
								.toString();
						break;
					} else {
						TestSession.logger
								.info("Map Task "
										+ aTaskReport.getTaskId()
										+ " is in "
										+ aTaskReport.getState()
										+ " hence looping..I want 'em completed before killing 'em");
					}

				}
			}
		}
		return attemptId;
	}

	void assertThatTheKilledMapTaskWasRespawnedAndCompleted(
			ArrayList<Future<Job>> futureCallableSleepJobs,
			String mapTaskAttemptIdToLookoutFor) throws IOException,
			IllegalArgumentException, InterruptedException, ExecutionException {
		Cluster aCluster = new Cluster(TestSession.cluster.getConf());
		boolean assertSuccessful = false;
		TestSession.logger.info("Looking out for map task attempt id:"
				+ mapTaskAttemptIdToLookoutFor);

		// Ensure that all tasks have completed
		waitTillAllMapTasksComplete(futureCallableSleepJobs);
		TestSession.logger.info("Looks like all the map tasks have completed.");
		for (Future<Job> aFutureJob : futureCallableSleepJobs) {
			for (TaskReport aTaskReport : aCluster.getJob(
					aFutureJob.get().getJobID()).getTaskReports(TaskType.MAP)) {
				if (aTaskReport.getSuccessfulTaskAttemptId().toString()
						.equalsIgnoreCase(mapTaskAttemptIdToLookoutFor)) {
					assertSuccessful = true;
					TestSession.logger
							.info("There, found what I was looking for, the re-spawned map task ["
									+ mapTaskAttemptIdToLookoutFor + "]");
					break;
				} else {
					TestSession.logger.info(aTaskReport
							.getSuccessfulTaskAttemptId().toString()
							+ " is not what I am looking for. I want:"
							+ mapTaskAttemptIdToLookoutFor);
				}
			}
		}

		Assert.assertTrue("Map task attempt id:" + mapTaskAttemptIdToLookoutFor
				+ " not found! ", assertSuccessful);
	}

	void waitTillAllMapTasksComplete(
			ArrayList<Future<Job>> futureCallableSleepJobs) throws IOException,
			InterruptedException, ExecutionException {
		Cluster aCluster = new Cluster(TestSession.cluster.getConf());
		int maxWait = 300;
		boolean allMapTasksCompleted = true;
		Date startTimeStamp = new Date();
		do {
			allMapTasksCompleted = true;

			for (Future<Job> aFutureJob : futureCallableSleepJobs) {
				for (TaskReport aTaskReport : aCluster.getJob(
						aFutureJob.get().getJobID()).getTaskReports(
						TaskType.MAP)) {
					TestSession.logger
							.info("Looping in the 5 min loop because "
									+ aTaskReport.getTaskId()
									+ " is in "
									+ aTaskReport.getState()
									+ " state, and we are waiting for all map tasks to complete.");
					if (aTaskReport.getState().toString().equals("RUNNING")
							|| aTaskReport.getState().toString()
									.equals("SCHEDULED")) {
						allMapTasksCompleted = false;
						TestSession.logger
								.info("See, map task "
										+ aTaskReport.getTaskId()
										+ " is in "
										+ aTaskReport.getState()
										+ " state, and we are waiting for all map tasks to complete. Sleeping for 1 sec and re-checking");

						break;
					}
				}

			}
			Thread.sleep(1000);
			maxWait--;
		} while (!allMapTasksCompleted && (maxWait > 0));
		Date endTimeStamp = new Date();
		TestSession.logger.info("========= Waited ["
				+ (endTimeStamp.getTime() - startTimeStamp.getTime()) / 1000
				+ "] seconds =======");
	}

	HashMap<String, String> getMapOfReduceTasksIdAndState(
			ArrayList<Future<Job>> futureCallableSleepJobs) throws IOException,
			InterruptedException, ExecutionException {
		HashMap reduceTasksAndState = new HashMap<String, String>();
		Cluster aCluster = new Cluster(TestSession.cluster.getConf());
		for (Future<Job> aFutureJob : futureCallableSleepJobs) {
			for (TaskReport aTaskReport : aCluster.getJob(
					aFutureJob.get().getJobID())
					.getTaskReports(TaskType.REDUCE)) {
				TestSession.logger.info("Compiling list of Reduce task["
						+ aTaskReport.getTaskId() + "] in state ["
						+ aTaskReport.getState() + "]");
				reduceTasksAndState.put(aTaskReport.getTaskId(),
						aTaskReport.getState());
			}
		}

		return reduceTasksAndState;
	}

	void assertThatAReduceTaskWasKilledToMakeRoom(
			ArrayList<Future<Job>> futureCallableSleepJobs,
			HashMap<String, String> prevListOfReduceTasksAndState)
			throws IOException, InterruptedException, ExecutionException {
		Cluster aCluster = new Cluster(TestSession.cluster.getConf());
		for (String aPrevMapTask : prevListOfReduceTasksAndState.keySet()) {
			Assert.assertTrue("A reduce task was already in the killed state!",
					!prevListOfReduceTasksAndState.get(aPrevMapTask)
							.equalsIgnoreCase("KILLED"));
		}
		boolean atLeastOneReduceTaskWasKilled = false;
		for (Future<Job> aFutureJob : futureCallableSleepJobs) {
			for (TaskReport aTaskReport : aCluster.getJob(
					aFutureJob.get().getJobID())
					.getTaskReports(TaskType.REDUCE)) {
				TestSession.logger.info("Reduce task["
						+ aTaskReport.getTaskId() + "] is in state ["
						+ aTaskReport.getState() + "]");
				if (aTaskReport.getState().equalsIgnoreCase("KILLED")) {
					atLeastOneReduceTaskWasKilled = true;
					TestSession.logger.info("Got it.........Reduce task["
							+ aTaskReport + "] is in state ["
							+ aTaskReport.getState() + "], BREAKING FROM LOOP");
					break;
				}
			}
		}
		Assert.assertTrue(
				"Nope, none of the reduce tasks were killed to make headroom",
				atLeastOneReduceTaskWasKilled);

	}

	class JobKiller implements Runnable {
		Future<Job> aFutureJob;
		CountDownLatch countDownKillLatch;

		JobKiller(CountDownLatch countDownKillLatch, Future<Job> aFutureJob) {
			this.aFutureJob = aFutureJob;
			this.countDownKillLatch = countDownKillLatch;
		}

		public void run() {
			try {
				aFutureJob.get().killJob();
				TestSession.logger
						.info("!!!!!!!!!!!!!!!! K I L L E D - J 0 B  - [ "
								+ aFutureJob.get().getJobName()
								+ " ]  !!!!!!!!!!!!!!!!!!");
				while (aFutureJob.get().getJobState() != State.KILLED
						&& aFutureJob.get().getJobState() != State.SUCCEEDED) {
					Thread.sleep(500);
				}
				this.countDownKillLatch.countDown();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		}

	}

	@AfterClass
	public static void restoreTheConfigFile() throws Exception {
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
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER,
				TestSession.cluster
						.getNodeNames(HadoopCluster.RESOURCE_MANAGER),
				TestSession.conf.getProperty("HADOOP_INSTALL_CONF_DIR"));

		Thread.sleep(20000);

                // wait up to 5 minutes for NN to be out of safemode
                DfsCliCommands dfsCliCommands = new DfsCliCommands();
                GenericCliResponseBO genericCliResponse;
                for (int waitCounter = 0; waitCounter < 30; waitCounter++) {
                  genericCliResponse = dfsCliCommands.dfsadmin(
                                DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
                                DfsTestsBaseClass.Report.NO,
                                "get",
                                DfsTestsBaseClass.ClearQuota.NO,
                                DfsTestsBaseClass.SetQuota.NO,
                                0,
                                DfsTestsBaseClass.ClearSpaceQuota.NO,
                                DfsTestsBaseClass.SetSpaceQuota.NO,
                                0,
                                DfsTestsBaseClass.PrintTopology.NO,
                                null);

                  if (genericCliResponse.response.contains("Safe mode is OFF")) {
                    TestSession.logger.info("NN is out of Safemode after " + (waitCounter*10) + " seconds");
                    break;
                  } else if (waitCounter >= 30) {
                    TestSession.logger.error("NN never left Safemode after " + (waitCounter*10) + " seconds!");
                    Assert.fail();
                  }
                  else
                  {
                    Thread.sleep(10000);
                  }
                }
	}

	class CapSchedCallableSleepJob implements Callable<Job> {
		HashMap<String, String> jobParamsMap;
		int numMapper;
		int numReducer;
		int mapSleepTime;
		int mapSleepCount;
		int reduceSleepTime;
		int reduceSleepCount;
		String userName;
		String jobName;
		ExpectToBomb expectedToBomb;

		public CapSchedCallableSleepJob(HashMap<String, String> jobParamsMap,
				int numMapper, int numReducer, int mapSleepTime,
				int mapSleepCount, int reduceSleepTime, int reduceSleepCount,
				String userName, String jobName, ExpectToBomb expectedToBomb) {
			this.jobParamsMap = jobParamsMap;
			this.numMapper = numMapper;
			this.numReducer = numReducer;
			this.mapSleepTime = mapSleepTime;
			this.mapSleepCount = mapSleepCount;
			this.reduceSleepTime = reduceSleepTime;
			this.reduceSleepCount = reduceSleepCount;
			this.userName = userName;
			this.jobName = jobName;
			this.expectedToBomb = expectedToBomb;

		}

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
				createdSleepJob.setJobName(jobName);
				TestSession.logger
						.info("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% "
								+ "submitting " + jobName
								+ " %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");

				createdSleepJob.submit();

			} catch (Exception e) {
				if (this.expectedToBomb == ExpectToBomb.YES) {
					TestSession.logger.fatal("YAY the job faile as expected "
							+ jobName);

					Assert.assertTrue(
							"Test failed on an " + "expected exception",
							e.getMessage().contains(
									"cannot accept submission of application"));

				} else {
					TestSession.logger.fatal("Submission of jobid [" + jobName
							+ "] via API barfed... !! expected exception "
							+ this.expectedToBomb);
					e.printStackTrace();
					TestSession.logger.info(e.getStackTrace());
					Assert.fail("Submission of jobid [" + jobName
							+ "] via API barfed... !! expected exception "
							+ this.expectedToBomb);
				}

			}
			return createdSleepJob;
		}

		// @Override
		// public Job call() {
		// Job createdSortJob = null;
		// jobParamsMap.put("mapreduce.randomwriter.bytespermap", "256000");
		// Configuration conf = TestSession.cluster.getConf();
		// for (String key : jobParamsMap.keySet()) {
		// conf.set(key, jobParamsMap.get(key));
		// }
		//
		// try {
		// TestSession.cluster.setSecurityAPI("keytab-" + userName,
		// "user-" + userName);
		//
		// Sort<Text, Text> sort = new Sort();
		// String DATA_DIR_IN_HDFS = "/HTF/testdata/dfs/";
		// String FSCK_TESTS_DIR_ON_HDFS = DATA_DIR_IN_HDFS
		// + "fsck_tests/";
		// String RANDOM_WRITER_DATA_DIR = FSCK_TESTS_DIR_ON_HDFS
		// + "randomWriter/testFsckResultsLeveragingRandomWriterAndSortJobs/";
		// Random rand = new Random();
		// int n = rand.nextInt(500) + 1;
		// String randomWriterOutputDir = "randWrtr" + rand;
		// sort.setConf(TestSession.cluster.getConf());
		// String[] args = new String[] {RANDOM_WRITER_DATA_DIR,
		// "/tmp/" + randomWriterOutputDir};
		// RunSortMethod runSortMethod = new RunSortMethod(sort, args);
		// Thread runnerThread = new Thread(runSortMethod);
		// TestSession.logger.info("AAAAAAAAAAAA Just before calling the start AAAAAAAAAAAA");
		// runnerThread.start();
		// TestSession.logger.info("BBBBBBBBBBBB Just after calling the start BBBBBBBBBBBB");
		// Thread.sleep(5000);
		// createdSortJob = sort.getResult();
		// TestSession.logger.info("CCCCCCCCCCCC Just after calling the getResult CCCCCCCCCCCCC");
		// createdSortJob.setJobName("htf-cap-sched-sort-job-named-"
		// + jobName + "-started-by-" + userName);
		// TestSession.logger
		// .info("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%"
		// + "%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%"
		// + "%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%"
		// + "%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
		//
		// // createdSortJob.submit();
		//
		// } catch (Exception e) {
		// Assert.fail("SleepJob invoked via API barfed...");
		//
		// }
		// return createdSortJob;
		// }
		// class RunSortMethod implements Runnable{
		// Sort<Text, Text> sort;
		// String[] args;
		// RunSortMethod(Sort<Text, Text> sort, String[] args){
		// this.sort = sort;
		// this.args = args;
		// }
		// @Override
		// public void run() {
		// try {
		// sort.run(args);
		// } catch (Exception e) {
		// e.printStackTrace();
		// }
		//
		// }
		//
		// };
		//
	}

	class SleepJobParams {
		public CalculatedCapacityLimitsBO calculatedCapacityLimitsBO;
		public HashMap<String, String> configProperties;
		public String queue;
		public String userName;
		public double factor;
		public int numTimesMapWouldSleep = 1;
		public int numTimesRedWouldSleep = 1;
		public int taskSleepDuration;
		public Double queueCapacityInTermsOfClusterMemory;
		public int numMapTasks = 0;
		public int numRedTasks = 0;
		String jobName;

		public SleepJobParams(CalculatedCapacityLimitsBO calculatedCapLimitsBO,
				HashMap<String, String> configProperties, String queue,
				String userName, double factor, int taskSleepDuration,
				String jobName) {
			super();
			this.calculatedCapacityLimitsBO = calculatedCapLimitsBO;
			this.configProperties = configProperties;
			this.userName = (userName.isEmpty() || userName == null) ? HadooptestConstants.UserNames.HADOOPQA
					: userName;
			this.queue = (queue.isEmpty() || queue == null) ? "default" : queue;
			this.factor = factor;
			this.taskSleepDuration = taskSleepDuration;
			this.jobName = jobName;

			QueueCapacityDetail queueDetails = null;
			for (QueueCapacityDetail aQueueDetail : calculatedCapLimitsBO.queueCapacityDetails) {
				if (aQueueDetail.name.equals(queue)) {
					queueDetails = aQueueDetail;
					break;
				}
			}
			this.queueCapacityInTermsOfClusterMemory = queueDetails.minCapacityForQueueInTermsOfTotalClusterMemoryInGB;
			int tempNumMapTasks = (this.queueCapacityInTermsOfClusterMemory
					.intValue() * 1024)
					/ Integer.parseInt(configProperties
							.get("mapreduce.map.memory.mb"));
			this.numMapTasks = (int) Math.ceil(tempNumMapTasks * factor);

			int tempNumReduceTasks = (this.queueCapacityInTermsOfClusterMemory
					.intValue() * 1024)
					/ Integer.parseInt(configProperties
							.get("mapreduce.reduce.memory.mb"));
			this.numRedTasks = (int) Math.ceil(tempNumReduceTasks * factor);

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
			TestSession.logger
					.info("Instantiating BarrierUntilAllThreadsRunning");

			this.maxWaitTimeForThread = maxWaitTimeForThread;
			this.futureCallableSleepJobs = futureCallableSleepJobs;

			final CyclicBarrier cyclicBarrierToWaitOnThreadStateRunnable = new CyclicBarrier(
					futureCallableSleepJobs.size(), new Runnable() {

						public void run() {
							// This task will be executed once all thread
							// reaches barrier
							System.out
									.println("All threads state==RUNNING now.....hence continue!");
						}
					});
			spawnedThreads = new ArrayList<Thread>();
			TestSession.logger
					.info("Before Looping through futures, size of futures:"
							+ futureCallableSleepJobs.size());
			for (Future<Job> aTetherToACallableSleepJob : futureCallableSleepJobs) {
				try {
					TestSession.logger
							.info("-------------[ About to start a blocking thread for "
									+ aTetherToACallableSleepJob.get()
											.getJobID() + "]-------------");
					ThreadThatWillWaitAtBarrier threadThatWillWaitAtBarrier = new ThreadThatWillWaitAtBarrier(
							cyclicBarrierToWaitOnThreadStateRunnable,
							aTetherToACallableSleepJob,
							this.maxWaitTimeForThread);
					Thread t = new Thread(threadThatWillWaitAtBarrier);
					spawnedThreads.add(t);
					t.start();
				} catch (ExecutionException e) {
					TestSession.logger.info(e.getCause());
					Assert.fail("Could not retrieve a future sleep job."
							+ " Are you sure you are firing the jobs within the premissible limits?");
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

			public void run() {
				try {
					TestSession.logger
							.info("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx Started blocking thread for "
									+ futureCallableSleepJob.get().getJobID()
									+ " xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
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
					while (aTetherToACallableSleepJob.get().getJobState() != expectedState
							&& (aTetherToACallableSleepJob.get().getJobState() != JobStatus.State.FAILED)) {
						TestSession.logger.trace("Current job state["
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

class QueueCapacityDetail {
	/**
	 * Stats (retrieved directly from REST). JobClient does not report pending
	 * apps, hence collecting the stats here.
	 */
	int numPendingApplications = 0;

	/**
	 * Queue specific stats
	 */
	String name;
	double minCapacityInTermsOfPercentage;
	double minCapacityForQueueInTermsOfTotalClusterMemoryInGB;
	double maxCapacityForQueueInTermsOfTotalClusterMemoryInGB;
	double userLimitFactor;
	double minimumUserLimitPercent;
	double maximumAmResourcePercent;
	double maxApplications;
	double maxApplicationsPerUser;
	double maxActiveApplications;
	double maxActiveApplicationsPerUser;

}
