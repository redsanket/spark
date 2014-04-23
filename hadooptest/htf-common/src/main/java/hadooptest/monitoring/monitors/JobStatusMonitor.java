package hadooptest.monitoring.monitors;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.TIPStatus;
import org.apache.hadoop.mapred.TaskReport;
import org.apache.hadoop.mapreduce.JobStatus.State;

public class JobStatusMonitor extends AbstractMonitor {
	private static final String JOB_STATUS_SUCCEEDED = "SUCCEEDED";
	private static final String JOB_STATUS_PREP = "PREP";
	private static final String JOB_STATUS_UNKNOWN = "UNKNOWN";
	private static final String JOB_STATUS_RUNNING = "RUNNING";
	private static final String JOB_STATUS_FAILED = "FAILED";
	private static final String JOB_STATUS_KILLED = "KILLED";
	
	private static final String TASK_STATUS_COMPLETED = "COMPLETE";
	private static final String TASK_STATUS_PENDING = "PENDING";
	private static final String TASK_STATUS_RUNNING = "RUNNING";
	private static final String TASK_STATUS_FAILED = "FAILED";
	private static final String TASK_STATUS_KILLED = "KILLED";


	HashSet<String> jobSetBeforeTestStarted;
	HashSet<String> jobSetOfInterest;
	long testStartTime;
	JobClient jobClient;
	public static ConcurrentHashMap<String, String> jobStatuses = new ConcurrentHashMap<String, String>();

	public JobStatusMonitor(String clusterName,
			HashMap<String, ArrayList<String>> sentComponentToHostMapping,
			Class<?> testClass, String testMethodName) {
		super(clusterName, sentComponentToHostMapping, testClass,
				testMethodName, 0, HadooptestConstants.Miscellaneous.JOB_STATUS);
		try {
			jobClient = new JobClient(TestSession.cluster.getConf());
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void logJobStatus() throws IOException {
		TestSession.logger
				.trace("````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````");
		String jobStatusDumpLocation;
		String mapTtaskStatusDumpLocation;
		String reduceTaskStatusDumpLocation;
		HashMap<String, Integer> jobStatusMap = new HashMap<String, Integer>();
		HashMap<String, Integer> mapTaskStatusMap = new HashMap<String, Integer>();
		HashMap<String, Integer> reduceTaskStatusMap = new HashMap<String, Integer>();
		TaskReport[] mapTaskReports;
		TaskReport[] reduceTaskReports;
		/**
		 * Initialize the counters
		 */
		initJobStatusMap(jobStatusMap);
		initTaskStatusMap(mapTaskStatusMap);
		initTaskStatusMap(reduceTaskStatusMap);

		/**
		 * Get the file locations
		 */
		jobStatusDumpLocation = baseDirPathToDump + this.kind;
		mapTtaskStatusDumpLocation = baseDirPathToDump
				+ HadooptestConstants.Miscellaneous.MAP_TASK_STATUS;
		reduceTaskStatusDumpLocation = baseDirPathToDump
				+ HadooptestConstants.Miscellaneous.REDUCE_TASK_STATUS;

		/**
		 * Get all the file handles
		 */
		File jobStatusFileHandle = new File(jobStatusDumpLocation);
		File mapTaskStatusFileHandle = new File(mapTtaskStatusDumpLocation);
		File reduceTaskStatusFileHandle = new File(
				reduceTaskStatusDumpLocation);
		/**
		 * Get all the print writers
		 */
		PrintWriter jobStatusPrintWriter = null;
		PrintWriter mapTaskStatusPrintWriter = null;
		PrintWriter reduceTaskStatusPrintWriter = null;

		jobSetOfInterest.removeAll(jobSetBeforeTestStarted);

		try {

			jobStatusFileHandle.getParentFile().mkdirs();
			// Create the files
			jobStatusFileHandle.createNewFile();
			mapTaskStatusFileHandle.createNewFile();
			reduceTaskStatusFileHandle.createNewFile();

			// Point the writers
			jobStatusPrintWriter = new PrintWriter(jobStatusFileHandle);
			mapTaskStatusPrintWriter = new PrintWriter(mapTaskStatusFileHandle);
			reduceTaskStatusPrintWriter = new PrintWriter(
					reduceTaskStatusFileHandle);

			for (String aJobThatRanDuringTest : jobSetOfInterest) {
				TestSession.logger.info("aJobThatRanDuringTest:"
						+ aJobThatRanDuringTest);

				for (JobStatus aJobStatus : jobClient.getAllJobs()) {
					if (aJobStatus.getJobID().toString().trim()
							.equals(aJobThatRanDuringTest)) {
						if (aJobStatus.getState().name().equals(JOB_STATUS_PREP)
								|| aJobStatus.getState().name()
										.equals(JOB_STATUS_RUNNING)) {
							/*
							 * These clearly are jobs fired by other tests,
							 * running in parallel. We are just interested in
							 * "completed" jobs, hence pruning the list.
							 */
							continue;
						}
						TestSession.logger.info(aJobThatRanDuringTest
								+ " MATCHED "
								+ aJobStatus.getJobID().toString().trim());
						jobStatusPrintWriter.print(aJobThatRanDuringTest + ":");
						State theJobState = aJobStatus.getState();
						jobStatusMap.put(theJobState.name(), 1);
						for (String aStateOfInterest : jobStatusMap.keySet()) {
							jobStatusPrintWriter.print(aStateOfInterest + "="
									+ jobStatusMap.get(aStateOfInterest) + " ");
						}
						jobStatusPrintWriter.println();
						initJobStatusMap(jobStatusMap);

						/**
						 * Get the tasks (Map/Reduce) that this job spawned
						 */
						try {
							mapTaskReports = jobClient.getMapTaskReports(JobID
									.forName(aJobThatRanDuringTest));
							reduceTaskReports = jobClient
									.getReduceTaskReports(JobID
											.forName(aJobThatRanDuringTest));
						} catch (IllegalArgumentException e) {
							TestSession.logger
									.info("Received IllegalArgumentException while looking for tasks for "
											+ aJobThatRanDuringTest);
							continue;
						} catch (IOException e) {
							TestSession.logger
									.info("Received IOException while looking for tasks for "
											+ aJobThatRanDuringTest);
							continue;
						}

						// Since a Job can spawn multiple Tasks
						for (TaskReport aMapTaskReport : mapTaskReports) {
							TestSession.logger.info("Job:"
									+ aJobThatRanDuringTest
									+ " had a map task "
									+ aMapTaskReport.getTaskId()
									+ " that had state "
									+ aMapTaskReport.getCurrentStatus());
							if (aMapTaskReport.getCurrentStatus() == TIPStatus.COMPLETE) {
								mapTaskStatusMap.put(TASK_STATUS_COMPLETED, 1);
							} else if (aMapTaskReport.getCurrentStatus() == TIPStatus.FAILED) {
								mapTaskStatusMap.put(TASK_STATUS_FAILED, 1);

							} else if (aMapTaskReport.getCurrentStatus() == TIPStatus.KILLED) {
								mapTaskStatusMap.put(TASK_STATUS_KILLED, 1);
							} else if (aMapTaskReport.getCurrentStatus() == TIPStatus.PENDING) {
								mapTaskStatusMap.put(TASK_STATUS_PENDING, 1);
							} else if (aMapTaskReport.getCurrentStatus() == TIPStatus.RUNNING) {
								mapTaskStatusMap.put(TASK_STATUS_RUNNING, 1);
							}

							/**
							 * Write task statuses to file
							 */
							mapTaskStatusPrintWriter.print(aMapTaskReport.getTaskId() + ":");
							for (String aStateOfInterest : mapTaskStatusMap.keySet()) {
								// Map
								mapTaskStatusPrintWriter.print(aStateOfInterest
										+ "="
										+ mapTaskStatusMap
												.get(aStateOfInterest) + " ");
							}
							mapTaskStatusPrintWriter.println();
							initTaskStatusMap(mapTaskStatusMap);

						}// End of FOR, loop for map tasks

						// Do the same for Reduce tasks
						for (TaskReport aReduceTaskReport : reduceTaskReports) {
							TestSession.logger.info("Job:"
									+ aJobThatRanDuringTest
									+ " had a reduce task "
									+ aReduceTaskReport.getTaskId()
									+ " that had state "
									+ aReduceTaskReport.getCurrentStatus());
							if (aReduceTaskReport.getCurrentStatus() == TIPStatus.COMPLETE) {
								reduceTaskStatusMap.put(TASK_STATUS_COMPLETED, 1);
							} else if (aReduceTaskReport.getCurrentStatus() == TIPStatus.FAILED) {
								reduceTaskStatusMap.put(TASK_STATUS_FAILED, 1);

							} else if (aReduceTaskReport.getCurrentStatus() == TIPStatus.KILLED) {
								reduceTaskStatusMap.put(TASK_STATUS_KILLED, 1);
							} else if (aReduceTaskReport.getCurrentStatus() == TIPStatus.PENDING) {
								reduceTaskStatusMap.put(TASK_STATUS_PENDING, 1);
							} else if (aReduceTaskReport.getCurrentStatus() == TIPStatus.RUNNING) {
								reduceTaskStatusMap.put(TASK_STATUS_RUNNING, 1);
							}

							/**
							 * Write task statuses to file
							 */
							reduceTaskStatusPrintWriter.print(aReduceTaskReport.getTaskId() + ":");
							for (String aStateOfInterest : reduceTaskStatusMap.keySet()) {
								// Map
								reduceTaskStatusPrintWriter
										.print(aStateOfInterest
												+ "="
												+ reduceTaskStatusMap.get(aStateOfInterest)
												+ " ");
							}
							reduceTaskStatusPrintWriter.println();
							initTaskStatusMap(reduceTaskStatusMap);							
						}// End of FOR loop for reduce tasks.
					}// End of IF
				}// End of FOR:JobStatus[]
			}// End of FOR:aJobThatRanDuringTest

			jobStatusPrintWriter.flush();
			mapTaskStatusPrintWriter.flush();
			reduceTaskStatusPrintWriter.flush();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (jobStatusPrintWriter != null) {
				jobStatusPrintWriter.close();
			}
			if (mapTaskStatusPrintWriter != null) {
				mapTaskStatusPrintWriter.close();
			}
			if (reduceTaskStatusPrintWriter != null) {
				reduceTaskStatusPrintWriter.close();
			}

		}

	}

	void initJobStatusMap(HashMap<String, Integer> jobStatusMap) {
		jobStatusMap.put(JOB_STATUS_SUCCEEDED, 0);
		jobStatusMap.put(JOB_STATUS_PREP, 0);
		jobStatusMap.put(JOB_STATUS_UNKNOWN, 0);
		jobStatusMap.put(JOB_STATUS_RUNNING, 0);
		jobStatusMap.put(JOB_STATUS_FAILED, 0);
		jobStatusMap.put(JOB_STATUS_KILLED, 0);

	}

	void initTaskStatusMap(HashMap<String, Integer> taskStatusMap) {
		taskStatusMap.put(TASK_STATUS_COMPLETED, 0);
		taskStatusMap.put(TASK_STATUS_PENDING, 0);
		taskStatusMap.put(TASK_STATUS_RUNNING, 0);		
		taskStatusMap.put(TASK_STATUS_FAILED, 0);
		taskStatusMap.put(TASK_STATUS_KILLED, 0);
	}


	@Override
	public void monitorDoYourThing(int tick) throws IOException {
		/*
		 * Not applicable, because TestStatusMonitor is not a monitor in a true
		 * sense
		 */

	}

	public void startMonitoring() {

		testStartTime = TestSession.startTime;
		testStartTime = (testStartTime / 1000000) * 1000000;
		try {

			jobSetBeforeTestStarted = new HashSet<String>();

			for (JobStatus aJobStatus : jobClient.getAllJobs()) {
				jobSetBeforeTestStarted.add(aJobStatus.getJobID().toString());
			}
			TestSession.logger.info("Count of jobs, before running test:"
					+ jobSetBeforeTestStarted.size() + " test start time is:"
					+ testStartTime);

		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

	@Override
	public void stopMonitoring() {
		TestSession.logger.info("Stop monitoring received for JOB Status for"
				+ testMethodBeingMonitored);

		try {
			jobSetOfInterest = new HashSet<String>();
			for (JobStatus aJobStatus : jobClient.getAllJobs()) {
				jobSetOfInterest.add(aJobStatus.getJobID().toString());
			}
			TestSession.logger.info("Count of jobs, after test:"
					+ jobSetOfInterest.size());

		} catch (IOException e1) {
			e1.printStackTrace();
		}

		try {
			logJobStatus();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	synchronized public void run() {
		/*
		 * Not applicable, because TestStatusMonitor is not a monitor in a true
		 * sense
		 */
	}

	@Override
	public void dumpData() {

	}

}
