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
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapreduce.JobStatus.State;

public class JobStatusMonitor extends AbstractMonitor {
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
		String dumpLocation;
		HashMap<String, Integer> jobStatusMap = new HashMap<String, Integer>();
		jobStatusMap.put("SUCCEEDED", 0);
		jobStatusMap.put("FAILED", 0);
		jobStatusMap.put("KILLED", 0);
		jobStatusMap.put("PREP", 0);
		jobStatusMap.put("UNKNOWN", 0);
		jobStatusMap.put("RUNNING", 0);

		dumpLocation = baseDirPathToDump + this.kind;

		jobSetOfInterest.removeAll(jobSetBeforeTestStarted);

		File jobStatusFileHandle = new File(dumpLocation);
		PrintWriter printWriter = null;

		try {

			jobStatusFileHandle.getParentFile().mkdirs();
			jobStatusFileHandle.createNewFile();

			printWriter = new PrintWriter(jobStatusFileHandle);
			TestSession.logger.info("Staging area dir:"
					+ jobClient.getStagingAreaDir());
			TestSession.logger.info("System dir:" + jobClient.getSystemDir());

			for (String aJobThatRanDuringTest : jobSetOfInterest) {
				TestSession.logger.info("aJobThatRanDuringTest:"
						+ aJobThatRanDuringTest);
				try {
					Thread.sleep(0);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				
				for (JobStatus aJobStatus : jobClient.getAllJobs()) {
					if (aJobStatus.getJobID().toString().trim()
							.equals(aJobThatRanDuringTest)) {
						if (aJobStatus.getState().name().equals("PREP")
								|| aJobStatus.getState().name()
										.equals("RUNNING")) {
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
						printWriter.print(aJobThatRanDuringTest + ":");
						State theJobState = aJobStatus.getState();
						jobStatusMap.put(theJobState.name(), 1);
						for (String aStateOfInterest : jobStatusMap.keySet()) {
							printWriter.print(aStateOfInterest + "="
									+ jobStatusMap.get(aStateOfInterest) + " ");
						}
						initJobStatusMap(jobStatusMap);
						printWriter.println();
					} else {
//						TestSession.logger.info(aJobThatRanDuringTest
//								+ " did not match "
//								+ aJobStatus.getJobID().toString().trim());
					}
				}

			}
			printWriter.flush();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (printWriter != null) {

				printWriter.close();
			}
		}

	}

	void initJobStatusMap(HashMap<String, Integer> jobStatusMap) {
		jobStatusMap.put("SUCCEEDED", 0);
		jobStatusMap.put("FAILED", 0);
		jobStatusMap.put("KILLED", 0);
		jobStatusMap.put("PREP", 0);
		jobStatusMap.put("UNKNOWN", 0);
		jobStatusMap.put("RUNNING", 0);

	}

	@Override
	public void fetchResourceUsageIntoMemory(int tick) throws IOException {
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
