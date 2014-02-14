package hadooptest.hadoop.stress;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.TIPStatus;
import org.apache.hadoop.mapred.TaskReport;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SerialTests.class)
public class GraphGenerationUtilAndNotATest extends TestSession {
	String packageFilters = null;
	String classFilters = null;;
	HashMap<String, ArrayList<HashMap<String, Integer>>> table1TestRunHash;
	HashMap<String, ArrayList<HashMap<String, Integer>>> table2TestRunHash;
	HashMap<String, ArrayList<HashMap<String, String>>> table3TestRunHash;
	HashMap<String, LinkedHashSet<String>> uniqueJobSetAgainstEachRun;
	HashMap<String, JobStatusCountStructure> realTable4TestRunHashForJobStatus;
	HashMap<String, TaskStatusCountStructure> realTable5TestRunHashForTaskStatus;

	LinkedHashSet<String> exceptionFootprint = new LinkedHashSet<String>();
	LinkedHashSet<String> testFootprint = new LinkedHashSet<String>();
	LinkedHashSet<String> runFootprint = new LinkedHashSet<String>();
	HashSet<String> testFootprintPrecededWithNumber = new HashSet<String>();

	private PrintWriter printWriter;
	private String table1DataConsumedByPerlForGraphGeneration = "/homes/hadoopqa/table1GraphData.txt";
	private String table2DataConsumedByPerlForGraphGeneration = "/homes/hadoopqa/table2GraphData.txt";
	private String table3DataConsumedByPerlForGraphGeneration = "/homes/hadoopqa/table3GraphData.txt";
	private String table4DataConsumedByPerlForGraphGeneration = "/homes/hadoopqa/table4GraphData.txt";
	private String table5DataConsumedByPerlForGraphGeneration = "/homes/hadoopqa/table4GraphData.txt";
	private String table1GraphTitle = "Count_of_exceptions_per_Test_across_different_test_runs";
	private String table2GraphTitle = "Total_count_of_exceptions_across_different_test_runs";
	private String table3GraphTitle = "Test_Failures";
	private String table4GraphTitle = "Job_Statuses";
	private String table5GraphTitle = "Task_Statuses";
	// Axes
	private String table1XAxisTitle = "Test_Names";
	private String table1YAxisTitle = "Count_of_Exceptions";
	private String table2XAxisTitle = "Exceptions";
	private String table2YAxisTitle = "Count_of_Exceptions";
	private String table3XAxisTitle = "Test_Names";
	private String table3YAxisTitle = "Test_Runs_Where_It_Failed";
	private String table4XAxisTitle = "Runs";
	private String table4YAxisTitle = "Counts";
	private String table5XAxisTitle = "Runs";
	private String table5YAxisTitle = "Counts";

	private String table1OutputGraphFileName = TestSession.conf
			.getProperty("WORKSPACE") + "/target/surefire-reports/GraphTestsVrsCountOfExceptions.png";
	private String table2OutputGraphFileName = TestSession.conf
			.getProperty("WORKSPACE") + "/target/surefire-reports/GraphCountOfExceptions.png";
	private String table3OutputGraphFileName = TestSession.conf
			.getProperty("WORKSPACE") + "/target/surefire-reports/GraphTestSuccessesFailures.png";
	private String table4OutputGraphFileName = TestSession.conf
			.getProperty("WORKSPACE") + "/target/surefire-reports/GraphJobStats.png";
	private String table5OutputGraphFileName = TestSession.conf
			.getProperty("WORKSPACE") + "/target/surefire-reports/GraphTaskStats.png";

	private String generateLineGraphForMonitoring = TestSession.conf
			.getProperty("WORKSPACE")
			+ "/scripts/generateLineGraphForMonitoring.pl";

	private String generateScatterPlotForMonitoring = TestSession.conf
			.getProperty("WORKSPACE")
			+ "/scripts/generateScatterPlotForMonitoring.pl";

	private String generateBarGraphForMonitoring = TestSession.conf
			.getProperty("WORKSPACE")
			+ "/scripts/generateBarGraphForMonitoring.pl";

	private String EITHER_PASSED_OR_NOT_RUN = "EITHER_PASSED_OR_NOT_RUN";

	private static final String SUCCEEDED = "SUCCEEDED";
	private static final String FAILED = "FAILED";
	private static final String KILLED = "KILLED";
	private static final String UNKNOWN = "UNKNOWN";

	public GraphGenerationUtilAndNotATest() {
		this.packageFilters = System.getProperty("PACKAGE_FILTERS");
		this.classFilters = System.getProperty("CLASS_FILTERS");
	}

	@BeforeClass
	public static void startTestSession() throws Exception {

		TestSession.start();
	}

	class JobStatusCountStructure {
		public JobStatusCountStructure(int succeeded, int failed, int killed,
				int unknown) {
			this.succeeded = succeeded;
			this.failed = failed;
			this.killed = killed;
			this.unknown = unknown;
		}

		int succeeded;
		int failed;
		int killed;
		int unknown;

		int get(String status) {
			if (status.equals(SUCCEEDED)) {
				return succeeded;
			} else if (status.equals(FAILED)) {
				return failed;
			} else if (status.equals(KILLED)) {
				return killed;
			} else if (status.equals(UNKNOWN)) {
				return unknown;
			} else {
				return -1;
			}
		}
	}

	class TaskStatusCountStructure {
		public TaskStatusCountStructure(int succeeded, int failed, int killed) {
			this.succeeded = succeeded;
			this.failed = failed;
			this.killed = killed;
			TestSession.logger.info("Created task with details: succeeded[" + succeeded+"] failed[" + failed +"] killed[" + killed +"]");

		}

		int succeeded;
		int failed;
		int killed;

		int get(String status) {
			if (status.equals(SUCCEEDED)) {
				return succeeded;
			} else if (status.equals(FAILED)) {
				return failed;
			} else if (status.equals(KILLED)) {
				return killed;
			} else {
				return -1;
			}
		}
	}

	@Test
	public void testProcessExcetions() throws Exception {
		// Table-1 vars
		HashMap<String, Integer> perTestExceptionCountHashNode;
		table1TestRunHash = new HashMap<String, ArrayList<HashMap<String, Integer>>>();

		// Table-2 vars
		HashMap<String, Integer> perRunExceptionCountHashNode;
		table2TestRunHash = new HashMap<String, ArrayList<HashMap<String, Integer>>>();

		// Table-3 vars
		HashMap<String, String> perRunTestStatusHashNode;
		table3TestRunHash = new HashMap<String, ArrayList<HashMap<String, String>>>();

		// Table-4 vars
		uniqueJobSetAgainstEachRun = new HashMap<String, LinkedHashSet<String>>();

		int exceptionCountPerTest = 0;

		String cluster = System.getProperty("CLUSTER_NAME");
		File clusterDir = new File("/grid/0/tmp/stressMonitoring/" + cluster);
		String[] packazes = clusterDir.list();
		for (String packaze : packazes) {
			if (this.packageFilters != null) {
				if (!this.packageFilters.contains(packaze)) {
					/*
					 * Disregard the packages we are not interested in.
					 */
					continue;
				}
			}
			File packageDir = new File("/grid/0/tmp/stressMonitoring/"
					+ cluster + "/" + packaze);
			TestSession.logger.trace("Processing Package:" + packaze);
			String[] classes = packageDir.list();
			for (String clazz : classes) {
				if (this.classFilters != null) {
					if (!this.classFilters.contains(clazz)) {
						/*
						 * Disregard the classes we are not interested in.
						 */
						continue;
					}
				}
				TestSession.logger.trace("Processing Class:" + clazz);
				File classDir = new File("/grid/0/tmp/stressMonitoring/"
						+ cluster + "/" + packaze + "/" + clazz);
				String[] tests = classDir.list();
				for (String test : tests) {
					TestSession.logger.trace("Processing test:" + test);
					testFootprint.add(test);
					TestSession.logger.trace("=====>[" + test + "]<=======");
					File testDir = new File("/grid/0/tmp/stressMonitoring/"
							+ cluster + "/" + packaze + "/" + clazz + "/"
							+ test);
					String[] runDates = testDir.list();
					Arrays.sort(runDates);
					for (String aRunDate : runDates) {
						runFootprint.add(aRunDate);
						/*
						 * Process Exceptions here. NOTE: TestStatus (failures)
						 * are processed later (in the same loop)
						 */
						TestSession.logger.trace("Processing testRun:"
								+ aRunDate);
						exceptionCountPerTest = 0;
						File exceptionsFile = new File(
								"/grid/0/tmp/stressMonitoring/" + cluster + "/"
										+ packaze + "/" + clazz + "/" + test
										+ "/" + aRunDate + "/" + "EXCEPTIONS");
						if (!exceptionsFile.exists()) {
							continue;
						}
						BufferedReader exceptionsBufferedReader = new BufferedReader(
								new FileReader("/grid/0/tmp/stressMonitoring/"
										+ cluster + "/" + packaze + "/" + clazz
										+ "/" + test + "/" + aRunDate + "/"
										+ "EXCEPTIONS"));
						String line = null;
						while ((line = exceptionsBufferedReader.readLine()) != null) {

							String[] exceptionDetails = line.split(":");
							String exceptionName = exceptionDetails[0];
							int newExceptionCount = Integer
									.parseInt(exceptionDetails[1]);
							exceptionFootprint.add(exceptionName);
							exceptionCountPerTest += newExceptionCount;
							/*
							 * Care for populating Table-2 here, as you run into
							 * new exceptions, as you will be plotting: X-Axis:
							 * Exceptions (kinds) Y-Axis: Exception (count)
							 */
							perRunExceptionCountHashNode = new HashMap<String, Integer>();
							perRunExceptionCountHashNode.put(exceptionName,
									newExceptionCount);

							if (table2TestRunHash.containsKey(aRunDate)) {
								boolean exceptionPresent = false;
								for (HashMap<String, Integer> exceptionTuple : table2TestRunHash
										.get(aRunDate)) {
									// Is our exception already present in the
									// list?
									if (exceptionTuple
											.containsKey(exceptionName)) {
										exceptionPresent = true;
										int registeredExceptionCount = exceptionTuple
												.get(exceptionName);
										exceptionTuple.put(exceptionName,
												registeredExceptionCount
														+ newExceptionCount);
										break;
									}
								}
								// Make an entry, if exception not present
								if (!exceptionPresent) {
									table2TestRunHash.get(aRunDate).add(
											perRunExceptionCountHashNode);

								}

							} else {
								ArrayList<HashMap<String, Integer>> listOfExceptionsCountPerRun = new ArrayList<HashMap<String, Integer>>();
								listOfExceptionsCountPerRun
										.add(perRunExceptionCountHashNode);
								table2TestRunHash.put(aRunDate,
										listOfExceptionsCountPerRun);

							}

						}// End of Exception file processing (while - loop)
						exceptionsBufferedReader.close();
						perTestExceptionCountHashNode = new HashMap<String, Integer>();
						perTestExceptionCountHashNode.put(test,
								exceptionCountPerTest);

						/*
						 * Care for Table-1 here. X:axis T1, T2, T3..
						 * Y:Axis:Count of Exceptions encountered/test
						 */
						if (table1TestRunHash.containsKey(aRunDate)) {
							table1TestRunHash.get(aRunDate).add(
									perTestExceptionCountHashNode);
						} else {
							ArrayList<HashMap<String, Integer>> listOfExceptionsCountPerTest = new ArrayList<HashMap<String, Integer>>();
							listOfExceptionsCountPerTest
									.add(perTestExceptionCountHashNode);
							table1TestRunHash.put(aRunDate,
									listOfExceptionsCountPerTest);

						}
						/*
						 * ---------------------------- Table-3
						 * --------------------
						 */
						BufferedReader testStatusBufferedReader = new BufferedReader(
								new FileReader("/grid/0/tmp/stressMonitoring/"
										+ cluster + "/" + packaze + "/" + clazz
										+ "/" + test + "/" + aRunDate + "/"
										+ "TEST_STATUS"));
						String failureLine = null;
						while ((failureLine = testStatusBufferedReader
								.readLine()) != null) {
							/*
							 * Care for Table-3 here X:axis failed tests, T1,
							 * T2, T3.. Y:Axis:Run
							 */
							perRunTestStatusHashNode = new HashMap<String, String>();
							if (failureLine.equals("FAILED")) {
								perRunTestStatusHashNode.put(test, aRunDate);
								if (table3TestRunHash.containsKey(aRunDate)) {
									table3TestRunHash.get(aRunDate).add(
											perRunTestStatusHashNode);
								} else {
									ArrayList<HashMap<String, String>> aRunOfTestFails = new ArrayList<HashMap<String, String>>();
									aRunOfTestFails
											.add(perRunTestStatusHashNode);
									table3TestRunHash.put(aRunDate,
											aRunOfTestFails);
								}
							} else {
								// Test has PASSED
								perRunTestStatusHashNode.put(test,
										EITHER_PASSED_OR_NOT_RUN);
								if (table3TestRunHash.containsKey(aRunDate)) {
									table3TestRunHash.get(aRunDate).add(
											perRunTestStatusHashNode);
								} else {
									ArrayList<HashMap<String, String>> aRunOfTestFails = new ArrayList<HashMap<String, String>>();
									aRunOfTestFails
											.add(perRunTestStatusHashNode);
									table3TestRunHash.put(aRunDate,
											aRunOfTestFails);
								}

							}

						}// End of testStatus (failed) file processing (while -
							// loop)
						testStatusBufferedReader.close();

						/*
						 * Table-4 Processing.............
						 */
						BufferedReader jobStatusBufferedReader = new BufferedReader(
								new FileReader("/grid/0/tmp/stressMonitoring/"
										+ cluster + "/" + packaze + "/" + clazz
										+ "/" + test + "/" + aRunDate + "/"
										+ "JOB_STATUS"));

						if (!uniqueJobSetAgainstEachRun.containsKey(aRunDate)) {
							LinkedHashSet<String> aSetOfjobStatusLines = new LinkedHashSet<String>();
							uniqueJobSetAgainstEachRun.put(aRunDate,
									aSetOfjobStatusLines);
						}

						String jobStatusLine = null;
						while ((jobStatusLine = jobStatusBufferedReader
								.readLine()) != null) {

							LinkedHashSet<String> uniqueJobStatuses = uniqueJobSetAgainstEachRun
									.get(aRunDate);
							uniqueJobStatuses.add(jobStatusLine);

						}// End of jobStatus file processing (while-loop)
						jobStatusBufferedReader.close();

					}// End of Test Run (for - loop)

				}

			}
		}
		TestSession.logger.trace(table1TestRunHash);
		System.out
				.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
		TestSession.logger.trace(table2TestRunHash);
		System.out
				.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
		TestSession.logger.trace(table3TestRunHash);

	}

	
	public void countTaskStatuses() {
		realTable5TestRunHashForTaskStatus = new HashMap<String, TaskStatusCountStructure>();
		for (String aRun : uniqueJobSetAgainstEachRun.keySet()) {
			TestSession.logger.info("Generating task reports for run:" + aRun);
			realTable5TestRunHashForTaskStatus.put(aRun,
					getTaskStatusCountsForRun(aRun));
		}
		/**
		 * Since the task stats are collected after the current test has run, and
		 * since one cannot retrieve for very old tasks, in order to cut down on
		 * the time whatever data has been collected go and write it to the respective
		 * run folders, so save time later.
		 */

	}

	@After
	public void generateGraphs() throws Exception {
		countTaskStatuses();
		String outputBaseDirPath = TestSession.conf.getProperty("WORKSPACE")
				+ "/target/surefire-reports";
		File outputDir = new File(outputBaseDirPath);
		outputDir.mkdirs();
		File outputFile1 = new File(table1OutputGraphFileName);
		File outputFile2 = new File(table2OutputGraphFileName);
		try {
			outputFile1.createNewFile();
			outputFile2.createNewFile();
		} catch (IOException ioe) {
			TestSession.logger.error("Could not create graph files", ioe);
		}

		// Table-1
		generateTable1Data(table1DataConsumedByPerlForGraphGeneration);
		runPerlScriptToGenerateGraph(generateLineGraphForMonitoring,
				table1GraphTitle, table1XAxisTitle, table1YAxisTitle,
				table1DataConsumedByPerlForGraphGeneration,
				table1OutputGraphFileName);
		// Table-2
		generateTable2Data(table2DataConsumedByPerlForGraphGeneration);
		runPerlScriptToGenerateGraph(generateLineGraphForMonitoring,
				table2GraphTitle, table2XAxisTitle, table2YAxisTitle,
				table2DataConsumedByPerlForGraphGeneration,
				table2OutputGraphFileName);
		// Table-3
		generateTable3Data(table3DataConsumedByPerlForGraphGeneration);
		runPerlScriptToGenerateGraph(generateScatterPlotForMonitoring,
				table3GraphTitle, table3XAxisTitle, table3YAxisTitle,
				table3DataConsumedByPerlForGraphGeneration,
				table3OutputGraphFileName);

		// Table-4
		realTable4TestRunHashForJobStatus = new HashMap<String, JobStatusCountStructure>();
		for (String aRun : uniqueJobSetAgainstEachRun.keySet()) {
			realTable4TestRunHashForJobStatus.put(aRun,
					getJobStatusCountsForRun(aRun));
		}
		generateTable4Data(table4DataConsumedByPerlForGraphGeneration);
		runPerlScriptToGenerateGraph(generateBarGraphForMonitoring,
				table4GraphTitle, table4XAxisTitle, table4YAxisTitle,
				table4DataConsumedByPerlForGraphGeneration,
				table4OutputGraphFileName);
		
		// Table-5
		generateTable5Data(table5DataConsumedByPerlForGraphGeneration);
		runPerlScriptToGenerateGraph(generateBarGraphForMonitoring,
				table5GraphTitle, table5XAxisTitle, table5YAxisTitle,
				table5DataConsumedByPerlForGraphGeneration,
				table5OutputGraphFileName);

	}

	JobStatusCountStructure getJobStatusCountsForRun(String aDate) {
		int succeeded = 0;
		int failed = 0;
		int killed = 0;
		int unknown = 0;

		LinkedHashSet<String> aSetOfjobStatusLines = uniqueJobSetAgainstEachRun
				.get(aDate);
		for (String aStatusLineWithJobId : aSetOfjobStatusLines) {
			String aStatusLineWithoutJobId = aStatusLineWithJobId.split(":")[1];
			for (String aStatus : aStatusLineWithoutJobId.split("\\s+")) {
				String aCounter = aStatus.split("=")[1];
				if (aStatus.contains(SUCCEEDED)) {

					if (aCounter.equals("1")) {
						succeeded++;
					}
				} else if (aStatus.contains(UNKNOWN)) {
					if (aCounter.equals("1")) {
						unknown++;
					}

				} else if (aStatus.contains(FAILED)) {
					if (aCounter.equals("1")) {
						failed++;
					}

				} else if (aStatus.contains(KILLED)) {
					if (aCounter.equals("1")) {
						killed++;
					}

				} else {
					// Don't care
				}
			}
		}
		JobStatusCountStructure jobStatusCounts = new JobStatusCountStructure(
				succeeded, failed, killed, unknown);
		return jobStatusCounts;
	}

	TaskStatusCountStructure getTaskStatusCountsForRun(String aDate) {
		int complete = 0;
		int failed = 0;
		int killed = 0;
		JobClient jobClient = null;
		LinkedHashSet<String> aSetOfjobStatusLines = uniqueJobSetAgainstEachRun
				.get(aDate);


			try {
				jobClient = new JobClient(TestSession.cluster.getConf());
			} catch (IOException e) {
				e.printStackTrace();
			}

			for (String aStatusLineWithJobId : aSetOfjobStatusLines) {
				String aJobId = aStatusLineWithJobId.split(":")[0];
				TaskReport[] taskReports = null;
				
				try {
					/**
					 * Since this class may be invoked, in some cases days after
					 * the original test may have run, it is likely when it queries
					 * for Jobs/Task, it would not be able to look them up.
					 */

					taskReports = jobClient.getMapTaskReports(JobID.forName(aJobId));
				} catch (IllegalArgumentException e) {
					TestSession.logger.error("Check why you received this exception, when looking up jobId: " + aJobId);
					continue;
				}catch ( IOException e){				
					TestSession.logger.info("Not able to lookup " + aJobId);
					continue;
				}
				for (TaskReport aTaskReport:taskReports){
					TestSession.logger.info("Job:" + aJobId + " had a task " + aTaskReport.getTaskId() + " that had state " + aTaskReport.getCurrentStatus());
					if (aTaskReport.getCurrentStatus() == TIPStatus.COMPLETE){
						complete++;
					}else if (aTaskReport.getCurrentStatus() == TIPStatus.FAILED){
						failed++;
					}else if(aTaskReport.getCurrentStatus() == TIPStatus.KILLED){
						killed++;
					}
					
				}
			}

		TaskStatusCountStructure taskStatusCounts = new TaskStatusCountStructure(
				complete, failed, killed);
		return taskStatusCounts;
	}

	public void runPerlScriptToGenerateGraph(String script, String graphTitle,
			String xAxisTitle, String yAxisTitle, String inputFile,
			String outputFile) throws Exception {

		StringBuilder sb = new StringBuilder();
		sb.append(HadooptestConstants.Location.Binary.PERL);
		sb.append(" ");
		sb.append(script);
		sb.append(" ");
		sb.append("-t");
		sb.append(" ");
		sb.append(graphTitle);
		sb.append(" ");
		sb.append("-x");
		sb.append(" ");
		sb.append(xAxisTitle);
		sb.append(" ");
		sb.append("-y");
		sb.append(" ");
		sb.append(yAxisTitle);
		sb.append(" ");
		sb.append("-i");
		sb.append(" ");
		sb.append(inputFile);
		sb.append(" ");
		sb.append("-o");
		sb.append(" ");
		sb.append(outputFile);
		Process process = TestSession.exec.runProcBuilderGetProc(sb.toString()
				.split("\\s+"));
		printResponseAndReturnItAsString(process);
		TestSession.logger.trace("Xit value:" + process.exitValue());
	}

	String printResponseAndReturnItAsString(Process process)
			throws InterruptedException {
		StringBuffer sb = new StringBuffer();
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				process.getInputStream()));

		String line;
		try {
			line = reader.readLine();
			while (line != null) {
				sb.append(line);
				sb.append("/n");
				TestSession.logger.debug(line);
				line = reader.readLine();
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		process.waitFor();
		return sb.toString();
	}

	/*
	 * Bar-graph depicting, job status (success/failed/killed/unknown)
	 */
	public void generateTable4Data(String dataOutputFile)
			throws FileNotFoundException {
		TestSession.logger.trace("Printing TABLE-4....");
		printWriter = new PrintWriter(dataOutputFile);
		Object[] runFootPrintArray = runFootprint.toArray();
		Arrays.sort(runFootPrintArray);
		for (Object aRunName : runFootPrintArray) {
			printWriter.print(aRunName + " ");
		}

		printWriter.println();
		String[] jobStatuses = new String[] { SUCCEEDED, FAILED, KILLED,
				UNKNOWN };
		for (String ajobStatus : jobStatuses) {
			for (String aRun : realTable4TestRunHashForJobStatus.keySet()) {
				JobStatusCountStructure jobStatusStruct = realTable4TestRunHashForJobStatus
						.get(aRun);
				printWriter.print(jobStatusStruct.get(ajobStatus) + " ");
			}
			printWriter.println();

		}
		printWriter.close();
	}

	/*
	 * Bar-graph depicting, task status (success/failed/killed)
	 */
	public void generateTable5Data(String dataOutputFile)
			throws FileNotFoundException {
		TestSession.logger.trace("Printing TABLE-5....");
		printWriter = new PrintWriter(dataOutputFile);
		Object[] runFootPrintArray = runFootprint.toArray();
		Arrays.sort(runFootPrintArray);
		for (Object aRunName : runFootPrintArray) {
			printWriter.print(aRunName + " ");
		}

		printWriter.println();
		String[] taskStatuses = new String[] { SUCCEEDED, FAILED, KILLED};
		for (String aTaskStatus : taskStatuses) {
			for (Object aRun : runFootPrintArray) {
				TaskStatusCountStructure taskStatusStruct = realTable5TestRunHashForTaskStatus
						.get((String)aRun);
				printWriter.print(taskStatusStruct.get(aTaskStatus) + " ");
			}
			printWriter.println();

		}
		printWriter.close();
	}

	/*
	 * Line graph, with Test cases in the X-axis Count of exceptions per test,
	 * on the Y-axis
	 */
	public void generateTable1Data(String dataOutputFile)
			throws FileNotFoundException {
		boolean nodeFoundInRun = false;
		TestSession.logger.trace("Printing TABLE-1....");
		printWriter = new PrintWriter(dataOutputFile);
		Object[] testFootPrintArray = testFootprint.toArray();
		Arrays.sort(testFootPrintArray);
		for (Object aTestName : testFootPrintArray) {
			printWriter.print(aTestName + " ");
		}

		printWriter.println();

		for (String aRun : table1TestRunHash.keySet()) {
			printWriter.println("Run:" + aRun);
			for (Object aTestNameOnXAxis : testFootPrintArray) {
				for (HashMap<String, Integer> dataNodeInTable1InSomeRun : table1TestRunHash
						.get(aRun)) {
					if (dataNodeInTable1InSomeRun
							.containsKey((String) aTestNameOnXAxis)) {
						nodeFoundInRun = true;
						printWriter.print(dataNodeInTable1InSomeRun
								.get((String) aTestNameOnXAxis) + " ");
						break;
					}
				}
				if (!nodeFoundInRun) {
					printWriter.print(0 + " ");
				} else {
					nodeFoundInRun = false;
				}

			}

			printWriter.println();
		}
		printWriter.close();
	}

	/*
	 * Data for plotting Exceptions in the X-axis Count of those exceptions/per
	 * run, in the Y-axis
	 */
	public void generateTable2Data(String dataOutputFile)
			throws FileNotFoundException {
		boolean nodeFoundInRun = false;
		TestSession.logger.trace("Printing TABLE-2....");
		printWriter = new PrintWriter(dataOutputFile);
		Object[] exceptionFootPrintArray = exceptionFootprint.toArray();
		Arrays.sort(exceptionFootPrintArray);
		for (Object anExceptionName : exceptionFootPrintArray) {
			printWriter.print(anExceptionName + "  ");
		}
		printWriter.println();
		for (String aRun : table2TestRunHash.keySet()) {
			printWriter.println("Run:" + aRun);
			for (Object anExceptionOnTheXAxis : exceptionFootPrintArray) {
				for (HashMap<String, Integer> nodeInARunInTable2 : table2TestRunHash
						.get(aRun)) {
					if (nodeInARunInTable2
							.containsKey((String) anExceptionOnTheXAxis)) {
						nodeFoundInRun = true;
						printWriter.print(nodeInARunInTable2
								.get((String) anExceptionOnTheXAxis) + " ");
					}

				}
				if (!nodeFoundInRun) {
					printWriter.print(0 + " ");
				}
				nodeFoundInRun = false;
			}
			printWriter.println();
		}
		printWriter.close();
	}

	/*
	 * Data for plotting test faiures X-Axis: test names Y-Axis: failures in a
	 * run
	 */
	public void generateTable3Data(String dataOutputFile)
			throws FileNotFoundException {
		boolean nodeFoundInRun = false;
		TestSession.logger.trace("Printing TABLE-3....");
		HashMap<String, String> aTestLookupHashMap = new HashMap<String, String>();
		int count = 1;
		for (String aTest : testFootprint) {
			aTestLookupHashMap.put(aTest, count + "-" + aTest);
			count++;
		}
		printWriter = new PrintWriter(dataOutputFile);

		// Run processing
		ArrayList<String> aRunList = new ArrayList<String>();
		Iterator<String> iterator = runFootprint.iterator();
		HashMap<String, String> aRunLookupHashMap = new HashMap<String, String>();
		count = 1;
		while (iterator.hasNext()) {
			String aRunn = iterator.next();
			aRunLookupHashMap.put(aRunn, count + "-" + aRunn);
			aRunList.add(aRunn);
			count++;
		}
		aRunList.add(0, EITHER_PASSED_OR_NOT_RUN);
		aRunList.add(aRunList.size(), "END");
		aRunLookupHashMap.put(EITHER_PASSED_OR_NOT_RUN, "0-"
				+ EITHER_PASSED_OR_NOT_RUN);
		aRunLookupHashMap.put("END", aRunList.size() - 1 + "-END");

		// Print the labels row
		printWriter.print("0-NONE ");
		for (String aTestName : testFootprint) {
			printWriter.print(aTestLookupHashMap.get(aTestName) + " ");
		}
		printWriter.println();
		// Print the runs row
		for (Object aRun : aRunList) {
			printWriter.print(aRunLookupHashMap.get((String) aRun) + " ");
		}

		printWriter.println();
		for (String aRun : table3TestRunHash.keySet()) {
			printWriter.println("Run:" + aRunLookupHashMap.get(aRun));

			for (Object aTestName : testFootprint) {
				printWriter.print(aTestLookupHashMap.get(aTestName) + " ");
			}
			printWriter.println();
			for (Object aTestName : testFootprint) {
				for (HashMap<String, String> aFailedTestNodeInTable3 : table3TestRunHash
						.get(aRun)) {
					if (aFailedTestNodeInTable3.containsKey((String) aTestName)) {
						nodeFoundInRun = true;
						// Print the run in which it failed
						printWriter.print(aRunLookupHashMap
								.get(aFailedTestNodeInTable3
										.get((String) aTestName))
								+ " ");
					}

				}
				if (!nodeFoundInRun) {
					printWriter.print("0-" + EITHER_PASSED_OR_NOT_RUN + " ");
				}
				nodeFoundInRun = false;
			}
			printWriter.println();
		}
		printWriter.close();
	}

	@After
	public void logTaskResportSummary() {

	}
}
