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
	HashMap<String, LinkedHashSet<String>> uniqueMapTaskSetAgainstEachRun;
	HashMap<String, LinkedHashSet<String>> uniqueReduceTaskSetAgainstEachRun;
	HashMap<String, JobStatusCountStructure> realTable4TestRunHashForJobStatus;
	HashMap<String, MapTaskStatusCountStructure> realTable5aTestRunHashForMapTaskStatus;
	HashMap<String, ReduceTaskStatusCountStructure> realTable5bTestRunHashForReduceTaskStatus;
	HashMap<String, ArrayList<HashMap<String, Float>>> table6TestRunHash;

	LinkedHashSet<String> exceptionFootprint = new LinkedHashSet<String>();
	LinkedHashSet<String> testFootprint = new LinkedHashSet<String>();
	LinkedHashSet<String> runFootprint = new LinkedHashSet<String>();
	HashSet<String> testFootprintPrecededWithNumber = new HashSet<String>();

	private PrintWriter printWriter;
	private String table1DataConsumedByPerlForGraphGeneration = "/homes/hadoopqa/table1GraphData.txt";
	private String table2DataConsumedByPerlForGraphGeneration = "/homes/hadoopqa/table2GraphData.txt";
	private String table3DataConsumedByPerlForGraphGeneration = "/homes/hadoopqa/table3GraphData.txt";
	private String table4DataConsumedByPerlForGraphGeneration = "/homes/hadoopqa/table4GraphData.txt";
	private String table5aDataConsumedByPerlForGraphGeneration = "/homes/hadoopqa/table5aGraphData.txt";
	private String table5bDataConsumedByPerlForGraphGeneration = "/homes/hadoopqa/table5bGraphData.txt";
	private String table6DataConsumedByPerlForGraphGeneration = "/homes/hadoopqa/table6GraphData.txt";

	private String table1GraphTitle = "Count_of_exceptions_per_Test_across_different_test_runs";
	private String table2GraphTitle = "Total_count_of_exceptions_across_different_test_runs";
	private String table3GraphTitle = "Test_Failures";
	private String table4GraphTitle = "Job_Statuses";
	private String table5aGraphTitle = "Map_Task_Statuses";
	private String table5bGraphTitle = "Reduce_Task_Statuses";
	private String table6GraphTitle = "Test_Durations_in_minutes";
	// Axes
	private String table1XAxisTitle = "Test_Names";
	private String table1YAxisTitle = "Count_of_Exceptions";
	private String table2XAxisTitle = "Exceptions";
	private String table2YAxisTitle = "Count_of_Exceptions";
	private String table3XAxisTitle = "Test_Names";
	private String table3YAxisTitle = "Test_Runs_Where_It_Failed";
	private String table4XAxisTitle = "Runs";
	private String table4YAxisTitle = "Counts";
	private String table5aXAxisTitle = "Runs";
	private String table5aYAxisTitle = "Counts";
	private String table5bXAxisTitle = "Runs";
	private String table5bYAxisTitle = "Counts";
	private String table6XAxisTitle = "Test_Names";
	private String table6YAxisTitle = "Duration_in_minutes";

	private String table1OutputGraphFileName = TestSession.conf
			.getProperty("WORKSPACE")
			+ "/htf-common/target/surefire-reports/Graph1_TestsVrsCountOfExceptions.jpeg";
	private String table2OutputGraphFileName = TestSession.conf
			.getProperty("WORKSPACE")
			+ "/htf-common/target/surefire-reports/Graph2_CountOfExceptions.jpeg";
	private String table3OutputGraphFileName = TestSession.conf
			.getProperty("WORKSPACE")
			+ "/htf-common/target/surefire-reports/Graph3_TestFailures.jpeg";
	private String table4OutputGraphFileName = TestSession.conf
			.getProperty("WORKSPACE")
			+ "/htf-common/target/surefire-reports/Graph4_JobStats.jpeg";
	private String table5aOutputGraphFileName = TestSession.conf
			.getProperty("WORKSPACE")
			+ "/htf-common/target/surefire-reports/Graph5a_MapTaskStats.jpeg";
	private String table5bOutputGraphFileName = TestSession.conf
			.getProperty("WORKSPACE")
			+ "/htf-common/target/surefire-reports/Graph5b_ReduceTaskStats.jpeg";
	private String table6OutputGraphFileName = TestSession.conf
			.getProperty("WORKSPACE")
			+ "/htf-common/target/surefire-reports/Graph6_TestDurations.jpeg";

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
	private static final String MONITORING_DATA_DUMP = "/grid/0/tmp/stressMonitoring/";
	private static final String SUCCEEDED = "SUCCEEDED";
	private static final String COMPLETE = "COMPLETE";
	private static final String FAILED = "FAILED";
	private static final String KILLED = "KILLED";
	private static final String UNKNOWN = "UNKNOWN";
	private static final String RUNNING = "RUNNING";

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

	class MapTaskStatusCountStructure {
		public MapTaskStatusCountStructure(int succeeded, int failed,
				int killed, int unknown) {
			this.succeeded = succeeded;
			this.failed = failed;
			this.killed = killed;
			this.unknown = unknown;
			TestSession.logger.info("Created map task with details: succeeded["
					+ succeeded + "] failed[" + failed + "] killed[" + killed
					+ "] unknown[" + unknown + "]");

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

	class ReduceTaskStatusCountStructure {
		public ReduceTaskStatusCountStructure(int completed, int failed,
				int killed, int running) {
			this.completed = completed;
			this.failed = failed;
			this.killed = killed;
			this.running = running;
			TestSession.logger.info("Created task with details: complete["
					+ completed + "] failed[" + failed + "] killed[" + killed
					+ "] running[" + running + "]");

		}

		int completed;
		int failed;
		int killed;
		int running;

		int get(String status) {
			if (status.equals(COMPLETE)) {
				return completed;
			} else if (status.equals(FAILED)) {
				return failed;
			} else if (status.equals(KILLED)) {
				return killed;
			} else if (status.equals(RUNNING)) {
				return running;
			} else {
				return -1;
			}
		}
	}

	@Test
	public void testProcessExcetions() throws Exception {
		collateDataForTable1();
		collateDataForTable2();
		collateDataForTable3();
		collateDataForTable4();
		collateDataForTable5(HadooptestConstants.Miscellaneous.MAP_TASK_STATUS);
		TestSession.logger.info("Map tasks hash" + uniqueMapTaskSetAgainstEachRun);
		collateDataForTable5(HadooptestConstants.Miscellaneous.REDUCE_TASK_STATUS);
		TestSession.logger.info("Reduce tasks hash" + uniqueReduceTaskSetAgainstEachRun);
		collateDataForTable6();

	}

	private void collateDataForTable1() throws NumberFormatException,
			IOException {
		HashMap<String, Integer> perTestExceptionCountHashNode;
		table1TestRunHash = new HashMap<String, ArrayList<HashMap<String, Integer>>>();

		String cluster = System.getProperty("CLUSTER_NAME");
		File clusterDir = new File(MONITORING_DATA_DUMP + cluster);
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

			File packageDir = new File(clusterDir.getAbsolutePath() + "/"
					+ packaze);
			TestSession.logger.info("Processing Package:" + packaze);
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
				TestSession.logger.info("Processing Class:" + clazz);
				File classDir = new File(packageDir.getAbsolutePath() + "/"
						+ clazz);
				String[] tests = classDir.list();
				for (String test : tests) {
					TestSession.logger.info("Processing test:" + test);
					testFootprint.add(clazz + "::" + test);
					TestSession.logger.info("=====>[" + test + "]<=======");
					File testDir = new File(classDir.getAbsolutePath() + "/"
							+ test);
					String[] runDates = testDir.list();
					Arrays.sort(runDates);
					for (String aRunDate : runDates) {
						runFootprint.add(aRunDate);
						int exceptionCountPerTest = 0;
						File exceptionsFile = new File(
								testDir.getAbsolutePath()
										+ "/"
										+ aRunDate
										+ "/"
										+ HadooptestConstants.Miscellaneous.EXCEPTIONS);
						if (!exceptionsFile.exists()) {
							TestSession.logger.info(exceptionsFile
									.getAbsoluteFile()
									+ " does not exist, hence contnuing");
							continue;
						}
						BufferedReader exceptionsBufferedReader = new BufferedReader(
								new FileReader(
										testDir.getAbsolutePath()
												+ "/"
												+ aRunDate
												+ "/"
												+ HadooptestConstants.Miscellaneous.EXCEPTIONS));
						String line = null;
						while ((line = exceptionsBufferedReader.readLine()) != null) {

							String[] exceptionDetails = line.split(":");
							String exceptionName = exceptionDetails[0];
							exceptionFootprint.add(exceptionName);
							int exceptionCount = Integer
									.parseInt(exceptionDetails[1]);
							exceptionCountPerTest += exceptionCount;

						}// End of Exception file processing (while - loop)
						exceptionsBufferedReader.close();
						perTestExceptionCountHashNode = new HashMap<String, Integer>();
						perTestExceptionCountHashNode.put(clazz + "::" + test,
								exceptionCountPerTest);

						if (!table1TestRunHash.containsKey(aRunDate)) {
							ArrayList<HashMap<String, Integer>> emptyListOfExceptionsCountPerTest = new ArrayList<HashMap<String, Integer>>();
							table1TestRunHash.put(aRunDate,
									emptyListOfExceptionsCountPerTest);
						}

						table1TestRunHash.get(aRunDate).add(
								perTestExceptionCountHashNode);

					}

				}// End of Test Run (for - loop)

			}

		}
		TestSession.logger.info(table1TestRunHash);
		System.out
				.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");

	}

	private void collateDataForTable2() throws IOException {

		table2TestRunHash = new HashMap<String, ArrayList<HashMap<String, Integer>>>();

		String cluster = System.getProperty("CLUSTER_NAME");
		File clusterDir = new File(MONITORING_DATA_DUMP + cluster);
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
			File packageDir = new File(clusterDir.getAbsolutePath() + "/"
					+ packaze);
			TestSession.logger.info("Processing Package (Table2):" + packaze);
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
				TestSession.logger.info("Processing Class(Table2):" + clazz);
				File classDir = new File(packageDir.getAbsolutePath() + "/"
						+ clazz);
				String[] tests = classDir.list();

				for (String test : tests) {
					TestSession.logger.info("Processing test(Table2):" + test);
					testFootprint.add(clazz + "::" + test);
					TestSession.logger.info("=====>[" + clazz + "::" + test
							+ "]<=======");
					File testDir = new File(classDir.getAbsolutePath() + "/"
							+ test);
					String[] runDates = testDir.list();
					Arrays.sort(runDates);

					for (String aRunDate : runDates) {
						TestSession.logger.info("Processing rundates(Table2):"
								+ aRunDate);
						runFootprint.add(aRunDate);
						File exceptionsFile = new File(
								testDir.getAbsolutePath() + "/" + aRunDate
										+ "/" + "EXCEPTIONS");
						if (!exceptionsFile.exists()) {
							continue;
						}
						BufferedReader exceptionsBufferedReader = new BufferedReader(
								new FileReader(
										testDir.getAbsolutePath()
												+ "/"
												+ aRunDate
												+ "/"
												+ HadooptestConstants.Miscellaneous.EXCEPTIONS));
						String line = null;
						while ((line = exceptionsBufferedReader.readLine()) != null) {

							String[] exceptionDetails = line.split(":");
							String exceptionName = exceptionDetails[0];
							int newExceptionCount = Integer
									.parseInt(exceptionDetails[1]);
							exceptionFootprint.add(exceptionName);

							if (!table2TestRunHash.containsKey(aRunDate)) {
								TestSession.logger
										.info("TABLE2: does not cotain run:"
												+ aRunDate
												+ " hence adding it!");
								ArrayList<HashMap<String, Integer>> emptyListOfExceptionsCountPerRun = new ArrayList<HashMap<String, Integer>>();
								table2TestRunHash.put(aRunDate,
										emptyListOfExceptionsCountPerRun);
							} else {
								TestSession.logger.info("TABLE2: cotains run:"
										+ aRunDate);
							}
							boolean exceptionPresent = false;
							for (HashMap<String, Integer> exceptionTuple : table2TestRunHash
									.get(aRunDate)) {
								if (exceptionTuple.containsKey(exceptionName)) {
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
								HashMap<String, Integer> uniqueExceptionAndItsCount = new HashMap<String, Integer>();
								uniqueExceptionAndItsCount.put(exceptionName,
										newExceptionCount);
								table2TestRunHash.get(aRunDate).add(
										uniqueExceptionAndItsCount);

							}

						}// End of Exception file processing (while - loop)
						exceptionsBufferedReader.close();
					}

				}
			}
			TestSession.logger.info(table2TestRunHash);
			System.out
					.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
		}
	}

	private void collateDataForTable3() throws IOException {
		// Table-3 vars
		HashMap<String, String> perRunTestStatusHashNode;
		table3TestRunHash = new HashMap<String, ArrayList<HashMap<String, String>>>();

		String cluster = System.getProperty("CLUSTER_NAME");
		File clusterDir = new File(MONITORING_DATA_DUMP + cluster);
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
			File packageDir = new File(clusterDir.getAbsolutePath() + "/"
					+ packaze);
			TestSession.logger.info("Processing Package:" + packaze);
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
				TestSession.logger.info("Processing Class:" + clazz);
				File classDir = new File(packageDir.getAbsolutePath() + "/"
						+ clazz);
				String[] tests = classDir.list();
				for (String test : tests) {
					TestSession.logger.info("Processing test:" + clazz + "::"
							+ test);
					testFootprint.add(clazz + "::" + test);
					TestSession.logger.info("(Table 3): =====>[" + clazz + "::"
							+ test + "]<=======");
					File testDir = new File(classDir.getAbsolutePath() + "/"
							+ test);
					String[] runDates = testDir.list();
					Arrays.sort(runDates);
					for (String aRunDate : runDates) {
						TestSession.logger.info("Processing run(Table 3):"
								+ aRunDate);
						runFootprint.add(aRunDate);
						BufferedReader testStatusBufferedReader = new BufferedReader(
								new FileReader(
										classDir.getAbsolutePath()
												+ "/"
												+ test
												+ "/"
												+ aRunDate
												+ "/"
												+ HadooptestConstants.Miscellaneous.TEST_STATUS));
						String failureLine = null;
						while ((failureLine = testStatusBufferedReader
								.readLine()) != null) {
							perRunTestStatusHashNode = new HashMap<String, String>();
							if (failureLine.equals("FAILED")) {
								perRunTestStatusHashNode.put(clazz + "::"
										+ test, aRunDate);
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
								perRunTestStatusHashNode.put(clazz + "::"
										+ test, EITHER_PASSED_OR_NOT_RUN);
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
					}// End of Test Run (for - loop)
				}
			}
		}
	}

	private void collateDataForTable4() throws IOException {
		// Table-4 vars
		uniqueJobSetAgainstEachRun = new HashMap<String, LinkedHashSet<String>>();

		String cluster = System.getProperty("CLUSTER_NAME");
		File clusterDir = new File(MONITORING_DATA_DUMP + cluster);
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
			File packageDir = new File(clusterDir.getAbsolutePath() + "/"
					+ packaze);
			TestSession.logger.info("Processing Package:" + packaze);
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
				TestSession.logger.info("Processing Class:" + clazz);
				File classDir = new File(packageDir.getAbsolutePath() + "/"
						+ clazz);
				String[] tests = classDir.list();
				for (String test : tests) {
					TestSession.logger.info("Processing test:" + clazz + "::"
							+ test);
					testFootprint.add(clazz + "::" + test);
					TestSession.logger.info("=====>[" + clazz + "::" + test
							+ "]<=======");
					File testDir = new File(packageDir.getAbsolutePath() + "/"
							+ clazz + "/" + test);
					String[] runDates = testDir.list();
					Arrays.sort(runDates);

					for (String aRunDate : runDates) {
						runFootprint.add(aRunDate);
						BufferedReader jobStatusBufferedReader = new BufferedReader(
								new FileReader(
										testDir.getAbsolutePath()
												+ "/"
												+ aRunDate
												+ "/"
												+ HadooptestConstants.Miscellaneous.JOB_STATUS));

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

	}

	private void collateDataForTable5(String taskTypeStatus) throws IOException {
		// Table-5 vars
		if (taskTypeStatus
				.equals(HadooptestConstants.Miscellaneous.MAP_TASK_STATUS)) {
			uniqueMapTaskSetAgainstEachRun = new HashMap<String, LinkedHashSet<String>>();
		} else {
			uniqueReduceTaskSetAgainstEachRun = new HashMap<String, LinkedHashSet<String>>();
		}

		String cluster = System.getProperty("CLUSTER_NAME");
		File clusterDir = new File(MONITORING_DATA_DUMP + cluster);
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
			File packageDir = new File(clusterDir.getAbsolutePath() + "/"
					+ packaze);
			TestSession.logger.info("Processing Package:" + packaze);
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
				TestSession.logger.info("Processing Class:" + clazz);
				File classDir = new File(packageDir.getAbsolutePath() + "/"
						+ clazz);
				String[] tests = classDir.list();
				for (String test : tests) {
					TestSession.logger.info("Processing test:" + clazz + "::"
							+ test);
					testFootprint.add(clazz + "::" + test);
					TestSession.logger.info("=====>[" + clazz + "::" + test
							+ "]<=======");
					File testDir = new File(packageDir.getAbsolutePath() + "/"
							+ clazz + "/" + test);
					String[] runDates = testDir.list();
					Arrays.sort(runDates);

					for (String aRunDate : runDates) {
						runFootprint.add(aRunDate);
						String fileLocation;
						if (taskTypeStatus
								.equals(HadooptestConstants.Miscellaneous.MAP_TASK_STATUS)) {
							fileLocation = testDir.getAbsolutePath()
									+ "/"
									+ aRunDate
									+ "/"
									+ HadooptestConstants.Miscellaneous.MAP_TASK_STATUS;
						} else {
							fileLocation = testDir.getAbsolutePath()
									+ "/"
									+ aRunDate
									+ "/"
									+ HadooptestConstants.Miscellaneous.REDUCE_TASK_STATUS;

						}
						File fileCheck = new File(fileLocation);
						if (!fileCheck.exists())
							continue;
						BufferedReader taskStatusBufferedReader = new BufferedReader(
								new FileReader(fileLocation));
						if (taskTypeStatus
								.equals(HadooptestConstants.Miscellaneous.MAP_TASK_STATUS)) {
							if (!uniqueMapTaskSetAgainstEachRun
									.containsKey(aRunDate)) {
								LinkedHashSet<String> aSetOfTaskStatusLines = new LinkedHashSet<String>();
								uniqueMapTaskSetAgainstEachRun.put(aRunDate,
										aSetOfTaskStatusLines);
							}

							String taskStatusLine = null;
							while ((taskStatusLine = taskStatusBufferedReader
									.readLine()) != null) {
								LinkedHashSet<String> uniqueTaskStatuses = uniqueMapTaskSetAgainstEachRun
										.get(aRunDate);
								uniqueTaskStatuses.add(taskStatusLine);
							}// End of jobStatus file processing (while-loop)
						} else if (taskTypeStatus
								.equals(HadooptestConstants.Miscellaneous.REDUCE_TASK_STATUS)) {
							if (!uniqueReduceTaskSetAgainstEachRun
									.containsKey(aRunDate)) {
								LinkedHashSet<String> aSetOfTaskStatusLines = new LinkedHashSet<String>();
								uniqueReduceTaskSetAgainstEachRun.put(aRunDate,
										aSetOfTaskStatusLines);
							}

							String taskStatusLine = null;
							while ((taskStatusLine = taskStatusBufferedReader
									.readLine()) != null) {
								LinkedHashSet<String> uniqueTaskStatuses = uniqueReduceTaskSetAgainstEachRun
										.get(aRunDate);
								uniqueTaskStatuses.add(taskStatusLine);
							}
						}
						taskStatusBufferedReader.close();

					}// End of Test Run (for - loop)

				}
			}
		}

	}

	private void collateDataForTable6() throws NumberFormatException,
			IOException {
		HashMap<String, Float> perTestDurationHashNode;
		table6TestRunHash = new HashMap<String, ArrayList<HashMap<String, Float>>>();

		String cluster = System.getProperty("CLUSTER_NAME");
		File clusterDir = new File(MONITORING_DATA_DUMP + cluster);
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

			File packageDir = new File(clusterDir.getAbsolutePath() + "/"
					+ packaze);
			TestSession.logger.info("Processing Package(table 6):" + packaze);
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
				TestSession.logger.info("Processing Class(table 6):" + clazz);
				File classDir = new File(packageDir.getAbsolutePath() + "/"
						+ clazz);
				String[] tests = classDir.list();
				for (String test : tests) {
					TestSession.logger.info("Processing test(table 6):" + test);
					testFootprint.add(clazz + "::" + test);
					TestSession.logger.info("=====>(table 6)[" + test
							+ "]<=======");
					File testDir = new File(classDir.getAbsolutePath() + "/"
							+ test);
					String[] runDates = testDir.list();
					Arrays.sort(runDates);
					for (String aRunDate : runDates) {
						runFootprint.add(aRunDate);
						File durationsFile = new File(
								testDir.getAbsolutePath()
										+ "/"
										+ aRunDate
										+ "/"
										+ HadooptestConstants.Miscellaneous.TEST_DURATION);
						if (!durationsFile.exists()) {
							TestSession.logger.info(durationsFile
									.getAbsoluteFile()
									+ " does not exist, hence contnuing");
							continue;
						}

						BufferedReader testDurationsBufferedReader = new BufferedReader(
								new FileReader(durationsFile.getAbsoluteFile()));
						String line = testDurationsBufferedReader.readLine()
								.trim();
						TestSession.logger.info(durationsFile.getAbsoluteFile()
								+ " exists.... and it contains " + line);
						testDurationsBufferedReader.close();
						perTestDurationHashNode = new HashMap<String, Float>();
						perTestDurationHashNode.put(clazz + "::" + test,
								Float.parseFloat(line));

						if (!table6TestRunHash.containsKey(aRunDate)) {
							ArrayList<HashMap<String, Float>> emptyListOfDurationsPerTest = new ArrayList<HashMap<String, Float>>();
							table6TestRunHash.put(aRunDate,
									emptyListOfDurationsPerTest);
						}

						table6TestRunHash.get(aRunDate).add(
								perTestDurationHashNode);

					}

				}// End of Test Run (for - loop)

			}

		}
		TestSession.logger.info(table6TestRunHash);
		System.out
				.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");

	}

	@After
	public void generateGraphs() throws Exception {
		String outputBaseDirPath = TestSession.conf.getProperty("WORKSPACE")
				+ "/htf-common/target/surefire-reports";
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

		// Table-4 Job Status
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

		// Table-5(a) Map Task
		realTable5aTestRunHashForMapTaskStatus = new HashMap<String, MapTaskStatusCountStructure>();
		for (String aRun : uniqueMapTaskSetAgainstEachRun.keySet()) {
			realTable5aTestRunHashForMapTaskStatus.put(aRun,
					getMapTaskStatusCountsForRun(aRun));
		}
		generateTable5aData(table5aDataConsumedByPerlForGraphGeneration);
		runPerlScriptToGenerateGraph(generateBarGraphForMonitoring,
				table5aGraphTitle, table5aXAxisTitle, table5aYAxisTitle,
				table5aDataConsumedByPerlForGraphGeneration,
				table5aOutputGraphFileName);

		// Table-5(b) Reduce Task
		realTable5bTestRunHashForReduceTaskStatus = new HashMap<String, ReduceTaskStatusCountStructure>();
		for (String aRun : uniqueReduceTaskSetAgainstEachRun.keySet()) {
			realTable5bTestRunHashForReduceTaskStatus.put(aRun,
					getReduceTaskStatusCountsForRun(aRun));
		}
		generateTable5bData(table5bDataConsumedByPerlForGraphGeneration);
		runPerlScriptToGenerateGraph(generateBarGraphForMonitoring,
				table5bGraphTitle, table5bXAxisTitle, table5bYAxisTitle,
				table5bDataConsumedByPerlForGraphGeneration,
				table5bOutputGraphFileName);

		// Table-6 (Test Durations)
		generateTable6Data(table6DataConsumedByPerlForGraphGeneration);
		runPerlScriptToGenerateGraph(generateLineGraphForMonitoring,
				table6GraphTitle, table6XAxisTitle, table6YAxisTitle,
				table6DataConsumedByPerlForGraphGeneration,
				table6OutputGraphFileName);

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

	MapTaskStatusCountStructure getMapTaskStatusCountsForRun(String aDate) {
		int succeeded = 0;
		int failed = 0;
		int killed = 0;
		int unknown = 0;
		LinkedHashSet<String> aSetOftaskStatusLines;
		aSetOftaskStatusLines = uniqueMapTaskSetAgainstEachRun.get(aDate);
		for (String aStatusLineWithTaskId : aSetOftaskStatusLines) {
			String aStatusLineWithoutTaskId = aStatusLineWithTaskId.split(":")[1];
			for (String aStatus : aStatusLineWithoutTaskId.split("\\s+")) {
				String aCounter = aStatus.split("=")[1];
				if (aStatus.contains(COMPLETE)) {
					if (aCounter.equals("1")) {
						succeeded++;
					}
				} else if (aStatus.contains(FAILED)) {
					if (aCounter.equals("1")) {
						failed++;
					}

				} else if (aStatus.contains(KILLED)) {
					if (aCounter.equals("1")) {
						killed++;
					}

				} else if (aStatus.contains(UNKNOWN)) {
					if (aCounter.equals("1")) {
						unknown++;
					}

				} else {
					// Don't care
				}
			}
		}
		MapTaskStatusCountStructure taskStatusCounts = new MapTaskStatusCountStructure(
				succeeded, failed, killed, unknown);
		return taskStatusCounts;
	}

	ReduceTaskStatusCountStructure getReduceTaskStatusCountsForRun(String aDate) {
		int completed = 0;
		int failed = 0;
		int killed = 0;
		int running = 0;
		LinkedHashSet<String> aSetOftaskStatusLines;
		aSetOftaskStatusLines = uniqueReduceTaskSetAgainstEachRun.get(aDate);

		for (String aStatusLineWithTaskId : aSetOftaskStatusLines) {
			String aStatusLineWithoutTaskId = aStatusLineWithTaskId.split(":")[1];
			for (String aStatus : aStatusLineWithoutTaskId.split("\\s+")) {
				String aCounter = aStatus.split("=")[1];
				if (aStatus.contains(COMPLETE)) {
					if (aCounter.equals("1")) {
						completed++;
					}
				} else if (aStatus.contains(FAILED)) {
					if (aCounter.equals("1")) {
						failed++;
					}

				} else if (aStatus.contains(KILLED)) {
					if (aCounter.equals("1")) {
						killed++;
					}

				} else if (aStatus.contains(RUNNING)) {
					if (aCounter.equals("1")) {
						running++;
					}

				} else {
					// Don't care
				}
			}
		}
		ReduceTaskStatusCountStructure taskStatusCounts = new ReduceTaskStatusCountStructure(
				completed, failed, killed, running);
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
		TestSession.logger.info("Xit value:" + process.exitValue());
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
	 * Line graph, with Test cases in the X-axis Count of exceptions per test,
	 * on the Y-axis
	 */
	public void generateTable1Data(String dataOutputFile)
			throws FileNotFoundException {
		boolean nodeFoundInRun = false;
		TestSession.logger.info("Printing TABLE-1....");
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
		TestSession.logger.info("Printing TABLE-2....");
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
		TestSession.logger.info("Printing TABLE-3....");
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
			aRunLookupHashMap.put(aRunn, count + "-----" + aRunn);
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

	/*
	 * Bar-graph depicting, job status (success/failed/killed/unknown)
	 */
	public void generateTable4Data(String dataOutputFile)
			throws FileNotFoundException {
		TestSession.logger.info("Printing TABLE-4....");
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
	 * Bar-graph depicting, map task status (success/failed/killed/unknown)
	 */
	public void generateTable5aData(String dataOutputFile)
			throws FileNotFoundException {
		TestSession.logger.info("Printing TABLE-5a....");

		printWriter = new PrintWriter(dataOutputFile);
		Object[] runFootPrintArray = runFootprint.toArray();
		Arrays.sort(runFootPrintArray);
		for (Object aRunName : runFootPrintArray) {
			printWriter.print(aRunName + " ");
		}

		printWriter.println();
		String[] taskStatuses = new String[] { SUCCEEDED, FAILED, KILLED,
				UNKNOWN };

		for (String aTaskStatus : taskStatuses) {
			for (Object aRunFootprintName : runFootPrintArray) {
				if (!realTable5aTestRunHashForMapTaskStatus
						.containsKey(aRunFootprintName)) {
					printWriter.print("0 ");
					continue;
				}
				MapTaskStatusCountStructure taskStatusStruct = realTable5aTestRunHashForMapTaskStatus
						.get(aRunFootprintName);
				printWriter.print(taskStatusStruct.get(aTaskStatus) + " ");

			}
			printWriter.println();
		}
		printWriter.close();
	}

	/*
	 * Bar-graph depicting, reduce task status (complete/failed/killed/running)
	 */
	public void generateTable5bData(String dataOutputFile)
			throws FileNotFoundException {
		TestSession.logger.info("Printing TABLE-5b....");

		printWriter = new PrintWriter(dataOutputFile);
		Object[] runFootPrintArray = runFootprint.toArray();
		Arrays.sort(runFootPrintArray);
		for (Object aRunName : runFootPrintArray) {
			printWriter.print(aRunName + " ");
		}

		printWriter.println();
		String[] reduceTaskStatuses = new String[] { COMPLETE, FAILED, KILLED,
				RUNNING };

		for (String aReduceTaskStatus : reduceTaskStatuses) {
			for (Object aRunFootprintName : runFootPrintArray) {
				if (!realTable5bTestRunHashForReduceTaskStatus
						.containsKey(aRunFootprintName)) {
					printWriter.print("0 ");
					continue;
				}
				ReduceTaskStatusCountStructure taskStatusStruct = realTable5bTestRunHashForReduceTaskStatus
						.get(aRunFootprintName);
				printWriter
						.print(taskStatusStruct.get(aReduceTaskStatus) + " ");

			}
			printWriter.println();
		}
		printWriter.close();
	}

	/*
	 * Line graph, with Test cases in the X-axis Count of exceptions per test,
	 * on the Y-axis
	 */
	public void generateTable6Data(String dataOutputFile)
			throws FileNotFoundException {
		boolean nodeFoundInRun = false;
		TestSession.logger.info("Printing TABLE-6....");
		printWriter = new PrintWriter(dataOutputFile);
		Object[] testFootPrintArray = testFootprint.toArray();
		Arrays.sort(testFootPrintArray);
		for (Object aTestName : testFootPrintArray) {
			printWriter.print(aTestName + " ");
		}

		printWriter.println();

		for (String aRun : table6TestRunHash.keySet()) {
			printWriter.println("Run:" + aRun);
			for (Object aTestNameOnXAxis : testFootPrintArray) {
				for (HashMap<String, Float> dataNodeInTable6InSomeRun : table6TestRunHash
						.get(aRun)) {
					if (dataNodeInTable6InSomeRun
							.containsKey((String) aTestNameOnXAxis)) {
						nodeFoundInRun = true;
						printWriter.print(dataNodeInTable6InSomeRun
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

	@After
	public void logTaskReportSummary() {

	}
}
