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

	LinkedHashSet<String> exceptionFootprint = new LinkedHashSet<String>();
	LinkedHashSet<String> testFootprint = new LinkedHashSet<String>();
	LinkedHashSet<String> runHandprint = new LinkedHashSet<String>();
	HashSet<String> testFootprintPrecededWithNumber = new HashSet<String>();

	private PrintWriter printWriter;
	private String table1InputDataForGraphGeneration = "/homes/hadoopqa/table1GraphData.txt";
	private String table2InputDataForGraphGeneration = "/homes/hadoopqa/table2GraphData.txt";
	private String table3InputDataForGraphGeneration = "/homes/hadoopqa/table3GraphData.txt";
	private String table1GraphTitle = "Count_of_exceptions_per_Test_across_different_test_runs";
	private String table2GraphTitle = "Total_count_of_exceptions_across_different_test_runs";
	private String table3GraphTitle = "Test_Failures";
	private String table1XAxisTitle = "Test_Names";
	private String table1YAxisTitle = "Count_of_Exceptions";
	private String table2XAxisTitle = "Exceptions";
	private String table2YAxisTitle = "Count_of_Exceptions";
	private String table3XAxisTitle = "Test_Names";
	private String table3YAxisTitle = "Test_Runs_Where_It_Failed";

	private String table1OutputGraphFileName = TestSession.conf
			.getProperty("WORKSPACE") + "/target/surefire-reports/table1.png";
	private String table2OutputGraphFileName = TestSession.conf
			.getProperty("WORKSPACE") + "/target/surefire-reports/table2.png";
	private String table3OutputGraphFileName = TestSession.conf
			.getProperty("WORKSPACE") + "/target/surefire-reports/table3.png";

	private String generateLineGraphForMonitoring = TestSession.conf
			.getProperty("WORKSPACE")
			+ "/scripts/generateLineGraphForMonitoring.pl";

	private String generateScatterPlotForMonitoring = TestSession.conf
			.getProperty("WORKSPACE")
			+ "/scripts/generateScatterPlotForMonitoring.pl";

	private String EITHER_PASSED_OR_NOT_RUN = "EITHER_PASSED_OR_NOT_RUN";

	public GraphGenerationUtilAndNotATest() {
		this.packageFilters = System.getProperty("PACKAGE_FILTERS");
		this.classFilters = System.getProperty("CLASS_FILTERS");
	}

	@BeforeClass
	public static void startTestSession() throws Exception {

		TestSession.start();
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
					String[] testRuns = testDir.list();
					Arrays.sort(testRuns);
					for (String testRun : testRuns) {
						runHandprint.add(testRun);
						/*
						 * Process Exceptions here. NOTE: TestStatus (failures)
						 * are processed later (in the same loop)
						 */
						TestSession.logger.trace("Processing testRun:"
								+ testRun);
						exceptionCountPerTest = 0;
						File exceptionsFile = new File(
								"/grid/0/tmp/stressMonitoring/" + cluster + "/"
										+ packaze + "/" + clazz + "/" + test
										+ "/" + testRun + "/" + "EXCEPTIONS");
						if (!exceptionsFile.exists()) {
							continue;
						}
						BufferedReader exceptionsBufferedReader = new BufferedReader(
								new FileReader("/grid/0/tmp/stressMonitoring/"
										+ cluster + "/" + packaze + "/" + clazz
										+ "/" + test + "/" + testRun + "/"
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

							if (table2TestRunHash.containsKey(testRun)) {
								boolean exceptionPresent = false;
								for (HashMap<String, Integer> exceptionTuple : table2TestRunHash
										.get(testRun)) {
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
									table2TestRunHash.get(testRun).add(
											perRunExceptionCountHashNode);

								}

							} else {
								ArrayList<HashMap<String, Integer>> listOfExceptionsCountPerRun = new ArrayList<HashMap<String, Integer>>();
								listOfExceptionsCountPerRun
										.add(perRunExceptionCountHashNode);
								table2TestRunHash.put(testRun,
										listOfExceptionsCountPerRun);

							}

						}// End of Exception file processing (while - loop)
						exceptionsBufferedReader.close();
						perTestExceptionCountHashNode = new HashMap<String, Integer>();
						perTestExceptionCountHashNode.put(test,
								exceptionCountPerTest);

						/*
						 * Care for Table-1 here X:axis T1, T2, T3..
						 * Y:Axis:Count of Exceptions encountered/test
						 */
						if (table1TestRunHash.containsKey(testRun)) {
							table1TestRunHash.get(testRun).add(
									perTestExceptionCountHashNode);
						} else {
							ArrayList<HashMap<String, Integer>> listOfExceptionsCountPerTest = new ArrayList<HashMap<String, Integer>>();
							listOfExceptionsCountPerTest
									.add(perTestExceptionCountHashNode);
							table1TestRunHash.put(testRun,
									listOfExceptionsCountPerTest);

						}
						/*
						 * ---------------------------- Table-3
						 * --------------------
						 */
						BufferedReader testStatusBufferedReader = new BufferedReader(
								new FileReader("/grid/0/tmp/stressMonitoring/"
										+ cluster + "/" + packaze + "/" + clazz
										+ "/" + test + "/" + testRun + "/"
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
								perRunTestStatusHashNode.put(test, testRun);
								if (table3TestRunHash.containsKey(testRun)) {
									table3TestRunHash.get(testRun).add(
											perRunTestStatusHashNode);
								} else {
									ArrayList<HashMap<String, String>> aRunOfTestFails = new ArrayList<HashMap<String, String>>();
									aRunOfTestFails
											.add(perRunTestStatusHashNode);
									table3TestRunHash.put(testRun,
											aRunOfTestFails);
								}
							} else {
								// Test has PASSED
								perRunTestStatusHashNode.put(test,
										EITHER_PASSED_OR_NOT_RUN);
								if (table3TestRunHash.containsKey(testRun)) {
									table3TestRunHash.get(testRun).add(
											perRunTestStatusHashNode);
								} else {
									ArrayList<HashMap<String, String>> aRunOfTestFails = new ArrayList<HashMap<String, String>>();
									aRunOfTestFails
											.add(perRunTestStatusHashNode);
									table3TestRunHash.put(testRun,
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
		TestSession.logger.trace(table1TestRunHash);
		System.out
				.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
		TestSession.logger.trace(table2TestRunHash);
		System.out
				.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
		TestSession.logger.trace(table3TestRunHash);

	}

	@After
	public void generateGraphs() throws Exception {
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
		generateTabe1Data(table1InputDataForGraphGeneration);
		runPerlScriptToGenerateGraph(generateLineGraphForMonitoring,
				table1GraphTitle, table1XAxisTitle, table1YAxisTitle,
				table1InputDataForGraphGeneration, table1OutputGraphFileName);
		// Table-2
		generateTabe2Data(table2InputDataForGraphGeneration);
		runPerlScriptToGenerateGraph(generateLineGraphForMonitoring,
				table2GraphTitle, table2XAxisTitle, table2YAxisTitle,
				table2InputDataForGraphGeneration, table2OutputGraphFileName);
		// Table-3
		generateTabe3Data(table3InputDataForGraphGeneration);
		runPerlScriptToGenerateGraph(generateScatterPlotForMonitoring,
				table3GraphTitle, table3XAxisTitle, table3YAxisTitle,
				table3InputDataForGraphGeneration, table3OutputGraphFileName);

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
	 * Line graph, with Test cases in the X-axis Count of exceptions per test,
	 * on the Y-axis
	 */
	public void generateTabe1Data(String dataOutputFile)
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
	public void generateTabe2Data(String dataOutputFile)
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
	public void generateTabe3Data(String dataOutputFile)
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
		Iterator<String> iterator = runHandprint.iterator();
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
