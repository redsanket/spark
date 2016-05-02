package hadooptest.gdm.regression.stackIntegration.db;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import hadooptest.TestSession;
import hadooptest.cluster.gdm.GdmUtils;

public class AggIntResult {

	private String currentDate;
	private java.util.List<String> hadoopVersionList;
	private Map<String,String> componentsResultMap;
	private DataBaseOperations dataBaseOperations;
	private java.sql.Connection connection;
	private static final int TEZ_TEST_CASES = 1;
	private static final int PIG_TEST_CASES = 1;
	private static final int HIVE_TEST_CASES = 4;
	private static final int HCATALOG_TEST_CASES = 1;
	private static final int HBASE_TEST_CASES = 4;
	private static final int OOZIE_TEST_CASES = 5;
	private static final int GDM_TEST_CASES = 1;

	public AggIntResult() {
		componentsResultMap = new HashMap<String,String>();
		java.text.SimpleDateFormat simpleDateFormat = new java.text.SimpleDateFormat("yyyyMMdd");
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
		String currentDate = simpleDateFormat.format(calendar.getTime());
		this.setCurrentDate(currentDate);
		this.dataBaseOperations = new DataBaseOperations();
		try {
			this.connection = this.dataBaseOperations.getConnection();
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
	}

	public void addResultToMap(String componentName, String result) {
		this.componentsResultMap.put(componentName, result);
	}

	public String getCurrentDate() {
		return currentDate;
	}

	public void setCurrentDate(String currentDate) {
		this.currentDate = currentDate;
	}

	public java.util.List<String> getTodayNumberHadoopVersions() {
		java.util.List<String> hadoopVersionList = new java.util.ArrayList<>();
		String QUERY = "select date , hadoopVersion from " + DBCommands.TABLE_NAME + "  where date=" + "\"" + getCurrentDate() + "\"" + "  group by hadoopVersion";
		TestSession.logger.info("QUERY = " + QUERY);
		java.sql.Statement statement = null;
		java.sql.ResultSet resultSet = null;	 
		try {
			statement = this.connection.createStatement();
			if (statement != null) {
				resultSet = statement.executeQuery(QUERY);
				if (resultSet != null) {
					while(resultSet.next()) {
						String hadoopVersion = resultSet.getString("hadoopVersion");
						hadoopVersionList.add(hadoopVersion);
					}
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return hadoopVersionList;
	}


	public void getResults() {
		boolean flag = true;
		int iteration = 0;
		java.sql.Statement statement = null;
		java.sql.ResultSet resultSet = null;
		java.util.List<String> dataSetNameList = new ArrayList<>();
		java.util.List<String> hadoopVersionList = this.getTodayNumberHadoopVersions();
		for (String hadoopVersion : hadoopVersionList ) {
			String QUERY = "select dataSetName from integration_test where hadoopVersion like " + "\"" + hadoopVersion + "\"" ;
			TestSession.logger.info("QUERY = " + QUERY);
			try {
				statement = this.connection.createStatement();
				if (statement != null) {
					resultSet = statement.executeQuery(QUERY);
					if (resultSet != null) {
						while(resultSet.next()) {
							String dataSetName =  resultSet.getString("dataSetName");
							String dsName = dataSetName.substring(0, (dataSetName.length() - 2));
							if (dataSetNameList.contains(dsName) == false) {
								dataSetNameList.add(dsName);
							}
						}
					}
				} 
			}catch (SQLException e) {
				e.printStackTrace();
			}
		}
		for (String dsName : dataSetNameList) {
			navigateRecords(dsName);	
		}
	}

	public void navigateRecords(String dataSetName ) {
		Statement statement;
		boolean flag = true;
		int index = 0;
		List<String> resultDataSet = new ArrayList<String>();
		Map<String,Boolean> dataSetResult = new HashMap<String,Boolean>();
		try {
			statement = this.connection.createStatement();
			if (statement != null) {
				String 	QUERY = "select * from  " + DBCommands.TABLE_NAME + " where dataSetName like " + "\""  + dataSetName + "%\"";
				TestSession.logger.info("QUERY = " + QUERY);
				ResultSet resultSet = statement.executeQuery(QUERY);
				if (resultSet != null) {
					while (resultSet.next()) {
						String dataSetName1 =  resultSet.getString("dataSetName");
						String hadoopResult =  resultSet.getString("hadoopResult");
						String gdmResult =  resultSet.getString("gdmResult");
						String tezResult =  resultSet.getString("tezResult");
						String hiveResult =  resultSet.getString("hiveResult");
						String hiveDropTable =  resultSet.getString("hiveDropTable");
						String hiveCreateTable =  resultSet.getString("hiveCreateTable");
						String hiveCopyDataToHive =  resultSet.getString("hiveCopyDataToHive");
						String hiveLoadDataToTable =  resultSet.getString("hiveLoadDataToTable");
						String hcatResult =  resultSet.getString("hcatResult");
						String hbaseResult =  resultSet.getString("hbaseResult");
						String hbaseCreateTable =  resultSet.getString("hbaseCreateTable");
						String hbaseInsertRecordTable =  resultSet.getString("hbaseInsertRecordTable");
						String hbaseScanRecordTable =  resultSet.getString("hbaseScanRecordTable");
						String hbaseDeleteTable =  resultSet.getString("hbaseDeleteTable");

						String oozieResult = resultSet.getString("oozieResult");
						String cleanup_outputResult = resultSet.getString("cleanup_outputResult");
						String check_inputResult = resultSet.getString("check_inputResult");
						String pig_abf_input_PageValidNewsResult = resultSet.getString("pig_abf_input_PageValidNewsResult");
						String hive_storageResult = resultSet.getString("hive_storageResult");
						String hive_verifyResult = resultSet.getString("hive_verifyResult");


						String result =  resultSet.getString("result");
						resultDataSet.add(dataSetName1);
						if( (hadoopResult.indexOf("PASS") > -1) && (gdmResult.indexOf("PASS") > -1) && (tezResult.indexOf("PASS") > -1) && (hiveResult.indexOf("PASS") > -1) && (hiveDropTable.indexOf("PASS") > -1) 
								&& (hiveCreateTable.indexOf("PASS") > -1)  && (hiveCopyDataToHive.indexOf("PASS") > -1)  && (hiveLoadDataToTable.indexOf("PASS") > -1)  && (hcatResult.indexOf("PASS") > -1) 
								&& (hbaseResult.indexOf("PASS") > -1) && (hbaseCreateTable.indexOf("PASS") > -1) && (hbaseInsertRecordTable.indexOf("PASS") > -1) && (hbaseScanRecordTable.indexOf("PASS") > -1) 
								&& (hbaseDeleteTable.indexOf("PASS") > -1) &&  (oozieResult.indexOf("PASS") > -1) && (cleanup_outputResult.indexOf("PASS") > -1) && (check_inputResult.indexOf("PASS") > -1) &&
								(pig_abf_input_PageValidNewsResult.indexOf("PASS") > -1) && (hive_storageResult.indexOf("PASS") > -1) &&  (hive_verifyResult.indexOf("PASS") > -1) &&  (result.indexOf("PASS") > -1)  == false) {
							break;
						}
						index++;
					}
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		if (resultDataSet.size() == 0) {
			TestSession.logger.info("There is no dataset selected ");
		} else {
			String dsName = resultDataSet.get(resultDataSet.size() - 1);
			TestSession.logger.info("********** " + resultDataSet);
			updateTheFinalResult(dsName);
		}
	}

	public void updateTheFinalResult(String dataSetName) {
		Statement statement;
		Map<String,String> tempMap = new HashMap<String, String>();
		int count = 0;
		try {
			statement = this.connection.createStatement();
			if ( statement != null) {
				String 	QUERY = "select * from  " + DBCommands.TABLE_NAME + " where dataSetName = " + "\""  + dataSetName + "\"";
				TestSession.logger.info("QUERY  = " + QUERY);
				ResultSet resultSet = statement.executeQuery(QUERY);
				if (resultSet != null) {
					while (resultSet.next()) {
						String dataSetName1 =  resultSet.getString("dataSetName");
						String date =  resultSet.getString("date");

						String hadoopVersion =  resultSet.getString("hadoopVersion");
						String hadoopCurrentState =  resultSet.getString("hadoopCurrentState");
						String hadoopResult =  resultSet.getString("hadoopResult");
						String hadoopComments =  resultSet.getString("hadoopComments");

						String gdmVersion =  resultSet.getString("gdmVersion");
						String gdmCurrentState =  resultSet.getString("gdmCurrentState");
						String gdmResult =  resultSet.getString("gdmResult");
						String gdmComments =  resultSet.getString("gdmComments");

						String pigVersion =  resultSet.getString("pigVersion");
						String pigComments =  resultSet.getString("pigComments");

						String tezVersion =  resultSet.getString("tezVersion");
						String tezCurrentState =  resultSet.getString("tezCurrentState");
						String tezMRJobURL =  resultSet.getString("tezMRJobURL");
						String tezResult =  resultSet.getString("tezResult");
						String tezComments =  resultSet.getString("tezComments");

						String hiveVersion =  resultSet.getString("hiveVersion");
						String hiveCurrentState =  resultSet.getString("hiveCurrentState");
						String hiveResult =  resultSet.getString("hiveResult");
						String hiveComment =  resultSet.getString("hiveComment");

						String hiveDropTable =  resultSet.getString("hiveDropTable");
						String hiveDropTableCurrentState =  resultSet.getString("hiveDropTableCurrentState");
						String hiveDropTableComment =  resultSet.getString("hiveDropTableComment");

						String hiveCreateTable =  resultSet.getString("hiveCreateTable");
						String hiveCreateTableCurrentState =  resultSet.getString("hiveCreateTableCurrentState");
						String hiveCreateTableComment =  resultSet.getString("hiveCreateTableComment");

						String hiveCopyDataToHive =  resultSet.getString("hiveCopyDataToHive");
						String hiveCopyDataToHiveMRJobURL =  resultSet.getString("hiveCopyDataToHiveMRJobURL");
						String hiveCopyDataToHiveCurrentState =  resultSet.getString("hiveCopyDataToHiveCurrentState");
						String hiveCopyDataToHiveComment =  resultSet.getString("hiveCopyDataToHiveComment");

						String hiveLoadDataToTable =  resultSet.getString("hiveLoadDataToTable");
						String hiveLoadDataToTableComment =  resultSet.getString("hiveLoadDataToTableComment");
						String hiveLoadDataToTableCurrentState =  resultSet.getString("hiveLoadDataToTableCurrentState");

						String hcatVersion =  resultSet.getString("hcatVersion");
						String hcatCurrentState =  resultSet.getString("hcatCurrentState");
						String hcatResult =  resultSet.getString("hcatResult");
						String hcatMRJobURL =  resultSet.getString("hcatMRJobURL");
						String hcatComment =  resultSet.getString("hcatComment");

						String hbaseVersion =  resultSet.getString("hbaseVersion");
						String hbaseCurrentState =  resultSet.getString("hbaseCurrentState");
						String hbaseResult =  resultSet.getString("hbaseResult");
						String hbaseComment =  resultSet.getString("hbaseComment");

						String hbaseCreateTable =  resultSet.getString("hbaseCreateTable");
						String hbaseCreateTableCurrentState =  resultSet.getString("hbaseCreateTableCurrentState");
						String hbaseCreateTableComment =  resultSet.getString("hbaseCreateTableComment");

						String hbaseInsertRecordTable =  resultSet.getString("hbaseInsertRecordTable");
						String hbaseInsertRecordTableMRJobURL =  resultSet.getString("hbaseInsertRecordTableMRJobURL");
						String hbaseInsertTableCurrentState =  resultSet.getString("hbaseInsertTableCurrentState");
						String hbaseInsertRecordTableComment =  resultSet.getString("hbaseInsertRecordTableComment");

						String hbaseScanRecordTable =  resultSet.getString("hbaseScanRecordTable");
						String hbaseScanRecordTableMRJobURL =  resultSet.getString("hbaseScanRecordTableMRJobURL");
						String hbaseScanRecordTableCurrentState =  resultSet.getString("hbaseScanRecordTableCurrentState");
						String hbaseScanRecordTableComment =  resultSet.getString("hbaseScanRecordTableComment");

						String hbaseDeleteTable =  resultSet.getString("hbaseDeleteTable");
						String hbaseDeleteTableCurrentState =  resultSet.getString("hbaseDeleteTableCurrentState");
						String hbaseDeleteTableComment =  resultSet.getString("hbaseDeleteTableComment");

						String oozieVersion =  resultSet.getString("oozieVersion");
						String oozieResult =  resultSet.getString("oozieResult");
						String oozieCurrentState =  resultSet.getString("oozieCurrentState");
						String oozieComments =  resultSet.getString("oozieComments");

						String cleanup_outputResult = resultSet.getString("cleanup_outputResult");
						String cleanup_outputCurrentState = resultSet.getString("cleanup_outputCurrentState");
						String cleanup_outputMRJobURL = resultSet.getString("cleanup_outputMRJobURL");
						String cleanup_outputComments  = resultSet.getString("cleanup_outputComments");

						String check_inputResult = resultSet.getString("check_inputResult");
						String check_inputCurrentState = resultSet.getString("check_inputCurrentState");
						String check_inputMRJobURL = resultSet.getString("check_inputMRJobURL");
						String check_inputComments = resultSet.getString("check_inputComments");

						String pig_abf_input_PageValidNewsResult = resultSet.getString("pig_abf_input_PageValidNewsResult");
						String pig_abf_input_PageValidNewsCurrentState = resultSet.getString("pig_abf_input_PageValidNewsCurrentState");
						String pig_abf_input_PageValidNewsMRJobURL = resultSet.getString("pig_abf_input_PageValidNewsMRJobURL");
						String pig_abf_input_PageValidNewsComments = resultSet.getString("pig_abf_input_PageValidNewsComments");

						String hive_storageResult = resultSet.getString("hive_storageResult");
						String hive_storageCurrentState = resultSet.getString("hive_storageCurrentState");
						String hive_storageMRJobURL = resultSet.getString("hive_storageMRJobURL");
						String hive_storageComments = resultSet.getString("hive_storageComments");

						String hive_verifyResult = resultSet.getString("hive_verifyResult");
						String hive_verifyCurrentState = resultSet.getString("hive_verifyCurrentState");
						String hive_verifyMRJobURL = resultSet.getString("hive_verifyMRJobURL");
						String hive_verifyComments = resultSet.getString("hive_verifyComments");


						String comments =  resultSet.getString("comments");
						String result =  resultSet.getString("result");

						if (isRecordAlreadyExists(dataSetName1) == false) {
							insertFinalResultIntoDB(
									dataSetName1,date,
									hadoopVersion,hadoopCurrentState,hadoopResult,hadoopComments,
									gdmVersion,gdmCurrentState,gdmResult,gdmComments,
									pigVersion,pigComments,
									tezVersion,tezCurrentState,tezMRJobURL,tezResult,tezComments,
									hiveVersion,hiveCurrentState,hiveResult,hiveComment,
									hiveDropTable,hiveDropTableCurrentState,hiveDropTableComment,
									hiveCreateTable,hiveCreateTableCurrentState,hiveCreateTableComment,
									hiveCopyDataToHive,hiveCopyDataToHiveMRJobURL,hiveCopyDataToHiveCurrentState,hiveCopyDataToHiveComment,
									hiveLoadDataToTable,hiveLoadDataToTableComment,hiveLoadDataToTableCurrentState,
									hcatVersion,hcatCurrentState,hcatResult,hcatMRJobURL,hcatComment,
									hbaseVersion,hbaseCurrentState,hbaseResult,hbaseComment,
									hbaseCreateTable,hbaseCreateTableCurrentState,hbaseCreateTableComment,
									hbaseInsertRecordTable,hbaseInsertRecordTableMRJobURL,hbaseInsertTableCurrentState,hbaseInsertRecordTableComment,
									hbaseScanRecordTable,hbaseScanRecordTableMRJobURL,hbaseScanRecordTableCurrentState,hbaseScanRecordTableComment,
									hbaseDeleteTable,hbaseDeleteTableCurrentState,hbaseDeleteTableComment,
									oozieVersion,oozieResult,oozieCurrentState,oozieComments,
									cleanup_outputResult,cleanup_outputCurrentState,cleanup_outputMRJobURL,cleanup_outputComments,
									check_inputResult,check_inputCurrentState,check_inputMRJobURL,check_inputComments,
									pig_abf_input_PageValidNewsResult,pig_abf_input_PageValidNewsCurrentState,pig_abf_input_PageValidNewsMRJobURL,pig_abf_input_PageValidNewsComments,
									hive_storageResult,hive_storageCurrentState, hive_storageMRJobURL,hive_storageComments,
									hive_verifyResult,hive_verifyCurrentState,hive_verifyMRJobURL,hive_verifyComments,
									comments, result
									);
						} else {
							TestSession.logger.info( dataSetName1 + "    already exists...!");
						}						
					}
				}

			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void insertFinalResultIntoDB( String dataSetName1, String  date,
			String  hadoopVersion, String  hadoopCurrentState, String  hadoopResult, String  hadoopComments,
			String  gdmVersion, String  gdmCurrentState, String  gdmResult, String  gdmComments,
			String  pigVersion, String  pigComments,
			String  tezVersion, String  tezCurrentState, String  tezMRJobURL, String tezResult, String  tezComments,
			String  hiveVersion, String  hiveCurrentState, String  hiveResult, String  hiveComment,
			String  hiveDropTable, String  hiveDropTableCurrentState, String  hiveDropTableComment,
			String  hiveCreateTable, String  hiveCreateTableCurrentState, String  hiveCreateTableComment,
			String  hiveCopyDataToHive, String  hiveCopyDataToHiveMRJobURL, String  hiveCopyDataToHiveCurrentState, String  hiveCopyDataToHiveComment,
			String  hiveLoadDataToTable, String  hiveLoadDataToTableComment, String hiveLoadDataToTableCurrentState,
			String  hcatVersion, String  hcatCurrentState, String  hcatResult, String  hcatMRJobURL, String  hcatComment,
			String  hbaseVersion, String   hbaseCurrentState, String  hbaseResult, String  hbaseComment,
			String  hbaseCreateTable, String  hbaseCreateTableCurrentState, String hbaseCreateTableComment,
			String  hbaseInsertRecordTable, String hbaseInsertRecordTableMRJobURL, String hbaseInsertTableCurrentState, String  hbaseInsertRecordTableComment,
			String  hbaseScanRecordTable, String  hbaseScanRecordTableMRJobURL, String  hbaseScanRecordTableCurrentState, String  hbaseScanRecordTableComment,
			String  hbaseDeleteTable, String  hbaseDeleteTableCurrentState, String  hbaseDeleteTableComment,
			String  oozieVersion, String  oozieResult, String  oozieCurrentState, String  oozieComments,
			String  cleanup_outputResult, String  cleanup_outputCurrentState, String  cleanup_outputMRJobURL, String  cleanup_outputComments,
			String  check_inputResult, String  check_inputCurrentState, String check_inputMRJobURL, String check_inputComments,
			String pig_abf_input_PageValidNewsResult, String  pig_abf_input_PageValidNewsCurrentState ,String  pig_abf_input_PageValidNewsMRJobURL, String  pig_abf_input_PageValidNewsComments,
			String  hive_storageResult,String  hive_storageCurrentState, String hive_storageMRJobURL, String  hive_storageComments,
			String hive_verifyResult, String  hive_verifyCurrentState , String  hive_verifyMRJobURL, String hive_verifyComments,
			String  comments, String result ) {


		String INSERT_DATASET_INTO_ROW = "INSERT INTO " + DBCommands.FINAL_RESULT_TABLE_NAME + "( dataSetName)  " +  "values ( ? )";
		String dsName = dataSetName1.substring(0, (dataSetName1.length() - 2));	
		PreparedStatement pStatment ;
		try {

			pStatment = this.connection.prepareCall(INSERT_DATASET_INTO_ROW);
			pStatment.setString(1, dsName);
			boolean isRecordInserted = pStatment.execute();
		} catch (SQLException e1) {
			e1.printStackTrace();
		}

		try {
			this.dataBaseOperations.updateRecord(this.connection , "date" , date,
					"hadoopVersion", hadoopVersion,
					"hadoopCurrentState" , hadoopCurrentState,
					"hadoopResult" , hadoopResult,
					"hadoopComments" , hadoopComments,
					"gdmVersion" , gdmVersion,
					"gdmCurrentState" , gdmCurrentState,
					"gdmResult" , gdmResult,
					"gdmComments" , gdmComments,
					"pigVersion" , pigVersion,
					"pigComments" , pigComments , 
					"tezVersion" , tezVersion,
					"tezCurrentState" , tezCurrentState,
					"tezMRJobURL",tezMRJobURL,
					"tezResult" , tezResult ,
					"tezComments" , tezComments,
					"hiveVersion" , hiveVersion,
					"hiveCurrentState" , hiveCurrentState,
					"hiveResult" , hiveResult,
					"hiveComment",hiveComment,
					"hiveDropTable", hiveDropTable,
					"hiveDropTableCurrentState" , hiveDropTableCurrentState,
					"hiveDropTableComment" , hiveDropTableComment,
					"hiveCreateTable", hiveCreateTable,
					"hiveCreateTableCurrentState" , hiveCreateTableCurrentState,
					"hiveCreateTableComment",hiveCreateTableComment,
					"hiveCopyDataToHive" , hiveCopyDataToHive,
					"hiveCopyDataToHiveMRJobURL" , hiveCopyDataToHiveMRJobURL,
					"hiveCopyDataToHiveCurrentState", hiveCopyDataToHiveCurrentState,
					"hiveCopyDataToHiveComment",hiveCopyDataToHiveComment,
					"hiveLoadDataToTable",hiveLoadDataToTable,
					"hiveLoadDataToTableComment", hiveLoadDataToTableComment,
					"hiveLoadDataToTableCurrentState", hiveLoadDataToTableCurrentState,
					"hcatVersion",hcatVersion,
					"hcatCurrentState" , hcatCurrentState,
					"hcatResult" , hcatResult,
					"hcatMRJobURL" , hcatMRJobURL,
					"hcatComment" , hcatComment,
					"hbaseVersion",hbaseVersion,
					"hbaseCurrentState" , hbaseCurrentState,
					"hbaseResult", hbaseResult,
					"hbaseComment",hbaseComment,
					"hbaseCreateTable", hbaseCreateTable,
					"hbaseCreateTableCurrentState",hbaseCreateTableCurrentState,
					"hbaseCreateTableComment",hbaseCreateTableComment,
					"hbaseInsertRecordTable" , hbaseInsertRecordTable,
					"hbaseInsertRecordTableMRJobURL",hbaseInsertRecordTableMRJobURL,
					"hbaseInsertTableCurrentState",hbaseInsertTableCurrentState,
					"hbaseInsertRecordTableComment" , hbaseInsertRecordTableComment,
					"hbaseScanRecordTable", hbaseScanRecordTable,
					"hbaseScanRecordTableMRJobURL" , hbaseScanRecordTableMRJobURL,
					"hbaseScanRecordTableCurrentState" , hbaseScanRecordTableCurrentState,
					"hbaseScanRecordTableComment" , hbaseScanRecordTableComment,
					"hbaseDeleteTable",hbaseDeleteTable,
					"hbaseDeleteTableCurrentState" , hbaseDeleteTableCurrentState,
					"hbaseDeleteTableComment" , hbaseDeleteTableComment,
					"oozieVersion",oozieVersion,
					"oozieResult", oozieResult,
					"oozieCurrentState", oozieCurrentState,
					"oozieComments" , oozieComments,
					"cleanup_outputResult", cleanup_outputResult,
					"cleanup_outputCurrentState" , cleanup_outputCurrentState,
					"cleanup_outputMRJobURL", cleanup_outputMRJobURL,
					"cleanup_outputComments",cleanup_outputComments,
					"check_inputResult",check_inputResult,
					"check_inputCurrentState",check_inputCurrentState,
					"check_inputMRJobURL",check_inputMRJobURL,
					"check_inputComments",check_inputComments,
					"pig_abf_input_PageValidNewsResult",pig_abf_input_PageValidNewsResult,
					"pig_abf_input_PageValidNewsCurrentState" , pig_abf_input_PageValidNewsCurrentState,
					"pig_abf_input_PageValidNewsMRJobURL" , pig_abf_input_PageValidNewsMRJobURL,
					"pig_abf_input_PageValidNewsComments" , pig_abf_input_PageValidNewsComments,
					"hive_storageResult" , hive_storageResult,
					"hive_storageCurrentState" , hive_storageCurrentState,
					"hive_storageMRJobURL" , hive_storageMRJobURL,
					"hive_storageComments" , hive_storageComments,
					"hive_verifyResult" , hive_verifyResult,
					"hive_verifyCurrentState" , hive_verifyCurrentState,
					"hive_verifyMRJobURL" , hive_verifyMRJobURL,
					"hive_verifyComments" , hive_verifyComments,
					"comments",comments,
					"result",result,
					dsName
					);
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
		getComponentResult("haoop" , hadoopResult);
		getComponentResult("gmd" , gdmResult);
		getComponentResult("tez" , tezResult);
		getComponentResult("hive" , hiveDropTable,hiveCreateTable,hiveCopyDataToHive,hiveLoadDataToTable);
		getComponentResult("hcat" , hcatResult);
		getComponentResult("hbase"  , hbaseCreateTable,hbaseInsertRecordTable,hbaseScanRecordTable,hbaseDeleteTable);
		getComponentResult("oozie"  , cleanup_outputResult , check_inputResult , pig_abf_input_PageValidNewsResult, hive_storageResult, hive_verifyResult);
		createTestReport(dsName , comments);
	}

	public void createTestReport(String dataSetName , String comments) {
		int total = 0, fail = 0, pass = 0 , skipped = 0;
		String absolutePath = new File("").getAbsolutePath();
		File folderPath = new File(absolutePath + "/resources/stack_integration/integration_result");
		if (!folderPath.exists()) {
			if (folderPath.mkdir() == true ) {
				File reportFile = new File(folderPath.toString() + File.separator + "IntegrationReport.txt");
				try {
					//	if (reportFile.createNewFile() == true) {
					PrintWriter printWriter = new PrintWriter(reportFile);

					String currentStackComponentTestList =  GdmUtils.getConfiguration("testconfig.TestWatchForDataDrop.stackComponents");
					TestSession.logger.info("test list - " + currentStackComponentTestList);
					List<String> tempStackComponentList = Arrays.asList(currentStackComponentTestList.split(" "));
					
					if (tempStackComponentList != null && tempStackComponentList.size() > 0 ) {
						for ( String component : tempStackComponentList) {
							TestSession.logger.info("component - " + component);
							if (this.componentsResultMap.containsKey(component) == true) {
								String result = this.componentsResultMap.get(component);
								if (result.indexOf(":") > -1 && result != null) {
									TestSession.logger.info("result = " + result);
									List<String> resultList = Arrays.asList(result.split(":"));
									printWriter.write(component + "\t\t" + "Total testcase - " +  resultList.get(0)  + "\t" + "Pass - " + resultList.get(1)  + "\t" + "Fail - " + resultList.get(2) + "\t" + "Skipped - " + resultList.get(3) + "\n");
									total += Integer.parseInt(resultList.get(0));
									pass += Integer.parseInt(resultList.get(1));
									fail += Integer.parseInt(resultList.get(2));
									skipped += Integer.parseInt(resultList.get(3));	
								}
							}	
						}
					}
					printWriter.write("--------------------------------------------------------------------------------------------------------------------\n");
					printWriter.write("Pass - " + pass + "\n");
					printWriter.write("Fail - " + fail + "\n");
					printWriter.write("Skipped - " + skipped + "\n");
					printWriter.write("Total - " + total + "\n");
					printWriter.write("--------------------------------------------------------------------------------------------------------------------");
					printWriter.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}


	public void getComponentResult(String... result) {
		int pass = 0;
		int fail = 0;
		int skipped = 0;
		int total = 0;
		String componentName = result[0].trim();
		for ( int i=1 ; i <  result.length ; i++) {
			String tcr = result[1];
			if (tcr.indexOf("PASS") > -1) {
				pass++;
			} else if (tcr.indexOf("FAIL") > -1) {
				fail++;
			} else if (tcr.indexOf("UNKNOWN") > -1) {
				skipped++;
			}
			total++;
		}
		this.addResultToMap(componentName, total + ":" + pass + ":" + fail + ":" + skipped);
	}

	public boolean isRecordAlreadyExists(String dataSetName) {
		String dsName = dataSetName.substring(0, (dataSetName.length() - 2));
		String 	QUERY = "select dataSetName from  " + DBCommands.FINAL_RESULT_TABLE_NAME + " where dataSetName = " + "\""  + dsName + "\"";
		TestSession.logger.info("QUERY  = " + QUERY);
		boolean flag = false;
		try {
			Statement statement = this.connection.createStatement();
			if (statement != null) {
				ResultSet resultSet = statement.executeQuery(QUERY);
				if (resultSet != null) {
					while (resultSet.next()) {
						String dsName1 = resultSet.getString("dataSetName");
						TestSession.logger.info("dsName1= " + dsName1);
						if (dsName1.indexOf(dsName) > -1) {
							flag = true;
							TestSession.logger.info("found record");
							break;
						} else {
							TestSession.logger.info("found not record");
						}
					}
				}
			}
		} catch (SQLException e) {
			TestSession.logger.error("exception arised - " + e);
			e.printStackTrace();
		}
		return flag;
	}

}
