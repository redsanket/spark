package hadooptest.gdm.regression.stackIntegration.db;

import java.sql.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import hadooptest.TestSession;

public class AggIntResult {
	
	private String currentDate;
	private java.util.List<String> hadoopVersionList;
	private DataBaseOperations dataBaseOperations;
	private java.sql.Connection connection;
	
	public AggIntResult() {
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
			} catch (SQLException e) {
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
							String result =  resultSet.getString("result");
							resultDataSet.add(dataSetName1);
							if( (hadoopResult.indexOf("PASS") > -1) && (gdmResult.indexOf("PASS") > -1) && (tezResult.indexOf("PASS") > -1) && (hiveResult.indexOf("PASS") > -1) && (hiveDropTable.indexOf("PASS") > -1) 
									&& (hiveCreateTable.indexOf("PASS") > -1)  && (hiveCopyDataToHive.indexOf("PASS") > -1)  && (hiveLoadDataToTable.indexOf("PASS") > -1)  && (hcatResult.indexOf("PASS") > -1) 
									&& (hbaseResult.indexOf("PASS") > -1) && (hbaseCreateTable.indexOf("PASS") > -1) && (hbaseInsertRecordTable.indexOf("PASS") > -1) && (hbaseScanRecordTable.indexOf("PASS") > -1) 
									&& (hbaseDeleteTable.indexOf("PASS") > -1) && (result.indexOf("PASS") > -1)  == false) {
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
							String result =  resultSet.getString("result");
							
							if (isRecordAlreadyExists(dataSetName1) == false) {
								insertFinalResultIntoDB(dataSetName1,date,hadoopVersion,hadoopCurrentState,hadoopResult,hadoopComments,gdmVersion,gdmCurrentState,gdmResult,pigVersion,pigComments,tezVersion,tezCurrentState,tezMRJobURL,
										tezResult,tezComments,hiveVersion,hiveCurrentState,hiveResult,hiveComment,hiveDropTable,hiveDropTableCurrentState,hiveDropTableComment,hiveCreateTable,hiveCreateTableCurrentState,
										hiveCreateTableComment,hiveCopyDataToHive,hiveCopyDataToHiveMRJobURL,hiveCopyDataToHiveCurrentState,hiveCopyDataToHiveComment,hiveLoadDataToTable,hiveLoadDataToTableComment,
										hiveLoadDataToTableCurrentState,hcatVersion,hcatCurrentState,hcatResult,hcatMRJobURL,hcatComment,hbaseVersion,hbaseCurrentState,hbaseResult,hbaseComment,hbaseCreateTable,
										hbaseCreateTableCurrentState,hbaseCreateTableComment,hbaseInsertRecordTable,hbaseInsertRecordTableMRJobURL,hbaseInsertTableCurrentState,hbaseInsertRecordTableComment,hbaseScanRecordTable,
										hbaseScanRecordTableMRJobURL,hbaseScanRecordTableCurrentState,hbaseScanRecordTableComment,hbaseDeleteTable,hbaseDeleteTableCurrentState,hbaseDeleteTableComment,oozieVersion,
										oozieResult,oozieCurrentState,oozieComments,result);
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
	
	public void insertFinalResultIntoDB(String dataSetName, String   date, String   hadoopVersion, String  hadoopCurrentState, String  hadoopResult, String  hadoopComments, 
			String  gdmVersion, String  gdmCurrentState, String  gdmResult, String  pigVersion, String  pigComments, String  tezVersion, String  tezCurrentState, String  tezMRJobURL, 
			String  tezResult, String  tezComments, String  hiveVersion, String  hiveCurrentState, String  hiveResult, String  hiveComment, String  hiveDropTable, String  hiveDropTableCurrentState, 
			String  hiveDropTableComment, String  hiveCreateTable, String  hiveCreateTableCurrentState, String   hiveCreateTableComment, String  hiveCopyDataToHive, String  hiveCopyDataToHiveMRJobURL, 
			String  hiveCopyDataToHiveCurrentState, String  hiveCopyDataToHiveComment, String  hiveLoadDataToTable, String  hiveLoadDataToTableComment, String hiveLoadDataToTableCurrentState, 
			String  hcatVersion, String  hcatCurrentState, String  hcatResult, String  hcatMRJobURL, String  hcatComment, String  hbaseVersion, String  hbaseCurrentState, String  hbaseResult, 
			String  hbaseComment, String  hbaseCreateTable, String   hbaseCreateTableCurrentState, String  hbaseCreateTableComment, String  hbaseInsertRecordTable, String  hbaseInsertRecordTableMRJobURL, 
			String  hbaseInsertTableCurrentState, String  hbaseInsertRecordTableComment, String  hbaseScanRecordTable, String   hbaseScanRecordTableMRJobURL, String  hbaseScanRecordTableCurrentState, 
			String  hbaseScanRecordTableComment, String  hbaseDeleteTable, String  hbaseDeleteTableCurrentState, String  hbaseDeleteTableComment, String  oozieVersion, String   oozieResult, String  oozieCurrentState, 
			String  oozieComments, String  result) {

		String INSERT_ROW = "INSERT INTO " + DBCommands.FINAL_RESULT_TABLE_NAME + " (dataSetName, date, hadoopVersion,hadoopCurrentState,hadoopResult,hadoopComments,gdmVersion,gdmCurrentState,gdmResult,pigVersion,pigComments,tezVersion,tezCurrentState,tezMRJobURL,"
				+ " tezResult,tezComments,hiveVersion,hiveCurrentState,hiveResult,hiveComment,hiveDropTable,hiveDropTableCurrentState,hiveDropTableComment,hiveCreateTable,hiveCreateTableCurrentState,   "
				+ " hiveCreateTableComment,hiveCopyDataToHive,hiveCopyDataToHiveMRJobURL,hiveCopyDataToHiveCurrentState,hiveCopyDataToHiveComment,hiveLoadDataToTable,hiveLoadDataToTableComment, "
				+ " hiveLoadDataToTableCurrentState,hcatVersion,hcatCurrentState,hcatResult,hcatMRJobURL,hcatComment,hbaseVersion,hbaseCurrentState,hbaseResult,hbaseComment,hbaseCreateTable, "
				+ " hbaseCreateTableCurrentState,hbaseCreateTableComment,hbaseInsertRecordTable,hbaseInsertRecordTableMRJobURL,hbaseInsertTableCurrentState,hbaseInsertRecordTableComment,hbaseScanRecordTable, "
				+ " hbaseScanRecordTableMRJobURL,hbaseScanRecordTableCurrentState,hbaseScanRecordTableComment,hbaseDeleteTable,hbaseDeleteTableCurrentState,hbaseDeleteTableComment,oozieVersion, "
				+ " oozieResult,oozieCurrentState,oozieComments,result )  "  + "  values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ";
		PreparedStatement preparedStatement;
		try {
			String dsName = dataSetName.substring(0, (dataSetName.length() - 2));
			preparedStatement = this.connection.prepareCall(INSERT_ROW);
			preparedStatement.setString(1, dsName);
			preparedStatement.setString(2, date);
			preparedStatement.setString(3, hadoopVersion);preparedStatement.setString(4, hadoopCurrentState);preparedStatement.setString(5, hadoopResult);preparedStatement.setString(6, hadoopComments);preparedStatement.setString(7, gdmVersion);
			preparedStatement.setString(8, gdmCurrentState);preparedStatement.setString(9, gdmResult);preparedStatement.setString(10, pigVersion);preparedStatement.setString(11, pigComments);	preparedStatement.setString(12, tezVersion);
			preparedStatement.setString(13, tezCurrentState);preparedStatement.setString(14, tezMRJobURL);preparedStatement.setString(15, tezResult);preparedStatement.setString(16, tezComments);
			preparedStatement.setString(17, hiveVersion);preparedStatement.setString(18, hiveCurrentState);preparedStatement.setString(19, hiveResult); preparedStatement.setString(20, hiveComment);
			preparedStatement.setString(21, hiveDropTable);preparedStatement.setString(22, hiveDropTableCurrentState);preparedStatement.setString(23, hiveDropTableComment);preparedStatement.setString(24, hiveCreateTable);
			preparedStatement.setString(25, hiveCreateTableCurrentState);preparedStatement.setString(26, hiveCreateTableComment);preparedStatement.setString(27, hiveCopyDataToHive);preparedStatement.setString(28, hiveCopyDataToHiveMRJobURL);
			preparedStatement.setString(29, hiveCopyDataToHiveCurrentState);preparedStatement.setString(30, hiveCopyDataToHiveComment);preparedStatement.setString(31, hiveLoadDataToTable);preparedStatement.setString(32, hiveLoadDataToTableComment);
			preparedStatement.setString(33, hiveLoadDataToTableCurrentState);preparedStatement.setString(34, hcatVersion);preparedStatement.setString(35, hcatCurrentState);preparedStatement.setString(36, hcatResult);
			preparedStatement.setString(37, hcatMRJobURL);preparedStatement.setString(38, hcatComment);preparedStatement.setString(39, hbaseVersion);preparedStatement.setString(40, hbaseCurrentState);preparedStatement.setString(41, hbaseResult);
			preparedStatement.setString(42, hbaseComment);preparedStatement.setString(43, hbaseCreateTable);preparedStatement.setString(44, hbaseCreateTableCurrentState);preparedStatement.setString(45, hbaseCreateTableComment);
			preparedStatement.setString(46, hbaseInsertRecordTable);preparedStatement.setString(47, hbaseInsertRecordTableMRJobURL);preparedStatement.setString(48, hbaseInsertTableCurrentState);preparedStatement.setString(49, hbaseInsertRecordTableComment);
			preparedStatement.setString(50, hbaseScanRecordTable);preparedStatement.setString(51, hbaseScanRecordTableMRJobURL);preparedStatement.setString(52, hbaseScanRecordTableCurrentState);
			preparedStatement.setString(53, hbaseScanRecordTableComment);preparedStatement.setString(54, hbaseDeleteTable);preparedStatement.setString(55, hbaseDeleteTableCurrentState);preparedStatement.setString(56, hbaseDeleteTableComment);
			preparedStatement.setString(57, oozieVersion);preparedStatement.setString(58, oozieResult);preparedStatement.setString(59, oozieCurrentState);preparedStatement.setString(60, oozieComments);preparedStatement.setString(61, result);
			boolean isRecordInserted = preparedStatement.execute();
		} catch (SQLException e) {
			TestSession.logger.error("Exception is thown while inserting the record - " + e);
			e.printStackTrace();
		}
		
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
