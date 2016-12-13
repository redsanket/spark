jackage hadooptest.gdm.regression.stackIntegration.db;

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
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import hadooptest.TestSession;
import hadooptest.cluster.gdm.GdmUtils;
import jdk.nashorn.api.scripting.JSObject;

public class AggIntResult {

	private String currentDate;
	private String currentPipeLineName;
	private String startDateTime, endDateTime, uniqueId;
	private java.util.List<String> hadoopVersionList;
	private Map<String,String> componentsResultMap;
	private DataBaseOperations dataBaseOperations;
	private java.sql.Connection connection;

	public AggIntResult() {
		componentsResultMap = new HashMap<String,String>();
		String pipeLineName = GdmUtils.getConfiguration("testconfig.TestWatchForDataDrop.pipeLineName");
		this.setCurrentPipeLineName(pipeLineName);
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

                // set the aggregated run's start date and time
                java.text.SimpleDateFormat sdfStartDateTime = new java.text.SimpleDateFormat("yyyyMMddhhmmss");
                String currentStartDateTime = sdfStartDateTime.format(calendar.getTime());
                this.setStartDateTime(currentStartDateTime);
                // set placeholder for end date and time
                this.setEndDateTime("To Be Set");
                // set placeholder for Unique ID 
                this.setUniqueId("To Be Set");

		TestSession.logger.info("GRIDCI-1667, currentStartDateTime is: " + this.getStartDateTime());
		TestSession.logger.info("GRIDCI-1667, currentEndDatTime is: " + this.getEndDateTime());
		TestSession.logger.info("GRIDCI-1667, currentUniqueId is: " + this.getUniqueId());
	}
	
	public String getCurrentPipeLineName() {
		return currentPipeLineName;
	}

	public void setCurrentPipeLineName(String currentPipeLineName) {
		this.currentPipeLineName = currentPipeLineName;
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
	
        public String getStartDateTime() {
                return startDateTime;
        }

        public void setStartDateTime(String currentStartDateTime) {
                this.startDateTime = currentStartDateTime;
        }

        public String getEndDateTime() {
                return endDateTime;
        }

        public void setEndDateTime(String currentEndDateTime) {
                this.endDateTime = currentEndDateTime;
        }

        public String getUniqueId() {
                return uniqueId;
        }

        public void setUniqueId(String currentUniqueId) {
                this.uniqueId = currentUniqueId;
        }


	public void finalResult() {
		java.util.List<String> versionList = getToDaysResult();
		if (versionList != null) {
			for ( String version : versionList) {
				TestSession.logger.info("version - " + version);
				int recordCount = getRecordCount(DBCommands.TABLE_NAME , this.getCurrentPipeLineName() , version ,  getCurrentDate() );
				if (recordCount == 3 ) {
					// navigate the records and check for results.
					String query = "select * from  " + DBCommands.TABLE_NAME + "  where date like  " + "\"" + getCurrentDate() + "\""  +"  and  " + this.getCurrentPipeLineName() + "Version"+ "  like  " +  "\"" +  version + "\"";
					TestSession.logger.info("query - " + query);
					getToDaysAllTheResult(query);
				}
				
				// Incase integration is run more than once  i,e manually
				if (recordCount > 3 ) {
					TestSession.logger.info("More than once integration test is run..!");
					
					//check whether current date and current build is shown in integration dashboard db. 
					int aggTableRecordCount = getRecordCount(DBCommands.FINAL_RESULT_TABLE_NAME , this.getCurrentPipeLineName() , version ,  getCurrentDate() );
					if (aggTableRecordCount == 0) {
						
						// insert the result in to the agg table
						String query = "select * from  " + DBCommands.TABLE_NAME + "  where date like  " + "\"" + getCurrentDate() + "\""  +"  and  " + this.getCurrentPipeLineName() + "Version"+ "  like  " +  "\"" +  version + "\"";
						getToDaysAllTheResult(query);
					} else if (aggTableRecordCount > 0) {
						TestSession.logger.info("Record already exists for " + getCurrentDate()  + "  & "  + this.getCurrentPipeLineName() + "Version"+  "  "  + version);
					}
				}
				if (recordCount == 0){
					TestSession.logger.info("Integration test dn't run for ");
				}  
			}
		} else {
			TestSession.logger.info("Looks like today integration did not run..");
		}
	}
	
	public java.util.List<String> getToDaysResult() {
		String query = "select dataSetName,date,gdmVersion,hadoopVersion,pigVersion,tezVersion,hiveVersion,hcatVersion,hbaseVersion,oozieVersion  from  " + DBCommands.TABLE_NAME + "  where date like " + "\"" + getCurrentDate() + "\""  
						+ " group by  " + this.getCurrentPipeLineName() + "Version";
		TestSession.logger.info("query - " + query);
		java.sql.ResultSet resultSet = this.getResultSet(query);
		java.util.List<String> compondentVersionList = new java.util.ArrayList<>();
		if (resultSet != null) {
			try {
				while (resultSet.next()) {
					String componentVersion = resultSet.getString( this.getCurrentPipeLineName() + "Version");
					if (compondentVersionList.contains(componentVersion) == false) {
						compondentVersionList.add(componentVersion);
					}
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return compondentVersionList;
	}
	
	public int getRecordCount(String tableName , String componentName , String componentVersion , String date) {
		int recordCount = 0;
		String QUERY = "select count(*) recordCount from  " + tableName + "  where " + componentName + "Version" + "  like  " +  "\"" +  componentVersion  + "\"" + "  and date like  "  + "\"" + date + "\"";
		TestSession.logger.info("QUERY = " + QUERY);
		java.sql.ResultSet resultSet = getResultSet(QUERY);
		if (resultSet != null) {
			try {
				while (resultSet.next()) {
					String rCount = resultSet.getString("recordCount");
					TestSession.logger.info("rCount = " + rCount);
					recordCount = Integer.parseInt(rCount);
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return recordCount;
	}
	
	public void getToDaysAllTheResult(String query) {
		java.sql.ResultSet resultSet = getResultSet(query);
		String compVersion = null , dataSetName1 = null;
		int count = 0;
		boolean isTestFailed = false;
		List<String>dataSetNameList = new ArrayList<String>();
		List<DBTableColumnsReplica> dbTableColumnsReplicaList = new ArrayList<DBTableColumnsReplica>();
		if (resultSet != null) {
			try {
				while(resultSet.next()) {
					compVersion = resultSet.getString(this.getCurrentPipeLineName() +"Version");
					dataSetName1 =  resultSet.getString("dataSetName");
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
					String pigCurrentState = resultSet.getString("pigCurrentState");
					String pigMRJobURL = resultSet.getString("pigMRJobURL");
					String pigResult = resultSet.getString("pigResult");
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
                                        String startDateTime =  resultSet.getString("startDateTime");
                                        String endDateTime =  resultSet.getString("endDateTime");
                                        String uniqueId =  resultSet.getString("uniqueId");

					
					dbTableColumnsReplicaList.add(new DBTableColumnsReplica(dataSetName1,date,
							hadoopVersion,hadoopCurrentState,hadoopResult,hadoopComments,
							gdmVersion,gdmCurrentState,gdmResult,gdmComments,
							pigVersion, pigCurrentState, pigMRJobURL, pigResult,pigComments,
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
							comments, result, startDateTime, endDateTime, uniqueId));
					
					TestSession.logger.info("dataSetName =  " + dataSetName1 + "    result - " + result);
					if(  (result.indexOf("PASS") > -1)  == false) {
						isTestFailed = true;
						TestSession.logger.info(" failed.........");
						int recordCount = getRecordCount(DBCommands.FINAL_RESULT_TABLE_NAME , this.getCurrentPipeLineName() , compVersion ,  getCurrentDate() );
						if (recordCount == 0) {
							TestSession.logger.info("Record for " +  getCurrentDate() +  " and for " +  this.getCurrentPipeLineName()  + " - " + this.getCurrentPipeLineName() + "Version"  + "  = " + compVersion + " does not exist, inserting a new record.");
							insertRecordIntoFinalTable(dataSetName1, this.getCurrentPipeLineName() , compVersion , getCurrentDate() );
							insertFinalResultIntoDB(
									dataSetName1,date,
									hadoopVersion,hadoopCurrentState,hadoopResult,hadoopComments,
									gdmVersion,gdmCurrentState,gdmResult,gdmComments,
									pigVersion, pigCurrentState, pigMRJobURL, pigResult,pigComments,
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
									comments, result, startDateTime, endDateTime, uniqueId
								);
						}else {
							TestSession.logger.info("Record for " +  getCurrentDate() +  " and for " +  this.getCurrentPipeLineName()  + " - " + this.getCurrentPipeLineName() + "Version" + " already exist.");
						}
					}
					count++;
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
		if (isTestFailed == false) {
			TestSession.logger.info("All components are passed.");
			if (compVersion != null) {
				int recordCount = getRecordCount(DBCommands.FINAL_RESULT_TABLE_NAME , this.getCurrentPipeLineName() , compVersion ,  getCurrentDate() );

				TestSession.logger.info("GRIDCI-1667: recordCount is: " + recordCount); 

				if (recordCount == 0) {
					TestSession.logger.info("Record for " +  getCurrentDate() +  " and for " +  this.getCurrentPipeLineName()  + " - " + this.getCurrentPipeLineName() + "Version" + " does not exist, inserting a new record.");

					TestSession.logger.info("GRIDCI-1667: dbTableColumnsReplicaList.size is: " + dbTableColumnsReplicaList.size() );

					insertRecordIntoFinalTable(dataSetName1, this.getCurrentPipeLineName() , compVersion , getCurrentDate() );
					if (dbTableColumnsReplicaList.size() > -1) {
						DBTableColumnsReplica dbTableColumnsReplicaObject = dbTableColumnsReplicaList.get(dbTableColumnsReplicaList.size()-1);

						TestSession.logger.info("GRIDCI-1667: new record case, startDateTime is: " + dbTableColumnsReplicaObject.getStartDateTime());
						TestSession.logger.info("GRIDCI-1667: new record case, endDateTime is: " + dbTableColumnsReplicaObject.getEndDateTime());
						TestSession.logger.info("GRIDCI-1667: new record case, uniqueId is: " + dbTableColumnsReplicaObject.getUniqueId());

						insertFinalResultIntoDB(
								dbTableColumnsReplicaObject.getDataSetName(),dbTableColumnsReplicaObject.getDate(),
								dbTableColumnsReplicaObject.getHadoopVersion(),dbTableColumnsReplicaObject.getHadoopCurrentState(),dbTableColumnsReplicaObject.getHadoopResult(),dbTableColumnsReplicaObject.getHadoopComments(),
								dbTableColumnsReplicaObject.getGdmVersion(),dbTableColumnsReplicaObject.getGdmCurrentState(),dbTableColumnsReplicaObject.getGdmResult(),dbTableColumnsReplicaObject.getGdmComments(),
								
								// pig
								dbTableColumnsReplicaObject.getPigVersion(), dbTableColumnsReplicaObject.getPigCurrentState(),dbTableColumnsReplicaObject.getPigMRJobURL(),
								dbTableColumnsReplicaObject.getPigResult(),dbTableColumnsReplicaObject.getPigComments(),
								
								// tez
								dbTableColumnsReplicaObject.getTezVersion(),dbTableColumnsReplicaObject.getTezCurrentState(),dbTableColumnsReplicaObject.getTezMRJobURL(),dbTableColumnsReplicaObject.getTezResult(),dbTableColumnsReplicaObject.getTezComments(),
								dbTableColumnsReplicaObject.getHiveVersion(),dbTableColumnsReplicaObject.getHiveCurrentState(),dbTableColumnsReplicaObject.getHiveResult(),dbTableColumnsReplicaObject.getHiveComment(),
								dbTableColumnsReplicaObject.getHiveDropTable(),dbTableColumnsReplicaObject.getHiveDropTableCurrentState(),dbTableColumnsReplicaObject.getHiveDropTableComment(),
								dbTableColumnsReplicaObject.getHiveCreateTable(),dbTableColumnsReplicaObject.getHiveCreateTableCurrentState(),dbTableColumnsReplicaObject.getHiveCreateTableComment(),
								dbTableColumnsReplicaObject.getHiveCopyDataToHive(),dbTableColumnsReplicaObject.getHiveCopyDataToHiveMRJobURL(),dbTableColumnsReplicaObject.getHiveCopyDataToHiveCurrentState(),dbTableColumnsReplicaObject.getHiveCopyDataToHiveComment(),
								dbTableColumnsReplicaObject.getHiveLoadDataToTable(),dbTableColumnsReplicaObject.getHiveLoadDataToTableComment(),dbTableColumnsReplicaObject.getHiveLoadDataToTableCurrentState(),
								dbTableColumnsReplicaObject.getHcatVersion(),dbTableColumnsReplicaObject.getHcatCurrentState(),dbTableColumnsReplicaObject.getHcatResult(),dbTableColumnsReplicaObject.getHcatMRJobURL(),dbTableColumnsReplicaObject.getHcatComment(),
								dbTableColumnsReplicaObject.getHbaseVersion(),dbTableColumnsReplicaObject.getHbaseCurrentState(),dbTableColumnsReplicaObject.getHbaseResult(),dbTableColumnsReplicaObject.getHbaseComment(),
								dbTableColumnsReplicaObject.getHbaseCreateTable(),dbTableColumnsReplicaObject.getHbaseCreateTableCurrentState(),dbTableColumnsReplicaObject.getHbaseCreateTableComment(),
								dbTableColumnsReplicaObject.getHbaseInsertRecordTable(),dbTableColumnsReplicaObject.getHbaseInsertRecordTableMRJobURL(),dbTableColumnsReplicaObject.getHbaseInsertTableCurrentState(),dbTableColumnsReplicaObject.getHbaseInsertRecordTableComment(),
								dbTableColumnsReplicaObject.getHbaseScanRecordTable(),dbTableColumnsReplicaObject.getHbaseScanRecordTableMRJobURL(),dbTableColumnsReplicaObject.getHbaseScanRecordTableCurrentState(),dbTableColumnsReplicaObject.getHbaseScanRecordTableCurrentState(),
								dbTableColumnsReplicaObject.getHbaseDeleteTable(),dbTableColumnsReplicaObject.getHbaseDeleteTableCurrentState(),dbTableColumnsReplicaObject.getHbaseDeleteTableComment(),
								dbTableColumnsReplicaObject.getOozieVersion(),dbTableColumnsReplicaObject.getOozieResult(),dbTableColumnsReplicaObject.getOozieCurrentState(),dbTableColumnsReplicaObject.getOozieComments(),
								dbTableColumnsReplicaObject.getCleanup_outputResult(),dbTableColumnsReplicaObject.getCleanup_outputCurrentState(),dbTableColumnsReplicaObject.getCleanup_outputMRJobURL(),dbTableColumnsReplicaObject.getCleanup_outputComments(),
								dbTableColumnsReplicaObject.getCheck_inputResult(),dbTableColumnsReplicaObject.getCheck_inputCurrentState(),dbTableColumnsReplicaObject.getCheck_inputMRJobURL(),dbTableColumnsReplicaObject.getCheck_inputComments(),
								dbTableColumnsReplicaObject.getPig_abf_input_PageValidNewsResult(),dbTableColumnsReplicaObject.getPig_abf_input_PageValidNewsCurrentState(),dbTableColumnsReplicaObject.getPig_abf_input_PageValidNewsMRJobURL(),dbTableColumnsReplicaObject.getPig_abf_input_PageValidNewsComments(),
								dbTableColumnsReplicaObject.getHive_storageResult(),dbTableColumnsReplicaObject.getHive_storageCurrentState(),dbTableColumnsReplicaObject.getHive_storageMRJobURL(),dbTableColumnsReplicaObject.getHive_storageComments(),
								dbTableColumnsReplicaObject.getHive_verifyResult(),dbTableColumnsReplicaObject.getHive_verifyCurrentState(),dbTableColumnsReplicaObject.getHive_verifyMRJobURL(),dbTableColumnsReplicaObject.getHive_verifyComments(),
								dbTableColumnsReplicaObject.getComments(),dbTableColumnsReplicaObject.getResult(),dbTableColumnsReplicaObject.getStartDateTime(),dbTableColumnsReplicaObject.getEndDateTime(),dbTableColumnsReplicaObject.getUniqueId()
							);
					}
				}else {
					TestSession.logger.info("Record for " +  getCurrentDate() +  " and for " +  this.getCurrentPipeLineName()  + " - " + this.getCurrentPipeLineName() + "Version" + " already exist, updating the result for this new run.");
				}
			}
		}
	}
	
	public void insertFinalResultIntoDB( String dataSetName, String  date,
			String  hadoopVersion, String  hadoopCurrentState, String  hadoopResult, String  hadoopComments,
			String  gdmVersion, String  gdmCurrentState, String  gdmResult, String  gdmComments,
			String  pigVersion, String pigCurrentState, String pigMRJobURL , String pigResult , String pigComments, 
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
			String  comments, String result, String startDateTime, String endDateTime, String uniqueId ) {		
		String dsName = null;

		TestSession.logger.info("GRIDCI-1667: startDateTime is: " + startDateTime);
		TestSession.logger.info("GRIDCI-1667: endDateTime is: " + endDateTime);
		TestSession.logger.info("GRIDCI-1667: uniqueId is: " + uniqueId);

		try {
			dsName = dataSetName.substring(0, (dataSetName.length() - 4));
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
					"pigCurrentState" , pigCurrentState,
					"pigMRJobURL" , pigMRJobURL,
					"pigResult" , pigResult,
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
                                        "startDateTime",startDateTime,
                                        "endDateTime",endDateTime,
                                        "uniqueId",uniqueId,
					dsName
					);
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
		getComponentResult("haoop" , hadoopResult);
		getComponentResult("gmd" , gdmResult);
		getComponentResult("pig" , pigResult);
		getComponentResult("tez" , tezResult);
		getComponentResult("hive" , hiveDropTable,hiveCreateTable,hiveCopyDataToHive,hiveLoadDataToTable);
		getComponentResult("hcat" , hcatResult);
		getComponentResult("hbase"  , hbaseCreateTable,hbaseInsertRecordTable,hbaseScanRecordTable,hbaseDeleteTable);
		getComponentResult("oozie"  , cleanup_outputResult , check_inputResult , pig_abf_input_PageValidNewsResult, hive_storageResult, hive_verifyResult);
		createTestReport(dsName , comments);
                createJsonReport(dataSetName, date, hadoopVersion, hadoopCurrentState, hadoopResult, hadoopComments, gdmVersion, gdmCurrentState, gdmResult,
                                gdmComments, pigVersion, pigCurrentState, pigMRJobURL, pigResult, pigComments, tezVersion, tezCurrentState, tezMRJobURL, tezResult,
                                tezComments, hiveVersion, hiveCurrentState, hiveResult, hiveComment, hiveDropTable, hiveDropTableCurrentState, hiveDropTableComment,
                                hiveCreateTable, hiveCreateTableCurrentState, hiveCreateTableComment, hiveCopyDataToHive, hiveCopyDataToHiveMRJobURL,
                                hiveCopyDataToHiveCurrentState, hiveCopyDataToHiveComment, hiveLoadDataToTable, hiveLoadDataToTableComment, hiveLoadDataToTableCurrentState, 
                                hcatVersion, hcatCurrentState, hcatResult, hcatMRJobURL, hcatComment, hbaseVersion, hbaseCurrentState, hbaseResult, hbaseComment,
                                hbaseCreateTable, hbaseCreateTableCurrentState, hbaseCreateTableComment, hbaseInsertRecordTable, hbaseInsertRecordTableMRJobURL,
                                hbaseInsertTableCurrentState, hbaseInsertRecordTableComment, hbaseScanRecordTable, hbaseScanRecordTableMRJobURL, hbaseScanRecordTableCurrentState,
                                hbaseScanRecordTableComment, hbaseDeleteTable, hbaseDeleteTableCurrentState, hbaseDeleteTableComment, oozieVersion, oozieResult, oozieCurrentState,
                                oozieComments, cleanup_outputResult, cleanup_outputCurrentState, cleanup_outputMRJobURL, cleanup_outputComments, check_inputResult,
                                check_inputCurrentState, check_inputMRJobURL, check_inputComments, pig_abf_input_PageValidNewsResult, pig_abf_input_PageValidNewsCurrentState,
                                pig_abf_input_PageValidNewsMRJobURL, pig_abf_input_PageValidNewsComments, hive_storageResult, hive_storageCurrentState, hive_storageMRJobURL,
                                hive_storageComments, hive_verifyResult, hive_verifyCurrentState, hive_verifyMRJobURL, hive_verifyComments, comments, result, startDateTime, 
				endDateTime, uniqueId);

				TestSession.logger.info("GRIDCI-1667, mark5"); 
	}
	
	
	public void insertRecordIntoFinalTable(String dataSetName, String componentName, String version, String date ) {

                // set the aggregated run's end date and time
                Calendar calendar = Calendar.getInstance();
                calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
                java.text.SimpleDateFormat sdfEndDateTime = new java.text.SimpleDateFormat("yyyyMMddhhmmss");
                String currentEndDateTime = sdfEndDateTime.format(calendar.getTime());

		TestSession.logger.info("GRIDCI-1667, currentEndDatTime is: " + currentEndDateTime);

		String colName = componentName + "Version" ;
		String INSERT_DATASET_INTO_ROW = "INSERT INTO " + DBCommands.FINAL_RESULT_TABLE_NAME + "( dataSetName, " + colName + ", date, endDateTime)  " +  "values ( ?,?,?,? )";
		String dsName = dataSetName.substring(0, (dataSetName.length() - 4));
		PreparedStatement pStatment ;
		try {
			pStatment = this.connection.prepareCall(INSERT_DATASET_INTO_ROW);
			pStatment.setString(1, dsName);
			pStatment.setString(2, version);
			pStatment.setString(3, date);
                        pStatment.setString(4, currentEndDateTime);
			boolean isRecordInserted = pStatment.execute();
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
	}
	
	public ResultSet getResultSet( final String query ) {
		java.sql.Statement statement = null;
		java.sql.ResultSet resultSet = null;
		try {
			statement = this.connection.createStatement();
			if (statement != null) {
				resultSet = statement.executeQuery(query);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return resultSet;
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
							TestSession.logger.info("record not found.");
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

        public void createJsonReport(String dataSetName, String  date,
                        String  hadoopVersion, String  hadoopCurrentState, String  hadoopResult, String  hadoopComments,
                        String  gdmVersion, String  gdmCurrentState, String  gdmResult, String  gdmComments,
                        String  pigVersion, String pigCurrentState, String pigMRJobURL , String pigResult , String pigComments, 
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
                        String  comments, String result, String startDateTime, String endDateTime, String uniqueId) {

                JSONObject jsonObj = new JSONObject();
                JSONObject jsonObjTop = new JSONObject();

                String dsName = dataSetName.substring(0, (dataSetName.length() - 4));
                jsonObj.put("dataSetName", dsName);
                jsonObj.put("date" , date);
                jsonObj.put("hadoopVersion", hadoopVersion);
                jsonObj.put("hadoopCurrentState" , hadoopCurrentState);
                jsonObj.put("hadoopResult" , hadoopResult);
                jsonObj.put("hadoopComments" , hadoopComments);
                jsonObj.put("gdmVersion" , gdmVersion);
                jsonObj.put("gdmCurrentState" , gdmCurrentState);
                jsonObj.put("gdmResult" , gdmResult);
                jsonObj.put("gdmComments" , gdmComments);
                jsonObj.put("pigVersion" , pigVersion);
                jsonObj.put("pigCurrentState" , pigCurrentState);
                jsonObj.put("pigMRJobURL" , pigMRJobURL);
                jsonObj.put("pigResult" , pigResult);
                jsonObj.put("pigComments" , pigComments); 
                jsonObj.put("tezVersion" , tezVersion);
                jsonObj.put("tezCurrentState" , tezCurrentState);
                jsonObj.put("tezMRJobURL",tezMRJobURL);
                jsonObj.put("tezResult" , tezResult);
                jsonObj.put("tezComments" , tezComments);
                jsonObj.put("hiveVersion" , hiveVersion);
                jsonObj.put("hiveCurrentState" , hiveCurrentState);
                jsonObj.put("hiveResult" , hiveResult);
                jsonObj.put("hiveComment",hiveComment);
                jsonObj.put("hiveDropTable", hiveDropTable);
                jsonObj.put("hiveDropTableCurrentState" , hiveDropTableCurrentState);
                jsonObj.put("hiveDropTableComment" , hiveDropTableComment);
                jsonObj.put("hiveCreateTable", hiveCreateTable);
                jsonObj.put("hiveCreateTableCurrentState" , hiveCreateTableCurrentState);
                jsonObj.put("hiveCreateTableComment",hiveCreateTableComment);
                jsonObj.put("hiveCopyDataToHive" , hiveCopyDataToHive);
                jsonObj.put("hiveCopyDataToHiveMRJobURL" , hiveCopyDataToHiveMRJobURL);
                jsonObj.put("hiveCopyDataToHiveCurrentState", hiveCopyDataToHiveCurrentState);
                jsonObj.put("hiveCopyDataToHiveComment",hiveCopyDataToHiveComment);
                jsonObj.put("hiveLoadDataToTable",hiveLoadDataToTable);
                jsonObj.put("hiveLoadDataToTableComment", hiveLoadDataToTableComment);
                jsonObj.put("hiveLoadDataToTableCurrentState", hiveLoadDataToTableCurrentState);
                jsonObj.put("hcatVersion",hcatVersion);
                jsonObj.put("hcatCurrentState" , hcatCurrentState);
                jsonObj.put("hcatResult" , hcatResult);
                jsonObj.put("hcatMRJobURL" , hcatMRJobURL);
                jsonObj.put("hcatComment" , hcatComment);
                jsonObj.put("hbaseVersion",hbaseVersion);
                jsonObj.put("hbaseCurrentState" , hbaseCurrentState);
                jsonObj.put("hbaseResult", hbaseResult);
                jsonObj.put("hbaseComment",hbaseComment);
                jsonObj.put("hbaseCreateTable", hbaseCreateTable);
                jsonObj.put("hbaseCreateTableCurrentState",hbaseCreateTableCurrentState);
                jsonObj.put("hbaseCreateTableComment",hbaseCreateTableComment);
                jsonObj.put("hbaseInsertRecordTable" , hbaseInsertRecordTable);
                jsonObj.put("hbaseInsertRecordTableMRJobURL",hbaseInsertRecordTableMRJobURL);
                jsonObj.put("hbaseInsertTableCurrentState",hbaseInsertTableCurrentState);
                jsonObj.put("hbaseInsertRecordTableComment" , hbaseInsertRecordTableComment);
                jsonObj.put("hbaseScanRecordTable", hbaseScanRecordTable);
                jsonObj.put("hbaseScanRecordTableMRJobURL" , hbaseScanRecordTableMRJobURL);
                jsonObj.put("hbaseScanRecordTableCurrentState" , hbaseScanRecordTableCurrentState);
                jsonObj.put("hbaseScanRecordTableComment" , hbaseScanRecordTableComment);
                jsonObj.put("hbaseDeleteTable",hbaseDeleteTable);
                jsonObj.put("hbaseDeleteTableCurrentState" , hbaseDeleteTableCurrentState);
                jsonObj.put("hbaseDeleteTableComment" , hbaseDeleteTableComment);
                jsonObj.put("oozieVersion",oozieVersion);
                jsonObj.put("oozieResult", oozieResult);
                jsonObj.put("oozieCurrentState", oozieCurrentState);
                jsonObj.put("oozieComments" , oozieComments);
                jsonObj.put("cleanup_outputResult", cleanup_outputResult);
                jsonObj.put("cleanup_outputCurrentState" , cleanup_outputCurrentState);
                jsonObj.put("cleanup_outputMRJobURL", cleanup_outputMRJobURL);
                jsonObj.put("cleanup_outputComments",cleanup_outputComments);
                jsonObj.put("check_inputResult",check_inputResult);
                jsonObj.put("check_inputCurrentState",check_inputCurrentState);
                jsonObj.put("check_inputMRJobURL",check_inputMRJobURL);
                jsonObj.put("check_inputComments",check_inputComments);
                jsonObj.put("pig_abf_input_PageValidNewsResult",pig_abf_input_PageValidNewsResult);
                jsonObj.put("pig_abf_input_PageValidNewsCurrentState" , pig_abf_input_PageValidNewsCurrentState);
                jsonObj.put("pig_abf_input_PageValidNewsMRJobURL" , pig_abf_input_PageValidNewsMRJobURL);
                jsonObj.put("pig_abf_input_PageValidNewsComments" , pig_abf_input_PageValidNewsComments);
                jsonObj.put("hive_storageResult" , hive_storageResult);
                jsonObj.put("hive_storageCurrentState" , hive_storageCurrentState);
                jsonObj.put("hive_storageMRJobURL" , hive_storageMRJobURL);
                jsonObj.put("hive_storageComments" , hive_storageComments);
                jsonObj.put("hive_verifyResult" , hive_verifyResult);
                jsonObj.put("hive_verifyCurrentState" , hive_verifyCurrentState);
                jsonObj.put("hive_verifyMRJobURL" , hive_verifyMRJobURL);
                jsonObj.put("hive_verifyComments" , hive_verifyComments);
                jsonObj.put("comments",comments);
                jsonObj.put("result",result);
                jsonObj.put("startDateTime",startDateTime);
		jsonObj.put("endDateTime",endDateTime);
		jsonObj.put("uniqueId",uniqueId);


                String pipelineName = this.getCurrentPipeLineName();
                jsonObjTop.put("pipeline", pipelineName);
                jsonObjTop.put("resultset", jsonObj);

                String absolutePath = new File("").getAbsolutePath();
                File folderPath = new File(absolutePath + "/target/surefire-reports/integration_result");
                if (!folderPath.exists()) {
                        if (folderPath.mkdirs() == true ) {
                                File reportFile = new File(folderPath.toString() + File.separator + "IntegrationReport.json");
                                TestSession.logger.info("Final test results json file - " + reportFile.toString());
                                try {
                                        PrintWriter printWriter = new PrintWriter(reportFile);
                                        printWriter.write(jsonObjTop.toJSONString());
                                        printWriter.close();
                                } catch (IOException e) {
                                        e.printStackTrace();
                                }
                        }
                }
        }
}
