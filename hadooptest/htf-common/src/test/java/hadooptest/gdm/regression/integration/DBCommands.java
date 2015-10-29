package hadooptest.gdm.regression.integration;


public interface DBCommands {
	String DB_NAME = "integration_test";
	String TABLE_NAME = "integration_test";
	String NAME_NODE_THREAD_INFO_TABLE = "name_node_thread_info";
	String NAME_NODE_DFS_MEMORY_INFO_TABLE = "name_node_memory_info";
	String HEALTH_CHECKUP_UP_TABLE = "health_checkup";
	String CREATE_DB = "CREATE DATABASE IF NOT EXISTS " + DB_NAME;
	String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS " + TABLE_NAME 
			+ " ( dataSetName VARCHAR(150) , " 
			+ "currentFrequency VARCHAR(50) , "
			+ "jobStarted VARCHAR(50)  DEFAULT 'UNKNOWN' , "
			+ "startTime VARCHAR(50)  DEFAULT 'START_TIME' , "
			+ "currentStep VARCHAR(25)  DEFAULT 'UNKNOWN' , "
			+ "status VARCHAR(25)  DEFAULT 'UNKNOWN' , "
			+ "endTime VARCHAR(50) DEFAULT 'END_TIME' , " 
			+ "dataAvailable VARCHAR(50) DEFAULT 'UNKNOWN' , "  // this will cover DATA_AVAILABE , DATA_INCOMPLETE, MISSED_SLA
			+ "oozieJobStarted VARCHAR(50) DEFAULT 'UNKNOWN' , " 
			+ "cleanUpOutput VARCHAR(1000) DEFAULT 'UNKNOWN' , " 
			+ "checkInput VARCHAR(1000) DEFAULT 'UNKNOWN' , " 
			+ "pigRawProcessor VARCHAR(1000) DEFAULT 'UNKNOWN' , "
			+ "hiveStorage VARCHAR(1000) DEFAULT 'UNKNOWN' , " 
			+ "hiveVerify VARCHAR(1000) DEFAULT 'UNKNOWN' , " 
			+ "oozieJobCompleted VARCHAR(50) DEFAULT 'UNKNOWN' , "
			+ "jobEnded VARCHAR(50)  DEFAULT 'UNKNOWN' , "
			+ "hbaseCreateTable VARCHAR(20)  DEFAULT 'UNKNOWN' , "
			+ "hbaseInsert VARCHAR(1000)  DEFAULT 'UNKNOWN' , "
			+ "hbaseScan VARCHAR(1000)  DEFAULT 'UNKNOWN' , "
			+ "hbaseDeleteTable VARCHAR(20)  DEFAULT 'UNKNOWN' , "
			+ "hiveTableDeleted VARCHAR(250)  DEFAULT 'UNKNOWN' , "
			+ "hiveTableCreate VARCHAR(250)  DEFAULT 'UNKNOWN' , "
			+ "hiveLoadData VARCHAR(250)  DEFAULT 'UNKNOWN' , "
			+ "hcat VARCHAR(250)  DEFAULT 'UNKNOWN' , "
			+ "tez VARCHAR(250)  DEFAULT 'UNKNOWN' , "
			+ "hadoopVersion VARCHAR(100)  DEFAULT 'UNKNOWN' , "
			+ "pigVersion VARCHAR(100)  DEFAULT 'UNKNOWN' , "
			+ "oozieVersion VARCHAR(100)  DEFAULT 'UNKNOWN' , "
			+ "hbaseVersion VARCHAR(100)  DEFAULT 'UNKNOWN' , "
			+ "tezVersion VARCHAR(100)  DEFAULT 'UNKNOWN' , "
			+ "hiveVersion VARCHAR(100)  DEFAULT 'UNKNOWN' , "
			+ "hcatVersion VARCHAR(100)  DEFAULT 'UNKNOWN' , "
			+ "result VARCHAR(50)  DEFAULT 'UNKNOWN' )";
	
	
	// TODO need to add tez & hbase version
	String INSERT_ROW = "INSERT INTO " + TABLE_NAME + " (dataSetName, currentFrequency, jobStarted, startTime, currentStep , hadoopVersion , pigVersion, oozieVersion, hbaseVersion, tezVersion, hiveVersion, hcatVersion)  " 
			+ "  values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ";
	
	String CREATE_NAME_NODE_THREAD_INFO_TABLE = "CREATE TABLE IF NOT EXISTS " + NAME_NODE_THREAD_INFO_TABLE
			+ " ( " 
					+ " NameNode_Name VARCHAR(50), "
					+ "HadoopVersion VARCHAR(50), "
					+ "TimeStamp VARCHAR(20)  DEFAULT '0' , " 
					+ "ThreadsNew VARCHAR(20)  DEFAULT '0' , " 
					+ "ThreadsRunnable VARCHAR(20) DEFAULT '0' , "
					+ "ThreadsBlocked VARCHAR(20) DEFAULT '0' , "
					+ "ThreadsWaiting VARCHAR(20) DEFAULT '0' , "
					+ "ThreadsTimedWaiting VARCHAR(20) DEFAULT '0' , "
					+ "ThreadsTerminated VARCHAR(20) DEFAULT '0' "
			+ " ) ";
	
	String CREATE_HEALTH_CHECKUP_TABLE= "CREATE TABLE IF NOT EXISTS " + HEALTH_CHECKUP_UP_TABLE 
			+ " ( " 
				+ " date VARCHAR(50) , "
				+ " Cluster_State VARCHAR(100) DEFAULT 'UNKNOWN' ," 
				+ " Oozie_State VARCHAR(100) DEFAULT 'UNKNOWN' ,"
				+ " Pig_State VARCHAR(100) DEFAULT 'UNKNOWN' ,"
				+ " Hive_State VARCHAR(100) DEFAULT 'UNKNOWN' ,"
				+ " Hcat_State VARCHAR(100) DEFAULT 'UNKNOWN' ,"
				+ " Hbase_State VARCHAR(100) DEFAULT 'UNKNOWN', "
				+ " Tez_State VARCHAR(100) DEFAULT 'UNKNOWN' , "
				+ " Last_updated VARCHAR(100) DEFAULT 'UNKNOWN' "
			+ " ) ";
	
	String INSERT_HEALTH_CHECKUP_INFO_ROW = "INSERT INTO " + HEALTH_CHECKUP_UP_TABLE + " ( date , Cluster_State , Pig_State,Hbase_State , tez_State, Hive_State , Hcat_State) "
			+ "  values ( ?, ?, ?, ?, ?, ?,?) ";
	
	String INSERT_NAME_NODE_THREAD_INFO_ROW = "INSERT INTO " + NAME_NODE_THREAD_INFO_TABLE + "  ( NameNode_Name , HadoopVersion , TimeStamp , ThreadsNew , ThreadsRunnable , ThreadsBlocked ,  ThreadsWaiting , ThreadsTimedWaiting , ThreadsTerminated ) " 
			+ " values (?, ?, ?, ?, ?, ?, ?, ?, ?) ";
	
	String CREATE_NAME_NODE_MEMORY_INFO_TABLE = "CREATE TABLE IF NOT EXISTS " + NAME_NODE_DFS_MEMORY_INFO_TABLE 
			+ " ( " 
					+ " NameNodeName VARCHAR(50), "
					+ "HadoopVersion VARCHAR(50), "
					+ "TimeStamp VARCHAR(20)  DEFAULT '0' , " 
					+ "TotalMemoryCapacity VARCHAR(100)  DEFAULT '0' , "
					+ "UsedMemoryCapacity VARCHAR(100) DEFAULT '0' , "
					+ "RemainingMemoryCapacity VARCHAR(100) DEFAULT '0' , "
					+ "MissingBlocks VARCHAR(100) DEFAULT '0'  "
			+ " ) ";
	
	String INSERT_NAME_NODE_DFS_MEMORY_ROW = "INSERT INTO " + NAME_NODE_DFS_MEMORY_INFO_TABLE + "  ( NameNodeName , HadoopVersion , TimeStamp , TotalMemoryCapacity , UsedMemoryCapacity , RemainingMemoryCapacity , MissingBlocks ) " 
			+ " values (?, ?, ?, ?, ?, ?, ?) ";

}
