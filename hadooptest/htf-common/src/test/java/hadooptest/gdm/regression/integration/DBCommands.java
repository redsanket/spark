package hadooptest.gdm.regression.integration;


public interface DBCommands {
	String DB_NAME = "integration_test";
	String TABLE_NAME = "integration_test";
	String NAME_NODE_THREAD_INFO_TABLE = "name_node_thread_info";
	String NAME_NODE_DFS_MEMORY_INFO_TABLE = "name_node_memory_info";
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
			+ "cleanUpOutput VARCHAR(50) DEFAULT 'UNKNOWN' , " 
			+ "checkInput VARCHAR(50) DEFAULT 'UNKNOWN' , " 
			+ "pigRawProcessor VARCHAR(50) DEFAULT 'UNKNOWN' , " 
			+ "hiveStorage VARCHAR(50) DEFAULT 'UNKNOWN' , " 
			+ "hiveVerify VARCHAR(50) DEFAULT 'UNKNOWN' , " 
			+ "oozieJobCompleted VARCHAR(50) DEFAULT 'UNKNOWN' , "
			+ "jobEnded VARCHAR(50)  DEFAULT 'UNKNOWN' , "
			+ "result VARCHAR(50)  DEFAULT 'UNKNOWN' )"; 

	String INSERT_ROW = "INSERT INTO " + TABLE_NAME + " (dataSetName, currentFrequency, jobStarted, startTime, currentStep)  " 
			+ "  values (?, ?, ?, ?, ?) ";
	
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
