package hadooptest.gdm.regression.integration;


public interface DBCommands {
	String DB_NAME = "intergationDB";
	String TABLE_NAME = "integration_test";
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
			+ "jobEnded VARCHAR(50)  DEFAULT 'UNKNOWN' )"; 

	String INSERT_ROW = "INSERT INTO " + TABLE_NAME + " (dataSetName, currentFrequency, jobStarted, startTime, currentStep)  " 
			+ "  values (?, ?, ?, ?, ?) ";

}
