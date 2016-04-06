package hadooptest.gdm.regression.stackIntegration.db;

public interface DBCommands {
	String DB_NAME = "stackIntegrationTestDB";
	String TABLE_NAME = "integration_test";
	String CREATE_DB = "CREATE DATABASE IF NOT EXISTS " + DB_NAME;
	
	String CREATE_INTEGRATION_TABLE = 
			"CREATE TABLE IF NOT EXISTS " + TABLE_NAME
			+ " ( dataSetName VARCHAR(150) , "
			
			// pig
			+ "pigVersion VARCHAR(30)  DEFAULT 'UNKNOWN' , "
			+ "pigComments TEXT   , "
			
			// tez
			+ "tezVersion VARCHAR(30)  DEFAULT 'UNKNOWN' , "
			+ "tezCurrentState VARCHAR(20)  DEFAULT 'UNKNOWN' , "
			+ "tezMRJobURL VARCHAR(250)  DEFAULT 'UNKNOWN' , "
			+ "tezResult VARCHAR(10)  DEFAULT 'UNKNOWN' , "
			+ "tezComments TEXT   , "
			
			// hive 
			+ "hiveVersion VARCHAR(30)  DEFAULT 'UNKNOWN' , "
			+ "hiveCurrentState VARCHAR(20)  DEFAULT 'UNKNOWN' , "
			+ "hiveResult VARCHAR(10)  DEFAULT 'UNKNOWN' , "
			+ "hiveComment TEXT    , "
			
			// hiveDrop table
			+ "hiveDropTable VARCHAR(10)  DEFAULT 'UNKNOWN' , "
			+ "hiveDropTableComment TEXT   , "
			
			// hiveCreate table
			+ "hiveCreateTable VARCHAR(10)  DEFAULT 'UNKNOWN' , "
			+ "hiveCreateTableComment TEXT   , "
			
			// hiveCopyData
			+ "hiveCopyDataToHive VARCHAR(10)  DEFAULT 'UNKNOWN' , "
			+ "hiveCopyDataToHiveMRJobURL VARCHAR(250)  DEFAULT 'UNKNOWN' , "
			+ "hiveCopyDataToHiveComment TEXT    , "
			
			// hiveLoadDataToTable
			+ "hiveLoadDataToTable VARCHAR(10)  DEFAULT 'UNKNOWN' , "
			+ "hiveLoadDataToTableComment TEXT   , "
			
			// hcat
			+ "hcatVersion VARCHAR(30)  DEFAULT 'UNKNOWN' , "
			+ "hcatCurrentState VARCHAR(20)  DEFAULT 'UNKNOWN' , "
			+ "hcatResult VARCHAR(10)  DEFAULT 'UNKNOWN' , "
			+ "hcatMRJobURL VARCHAR(250)  DEFAULT 'UNKNOWN' , "
			+ "hcatComment TEXT    , "
			
			// hbase
			+ "hbaseVersion VARCHAR(30)  DEFAULT 'UNKNOWN' , "
			+ "hbaseCurrentState VARCHAR(20)  DEFAULT 'UNKNOWN' , "
			+ "hbaseResult VARCHAR(10)  DEFAULT 'UNKNOWN' , "
			+ "hbaseComment TEXT    , "
			
			// hbase Table create
			+ "hbaseCreateTable VARCHAR(10)  DEFAULT 'UNKNOWN' , "
			+ "hbaseCreateTableComment TEXT  , "
			
			// hbase record insert into table
			+ "hbaseInsertRecordTable VARCHAR(10)  DEFAULT 'UNKNOWN' , "
			+ "hbaseInsertRecordTableMRJobURL VARCHAR(250)  DEFAULT 'UNKNOWN' , "
			+ "hbaseInsertRecordTableComment TEXT   , "

			// hbase record scan into table
			+ "hbaseScanRecordTable VARCHAR(10)  DEFAULT 'UNKNOWN' , "
			+ "hbaseScanRecordTableMRJobURL VARCHAR(250)  DEFAULT 'UNKNOWN' , "
			+ "hbaseScanRecordTableComment TEXT    , "
			
			// hbase table delete
			+ "hbaseDeleteTable VARCHAR(10)  DEFAULT 'UNKNOWN' , "
			+ "hbaseDeleteTableComment TEXT   , "
			
			// oozie
			+ "oozieVersion VARCHAR(30)  DEFAULT 'UNKNOWN' , "
			+ "oozieComments TEXT   , "
			
			+ "result VARCHAR(50)  DEFAULT 'UNKNOWN' )";
}
