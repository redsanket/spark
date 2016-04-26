package hadooptest.gdm.regression.stackIntegration.db;

public interface DBCommands {
	String DB_NAME = "stackIntegrationTestDB";
	String TABLE_NAME = "integration_test";
	String FINAL_RESULT_TABLE_NAME = "integrationFinalResult";
	String CREATE_DB = "CREATE DATABASE IF NOT EXISTS " + DB_NAME;
	
	String CREATE_INTEGRATION_TABLE =  "CREATE TABLE IF NOT EXISTS " + "TB_NAME"
				+ " ( dataSetName VARCHAR(150) NOT NULL PRIMARY KEY , "
				
				// date
				+ "date VARCHAR(30)  DEFAULT 'UNKNOWN' , "
							
				// hadoop
				+ "hadoopVersion VARCHAR(30)  DEFAULT 'UNKNOWN' , "
				+ "hadoopCurrentState VARCHAR(20)  DEFAULT 'UNKNOWN' , "
				+ "hadoopResult VARCHAR(30)  DEFAULT 'UNKNOWN' , "
				+ "hadoopComments TEXT   , "
				
				// gdm
				+ "gdmVersion VARCHAR(30)  DEFAULT 'UNKNOWN' , "
				+ "gdmCurrentState VARCHAR(20)  DEFAULT 'UNKNOWN' , "
				+ "gdmResult VARCHAR(30)  DEFAULT 'UNKNOWN' , "
				+ "gdmComments TEXT   , "
				
				// pig
				+ "pigVersion VARCHAR(30)  DEFAULT 'UNKNOWN' , "
				+ "pigComments TEXT   , "
				
				// tez
				+ "tezVersion VARCHAR(30)  DEFAULT 'UNKNOWN' , "
				+ "tezCurrentState VARCHAR(20)  DEFAULT 'UNKNOWN' , "
				+ "tezMRJobURL VARCHAR(250)  DEFAULT 'UNKNOWN' , "
				+ "tezResult VARCHAR(30)  DEFAULT 'UNKNOWN' , "
				+ "tezComments TEXT , "
				
				// hive 
				+ "hiveVersion VARCHAR(30)  DEFAULT 'UNKNOWN' , "
				+ "hiveCurrentState VARCHAR(20)  DEFAULT 'UNKNOWN' , "
				+ "hiveResult VARCHAR(30)  DEFAULT 'UNKNOWN' , "
				+ "hiveComment TEXT    , "
				
				// hiveDrop table
				+ "hiveDropTable VARCHAR(30)  DEFAULT 'UNKNOWN' , "
				+ "hiveDropTableCurrentState VARCHAR(10)  DEFAULT 'UNKNOWN' , "
				+ "hiveDropTableComment TEXT   , "
				
				// hiveCreate table
				+ "hiveCreateTable VARCHAR(30)  DEFAULT 'UNKNOWN' , "
				+ "hiveCreateTableCurrentState VARCHAR(10)  DEFAULT 'UNKNOWN' , "
				+ "hiveCreateTableComment TEXT   , "
				
				// hiveCopyData
				+ "hiveCopyDataToHive VARCHAR(30)  DEFAULT 'UNKNOWN' , "
				+ "hiveCopyDataToHiveMRJobURL VARCHAR(250)  DEFAULT 'UNKNOWN' , "
				+ "hiveCopyDataToHiveCurrentState VARCHAR(10)  DEFAULT 'UNKNOWN' , "
				+ "hiveCopyDataToHiveComment TEXT    , "
				
				// hiveLoadDataToTable
				+ "hiveLoadDataToTable VARCHAR(30)  DEFAULT 'UNKNOWN' , "
				+ "hiveLoadDataToTableComment TEXT   , "
				+ "hiveLoadDataToTableCurrentState VARCHAR(10)  DEFAULT 'UNKNOWN' , "
				
				// hcat
				+ "hcatVersion VARCHAR(30)  DEFAULT 'UNKNOWN' , "
				+ "hcatCurrentState VARCHAR(20)  DEFAULT 'UNKNOWN' , "
				+ "hcatResult VARCHAR(10)  DEFAULT 'UNKNOWN' , "
				+ "hcatMRJobURL VARCHAR(250)  DEFAULT 'UNKNOWN' , "
				+ "hcatComment TEXT    , "
				
				// hbase
				+ "hbaseVersion VARCHAR(30)  DEFAULT 'UNKNOWN' , "
				+ "hbaseCurrentState VARCHAR(20)  DEFAULT 'UNKNOWN' , "
				+ "hbaseResult VARCHAR(30)  DEFAULT 'UNKNOWN' , "
				+ "hbaseComment TEXT    , "
				
				// hbase Table create
				+ "hbaseCreateTable VARCHAR(30)  DEFAULT 'UNKNOWN' , "
				+ "hbaseCreateTableCurrentState VARCHAR(10)  DEFAULT 'UNKNOWN' , "
				+ "hbaseCreateTableComment TEXT  , "
				
				// hbase record insert into table
				+ "hbaseInsertRecordTable VARCHAR(30)  DEFAULT 'UNKNOWN' , "
				+ "hbaseInsertRecordTableMRJobURL VARCHAR(250)  DEFAULT 'UNKNOWN' , "
				+ "hbaseInsertTableCurrentState VARCHAR(10)  DEFAULT 'UNKNOWN' , "
				+ "hbaseInsertRecordTableComment TEXT   , "

				// hbase record scan into table
				+ "hbaseScanRecordTable VARCHAR(30)  DEFAULT 'UNKNOWN' , "
				+ "hbaseScanRecordTableMRJobURL VARCHAR(250)  DEFAULT 'UNKNOWN' , "
				+ "hbaseScanRecordTableCurrentState VARCHAR(10)  DEFAULT 'UNKNOWN' , "
				+ "hbaseScanRecordTableComment TEXT    , "
				
				// hbase table delete
				+ "hbaseDeleteTable VARCHAR(30)  DEFAULT 'UNKNOWN' , "
				+ "hbaseDeleteTableCurrentState VARCHAR(10)  DEFAULT 'UNKNOWN' , "
				+ "hbaseDeleteTableComment TEXT   , "
				
				// oozie
				+ "oozieVersion VARCHAR(30)  DEFAULT 'UNKNOWN' , "
				+ "oozieResult VARCHAR(30)  DEFAULT 'UNKNOWN' , "
				+ "oozieCurrentState VARCHAR(10)  DEFAULT 'UNKNOWN' , "
				+ "oozieComments TEXT   , "
				
				
				+ "comments TEXT   , "
				+ "result VARCHAR(50)  DEFAULT 'UNKNOWN' )";
}
