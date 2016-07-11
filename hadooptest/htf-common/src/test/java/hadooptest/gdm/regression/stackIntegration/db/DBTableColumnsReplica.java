package hadooptest.gdm.regression.stackIntegration.db;

/**
 * Class the to set and get the value of column of table.
 * If any new column is added to the db, then a data memeber should be added to
 * this class representing the column
 * 
 */
public class DBTableColumnsReplica {
	String compVersion;
	String dataSetName;
	String date;
	String hadoopVersion;
	String hadoopCurrentState;
	String hadoopResult;
	String hadoopComments;
	String gdmVersion;
	String gdmCurrentState;
	String gdmResult;
	String gdmComments;
	String pigVersion;
	String pigCurrentState;
	String pigMRJobURL;
	String pigResult;
	String pigComments;
	String tezVersion;
	String tezCurrentState;
	String tezMRJobURL;
	String tezResult;
	String tezComments;
	String hiveVersion;
	String hiveCurrentState;
	String hiveResult;
	String hiveComment;
	String hiveDropTable;
	String hiveDropTableCurrentState;
	String hiveDropTableComment;
	String hiveCreateTable;
	String hiveCreateTableCurrentState;
	String hiveCreateTableComment;
	String hiveCopyDataToHive;
	String hiveCopyDataToHiveMRJobURL;
	String hiveCopyDataToHiveCurrentState;
	String hiveCopyDataToHiveComment;
	String hiveLoadDataToTable;
	String hiveLoadDataToTableComment;
	String hiveLoadDataToTableCurrentState;
	String hcatVersion;
	String hcatCurrentState;
	String hcatResult;
	String hcatMRJobURL;
	String hcatComment;
	String hbaseVersion;
	String hbaseCurrentState;
	String hbaseResult;
	String hbaseComment;
	String hbaseCreateTable;
	String hbaseCreateTableCurrentState;
	String hbaseCreateTableComment;
	String hbaseInsertRecordTable;
	String hbaseInsertRecordTableMRJobURL;
	String hbaseInsertTableCurrentState;
	String hbaseInsertRecordTableComment;
	String hbaseScanRecordTable;
	String hbaseScanRecordTableMRJobURL;
	String hbaseScanRecordTableCurrentState;
	String hbaseScanRecordTableComment;
	String hbaseDeleteTable;
	String hbaseDeleteTableCurrentState;
	String hbaseDeleteTableComment;
	String oozieVersion ;
	String oozieResult ;
	String oozieCurrentState;  
	String oozieComments ;
	String cleanup_outputResult;
	String cleanup_outputCurrentState;
	String cleanup_outputMRJobURL;
	String cleanup_outputComments;
	String check_inputResult;
	String check_inputCurrentState;
	String check_inputMRJobURL;
	String check_inputComments;
	String pig_abf_input_PageValidNewsResult;
	String pig_abf_input_PageValidNewsCurrentState;
	String pig_abf_input_PageValidNewsMRJobURL;
	String pig_abf_input_PageValidNewsComments;
	String hive_storageResult;
	String hive_storageCurrentState;
	String hive_storageMRJobURL;
	String hive_storageComments;
	String hive_verifyResult;
	String hive_verifyCurrentState;
	String hive_verifyMRJobURL;
	String hive_verifyComments;
	String comments;
	String result;
	
	public DBTableColumnsReplica(String dataSetName, String date, String hadoopVersion, String hadoopCurrentState,
			String hadoopResult, String hadoopComments, String gdmVersion, String gdmCurrentState, String gdmResult,
			String gdmComments, 
			String pigVersion,String pigCurrentState,String pigMRJobURL,String pigResult, String pigComments, 
			String tezVersion, String tezCurrentState,
			String tezMRJobURL, String tezResult, String tezComments, String hiveVersion, String hiveCurrentState,
			String hiveResult, String hiveComment, String hiveDropTable, String hiveDropTableCurrentState,
			String hiveDropTableComment, String hiveCreateTable, String hiveCreateTableCurrentState,
			String hiveCreateTableComment, String hiveCopyDataToHive, String hiveCopyDataToHiveMRJobURL,
			String hiveCopyDataToHiveCurrentState, String hiveCopyDataToHiveComment, String hiveLoadDataToTable,
			String hiveLoadDataToTableComment, String hiveLoadDataToTableCurrentState, String hcatVersion,
			String hcatCurrentState, String hcatResult, String hcatMRJobURL, String hcatComment, String hbaseVersion,
			String hbaseCurrentState, String hbaseResult, String hbaseComment, String hbaseCreateTable,
			String hbaseCreateTableCurrentState, String hbaseCreateTableComment, String hbaseInsertRecordTable,
			String hbaseInsertRecordTableMRJobURL, String hbaseInsertTableCurrentState,
			String hbaseInsertRecordTableComment, String hbaseScanRecordTable, String hbaseScanRecordTableMRJobURL,
			String hbaseScanRecordTableCurrentState, String hbaseScanRecordTableComment, String hbaseDeleteTable,
			String hbaseDeleteTableCurrentState, String hbaseDeleteTableComment, String oozieVersion,
			String oozieResult, String oozieCurrentState, String oozieComments, String cleanup_outputResult,
			String cleanup_outputCurrentState, String cleanup_outputMRJobURL, String cleanup_outputComments,
			String check_inputResult, String check_inputCurrentState, String check_inputMRJobURL,
			String check_inputComments, String pig_abf_input_PageValidNewsResult,
			String pig_abf_input_PageValidNewsCurrentState, String pig_abf_input_PageValidNewsMRJobURL,
			String pig_abf_input_PageValidNewsComments, String hive_storageResult, String hive_storageCurrentState,
			String hive_storageMRJobURL, String hive_storageComments, String hive_verifyResult,
			String hive_verifyCurrentState, String hive_verifyMRJobURL, String hive_verifyComments, String comments,
			String result) {
		super();
		this.dataSetName = dataSetName;
		this.date = date;
		this.hadoopVersion = hadoopVersion;
		this.hadoopCurrentState = hadoopCurrentState;
		this.hadoopResult = hadoopResult;
		this.hadoopComments = hadoopComments;
		this.gdmVersion = gdmVersion;
		this.gdmCurrentState = gdmCurrentState;
		this.gdmResult = gdmResult;
		this.gdmComments = gdmComments;
		this.pigVersion = pigVersion;
		this.pigCurrentState = pigCurrentState;
		this.pigMRJobURL = pigMRJobURL;
		this.pigResult = pigResult;
		this.pigComments = pigComments;
		this.tezVersion = tezVersion;
		this.tezCurrentState = tezCurrentState;
		this.tezMRJobURL = tezMRJobURL;
		this.tezResult = tezResult;
		this.tezComments = tezComments;
		this.hiveVersion = hiveVersion;
		this.hiveCurrentState = hiveCurrentState;
		this.hiveResult = hiveResult;
		this.hiveComment = hiveComment;
		this.hiveDropTable = hiveDropTable;
		this.hiveDropTableCurrentState = hiveDropTableCurrentState;
		this.hiveDropTableComment = hiveDropTableComment;
		this.hiveCreateTable = hiveCreateTable;
		this.hiveCreateTableCurrentState = hiveCreateTableCurrentState;
		this.hiveCreateTableComment = hiveCreateTableComment;
		this.hiveCopyDataToHive = hiveCopyDataToHive;
		this.hiveCopyDataToHiveMRJobURL = hiveCopyDataToHiveMRJobURL;
		this.hiveCopyDataToHiveCurrentState = hiveCopyDataToHiveCurrentState;
		this.hiveCopyDataToHiveComment = hiveCopyDataToHiveComment;
		this.hiveLoadDataToTable = hiveLoadDataToTable;
		this.hiveLoadDataToTableComment = hiveLoadDataToTableComment;
		this.hiveLoadDataToTableCurrentState = hiveLoadDataToTableCurrentState;
		this.hcatVersion = hcatVersion;
		this.hcatCurrentState = hcatCurrentState;
		this.hcatResult = hcatResult;
		this.hcatMRJobURL = hcatMRJobURL;
		this.hcatComment = hcatComment;
		this.hbaseVersion = hbaseVersion;
		this.hbaseCurrentState = hbaseCurrentState;
		this.hbaseResult = hbaseResult;
		this.hbaseComment = hbaseComment;
		this.hbaseCreateTable = hbaseCreateTable;
		this.hbaseCreateTableCurrentState = hbaseCreateTableCurrentState;
		this.hbaseCreateTableComment = hbaseCreateTableComment;
		this.hbaseInsertRecordTable = hbaseInsertRecordTable;
		this.hbaseInsertRecordTableMRJobURL = hbaseInsertRecordTableMRJobURL;
		this.hbaseInsertTableCurrentState = hbaseInsertTableCurrentState;
		this.hbaseInsertRecordTableComment = hbaseInsertRecordTableComment;
		this.hbaseScanRecordTable = hbaseScanRecordTable;
		this.hbaseScanRecordTableMRJobURL = hbaseScanRecordTableMRJobURL;
		this.hbaseScanRecordTableCurrentState = hbaseScanRecordTableCurrentState;
		this.hbaseScanRecordTableComment = hbaseScanRecordTableComment;
		this.hbaseDeleteTable = hbaseDeleteTable;
		this.hbaseDeleteTableCurrentState = hbaseDeleteTableCurrentState;
		this.hbaseDeleteTableComment = hbaseDeleteTableComment;
		this.oozieVersion = oozieVersion;
		this.oozieResult = oozieResult;
		this.oozieCurrentState = oozieCurrentState;
		this.oozieComments = oozieComments;
		this.cleanup_outputResult = cleanup_outputResult;
		this.cleanup_outputCurrentState = cleanup_outputCurrentState;
		this.cleanup_outputMRJobURL = cleanup_outputMRJobURL;
		this.cleanup_outputComments = cleanup_outputComments;
		this.check_inputResult = check_inputResult;
		this.check_inputCurrentState = check_inputCurrentState;
		this.check_inputMRJobURL = check_inputMRJobURL;
		this.check_inputComments = check_inputComments;
		this.pig_abf_input_PageValidNewsResult = pig_abf_input_PageValidNewsResult;
		this.pig_abf_input_PageValidNewsCurrentState = pig_abf_input_PageValidNewsCurrentState;
		this.pig_abf_input_PageValidNewsMRJobURL = pig_abf_input_PageValidNewsMRJobURL;
		this.pig_abf_input_PageValidNewsComments = pig_abf_input_PageValidNewsComments;
		this.hive_storageResult = hive_storageResult;
		this.hive_storageCurrentState = hive_storageCurrentState;
		this.hive_storageMRJobURL = hive_storageMRJobURL;
		this.hive_storageComments = hive_storageComments;
		this.hive_verifyResult = hive_verifyResult;
		this.hive_verifyCurrentState = hive_verifyCurrentState;
		this.hive_verifyMRJobURL = hive_verifyMRJobURL;
		this.hive_verifyComments = hive_verifyComments;
		this.comments = comments;
		this.result = result;
	}

	public String getDataSetName() {
		return dataSetName;
	}

	public void setDataSetName(String dataSetName) {
		this.dataSetName = dataSetName;
	}
	
	public String getPigCurrentState() {
		return pigCurrentState;
	}

	public void setPigCurrentState(String pigCurrentState) {
		this.pigCurrentState = pigCurrentState;
	}

	public String getPigMRJobURL() {
		return pigMRJobURL;
	}

	public void setPigMRJobURL(String pigMRJobURL) {
		this.pigMRJobURL = pigMRJobURL;
	}

	public String getPigResult() {
		return pigResult;
	}

	public void setPigResult(String pigResult) {
		this.pigResult = pigResult;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getHadoopVersion() {
		return hadoopVersion;
	}

	public void setHadoopVersion(String hadoopVersion) {
		this.hadoopVersion = hadoopVersion;
	}

	public String getHadoopCurrentState() {
		return hadoopCurrentState;
	}

	public void setHadoopCurrentState(String hadoopCurrentState) {
		this.hadoopCurrentState = hadoopCurrentState;
	}

	public String getHadoopResult() {
		return hadoopResult;
	}

	public void setHadoopResult(String hadoopResult) {
		this.hadoopResult = hadoopResult;
	}

	public String getHadoopComments() {
		return hadoopComments;
	}

	public void setHadoopComments(String hadoopComments) {
		this.hadoopComments = hadoopComments;
	}

	public String getGdmVersion() {
		return gdmVersion;
	}

	public void setGdmVersion(String gdmVersion) {
		this.gdmVersion = gdmVersion;
	}

	public String getGdmCurrentState() {
		return gdmCurrentState;
	}

	public void setGdmCurrentState(String gdmCurrentState) {
		this.gdmCurrentState = gdmCurrentState;
	}

	public String getGdmResult() {
		return gdmResult;
	}

	public void setGdmResult(String gdmResult) {
		this.gdmResult = gdmResult;
	}

	public String getGdmComments() {
		return gdmComments;
	}

	public void setGdmComments(String gdmComments) {
		this.gdmComments = gdmComments;
	}

	public String getPigVersion() {
		return pigVersion;
	}

	public void setPigVersion(String pigVersion) {
		this.pigVersion = pigVersion;
	}

	public String getPigComments() {
		return pigComments;
	}

	public void setPigComments(String pigComments) {
		this.pigComments = pigComments;
	}

	public String getTezVersion() {
		return tezVersion;
	}

	public void setTezVersion(String tezVersion) {
		this.tezVersion = tezVersion;
	}

	public String getTezCurrentState() {
		return tezCurrentState;
	}

	public void setTezCurrentState(String tezCurrentState) {
		this.tezCurrentState = tezCurrentState;
	}

	public String getTezMRJobURL() {
		return tezMRJobURL;
	}

	public void setTezMRJobURL(String tezMRJobURL) {
		this.tezMRJobURL = tezMRJobURL;
	}

	public String getTezResult() {
		return tezResult;
	}

	public void setTezResult(String tezResult) {
		this.tezResult = tezResult;
	}

	public String getTezComments() {
		return tezComments;
	}

	public void setTezComments(String tezComments) {
		this.tezComments = tezComments;
	}

	public String getHiveVersion() {
		return hiveVersion;
	}

	public void setHiveVersion(String hiveVersion) {
		this.hiveVersion = hiveVersion;
	}

	public String getHiveCurrentState() {
		return hiveCurrentState;
	}

	public void setHiveCurrentState(String hiveCurrentState) {
		this.hiveCurrentState = hiveCurrentState;
	}

	public String getHiveResult() {
		return hiveResult;
	}

	public void setHiveResult(String hiveResult) {
		this.hiveResult = hiveResult;
	}

	public String getHiveComment() {
		return hiveComment;
	}

	public void setHiveComment(String hiveComment) {
		this.hiveComment = hiveComment;
	}

	public String getHiveDropTable() {
		return hiveDropTable;
	}

	public void setHiveDropTable(String hiveDropTable) {
		this.hiveDropTable = hiveDropTable;
	}

	public String getHiveDropTableCurrentState() {
		return hiveDropTableCurrentState;
	}

	public void setHiveDropTableCurrentState(String hiveDropTableCurrentState) {
		this.hiveDropTableCurrentState = hiveDropTableCurrentState;
	}

	public String getHiveDropTableComment() {
		return hiveDropTableComment;
	}

	public void setHiveDropTableComment(String hiveDropTableComment) {
		this.hiveDropTableComment = hiveDropTableComment;
	}

	public String getHiveCreateTable() {
		return hiveCreateTable;
	}

	public void setHiveCreateTable(String hiveCreateTable) {
		this.hiveCreateTable = hiveCreateTable;
	}

	public String getHiveCreateTableCurrentState() {
		return hiveCreateTableCurrentState;
	}

	public void setHiveCreateTableCurrentState(String hiveCreateTableCurrentState) {
		this.hiveCreateTableCurrentState = hiveCreateTableCurrentState;
	}

	public String getHiveCreateTableComment() {
		return hiveCreateTableComment;
	}

	public void setHiveCreateTableComment(String hiveCreateTableComment) {
		this.hiveCreateTableComment = hiveCreateTableComment;
	}

	public String getHiveCopyDataToHive() {
		return hiveCopyDataToHive;
	}

	public void setHiveCopyDataToHive(String hiveCopyDataToHive) {
		this.hiveCopyDataToHive = hiveCopyDataToHive;
	}

	public String getHiveCopyDataToHiveMRJobURL() {
		return hiveCopyDataToHiveMRJobURL;
	}

	public void setHiveCopyDataToHiveMRJobURL(String hiveCopyDataToHiveMRJobURL) {
		this.hiveCopyDataToHiveMRJobURL = hiveCopyDataToHiveMRJobURL;
	}

	public String getHiveCopyDataToHiveCurrentState() {
		return hiveCopyDataToHiveCurrentState;
	}

	public void setHiveCopyDataToHiveCurrentState(String hiveCopyDataToHiveCurrentState) {
		this.hiveCopyDataToHiveCurrentState = hiveCopyDataToHiveCurrentState;
	}

	public String getHiveCopyDataToHiveComment() {
		return hiveCopyDataToHiveComment;
	}

	public void setHiveCopyDataToHiveComment(String hiveCopyDataToHiveComment) {
		this.hiveCopyDataToHiveComment = hiveCopyDataToHiveComment;
	}

	public String getHiveLoadDataToTable() {
		return hiveLoadDataToTable;
	}

	public void setHiveLoadDataToTable(String hiveLoadDataToTable) {
		this.hiveLoadDataToTable = hiveLoadDataToTable;
	}

	public String getHiveLoadDataToTableComment() {
		return hiveLoadDataToTableComment;
	}

	public void setHiveLoadDataToTableComment(String hiveLoadDataToTableComment) {
		this.hiveLoadDataToTableComment = hiveLoadDataToTableComment;
	}

	public String getHiveLoadDataToTableCurrentState() {
		return hiveLoadDataToTableCurrentState;
	}

	public void setHiveLoadDataToTableCurrentState(String hiveLoadDataToTableCurrentState) {
		this.hiveLoadDataToTableCurrentState = hiveLoadDataToTableCurrentState;
	}

	public String getHcatVersion() {
		return hcatVersion;
	}

	public void setHcatVersion(String hcatVersion) {
		this.hcatVersion = hcatVersion;
	}

	public String getHcatCurrentState() {
		return hcatCurrentState;
	}

	public void setHcatCurrentState(String hcatCurrentState) {
		this.hcatCurrentState = hcatCurrentState;
	}

	public String getHcatResult() {
		return hcatResult;
	}

	public void setHcatResult(String hcatResult) {
		this.hcatResult = hcatResult;
	}

	public String getHcatMRJobURL() {
		return hcatMRJobURL;
	}

	public void setHcatMRJobURL(String hcatMRJobURL) {
		this.hcatMRJobURL = hcatMRJobURL;
	}

	public String getHcatComment() {
		return hcatComment;
	}

	public void setHcatComment(String hcatComment) {
		this.hcatComment = hcatComment;
	}

	public String getHbaseVersion() {
		return hbaseVersion;
	}

	public void setHbaseVersion(String hbaseVersion) {
		this.hbaseVersion = hbaseVersion;
	}

	public String getHbaseCurrentState() {
		return hbaseCurrentState;
	}

	public void setHbaseCurrentState(String hbaseCurrentState) {
		this.hbaseCurrentState = hbaseCurrentState;
	}

	public String getHbaseResult() {
		return hbaseResult;
	}

	public void setHbaseResult(String hbaseResult) {
		this.hbaseResult = hbaseResult;
	}

	public String getHbaseComment() {
		return hbaseComment;
	}

	public void setHbaseComment(String hbaseComment) {
		this.hbaseComment = hbaseComment;
	}

	public String getHbaseCreateTable() {
		return hbaseCreateTable;
	}

	public void setHbaseCreateTable(String hbaseCreateTable) {
		this.hbaseCreateTable = hbaseCreateTable;
	}

	public String getHbaseCreateTableCurrentState() {
		return hbaseCreateTableCurrentState;
	}

	public void setHbaseCreateTableCurrentState(String hbaseCreateTableCurrentState) {
		this.hbaseCreateTableCurrentState = hbaseCreateTableCurrentState;
	}

	public String getHbaseCreateTableComment() {
		return hbaseCreateTableComment;
	}

	public void setHbaseCreateTableComment(String hbaseCreateTableComment) {
		this.hbaseCreateTableComment = hbaseCreateTableComment;
	}

	public String getHbaseInsertRecordTable() {
		return hbaseInsertRecordTable;
	}

	public void setHbaseInsertRecordTable(String hbaseInsertRecordTable) {
		this.hbaseInsertRecordTable = hbaseInsertRecordTable;
	}

	public String getHbaseInsertRecordTableMRJobURL() {
		return hbaseInsertRecordTableMRJobURL;
	}

	public void setHbaseInsertRecordTableMRJobURL(String hbaseInsertRecordTableMRJobURL) {
		this.hbaseInsertRecordTableMRJobURL = hbaseInsertRecordTableMRJobURL;
	}

	public String getHbaseInsertTableCurrentState() {
		return hbaseInsertTableCurrentState;
	}

	public void setHbaseInsertTableCurrentState(String hbaseInsertTableCurrentState) {
		this.hbaseInsertTableCurrentState = hbaseInsertTableCurrentState;
	}

	public String getHbaseInsertRecordTableComment() {
		return hbaseInsertRecordTableComment;
	}

	public void setHbaseInsertRecordTableComment(String hbaseInsertRecordTableComment) {
		this.hbaseInsertRecordTableComment = hbaseInsertRecordTableComment;
	}

	public String getHbaseScanRecordTable() {
		return hbaseScanRecordTable;
	}

	public void setHbaseScanRecordTable(String hbaseScanRecordTable) {
		this.hbaseScanRecordTable = hbaseScanRecordTable;
	}

	public String getHbaseScanRecordTableMRJobURL() {
		return hbaseScanRecordTableMRJobURL;
	}

	public void setHbaseScanRecordTableMRJobURL(String hbaseScanRecordTableMRJobURL) {
		this.hbaseScanRecordTableMRJobURL = hbaseScanRecordTableMRJobURL;
	}

	public String getHbaseScanRecordTableCurrentState() {
		return hbaseScanRecordTableCurrentState;
	}

	public void setHbaseScanRecordTableCurrentState(String hbaseScanRecordTableCurrentState) {
		this.hbaseScanRecordTableCurrentState = hbaseScanRecordTableCurrentState;
	}

	public String getHbaseScanRecordTableComment() {
		return hbaseScanRecordTableComment;
	}

	public void setHbaseScanRecordTableComment(String hbaseScanRecordTableComment) {
		this.hbaseScanRecordTableComment = hbaseScanRecordTableComment;
	}

	public String getHbaseDeleteTable() {
		return hbaseDeleteTable;
	}

	public void setHbaseDeleteTable(String hbaseDeleteTable) {
		this.hbaseDeleteTable = hbaseDeleteTable;
	}

	public String getHbaseDeleteTableCurrentState() {
		return hbaseDeleteTableCurrentState;
	}

	public void setHbaseDeleteTableCurrentState(String hbaseDeleteTableCurrentState) {
		this.hbaseDeleteTableCurrentState = hbaseDeleteTableCurrentState;
	}

	public String getHbaseDeleteTableComment() {
		return hbaseDeleteTableComment;
	}

	public void setHbaseDeleteTableComment(String hbaseDeleteTableComment) {
		this.hbaseDeleteTableComment = hbaseDeleteTableComment;
	}

	public String getOozieVersion() {
		return oozieVersion;
	}

	public void setOozieVersion(String oozieVersion) {
		this.oozieVersion = oozieVersion;
	}

	public String getOozieResult() {
		return oozieResult;
	}

	public void setOozieResult(String oozieResult) {
		this.oozieResult = oozieResult;
	}

	public String getOozieCurrentState() {
		return oozieCurrentState;
	}

	public void setOozieCurrentState(String oozieCurrentState) {
		this.oozieCurrentState = oozieCurrentState;
	}

	public String getOozieComments() {
		return oozieComments;
	}

	public void setOozieComments(String oozieComments) {
		this.oozieComments = oozieComments;
	}

	public String getCleanup_outputResult() {
		return cleanup_outputResult;
	}

	public void setCleanup_outputResult(String cleanup_outputResult) {
		this.cleanup_outputResult = cleanup_outputResult;
	}

	public String getCleanup_outputCurrentState() {
		return cleanup_outputCurrentState;
	}

	public void setCleanup_outputCurrentState(String cleanup_outputCurrentState) {
		this.cleanup_outputCurrentState = cleanup_outputCurrentState;
	}

	public String getCleanup_outputMRJobURL() {
		return cleanup_outputMRJobURL;
	}

	public void setCleanup_outputMRJobURL(String cleanup_outputMRJobURL) {
		this.cleanup_outputMRJobURL = cleanup_outputMRJobURL;
	}

	public String getCleanup_outputComments() {
		return cleanup_outputComments;
	}

	public void setCleanup_outputComments(String cleanup_outputComments) {
		this.cleanup_outputComments = cleanup_outputComments;
	}

	public String getCheck_inputResult() {
		return check_inputResult;
	}

	public void setCheck_inputResult(String check_inputResult) {
		this.check_inputResult = check_inputResult;
	}

	public String getCheck_inputCurrentState() {
		return check_inputCurrentState;
	}

	public void setCheck_inputCurrentState(String check_inputCurrentState) {
		this.check_inputCurrentState = check_inputCurrentState;
	}

	public String getCheck_inputMRJobURL() {
		return check_inputMRJobURL;
	}

	public void setCheck_inputMRJobURL(String check_inputMRJobURL) {
		this.check_inputMRJobURL = check_inputMRJobURL;
	}

	public String getCheck_inputComments() {
		return check_inputComments;
	}

	public void setCheck_inputComments(String check_inputComments) {
		this.check_inputComments = check_inputComments;
	}

	public String getPig_abf_input_PageValidNewsResult() {
		return pig_abf_input_PageValidNewsResult;
	}

	public void setPig_abf_input_PageValidNewsResult(String pig_abf_input_PageValidNewsResult) {
		this.pig_abf_input_PageValidNewsResult = pig_abf_input_PageValidNewsResult;
	}

	public String getPig_abf_input_PageValidNewsCurrentState() {
		return pig_abf_input_PageValidNewsCurrentState;
	}

	public void setPig_abf_input_PageValidNewsCurrentState(String pig_abf_input_PageValidNewsCurrentState) {
		this.pig_abf_input_PageValidNewsCurrentState = pig_abf_input_PageValidNewsCurrentState;
	}

	public String getPig_abf_input_PageValidNewsMRJobURL() {
		return pig_abf_input_PageValidNewsMRJobURL;
	}

	public void setPig_abf_input_PageValidNewsMRJobURL(String pig_abf_input_PageValidNewsMRJobURL) {
		this.pig_abf_input_PageValidNewsMRJobURL = pig_abf_input_PageValidNewsMRJobURL;
	}

	public String getPig_abf_input_PageValidNewsComments() {
		return pig_abf_input_PageValidNewsComments;
	}

	public void setPig_abf_input_PageValidNewsComments(String pig_abf_input_PageValidNewsComments) {
		this.pig_abf_input_PageValidNewsComments = pig_abf_input_PageValidNewsComments;
	}

	public String getHive_storageResult() {
		return hive_storageResult;
	}

	public void setHive_storageResult(String hive_storageResult) {
		this.hive_storageResult = hive_storageResult;
	}

	public String getHive_storageCurrentState() {
		return hive_storageCurrentState;
	}

	public void setHive_storageCurrentState(String hive_storageCurrentState) {
		this.hive_storageCurrentState = hive_storageCurrentState;
	}

	public String getHive_storageMRJobURL() {
		return hive_storageMRJobURL;
	}

	public void setHive_storageMRJobURL(String hive_storageMRJobURL) {
		this.hive_storageMRJobURL = hive_storageMRJobURL;
	}

	public String getHive_storageComments() {
		return hive_storageComments;
	}

	public void setHive_storageComments(String hive_storageComments) {
		this.hive_storageComments = hive_storageComments;
	}

	public String getHive_verifyResult() {
		return hive_verifyResult;
	}

	public void setHive_verifyResult(String hive_verifyResult) {
		this.hive_verifyResult = hive_verifyResult;
	}

	public String getHive_verifyCurrentState() {
		return hive_verifyCurrentState;
	}

	public void setHive_verifyCurrentState(String hive_verifyCurrentState) {
		this.hive_verifyCurrentState = hive_verifyCurrentState;
	}

	public String getHive_verifyMRJobURL() {
		return hive_verifyMRJobURL;
	}

	public void setHive_verifyMRJobURL(String hive_verifyMRJobURL) {
		this.hive_verifyMRJobURL = hive_verifyMRJobURL;
	}

	public String getHive_verifyComments() {
		return hive_verifyComments;
	}

	public void setHive_verifyComments(String hive_verifyComments) {
		this.hive_verifyComments = hive_verifyComments;
	}

	public String getComments() {
		return comments;
	}

	public void setComments(String comments) {
		this.comments = comments;
	}

	public String getResult() {
		return result;
	}

	public void setResult(String result) {
		this.result = result;
	}
}
