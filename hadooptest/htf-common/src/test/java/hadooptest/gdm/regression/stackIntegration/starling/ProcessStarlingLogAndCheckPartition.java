package hadooptest.gdm.regression.stackIntegration.starling;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import hadooptest.TestSession;
import hadooptest.gdm.regression.stackIntegration.lib.CommonFunctions;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class ProcessStarlingLogAndCheckPartition {

    private String logType;
    private String logDate;
    private String clusterName;
    private String starlingHostName;
    private String hiveHostName;
    private boolean logCollected;
    private boolean processing;
    private boolean processed;
    private String mrJobURL;
    public static JSONObject finalResultJSONObject;
    private JSONObject resultJsonObject;
    public static JSONArray starlingResultJsonArray;
    private Map<String, String> starlingLogTableMapping = new HashMap<String,String>();
    private CommonFunctions commonFunctions;

    // TODO : Get db name from starling.properties file   
    private final static String STARLING_DB_NAME = "starling_integration_test";
    private final static String HADOOP_HOME="export HADOOP_HOME=/home/gs/hadoop/current;";
    private final static String JAVA_HOME="export JAVA_HOME=/home/gs/java/jdk64/current/;";
    private final static String HADOOP_CONF_DIR="export HADOOP_CONF_DIR=/home/gs/conf/current;";
    private final static String HADOOPQA_KNITI = "kinit -k -t /homes/hadoopqa/hadoopqa.dev.headless.keytab hadoopqa@DEV.YGRID.YAHOO.COM;";
    private final static String STARLING_CMD = "export HADOOP_HOME=/home/gs/hadoop/current; /home/y/share/starling_proc/bin/runstar -Dstarling.conf.dir=/home/y/etc/starling_proc/ -l ";

    public ProcessStarlingLogAndCheckPartition(String starlingHostName, String clusterName, String hiveHostName , Map<String, String> starlingLogTableMapping , String logType , String logDate) {
	this.setStarlingHostName(starlingHostName);
	this.setClusterName(clusterName);
	this.setHiveHostName(hiveHostName);
	this.setLogType(logType);
	this.setLogDate(logDate);
	this.commonFunctions = new  CommonFunctions();
	this.resultJsonObject = new JSONObject();
	this.starlingResultJsonArray = new JSONArray();
	this.finalResultJSONObject = new JSONObject();
	this.finalResultJSONObject.put("starlingIntResult", this.starlingResultJsonArray);
	this.starlingLogTableMapping = starlingLogTableMapping;
    }

    public JSONObject getResultJsonObject() {
	return resultJsonObject;
    }

    private void setResultJsonObject(JSONObject resultJsonObject) {
	this.resultJsonObject = resultJsonObject;
    }

    private String getHiveHostName() {
	return hiveHostName;
    }

    private void setHiveHostName(String hiveHostName) {
	this.hiveHostName = hiveHostName;
    }

    private String getLogType() {
	return logType;
    }

    private void setLogType(String logType) {
	this.logType = logType;
    }

    private String getLogDate() {
	return logDate;
    }

    private void setLogDate(String logDate) {
	this.logDate = logDate;
    }

    private String getStarlingHostName() {
	return starlingHostName;
    }

    private void setStarlingHostName(String starlingHostName) {
	this.starlingHostName = starlingHostName;
    }

    private String getClusterName() {
	return clusterName;
    }

    private void setClusterName(String clusterName) {
	this.clusterName = clusterName;
    }

    private boolean isLogCollected() {
	return logCollected;
    }

    private void setLogCollected(boolean logCollected) {
	this.logCollected = logCollected;
    }

    private boolean isProcessing() {
	return processing;
    }

    private void setProcessing(boolean processing) {
	this.processing = processing;
    }

    private boolean isProcessed() {
	return processed;
    }

    private void setProcessed(boolean processed) {
	this.processed = processed;
    }

    private String getMrJobURL() {
	return mrJobURL;
    }

    private void setMrJobURL(String mrJobURL) {
	this.mrJobURL = mrJobURL;
    }

    private Map<String, String> getStarlingLogTableMapping() {
	return starlingLogTableMapping;
    }

    private void setStarlingLogTableMapping(Map<String, String> starlingLogTableMapping) {
	this.starlingLogTableMapping = starlingLogTableMapping;
    }

    public String runStar() {
	this.getResultJsonObject().put("logType", this.getLogType());
	this.getResultJsonObject().put("logDate", this.getLogDate());

	String starlingRunStarCmd = "ssh " + this.getStarlingHostName() + " \"" +  HADOOP_HOME + HADOOPQA_KNITI +  STARLING_CMD + this.getLogType() + " -t " + this.getLogDate() + "T00:00:00Z  " +
		this.getClusterName() + " & \"";
	TestSession.logger.info("starlingRunStarCmd  = " + starlingRunStarCmd);
	String output = this.commonFunctions.executeCommand(starlingRunStarCmd).trim();

	TestSession.logger.info("************  output start ********************");
	TestSession.logger.info("output - " + output);
	TestSession.logger.info("************  output end ********************");
	List<String> outputList = Arrays.asList(output.split("\n"));

	outputList.stream().parallel().forEach( item -> {

	    if (item.startsWith("No") && item.indexOf("logs will be collected (see Starling logs for details).") > -1) {
		this.getResultJsonObject().put("newLog", "no" );
	    } else if (item.startsWith("Collecting") & item.endsWith("logs.")) {
		this.getResultJsonObject().put("newLog", "yes");
	    }

	    if (item.startsWith("Finished collecting") ) {
		this.setLogCollected(true);
	    }

	    if (item.startsWith("Processing")) {
		this.setProcessing(true);
	    }

	    if (item.startsWith("Starling_" + this.getLogType() + "_" + this.getClusterName()) ) {
		int startStr = item.indexOf("URL:");
		String mrurl = item.substring(startStr,  item.length() );
		this.setMrJobURL(mrurl);
		this.getResultJsonObject().put("mrURL", mrurl);
	    }

	    if (item.startsWith("Finished processing")) {
		processed = true;
		this.setProcessed(true);
	    }
	});

	if ( this.isLogCollected() == true && this.isProcessing() == true && this.isProcessed() == true) {
	    return "starling_" + this.getLogType() + true;
	}

	TestSession.logger.info("Result - " + this.getResultJsonObject().toString());

	return "starling_" + this.getLogType() + false;
    }

    public String checkPartitionExist() {
	TestSession.logger.info("==== checkPartitionExist start () =====");
	String hiveCommand = "ssh " + this.getHiveHostName() + " \"" +  JAVA_HOME + HADOOP_HOME + HADOOP_CONF_DIR + HADOOPQA_KNITI
		+ " hive -v -e \\\""  + "show partitions "   + STARLING_DB_NAME + "." + this.getStarlingLogTableMapping().get(this.getLogType().trim()).toString() + "\\\" \"";

	TestSession.logger.info("hiveCommand - "  + hiveCommand);

	String output = this.commonFunctions.executeCommand(hiveCommand.trim());
	if (StringUtils.isNotBlank(output)) {
	    String key = "grid=" + this.getClusterName().trim() + "/dt=" + this.getLogDate().replace("-", "_").trim();
	    List<String> resultList = Arrays.asList(output.split("\n")).stream().parallel().filter(line -> line.indexOf(key) > -1).collect(Collectors.toList());
	    String resultStr = "";
	    if ( resultList.size() > 0) {
		resultStr = resultList.get(0);
		TestSession.logger.info("DonecheckPartitionExist - " + this.getLogType()  + " - " + resultStr);
		if ( StringUtils.isNotBlank(resultList.get(0).trim())) {
		    this.getResultJsonObject().put("partitionExist", "yes");
		    this.getResultJsonObject().put("partition", resultStr);
		} else {
		    this.getResultJsonObject().put("partitionExist", "no");
		}
		this.getResultJsonObject().put("result", "pass");
		TestSession.logger.info("checkPartitionExist() - partition - " + this.getResultJsonObject().toString());
	    }
	} else {
	    this.getResultJsonObject().put("result", "fail");
	    TestSession.logger.error("-------------   failed ---------");
	}
	TestSession.logger.info("==== checkPartitionExist end () =====");
	//this.starlingResultJsonArray.add(this.getResultJsonObject());
	return "starling_" + this.getLogType() + this.getResultJsonObject().getString("partitionExist");
    }
    
    public String addExecutionLogResult() {
	starlingResultJsonArray.add(this.getResultJsonObject());
	TestSession.logger.info(" ---- addExecutionLogResult  --- " + starlingResultJsonArray.toString());
	TestSession.logger.info("  final result - " + this.getResultJsonObject().toString());
	TestSession.logger.info(" finalResultJSONObject - " + finalResultJSONObject.toString());
	
	return this.getResultJsonObject().toString();
    }

    public void print(){
	TestSession.logger.info("result - " + this.starlingResultJsonArray.toString());
	TestSession.logger.info("Done log type - " + this.getLogType());
    }

    public static String getFinalResultJSONObject() {
        return starlingResultJsonArray.toString();
    }

    public static JSONObject getStarlingResultFinalJsonObject() {
	JSONObject jObj = new JSONObject();
	jObj.put("result", starlingResultJsonArray);
	return jObj;
    }

}