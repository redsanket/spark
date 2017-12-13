package hadooptest.gdm.regression.stackIntegration.starling;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import java.io.File;
import java.io.FileNotFoundException;
import java.text.SimpleDateFormat;

import hadooptest.TestSession;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.gdm.regression.stackIntegration.StackComponent;
import hadooptest.gdm.regression.stackIntegration.db.DataBaseOperations;
import hadooptest.gdm.regression.stackIntegration.lib.CommonFunctions;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

/**
 * Starling integration testcase.
 */
public class TestIntStarling implements java.util.concurrent.Callable<String> {
    
    private String clusterName;
    private String dataSetName;
    private String scriptLocation;
    private String starlingCommand;
    private String starlingHostName;
    private String hiveHostName;
    private List<String> logTypesList;
    private StackComponent stackComponent;
    private CommonFunctions commonFunctions;
    private Map <String,String> starlingLogMap = new HashMap<String,String>();
    private Map<String,String> starlingLogTableMapping = new HashMap<String,String>();

    public TestIntStarling(StackComponent stackComponent, String hostName, String clusterName , String hiveHostName) {
	this.stackComponent = stackComponent;
	this.setStarlingHostName(hostName);
	this.setClusterName(clusterName);
	this.setHiveHostName(hiveHostName);
	this.commonFunctions = new CommonFunctions(this.getClusterName());
	System.out.println("------------------starling logTypesList -------------------");
	String logs = this.commonFunctions.getStarlingLogTypes().trim();
	System.out.println("logs - " + logs);
	String starlingTestLogs = GdmUtils.getConfiguration("testconfig.TestWatchForDataDrop.starlingLogTypes").trim();
	System.out.println("starlingTestLogs - " + starlingTestLogs);
	this.logTypesList = Arrays.asList(logs.split(" "));
	System.out.println(this.logTypesList.toString());
    }
    
    private String getHiveHostName() {
        return hiveHostName;
    }

    private void setHiveHostName(String hiveHostName) {
        this.hiveHostName = hiveHostName;
    }

    public String getStarlingHostName() {
        return starlingHostName;
    }

    public void setStarlingHostName(String starlingHostName) {
        this.starlingHostName = starlingHostName;
    }

    public String getScriptLocation() {
	return scriptLocation;
    }

    public void setScriptLocation(String scriptLocation) {
	this.scriptLocation = scriptLocation;
    }

    public String getClusterName() {
	return clusterName;
    }

    public void setClusterName(String clusterName) {
	this.clusterName = clusterName;
    }

    public String getDataSetName() {
	return dataSetName;
    }

    public void setDataSetName(String dataSetName) {
	this.dataSetName = dataSetName;
    }

    public String getStarlingCommand() {
	return starlingCommand;
    }

    public void setStarlingCommand(String starlingCommand) {
	this.starlingCommand = starlingCommand;
    }

    private void init() throws InterruptedException, ExecutionException, FileNotFoundException {
	readStarlingLogTableMapping();
	
	ExecutorService executors = Executors.newFixedThreadPool(5);
	List<Callable<String>> list = new java.util.ArrayList<Callable<String>>();
	TestSession.logger.info("Starling logs to test - " + this.logTypesList.toString());
	for ( String logType : this.logTypesList) {
	    System.out.println("logType = " + logType);

	    String nameNodeName = GdmUtils.getConfiguration("testconfig.TestWatchForDataDrop.clusterName").trim();

	    // TODO , use yinst to get the namenode hostname
	    // TODO, get the name node form common funtion
	    Callable getLogInstaceInfo = new GetLogInstaceInfo( nameNodeName + "-n2.blue.ygrid.yahoo.com", logType.trim());
	    list.add(getLogInstaceInfo);
	}
	/*
	this.logTypesList.stream().parallel().forEach( logType -> {
	    
	    System.out.println("logType = " + logType);
	    
	    String nameNodeName = GdmUtils.getConfiguration("testconfig.TestWatchForDataDrop.clusterName").trim();
	    
	    // TODO , use yinst to get the namenode hostname
	    // TODO, get the name node form common funtion
	    Callable getLogInstaceInfo = new GetLogInstaceInfo( nameNodeName + "-n2.blue.ygrid.yahoo.com", logType.trim());
	    list.add(getLogInstaceInfo);
	});
*/
	if ( list.size() > 0) {
	    List<Future<String>> testExecutionList = executors.invokeAll(list);
	    for ( Future<String> result : testExecutionList) {
		TestSession.logger.info("------------------ TestIntStarling -----------------------");
		TestSession.logger.info("result - " + result.get());
		List<String> fields = Arrays.asList(result.get().split("~"));
		if (fields.size() > 1 ){
		    String logName = fields.get(1);
		    String dt = fields.get(2);
		    starlingLogMap.put(logName, dt);
		}
	    }
	    executors.shutdown();
	}
	list.clear();
    }
    
    /**
     * Read starlingtableMapping.properties file and get the log and table name mapping. 
     * @throws FileNotFoundException
     */
    private void readStarlingLogTableMapping() throws FileNotFoundException {
	String basePath = new File("").getAbsolutePath();
	File sPropertyFile = new File( basePath+ "/resources/stack_integration/starling/starlingtableMapping.properties");
	if (sPropertyFile != null) {
	    Scanner scanner = new Scanner(sPropertyFile);
	    while (scanner.hasNextLine()) {
		String line = scanner.nextLine().trim();
		String fields [] = line.split("=");
		if (fields.length > 0) {
		    if (fields[1].trim().indexOf(",") > -1) {
			List<String> multipleTables = Arrays.asList(fields[1].trim().split(","));
			this.starlingLogTableMapping.put(fields[0].trim(), multipleTables.get(0));
		    } else {
			this.starlingLogTableMapping.put(fields[0].trim(), fields[1].trim());
		    }
		}
	    }
	    if ( scanner != null )
		scanner.close();
	} else {
	    TestSession.logger.info(basePath+ "/resources/stack_integration/starling/starlingtableMapping.properties" + " file does not exists");
	}
    }

    @Override
    public String call() throws Exception {
	init();
	List<ProcessStarlingLogAndCheckPartition> tempList = new ArrayList<ProcessStarlingLogAndCheckPartition>();
	if (starlingLogMap.size() > 0 ) {
	    List<CompletableFuture<String>> processLogList = new java.util.ArrayList<CompletableFuture<String>>();
	    ExecutorService exService = Executors.newCachedThreadPool();
	    for (Map.Entry<String, String> entry : starlingLogMap.entrySet()) {
		ProcessStarlingLogAndCheckPartition processStarlingLogAndCheckPartitionObj = new ProcessStarlingLogAndCheckPartition(this.getStarlingHostName() , this.getClusterName() , this.getHiveHostName(), this.starlingLogTableMapping , entry.getKey().trim() , entry.getValue().trim());
		tempList.add(processStarlingLogAndCheckPartitionObj);
		CompletableFuture<String> cFuture = supplyAsync( () -> processStarlingLogAndCheckPartitionObj.runStar() , exService);
		processLogList.add(cFuture);
	    }
	    
	    CompletableFuture<Void> allDoneFuture = CompletableFuture.allOf(processLogList.toArray(new CompletableFuture[processLogList.size()]));
	    allDoneFuture.get();
	    boolean done = allDoneFuture.isDone();
	    System.out.println("is job done -" + done);
	    if (done) {
		processLogList.clear();
		for ( ProcessStarlingLogAndCheckPartition tempProcessStarlingLogAndCheckPartition : tempList ) {
		    TestSession.logger.info("json result - " + tempProcessStarlingLogAndCheckPartition.getResultJsonObject().toString());
		    CompletableFuture<String> future = supplyAsync( () -> tempProcessStarlingLogAndCheckPartition.checkPartitionExist() );
		    future.thenRunAsync( () -> tempProcessStarlingLogAndCheckPartition.addExecutionLogResult());
		    processLogList.add(future);
		}
		allDoneFuture = CompletableFuture.allOf(processLogList.toArray(new CompletableFuture[processLogList.size()]));
		allDoneFuture.get();
		TestSession.logger.info(" ------ finally job done  --------"+ allDoneFuture.isDone());
	    }
	   JSONObject finalResult = ProcessStarlingLogAndCheckPartition.getStarlingResultFinalJsonObject();
	   TestSession.logger.info("Final Result jsonobject - " + finalResult);
	   checkStarlingResultsAndUpdateDB(finalResult);
	}
	TestSession.logger.info("------------------ TestIntStarling done -----------------------");
	return this.stackComponent.getStackComponentName() + "-" + true;
    }

    private void checkStarlingResultsAndUpdateDB(JSONObject resultJsonObject) {
	StringBuffer failedResultBuffer = new StringBuffer();
	String starlingResult = "";
	String starlingComments = "";
	String starlingJSONResults = "";
	boolean failedFlag = false;
	if (resultJsonObject.containsKey("starlingIntResult")) {
	    JSONArray resultsJsonArray = resultJsonObject.getJSONArray("starlingIntResult");
	    for ( int i = 0; i < resultsJsonArray.size() ; i++) {
		JSONObject logExecutionJsonObject = resultsJsonArray.getJSONObject(i);
		String logType = logExecutionJsonObject.getString("logType");
		if ( this.logTypesList.contains(logType) ) {
		    String result = logExecutionJsonObject.getString("result").trim();
		    TestSession.logger.info("logType - " + logType + "    result - " + result);

		    if ( ! result.equalsIgnoreCase("pass")) {
			failedFlag = true;
			failedResultBuffer.append("[ ").append("logType - ").append(logType)
			.append(", logDate - ").append(logExecutionJsonObject.getString("logDate").trim())
			.append(", newLog - ").append(logExecutionJsonObject.getString("newLog").trim())
			.append(", mrURL - ").append(logExecutionJsonObject.getString("mrURL").trim())
			.append(", partitionExist - ").append(logExecutionJsonObject.getString("partitionExist").trim())
			.append(" ] , ");
		    }
		} else {
		    // TODO
		}
	    }

	    if ( failedFlag ) {
		// there is a failure
		starlingResult = "failed";
		starlingComments = "failed reason : " + failedResultBuffer.toString();
		starlingJSONResults  = failedResultBuffer.toString();
	    } else {
		starlingResult = "passed";
		starlingComments = "-";
		starlingJSONResults  = resultJsonObject.toString();
	    }

	  //  getDataSetNames
	    DataBaseOperations dbOperations = new DataBaseOperations();
	    if (dbOperations != null) {

		// get current date
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
		String currentHrPath = simpleDateFormat.format(calendar.getTime());

		List<String> dataSetNames = dbOperations.getDataSetNames(currentHrPath);
		TestSession.logger.info("dataSetNames - " + dataSetNames);
		for ( String dataSetName : dataSetNames) {
		    this.commonFunctions.updateDB(dataSetName, "starlingResult", starlingResult);
		    this.commonFunctions.updateDB(dataSetName, "starlingComments", starlingComments);
		    if (failedFlag) {
			this.commonFunctions.updateDB(dataSetName, "starlingJSONResults", starlingJSONResults);
		    }
		}
	    } else {
		TestSession.logger.error("Failed to create an instance of DataBaseOperations.");
	    }
	}
    }

}