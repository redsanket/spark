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
import hadooptest.gdm.regression.stackIntegration.db.DBCommands;
import hadooptest.gdm.regression.stackIntegration.db.DataBaseOperations;
import hadooptest.gdm.regression.stackIntegration.healthCheckUp.StarlingHealthCheckUp;
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
    private final String STARLING_VERSION_COMMAND = "yinst ls | grep starling_proc";

    public TestIntStarling(StackComponent stackComponent, String hostName, String clusterName ) {
	this.stackComponent = stackComponent;
	this.setStarlingHostName(hostName);
	this.setClusterName(clusterName);
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
	String nNodeName = getComponentHostName("namenode");
	String hName = getComponentHostName("hive");
	this.setHiveHostName(hName);

	ExecutorService executors = Executors.newFixedThreadPool(5);
	List<Callable<String>> list = new java.util.ArrayList<Callable<String>>();
	TestSession.logger.info("Starling logs to test - " + this.logTypesList.toString());
	for ( String logType : this.logTypesList) {
	    String nameNodeName = GdmUtils.getConfiguration("testconfig.TestWatchForDataDrop.clusterName").trim();
	    Callable getLogInstaceInfo = new GetLogInstaceInfo( nNodeName , logType.trim());
	    list.add(getLogInstaceInfo);
	}
	
	if ( list.size() > 0) {
	    List<Future<String>> testExecutionList = executors.invokeAll(list);
	    for ( Future<String> result : testExecutionList) {
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
	    TestSession.logger.info("is job done -" + done);
	    if (done) {
		processLogList.clear();
		for ( ProcessStarlingLogAndCheckPartition tempProcessStarlingLogAndCheckPartition : tempList ) {
		    CompletableFuture<String> future = supplyAsync( () -> tempProcessStarlingLogAndCheckPartition.checkPartitionExist() );
		    processLogList.add(future);
		}
		allDoneFuture = CompletableFuture.allOf(processLogList.toArray(new CompletableFuture[processLogList.size()]));
		allDoneFuture.get();
		TestSession.logger.info(" ------ finally job done  --------"+ allDoneFuture.isDone());
	    }
	    
	    Map<String ,StarlingExecutionResult > starlingResult = ProcessStarlingLogAndCheckPartition.getStarlingExecutionResult();
	    updateStarlingResultToDB(starlingResult);
	}
	TestSession.logger.info("------------------ TestIntStarling done -----------------------");
	return this.stackComponent.getStackComponentName() + "-" + true;
    }
    
    public void updateStarlingResultToDB(Map<String ,StarlingExecutionResult > starlingResult) {
	String overAllResult = "";
	StringBuffer starlingComments = new StringBuffer();
	StringBuffer starlingFailedJobs = new StringBuffer();
	StringBuffer passedJobs = new StringBuffer();
	String dbUpdateComment = ""; 
	String dbUpdateResult = "";
	boolean failedFlag = false;
	
	TestSession.logger.info("TestIntStarling : starlingResult = "+starlingResult);
	// navigate the results
	for ( String logType  : this.logTypesList) {
	    StarlingExecutionResult starlingExecutionResultObject = starlingResult.get(logType.trim());
	    if ( starlingExecutionResultObject.getResults().equals("fail")) {
		failedFlag = true;
		starlingComments.append(starlingExecutionResultObject.getLogType()).append(",  ");
		starlingFailedJobs.append(starlingExecutionResultObject.toString()).append(",  ");
	    } else {
		passedJobs.append(starlingExecutionResultObject.toString()).append(",  ");
	    }
	}
	
	if (failedFlag ) {
	    overAllResult = "fail";
	    dbUpdateComment = starlingComments.append("  logs failed").toString();
	    dbUpdateResult = starlingFailedJobs.toString();
	} else {
	    overAllResult = "pass";
	    dbUpdateComment = starlingComments.append("All logs were processed successfully").toString();
	    dbUpdateResult = passedJobs.toString();
	}
	
	// updatedb with results
	DataBaseOperations dbOperations = new DataBaseOperations();
	 if (dbOperations != null) {

		// get current date
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
		String currentHrPath = simpleDateFormat.format(calendar.getTime());
		
		String tableName = DBCommands.DB_NAME + "." + DBCommands.TABLE_NAME;
		List<String> dataSetNames = dbOperations.getDataSetNames(tableName , currentHrPath);
		String starlingVersion = getStarlingDeployedVersion();
		TestSession.logger.info("dataSetNames - " + dataSetNames);
		for ( String dataSetName : dataSetNames) {
		    this.commonFunctions.updateDB(dataSetName, "starlingResult", overAllResult);
		    this.commonFunctions.updateDB(dataSetName, "starlingVersion", starlingVersion);
		    this.commonFunctions.updateDB(dataSetName, "starlingComments", dbUpdateComment);
		    this.commonFunctions.updateDB(dataSetName, "starlingJSONResults", dbUpdateResult);
		    if (failedFlag) {
			this.commonFunctions.updateDB(dataSetName, "starlingJSONResults", dbUpdateResult);
		    }
		}
	 }
    }

    public String getStarlingDeployedVersion() {
	String starlingHostName = GdmUtils.getConfiguration("testconfig.TestWatchForDataDrop.starlingHostName").trim();
	String command = "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null  " +starlingHostName  + "  \"" + STARLING_VERSION_COMMAND  + "\" ";
	TestSession.logger.info("command -" + command);
	String result = this.commonFunctions.executeCommand(command.trim()).trim();

	String version = "";
	java.util.List<String>outputList = Arrays.asList(result.split("\n"));
	String starlingVersion = null;
	boolean flag = false;
	for ( String str : outputList) {
	    TestSession.logger.info(str);
	    if ( str.startsWith("starling_proc-") == true ) {
		starlingVersion = Arrays.asList(str.split("-")).get(1).trim();
		flag = true;
		break;
	    }
	}
	TestSession.logger.info("starlingVersion - " + starlingVersion);
	if (flag) {
	    version = starlingVersion.trim();
	}
	return version;
    }

    private String getComponentHostName( String componentName ) throws InterruptedException {
	String command = "yinst range -ir \"(@grid_re.clusters." + this.getClusterName() + "." + componentName.trim() +")\"";
	TestSession.logger.info("Command = " + command);
	String componentHostName = this.commonFunctions.executeCommand(command).trim();
	Thread.sleep(100);
	return componentHostName;
    }
}