package hadooptest.gdm.regression.stackIntegration.lib;

import static com.jayway.restassured.RestAssured.given;
import static java.nio.file.Paths.get;
import static java.nio.file.Files.readAllBytes;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.gdm.regression.stackIntegration.StackComponent;
import hadooptest.gdm.regression.stackIntegration.db.DataBaseOperations;
import hadooptest.gdm.regression.stackIntegration.healthCheckUp.GetStackComponentHostName;
import hadooptest.gdm.regression.stackIntegration.healthCheckUp.HBaseHealthCheckUp;
import hadooptest.gdm.regression.stackIntegration.healthCheckUp.HCatalogHealthCheckUp;
import hadooptest.gdm.regression.stackIntegration.healthCheckUp.HiveHealthCheckup;
import hadooptest.gdm.regression.stackIntegration.healthCheckUp.OozieHealthCheckUp;
import hadooptest.gdm.regression.stackIntegration.healthCheckUp.PigHealthCheckup;
import hadooptest.gdm.regression.stackIntegration.healthCheckUp.TezHealthCheckUp;
import hadooptest.gdm.regression.stackIntegration.tests.hbase.TestIntHBase;
import hadooptest.gdm.regression.stackIntegration.tests.hive.TestIntHive;
import hadooptest.gdm.regression.stackIntegration.tests.tez.TestTez;
import hadooptest.gdm.regression.stackIntegration.lib.SystemCommand;

public class CommonFunctions {

	private String clusterName;
	private String nameNodeName;
	private int totalCount;
	private String currentHourMin;
	private String cookie;
	private String errorMessage;
	private String scriptLocation;
	private String pipeLineName;
	private Map<String,StackComponent> healthyStackComponentsMap;
	private Map<String,String> hostsNames;
	private List<String> stackComponentList;
	private ConsoleHandle consoleHandle ;
	private DataBaseOperations dbOperations;
	private static final String TESTCASE_PATH = "/resources/stack_integration";
	private final static String PATH = "/data/daqdev/abf/data/";
	private static final String PROCOTOL = "hdfs://";
	private final int THREAD_POOL_SIZE = 5;
	private static final String HBASE_HOST_NAME = "fsbl106n03.blue.ygrid.yahoo.com";
	private final static String kINIT_COMMAND = "kinit -k -t /homes/dfsload/dfsload.dev.headless.keytab dfsload@DEV.YGRID.YAHOO.COM";
	private final static String HADOOPQA_kINIT_COMMAND = "kinit -k -t /homes/dfsload/dfsload.dev.headless.keytab dfsload@DEV.YGRID.YAHOO.COM";
	public static final String INTEGRATION_JAR="/tmp/integration_test_files/lib/*.jar";
	public static final String PIG_HOME = "/home/y/share/pig";
	public static final String TEZ_HOME = "/home/gs/tez/current/";
	public static final String TEZ_CONF_DIR ="/home/gs/conf/tez/current";
	public static final String TEZ_HEALTH_CHECKUP = "tez_health";
	public static final String TEZ_RESULT = "tez_result";
	public static final String HADOOP_HOME="/home/gs/hadoop/current";

	public CommonFunctions() {
		this.consoleHandle = new ConsoleHandle();
		this.setCookie(this.consoleHandle.httpHandle.getBouncerCookie());
		this.constructCurrentHrMin();
		this.setPipeLineName(GdmUtils.getConfiguration("testconfig.TestWatchForDataDrop.pipeLineName"));
		dbOperations = new DataBaseOperations();
		this.createDB();
	}

	public CommonFunctions(String clusterName) {
		this.clusterName = clusterName;
		this.consoleHandle = new ConsoleHandle();
		this.setCookie(this.consoleHandle.httpHandle.getBouncerCookie());
		this.constructCurrentHrMin();
		this.setPipeLineName(GdmUtils.getConfiguration("testconfig.TestWatchForDataDrop.pipeLineName"));
		dbOperations = new DataBaseOperations();
		this.createDB();
	}
	
	public String getPipeLineName() {
		return pipeLineName;
	}

	public void setPipeLineName(String pipeLineName) {
		this.pipeLineName = pipeLineName;
	}

	public void setCurrentHourMin(String currentHourMin) {
		 this.currentHourMin = currentHourMin;
	}

	public Map<String, StackComponent> getHealthyStackComponentsMap() {
		return this.healthyStackComponentsMap;
	}

	public void setHealthyStackComponentsMap(Map<String, StackComponent> healthyStackComponentsMap) {
		this.healthyStackComponentsMap = healthyStackComponentsMap;
	}

	public Map<String, String> getHostsNames() {
		return hostsNames;
	}

	public void setHostsNames(Map<String, String> hostsNames) {
		this.hostsNames = hostsNames;
	}

	public String getErrorMessage() {
		return errorMessage;
	}

	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}

	public String getScriptLocation() {
		return scriptLocation;
	}

	public void setScriptLocation(String scriptLocation) {
		this.scriptLocation = scriptLocation;
	}

	public String getCookie() {
		return cookie;
	}

	public void setCookie(String cookie) {
		this.cookie = cookie;
	}

	public List<String> getStackComponentList() {
		return stackComponentList;
	}
	public void setStackComponentList(List<String> stackComponentList) {
		this.stackComponentList = stackComponentList;
	}

	public String getNameNodeName() {
		return nameNodeName;
	}

	public void setNameNodeName(String nameNodeName) {
		this.nameNodeName = nameNodeName;
	}

	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	public int getTotalCount() {
		return totalCount;
	}

	public void setTotalCount(int totalCount) {
		this.totalCount = totalCount;
	}
	
	public String getCurrentHourPath() {
		String currentHrPath = null;
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHH");
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
		currentHrPath = "Integration_Testing_DS_" + simpleDateFormat.format(calendar.getTime()) + "00";
		TestSession.logger.info("currentHrPath  = " + currentHrPath);
		return currentHrPath;
	}

	/**
	 * Execute the given command.
	 * @param command
	 * @return
	 */
	public String executeCommand(String command ) {
		String output = null;
		TestSession.logger.info("command - " + command);
		ImmutablePair<Integer, String> result = SystemCommand.runCommand(command);
		if ((result == null) || (result.getLeft() != 0)) {
			if (result != null) {
				// save script output to log
				TestSession.logger.info("Command exit value: " + result.getLeft());
				TestSession.logger.info(result.getRight());
			} else {
				TestSession.logger.error("Failed to execute " + command);
				//this.setErrorMessage(result.getLeft());
				return null;
			}
		} else {
			output = result.getRight();
			TestSession.logger.info("log = " + output);
		}
		return output;
	}

	/**
	 * Returns the remote cluster configuration object.
	 * @param aUser  - user
	 * @param nameNode - name of the cluster namenode. 
	 * @return
	 * @throws IOException 
	 */
	public Configuration getConfForRemoteFS() throws IOException {
		Configuration conf = new Configuration(true);
		String namenodeWithChangedSchemaAndPort = this.PROCOTOL + this.getNameNodeName();
		TestSession.logger.info("For HDFS set the namenode to:[" + namenodeWithChangedSchemaAndPort + "]");
		conf.set("fs.defaultFS", namenodeWithChangedSchemaAndPort);
		conf.set("hadoop.security.authentication", "Kerberos");
		UserGroupInformation.setConfiguration(conf);
		UserGroupInformation.loginUserFromKeytab(HadooptestConstants.UserNames.DFSLOAD, HadooptestConstants.Location.Keytab.DFSLOAD);
		TestSession.logger.info(conf);
		return conf;
	}
	
	public Configuration getNameConfForRemoteFS(String nameNode) throws IOException {
		Configuration conf = new Configuration(true);
		String namenodeWithChangedSchemaAndPort = this.PROCOTOL + nameNode;
		TestSession.logger.info("For HDFS set the namenode to:[" + namenodeWithChangedSchemaAndPort + "]");
		conf.set("fs.defaultFS", namenodeWithChangedSchemaAndPort);
		conf.set("hadoop.security.authentication", "Kerberos");
		UserGroupInformation.setConfiguration(conf);
		UserGroupInformation.loginUserFromKeytab(HadooptestConstants.UserNames.DFSLOAD, HadooptestConstants.Location.Keytab.DFSLOAD);
		TestSession.logger.info(conf);
		return conf;
	}

	public Map<String,String> getAllStackComponentHostNames() throws InterruptedException, ExecutionException {
		Map<String,String> componentHostNames = new HashMap<String,String>();
		Collection<Callable<String>> componentsHostList = new ArrayList<Callable<String>>();
		for ( String component : this.getStackComponentList() ) {
			GetStackComponentHostName obj = new GetStackComponentHostName(this.getClusterName() , component);
			componentsHostList.add(obj);
		}
		ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
		List<Future<String>> executorResultList = executor.invokeAll(componentsHostList);
		for (Future<String> result : executorResultList) {
			List<String> value = Arrays.asList(result.get().split("~"));
			String componentName = value.get(0).trim();
			String hostName = value.get(1).trim();
			componentHostNames.put(componentName, hostName);
			TestSession.logger.info("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!   componentName  =  " + componentName  +   "     hostName  = " + hostName);
			if (componentName.indexOf("namenode") > 0) {
				TestSession.logger.info("!!!!!!!!!   found   !!!!!!!!!!!!!!!!!!!   componentName  =  " + componentName  +   "     hostName  = " + hostName);
				this.setNameNodeName(hostName);
			}
		}
		executor.shutdown();
		return componentHostNames;
	}

	public void checkClusterHealth() throws InterruptedException, ExecutionException {
		
		// insert current dataSetName into the db
		this.dbOperations.insertDataSetName(this.getCurrentHourPath());
		
		Map<String,StackComponent>healthyStackComponentsMap = this.getStackComponentHealthCheckUp();
		setHealthyStackComponentsMap(healthyStackComponentsMap);
	}

	public void preInit() {
		ModifyStackComponentsScripts modifyStackComponentsScripts = new ModifyStackComponentsScripts(this.getNameNodeName() , PATH + this.getCurrentHourPath() );
		try {
			modifyStackComponentsScripts.execute();
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
	}
	
	public void initComponents() {
		List<Callable<Boolean>> initComponentsList = new ArrayList<Callable<Boolean>>();
		for ( String key : this.getHealthyStackComponentsMap().keySet()) {
			TestSession.logger.info(key);
			StackComponent stackComponent = healthyStackComponentsMap.get(key);
			Callable<Boolean> componentInit = new InitComponents(stackComponent , this.TESTCASE_PATH);
			initComponentsList.add(componentInit);
			TestSession.logger.info(stackComponent.getStackComponentName());
		}

		TestSession.logger.info("component Size = " + initComponentsList.size());
		ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
		try {
			List<Future<Boolean>> executorResultList = executor.invokeAll(initComponentsList);
			for (Future<Boolean> result : executorResultList) {
				try {
					boolean flag = result.get();
					TestSession.logger.info("flag = " + flag);
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		executor.shutdown();
	}

	public void testStackComponent() throws InterruptedException, ExecutionException {
		List<Callable<String>> testList = new ArrayList<Callable<String>>();
		Map<String,String> hostsNames = this.getAllStackComponentHostNames();
		String nNodeName = hostsNames.get("namenode") ;
		Map<String, StackComponent> stackComponentMap = this.getHealthyStackComponentsMap();
		TestSession.logger.info("stackComponentMap size - " + stackComponentMap.size()  + "   \n " + stackComponentMap.toString());
					
		StackComponent tezStackComponent = stackComponentMap.get("tez");
		Callable<String> testTezComponent = null;
		if (tezStackComponent != null ) {
			TestSession.logger.info("nNodeName  = " + nNodeName  +  "  tezStackComponent = " + tezStackComponent.toString());
			testTezComponent = new  TestTez(tezStackComponent ,tezStackComponent.getHostName() ,  nNodeName ,  this.getCurrentHourPath());
			testList.add(testTezComponent);
		}
	
		StackComponent hiveStackComponent = stackComponentMap.get("hive");
		Callable<String> testIntHive = null;
		if (hiveStackComponent != null) {
			testIntHive = new TestIntHive(hiveStackComponent , hiveStackComponent.getHostName(), nNodeName , hiveStackComponent.getScriptLocation());
			testList.add(testIntHive);	
		}
		
		StackComponent hbaseStackComponent = stackComponentMap.get("hbase");
		Callable<String> testHbaseComponent = null;
		if (hbaseStackComponent != null)  {
			TestSession.logger.info("hbase hostname  = " + hbaseStackComponent.getHostName()  +  "  hbaseStackComponent = " + hbaseStackComponent.toString() + "  script location = " + hbaseStackComponent.getScriptLocation());
			testHbaseComponent = new TestIntHBase(hbaseStackComponent , nNodeName);
			testList.add(testHbaseComponent);
		}
		
		String overAllResult = "PASS";
		
		if (testList.size() > 0) {
			ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
			List<Future<String>> testExecutionList = executor.invokeAll(testList);
			for ( Future<String> result : testExecutionList) {
				List<String> testExecutionResult = Arrays.asList(result.get().split("-"));
				if (testExecutionResult.size() > 0) {
					TestSession.logger.info("result - " + result.get());
					if (testExecutionResult.get(1).equals("false") == true) {
						overAllResult = "FAIL";
						TestSession.logger.info(testExecutionResult.get(1) + " stack component failed.");
						break;
					}	
				}
			}
			executor.shutdown();
		}
		
		this.updateDB(this.getCurrentHourPath().trim(), "result", overAllResult);
		
	}

	public Map<String,StackComponent> getStackComponentHealthCheckUp() throws InterruptedException, ExecutionException {

		/**
		 * TODO : Get hadoop version , GDM version.
		 */

		// TODO : use this.getHostsNames method
		Map<String,String> hostsNames = this.getAllStackComponentHostNames();
		this.setHostsNames(hostsNames);

		String nn = hostsNames.get("namenode");
		this.setNameNodeName(nn);
		Map<String, StackComponent> healthyStackComponentsMap = new HashMap<>();
		List<Callable<StackComponent>> healthCheckList = new ArrayList<Callable<StackComponent>>();
		Callable tezHealthCheckUpObj = new TezHealthCheckUp(hostsNames.get("gateway"));
		Callable pigHealthCheckupObj = new PigHealthCheckup(hostsNames.get("gateway"));
		Callable hiveHealthCheckupObj = new HiveHealthCheckup(hostsNames.get("hive"));
		Callable hCatalogHealthCheckUpObj = new HCatalogHealthCheckUp(hostsNames.get("hive"));
		Callable hBaseHealthCheckUpObj = new HBaseHealthCheckUp();
		Callable oOzieHealthCheckUpObj = new OozieHealthCheckUp(hostsNames.get("oozie"));
		healthCheckList.add(tezHealthCheckUpObj);
		healthCheckList.add(pigHealthCheckupObj);
		healthCheckList.add(hiveHealthCheckupObj);
		healthCheckList.add(hCatalogHealthCheckUpObj);
		healthCheckList.add(hBaseHealthCheckUpObj);
		healthCheckList.add(oOzieHealthCheckUpObj);
		ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
		List<Future<StackComponent>> healthCheckUpResultList = executor.invokeAll(healthCheckList);
		for ( Future<StackComponent> result : healthCheckUpResultList) {
			StackComponent obj = result.get();
			if ( obj.getHealth() == true){
				healthyStackComponentsMap.put(obj.getStackComponentName(), obj);
			} else if ( obj.getHealth() == false) {
				TestSession.logger.info("component Name = " + obj.getStackComponentName() + "   status - " + obj.getHealth()  + "  version =  " + obj.getStackComponentVersion() + "  error = " + obj.getErrorString());
			}
			TestSession.logger.info("component Name = " + obj.getStackComponentName() + "   status - " + obj.getHealth()  + "  version =  " + obj.getStackComponentVersion());
		}
		executor.shutdown();

		// navigate healthyStackComponentsMap
		StringBuilder resultBuilder = new StringBuilder();
		for ( String key : healthyStackComponentsMap.keySet()){
			TestSession.logger.info(key);
			StackComponent obj = healthyStackComponentsMap.get(key);
			String componentName = obj.getStackComponentName();
			TestSession.logger.info("component Name = " + obj.getStackComponentName()  + "  health status = " + obj.getHealth()  + "    version = " + obj.getStackComponentVersion());
		}
		TestSession.logger.info("____________________________________________________________________________________________________");
		return healthyStackComponentsMap;
	}

	public  boolean isJarFileExist(String hostName) {
		boolean mkdirFlag = false ,scpFlag = false;
		String errorMessage = null;
		if (checkWhetherJarFileExistsOnTezHost(hostName) == false) {
			String  absolutePath = new File("").getAbsolutePath();
			TestSession.logger.info("There is no folder, so creating the folder.");
			String currentHrMin = this.getCurrentHourPath();
			String mkdirCommand = "ssh " + hostName + "  \"mkdir -p /tmp/integration_test_files/lib/\" | echo $?";
			String result = this.executeCommand(mkdirCommand);
			if (result != null) {
				TestSession.logger.info("mkdir result  = " + result);
				mkdirFlag = true;
				TestSession.logger.info("Copying jar files...");
				String scpCommand = "  scp " + absolutePath + "/resources/stack_integration/lib/*.jar "  +  hostName + ":/tmp/integration_test_files/lib/"  + " |  echo $?";
				result = this.executeCommand(scpCommand);
				if ( result != null ) {
					if (checkWhetherJarFileExistsOnTezHost(hostName) == true ) {
						if (this.getTotalCount() > 0) {
							TestSession.logger.info("Jar files copied successfully..");
							scpFlag = true;		
						}
					}
				}
			}
		} else {
			mkdirFlag = true;
			scpFlag = true;
		}
		TestSession.logger.info("mkdirFlag = " + mkdirFlag  + "  scpFlag = " + scpFlag);
		return mkdirFlag && scpFlag;
	}

	public String getCurrentHrMin() {
		return this.currentHourMin;
	}
	
	public void constructCurrentHrMin() {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmm");
		simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
		Calendar calendar = Calendar.getInstance();
		this.setCurrentHourMin(simpleDateFormat.format(calendar.getTime()));
	}
	

	/*
	 *  Check if FETLProjector.jar & BaseFeed.jar files exists i,e total count of files in the directory should be 2
	 */
	public boolean checkWhetherJarFileExistsOnTezHost(String hostName) {
		boolean flag = false;
		String command = "ssh "  + hostName  + "  \"ls -ltr /tmp/integration_test_files/lib/\"";
		String result = this.executeCommand(command);
		if (result == null) {
			flag = false;
			TestSession.logger.info("Error Message = " + this.getErrorMessage());
		} else if (result != null ){
			TestSession.logger.info("--------------------------------------");
			java.util.List<String> resultList = Arrays.asList(result.split("\n"));
			TestSession.logger.info(resultList.toString());
			int index = 0;
			for ( String str : resultList) {
				if ( str.indexOf("total") > -1) {
					break;
				}
				index++;
			}
			java.util.List<String>tempList = Arrays.asList(resultList.get(index).split(" "));
			int total = Integer.parseInt(tempList.get(tempList.size() - 1));
			TestSession.logger.info("total - " + total);
			this.setTotalCount(total);
			flag = true;
			TestSession.logger.info("--------------------------------------");
		}
		return flag;
	}

	public boolean copyTestCases(String componentName , String testCasesPath , String hostName) {
		boolean isTestCaseCopiedFlag = false , mkdirFlag = false;
		String  absolutePath = new File("").getAbsolutePath();
		File componentFile = new File(absolutePath + testCasesPath + File.separator + componentName);
		if (componentFile.exists()) {
			// create the testcase folder for the current hr and min
			SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmm");
			simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
			Calendar calendar = Calendar.getInstance();
			String folderName = "/tmp/integration-testing/" + componentName  + File.separator +  simpleDateFormat.format(calendar.getTime());
			String mkdirCommand = "ssh " +  hostName  + "  mkdir -p " + folderName  + "  | echo $?";
			String mkDirCommandExecResult  = this.executeCommand(mkdirCommand);
			if (mkDirCommandExecResult != null) {
				mkdirFlag = true;
				TestSession.logger.info(folderName + "  created successfully on " + hostName);
				String scpCommand = "scp " + componentFile.toString() +  File.separator + "*" + "  "  + hostName + ":" + folderName;
				String scpCommandResutlt = this.executeCommand(scpCommand);
				if (scpCommandResutlt != null) {
					isTestCaseCopiedFlag = true;
					this.setScriptLocation(folderName);
					TestSession.logger.info(componentFile.toString() + "  copied successfully on " + hostName);
				}
			}
		} else {
			TestSession.logger.error(componentFile.toString() + " path does not exists. Please check whether you have specified testcase path.");
		}
		return mkdirFlag && isTestCaseCopiedFlag;
	}

	public String getKinitCommand() {
		String kinitCommand = HADOOPQA_kINIT_COMMAND;
		TestSession.logger.info("kinit command - " + kinitCommand);
		return kinitCommand;
	}

	public String getPathCommand() {
		String pathCommand = "export PATH=$PATH:" + HADOOP_HOME + ":" +PIG_HOME + ":" + TEZ_HOME + "/bin" + ":" + INTEGRATION_JAR + ":" + TEZ_HOME + ":" +  TEZ_CONF_DIR;
		TestSession.logger.info("export path value - " + pathCommand);
		return pathCommand.trim();
	}

	public String getTezPathCommand() {
		return this.getPathCommand();
	}

	public String getPigPathCommand() {
		return this.getPathCommand();
	}
	
	public void createDB(){
		try {
			this.dbOperations.createDB();
			this.dbOperations.createIntegrationResultTable();
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
	}
	
	public synchronized void updateDB(String dataSetName, String columnName , String columnValue) {
		DataBaseOperations dbOperations = new DataBaseOperations();
		if (dbOperations != null) {
			dbOperations.insertComponentTestResult(dataSetName, columnName , columnValue);
		} else {
			TestSession.logger.error("Failed to create an instance of DataBaseOperations.");
		}
	}
}