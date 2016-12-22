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

import javax.mail.MessagingException;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.gdm.regression.stackIntegration.StackComponent;
import hadooptest.gdm.regression.stackIntegration.db.AggIntResult;
import hadooptest.gdm.regression.stackIntegration.db.StackComponentAggResult;
import hadooptest.gdm.regression.stackIntegration.db.DBCommands;
import hadooptest.gdm.regression.stackIntegration.db.DataBaseOperations;
import hadooptest.gdm.regression.stackIntegration.healthCheckUp.GDMHealthCheckUp;
import hadooptest.gdm.regression.stackIntegration.healthCheckUp.GetStackComponentHostName;
import hadooptest.gdm.regression.stackIntegration.healthCheckUp.HBaseHealthCheckUp;
import hadooptest.gdm.regression.stackIntegration.healthCheckUp.HCatalogHealthCheckUp;
import hadooptest.gdm.regression.stackIntegration.healthCheckUp.HadoopHealthCheckup;
import hadooptest.gdm.regression.stackIntegration.healthCheckUp.HiveHealthCheckup;
import hadooptest.gdm.regression.stackIntegration.healthCheckUp.OozieHealthCheckUp;
import hadooptest.gdm.regression.stackIntegration.healthCheckUp.PigHealthCheckup;
import hadooptest.gdm.regression.stackIntegration.healthCheckUp.TezHealthCheckUp;
import hadooptest.gdm.regression.stackIntegration.tests.hbase.TestIntHBase;
import hadooptest.gdm.regression.stackIntegration.tests.hive.TestIntHive;
import hadooptest.gdm.regression.stackIntegration.tests.oozie.TestIntOozie;
import hadooptest.gdm.regression.stackIntegration.tests.pig.TestPig;
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
	private static String dataSetName;
	private Map<String,StackComponent> healthyStackComponentsMap;
	private Map<String,String> hostsNames;
	private List<String> stackComponentList;
	private List<String> currentStackComponentTestList;
	private ConsoleHandle consoleHandle ;
	private DataBaseOperations dbOperations;
	public static final String TESTCASE_PATH = "/resources/stack_integration";
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
	}

	public CommonFunctions(String clusterName) {
		this.setClusterName(clusterName);
		//this.clusterName = clusterName;
		this.consoleHandle = new ConsoleHandle();
		this.setCookie(this.consoleHandle.httpHandle.getBouncerCookie());
		this.constructCurrentHrMin();
		this.setPipeLineName(GdmUtils.getConfiguration("testconfig.TestWatchForDataDrop.pipeLineName"));
		dbOperations = new DataBaseOperations();
	}

	public List<String> getCurrentStackComponentTestList() {
		return currentStackComponentTestList;
	}

	public void setCurrentStackComponentTestList(List<String> currentStackComponentTestList) {
		this.currentStackComponentTestList = currentStackComponentTestList;
	}

	public String getDataSetName() {
		TestSession.logger.info("```````````````````````````````````````  getDataSetName( ) ```````````````````````````````````````````````" + dataSetName);
		return dataSetName;
	}

	public void setDataSetName(String dataSetName) {
		this.dataSetName = dataSetName;
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
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
		String cName = this.getClusterName();
		currentHrPath = cName + "_Integration_Testing_DS_" + simpleDateFormat.format(calendar.getTime()) + "00";
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

	public void checkClusterHealth( ) throws InterruptedException, ExecutionException {
		
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
		String currentHrPath = simpleDateFormat.format(calendar.getTime());
		
		// insert current dataSetName into the db
		this.dbOperations.insertDataSetName(this.getDataSetName() , currentHrPath);
		
		Map<String,StackComponent>healthyStackComponentsMap = this.getStackComponentHealthCheckUp();
		setHealthyStackComponentsMap(healthyStackComponentsMap);

                // set the individual run's start date and time
                java.text.SimpleDateFormat sdfStartDateTime = new java.text.SimpleDateFormat("yyyyMMddhhmmss");
                String currentStartDateTime = sdfStartDateTime.format(calendar.getTime());

                TestSession.logger.info("GRIDCI-1667, populate startDateTime for this iteration pass, startDateTime is: " + 
		  currentStartDateTime);
                this.updateDB(this.getDataSetName(), "startDateTime", currentStartDateTime);
	}

	public void preInit() {
		try {
			Map<String,String> hostsNames = this.getAllStackComponentHostNames();
			ModifyStackComponentsScripts modifyStackComponentsScripts = new ModifyStackComponentsScripts(this.getNameNodeName() ,hostsNames.get("jobTracker") , hostsNames.get("oozie") , 
					PATH + this.getCurrentHourPath() , this.getClusterName());
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
		
		
		List<String> currentStackTestComponent = this.getCurrentStackComponentTestList();
		
		if (currentStackTestComponent.contains("tez")) {
			StackComponent tezStackComponent = stackComponentMap.get("tez");
			Callable<String> testTezComponent = null;
			if (tezStackComponent != null ) {
				TestSession.logger.info("nNodeName  = " + nNodeName  +  "  tezStackComponent = " + tezStackComponent.toString());
				testTezComponent = new  TestTez(tezStackComponent ,tezStackComponent.getHostName() ,  nNodeName ,  this.getCurrentHourPath());
				testList.add(testTezComponent);
			}
		}
		
		if (currentStackTestComponent.contains("pig")) {
			StackComponent tezStackComponent = stackComponentMap.get("pig");
			Callable<String> testPigComponent = null;
			if (tezStackComponent != null ) {
				TestSession.logger.info("nNodeName  = " + nNodeName  +  "  pigStackComponent = " + tezStackComponent.toString());
				testPigComponent = new  TestPig(tezStackComponent,tezStackComponent.getHostName(),nNodeName,this.getCurrentHourPath() );
				testList.add(testPigComponent);
			}
		}
		
		if (currentStackTestComponent.contains("hive")) {
			StackComponent hiveStackComponent = stackComponentMap.get("hive");
			Callable<String> testIntHive = null;
			if (hiveStackComponent != null) {
				testIntHive = new TestIntHive(hiveStackComponent , hiveStackComponent.getHostName(), nNodeName , hiveStackComponent.getScriptLocation() , this.getClusterName());
				testList.add(testIntHive);	
			}
		}
			
		if (currentStackTestComponent.contains("hbase")) {
			StackComponent hbaseStackComponent = stackComponentMap.get("hbase");
			Callable<String> testHbaseComponent = null;
			if (hbaseStackComponent != null)  {
				TestSession.logger.info("hbase hostname  = " + hbaseStackComponent.getHostName()  +  "  hbaseStackComponent = " + hbaseStackComponent.toString() + "  script location = " + hbaseStackComponent.getScriptLocation());
				testHbaseComponent = new TestIntHBase(hbaseStackComponent , nNodeName);
				testList.add(testHbaseComponent);
			}			
		}
		
		if (currentStackTestComponent.contains("oozie")) {
			StackComponent oozieStackComponent = stackComponentMap.get("oozie");
			Callable<String> testIntOozie = null;
			if (oozieStackComponent != null) {
				try {
					org.apache.hadoop.conf.Configuration configuration = this.getNameConfForRemoteFS(nNodeName);
					testIntOozie = new TestIntOozie(oozieStackComponent , oozieStackComponent.getHostName() , configuration);
					testList.add(testIntOozie);
				} catch (IOException e) {
					TestSession.logger.error("Failed to create configuration " + e);
					e.printStackTrace();
				}
			}
		}
				
		boolean overAllExecutionResult = true; 
		if (testList.size() > 0) {
			ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
			List<Future<String>> testExecutionList = executor.invokeAll(testList);
			for ( Future<String> result : testExecutionList) {
				List<String> testExecutionResult = Arrays.asList(result.get().split("-"));
				if (testExecutionResult.size() > 0) {
					if (testExecutionResult.get(1).equals("false") == true) {
						overAllExecutionResult = false;
						TestSession.logger.info(testExecutionResult.get(1) + " stack component failed.");
						break;
					}
				}
			}
			executor.shutdown();
		}
		
		// navigate the health of the stack component and check whether any one of the component is down. If down mark the testcase as fail.
		boolean isHealth = true;
		for ( String key : stackComponentMap.keySet()) {
			StackComponent scomponent = stackComponentMap.get(key);
			boolean flag = scomponent.getHealth();
			// if one of the component health is bad , then mark the execution test as failed.
			if (flag == false) {
				isHealth = false;
			}
		}
		
		String overAllResult = null;
		if ( ( isHealth == true) && (overAllExecutionResult == true) ) {
			overAllResult = "PASS";
		} else  {
			overAllResult = "FAIL";
		}
		this.updateDB(this.getDataSetName(), "result", overAllResult);

                // set the individual run's end date and time
                Calendar calendar = Calendar.getInstance();
                calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
                java.text.SimpleDateFormat sdfEndDateTime = new java.text.SimpleDateFormat("yyyyMMddhhmmss");
                String currentEndDateTime = sdfEndDateTime.format(calendar.getTime());

                TestSession.logger.info("GRIDCI-1667, populate endDateTime for this iteration pass, endDateTime is: " + currentEndDateTime);
		this.updateDB(this.getDataSetName(), "endDateTime", currentEndDateTime);

                // set the interative run's uniqueId
                String uniqueId = "This_Is_My_Unique_ID_1234";
                TestSession.logger.info("GRIDCI-1667, populate uniqueId for interative run, uniqueId is: " +
                        uniqueId);
                this.updateDB(this.getDataSetName(), "uniqueId", uniqueId);

                // set the total run's uniqueId 
                String uniqueId = "This_Is_My_Unique_ID_1234";
                TestSession.logger.info("GRIDCI-1667, populate uniqueId for total run, uniqueId is: " +
                        uniqueId);
                TestSession.logger.info("GRIDCI-1667, tmpStr is: " + tmpStr);
		String tmpStr = dataSetName.substring(0, (dataSetName.length() - 4));
                this.updateDB(true, tmpStr, "uniqueId", uniqueId);

		if ( (this.getPipeLineName().indexOf("hadoop") > -1) == true || (this.getPipeLineName().indexOf("tez") > -1) == true)  {
			AggIntResult aggIntResultObj = new AggIntResult();
			aggIntResultObj.finalResult();
			SendIntegrationResultMail obj = new SendIntegrationResultMail();
			try {
				obj.sendMail();
			} catch (ClassNotFoundException | InstantiationException | IllegalAccessException | SQLException
					| MessagingException e) {
				e.printStackTrace();
			}
		} else {
			StackComponentAggResult stackComponentAggResultObj = new StackComponentAggResult();
			stackComponentAggResultObj.test();
		}
	}
	
	public Map<String,StackComponent> getStackComponentHealthCheckUp() throws InterruptedException, ExecutionException {
		StringBuilder unTestedComponentListString = new StringBuilder();
		Map<String,String> hostsNames = this.getAllStackComponentHostNames();
		this.setHostsNames(hostsNames);
		String nn = hostsNames.get("namenode");
		this.setNameNodeName(nn);
		Map<String, StackComponent> healthyStackComponentsMap = new HashMap<>();
		List<Callable<StackComponent>> healthCheckList = new ArrayList<Callable<StackComponent>>();
		List<String> currentTestComponentList = this.getCurrentStackComponentTestList();
		if ( currentTestComponentList == null || currentTestComponentList.size() == 0) {
			try {
				throw new Exception("Please specify the test component List..!");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		boolean gdmFlag = false, hadoopFlag = false, tezFlag = false, pigFlag = false, hiveFlag=false, hcatalogFlag = false, hbaseFlag = false, oozieFlag = false;
		 
		if (currentTestComponentList.contains("gdm")) {
			Callable gdmHealthCheckUpObj = new GDMHealthCheckUp();
			healthCheckList.add(gdmHealthCheckUpObj);
			gdmFlag = true;
		} else {
			unTestedComponentListString.append("gdm").append(",");
			this.updateDB(this.getDataSetName(), "gdmResult", "SKIPPED");
		}
		
		if (currentTestComponentList.contains("hadoop")) {
			Callable hadoopHealthCheckupObj = new HadoopHealthCheckup(hostsNames.get("namenode"));
			healthCheckList.add(hadoopHealthCheckupObj);
			hadoopFlag = true;
		}  else {
			unTestedComponentListString.append("hadoop").append(",");
			this.updateDB(this.getDataSetName(), "hadoopResult", "SKIPPED");
		}
		
		if (currentTestComponentList.contains("tez")) {
			Callable tezHealthCheckUpObj = new TezHealthCheckUp(hostsNames.get("gateway"));	
			healthCheckList.add(tezHealthCheckUpObj);
			tezFlag = true;
		} else {
			unTestedComponentListString.append("tez").append(",");
			this.updateDB(this.getDataSetName(), "tezResult", "SKIPPED");
		}
		
		if (currentTestComponentList.contains("pig")) {
			Callable pigHealthCheckupObj = new PigHealthCheckup(hostsNames.get("gateway"));
			healthCheckList.add(pigHealthCheckupObj);
			pigFlag = true;
		}else {
			// since tez is executed using pig, so skipping this
			//unTestedComponentListString.append("pig").append(",");
			;	
		}
		
		if (currentTestComponentList.contains("hive")) {
			Callable hiveHealthCheckupObj = new HiveHealthCheckup(hostsNames.get("hive"));
			healthCheckList.add(hiveHealthCheckupObj);
			hiveFlag = true;
		}else {
			unTestedComponentListString.append("hive").append(",");
			this.updateDB(this.getDataSetName(), "hiveResult", "SKIPPED");
		}
		
		if (currentTestComponentList.contains("hcat")) {
			Callable hCatalogHealthCheckUpObj = new HCatalogHealthCheckUp(hostsNames.get("hive"));
			healthCheckList.add(hCatalogHealthCheckUpObj);
			hcatalogFlag = true;
		} else {
			unTestedComponentListString.append("hcat").append(",");
			this.updateDB(this.getDataSetName(), "hcatResult", "SKIPPED");
		}
		
		if (currentTestComponentList.contains("hbase")) {
			Callable hBaseHealthCheckUpObj = new HBaseHealthCheckUp();
			healthCheckList.add(hBaseHealthCheckUpObj);
			hbaseFlag = true;
		} else {
			unTestedComponentListString.append("hbase").append(",");
			this.updateDB(this.getDataSetName(), "hbaseResult", "SKIPPED");
		}
		
		if (currentTestComponentList.contains("oozie")) {
			Callable oozieHealthCheckUpObj = new OozieHealthCheckUp(hostsNames.get("oozie"));
			healthCheckList.add(oozieHealthCheckUpObj);
			oozieFlag = true;
		} else {
			unTestedComponentListString.append("oozie");
			this.updateDB(this.getDataSetName(), "oozieResult", "SKIPPED");
		}
		
		ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
		List<Future<StackComponent>> healthCheckUpResultList = executor.invokeAll(healthCheckList);
		for ( Future<StackComponent> result : healthCheckUpResultList) {
			StackComponent obj = result.get();
			if ( obj.getHealth() == true) {
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
		
		// mark the individual components are not tested, since the user has not want them to test.
		if (gdmFlag == false) {
			this.updateDB(this.getDataSetName(), "gdmComments", "SKIPPED");
			
			// since gdm uses hadoop
			this.updateDB(this.getDataSetName(), "hadoopComments", "SKIPPED");
		}

		if (pigFlag == false) {
			this.updateDB(this.getDataSetName(), "pigComments", "SKIPPED");
		}
		if (tezFlag == false) {
			this.updateDB(this.getDataSetName(), "tezComments", "SKIPPED");
		}
		if (hiveFlag == false) {
			this.updateDB(this.getDataSetName(), "hiveComment", "SKIPPED");
		}
		if (hcatalogFlag == false) {
			this.updateDB(this.getDataSetName(), "hcatComment", "SKIPPED");
		}
		if (hbaseFlag == false) {
			this.updateDB(this.getDataSetName(), "hbaseComment", "SKIPPED");
		}
		if (oozieFlag == false) {
			this.updateDB(this.getDataSetName(), "oozieComments", "SKIPPED");
		}
		
		if (unTestedComponentListString.length() > 1) {
			this.updateDB(this.getDataSetName(), "comments", unTestedComponentListString.toString() + "  SKIPPED" );
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
			String mkdirCommand = "ssh " + hostName + "  \"mkdir -p /tmp/integration_test_files/lib/ | echo $? \"";
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
		} else if (result != null ) {
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

	public synchronized boolean copyTestCases(String componentName , String testCasesPath , String hostName) {
		TestSession.logger.info("-----copyTestCases-----" + componentName);
		boolean isTestCaseCopiedFlag = false , mkdirFlag = false;
		String componentFolder = null , mkDirCommand = null, scpCommand = null;
		String  absolutePath = new File("").getAbsolutePath();
		File componentFile = new File(absolutePath + testCasesPath + File.separator + componentName);
		if (componentFile.exists()) {
			this.getCurrentHrMin();
			// create the testcase folder for the current hr and min
			SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHH");
			simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
			Calendar calendar = Calendar.getInstance();
			String folderName = "/tmp/integration-testing/" + componentName;
			String rmDirCommand = " rm -rf " + folderName;
			if (componentName.indexOf("oozie") > -1) {
				TestSession.logger.info("*****creating_Oozie_Folder****");
				componentFolder = folderName  + File.separator +  simpleDateFormat.format(calendar.getTime());
				mkDirCommand = " mkdir -p " + componentFolder + "/outputDir/" + "  | echo $?";
			} else {
				componentFolder = folderName  + File.separator +  simpleDateFormat.format(calendar.getTime());
				mkDirCommand = " mkdir -p " + componentFolder  + "  | echo $?";
			}
			String command = "ssh " +  hostName + "  \"" +  rmDirCommand + ";" + mkDirCommand + "\"" ;
			TestSession.logger.info("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
			String mkDirCommandExecResult  = this.executeCommand(command);
			TestSession.logger.info("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
			if (mkDirCommandExecResult != null) {
				mkdirFlag = true;
				TestSession.logger.info(componentFolder + "  created successfully on " + hostName);
				scpCommand = "scp " + componentFile.toString() +  File.separator + "*" + "  "  + hostName + ":" + componentFolder;
				String scpCommandResutlt = this.executeCommand(scpCommand);
				if (scpCommandResutlt != null) {
					isTestCaseCopiedFlag = true;
					this.setScriptLocation(componentFolder);
					TestSession.logger.info(componentFile.toString() + "  copied successfully on " + hostName);
				}
			}
			TestSession.logger.info("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
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
			String tableName = DBCommands.CREATE_INTEGRATION_TABLE;
			tableName = tableName.replaceAll("TB_NAME", DBCommands.FINAL_RESULT_TABLE_NAME);
			TestSession.logger.info("tableName = " + tableName);
			this.dbOperations.createTable(tableName);
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

        public synchronized void updateDB(Boolean isFinalTable, String dataSetName, String columnName , String columnValue) {
                DataBaseOperations dbOperations = new DataBaseOperations();
                if (dbOperations != null) {
                        dbOperations.insertComponentTestResult(true, dataSetName, columnName , columnValue);
                } else {
                        TestSession.logger.error("Failed to create an instance of DataBaseOperations.");
                }
        }

}
