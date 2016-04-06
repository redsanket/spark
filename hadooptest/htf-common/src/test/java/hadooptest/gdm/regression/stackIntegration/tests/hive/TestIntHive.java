package hadooptest.gdm.regression.stackIntegration.tests.hive;

import hadooptest.TestSession;
import hadooptest.gdm.regression.stackIntegration.StackComponent;
import hadooptest.gdm.regression.stackIntegration.lib.CommonFunctions;

public class TestIntHive implements java.util.concurrent.Callable<String>{
	
	private StackComponent stackComponent;
	private CommonFunctions commonFunctions;
	private String hostName;
	private String scriptLocation;
	private String dataSetName;
	private String hiveInitCommand;
	private String nameNodeName;
	private final static String HADOOP_HOME="export HADOOP_HOME=/home/gs/hadoop/current";
	private final static String JAVA_HOME="export JAVA_HOME=/home/gs/java/jdk64/current/";
	private final static String HADOOP_CONF_DIR="export HADOOP_CONF_DIR=/home/gs/conf/current";
	private final static String DFSLOAD_KNITI = "kinit -k -t /homes/dfsload/dfsload.dev.headless.keytab dfsload@DEV.YGRID.YAHOO.COM";
	private final static String HADOOPQA_KNITI = "kinit -k -t /homes/hadoopqa/hadoopqa.dev.headless.keytab hadoopqa@DEV.YGRID.YAHOO.COM";
	private final static String HIVE_VERSION_COMMAND = "hive --version";
	public static final String PIG_HOME = "export PIG_HOME=/home/y/share/pig";
	private final static String PATH_COMMAND = "export PATH=$PATH:";

	public TestIntHive(StackComponent stackComponent , String hostName , String nameNodeName , String dataSetName) {
		this.stackComponent = stackComponent;
		TestSession.logger.info("component name - " + this.stackComponent.getStackComponentName() +
				"  hostname - " + this.stackComponent.getHostName()
				+ "  dataSetName = " + this.stackComponent.getScriptLocation() 
				+ " nameNodeName =  " + nameNodeName);
		this.commonFunctions = new CommonFunctions();
		this.setHostName(hostName);
		this.setDataSetName(dataSetName);
		this.setNameNodeName(nameNodeName);
	}
	
	public String getNameNodeName() {
		return nameNodeName;
	}

	public void setNameNodeName(String nameNodeName) {
		this.nameNodeName = nameNodeName;
	}

	public String getScriptLocation() {
		return scriptLocation;
	}

	public void setScriptLocation(String scriptLocation) {
		this.scriptLocation = scriptLocation;
	}
	
	public String getHostName() {
		return hostName;
	}
	
	public void constructCommand() {
		this.hiveInitCommand = "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null  " +  this.getHostName() + "  \"" +HADOOP_HOME + ";" + JAVA_HOME + ";" +  HADOOP_CONF_DIR + ";"  + HADOOPQA_KNITI  + ";" ;
	}
	
	public String getInitCommand() {
		return this.hiveInitCommand;
	}
	
	public void setHostName(String hostName) {
		this.hostName = hostName;
	}

	public String getDataSetName() {
		return dataSetName;
	}

	public void setDataSetName(String dataSetName) {
		this.dataSetName = dataSetName;
	}
	
	@Override
	public String call() throws Exception {
		CommonFunctions commonFunctions = new CommonFunctions();
		commonFunctions.updateDB(this.getDataSetName() , "hiveCurrentState" , "STARTED");
		TestSession.logger.info("--------------------------------------------------------------- TestIntHive  start ------------------------------------------------------------------------");
		boolean isTableDropped = false , isTableCreated = false ,  isDataCopied = false , isDataInserted = false , isDataFetchedUsingHCatalog = false;
		String hiveInitCommand = "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null  " +  this.stackComponent.getHostName() + "  \"" +HADOOP_HOME + ";" + JAVA_HOME + ";" +  HADOOP_CONF_DIR + ";"  + HADOOPQA_KNITI  + ";" ;
		
		
		String dataSetName = this.commonFunctions.getCurrentHourPath();
		commonFunctions.updateDB(dataSetName , "hiveCurrentState" , "RUNNING");

		TestDropHiveTable testDropHiveTable = new TestDropHiveTable( hiveInitCommand + "," +   this.stackComponent.getScriptLocation());
		TestCreateHiveTable testCreateHiveTable = new TestCreateHiveTable(hiveInitCommand + "," +   this.stackComponent.getScriptLocation());
		TestCopyDataToHive testCopyDataToHive = new TestCopyDataToHive(this.stackComponent ,  this.stackComponent.getHostName() , this.getNameNodeName() ,  this.getDataSetName() ,hiveInitCommand );
		
		if ((isTableDropped = testDropHiveTable.execute()) == true) {
			
			TestSession.logger.info("Hive table dropped successfully");
			commonFunctions.updateDB(dataSetName , "hiveDropTable" , "PASS");
			
			if ((isTableCreated = testCreateHiveTable.execute()) == true) {
				
				TestSession.logger.info("Hive table created successfully");
				commonFunctions.updateDB(dataSetName , "hiveCreateTable" , "PASS");
				
				if ((isDataCopied = testCopyDataToHive.execute()) == true) {
					
					TestSession.logger.info("coping hive data  successfully");
					String currentHiveDataPath = testCopyDataToHive.getCurrentHivePath();
					TestSession.logger.info("currentHiveDataPath  = " + currentHiveDataPath);
					TestLoadDataToHiveTable testLoadDataToHiveTable = new TestLoadDataToHiveTable(this.stackComponent , hiveInitCommand, currentHiveDataPath); 
					isDataInserted =  testLoadDataToHiveTable.execute();
					if (isDataInserted == true) {
						TestSession.logger.info("Record inserted into the table successfully...!");
						commonFunctions.updateDB(dataSetName , "hiveCurrentState" , "COMPLETED");
						commonFunctions.updateDB(dataSetName , "hiveResult" , "PASS");
						TestHCatalog testHCatalog =  new TestHCatalog(hiveInitCommand ,  this.stackComponent.getScriptLocation());
						isDataFetchedUsingHCatalog = testHCatalog.execute();
						if (isDataFetchedUsingHCatalog == true) {
							TestSession.logger.info("Successfully fetched the record using hcat.");
						} else {
							commonFunctions.updateDB(dataSetName , "hiveCurrentState" , "COMPLETED");
							commonFunctions.updateDB(dataSetName , "hiveResult" , "FAIL");
							TestSession.logger.error("failed to fetch the record using hcat.");
						}
					} else {
						commonFunctions.updateDB(dataSetName , "hiveCurrentState" , "COMPLETED");
						commonFunctions.updateDB(dataSetName , "hiveResult" , "FAIL");
						TestSession.logger.info("Failed to insert records into the table. Reason : " + testLoadDataToHiveTable.getErrorMessage());
					}
				} else {
					commonFunctions.updateDB(dataSetName , "hiveCurrentState" , "COMPLETED");
					commonFunctions.updateDB(dataSetName , "hiveResult" , "FAIL");
					TestSession.logger.info("coping hive data failed successfully. Reason : " + testCopyDataToHive.getErrorMessage());
				}
			} else {
				commonFunctions.updateDB(dataSetName , "hiveCreateTable" , "FAIL");
				commonFunctions.updateDB(dataSetName , "hiveResult" , "FAIL");
				commonFunctions.updateDB(dataSetName , "hiveCreateTableComment" , testCreateHiveTable.getErrorMessage());
				commonFunctions.updateDB(dataSetName , "hiveCurrentState" , "COMPLETED");
				TestSession.logger.info("Hive table created failed. Reason : " + testCreateHiveTable.getErrorMessage());
			}
		} else {
			commonFunctions.updateDB(dataSetName , "hiveDropTable" , "FAIL");
			commonFunctions.updateDB(dataSetName , "hiveResult" , "FAIL");
			commonFunctions.updateDB(dataSetName , "hiveDropTableComment" , testDropHiveTable.getErrorMessage());
			commonFunctions.updateDB(dataSetName , "hiveCurrentState" , "COMPLETED");
			TestSession.logger.error("Hive table dropped failed. Reason : " + testDropHiveTable.getErrorMessage());
		}
		TestSession.logger.info("isTableDroped = " + isTableDropped  + "  isTableCreated =  " + isTableCreated  +  "   isDataCopied "  + isDataCopied  + "    isDataInserted = " + isDataInserted  + "   isDataFetchedUsingHCatalog = " + isDataFetchedUsingHCatalog);
		boolean result = isTableDropped && isTableCreated && isDataCopied && isDataInserted &&  isDataFetchedUsingHCatalog;
		String componentName =  this.stackComponent.getStackComponentName() + "-" + result;
		
		TestSession.logger.info("--------------------------------------------------------------- TestIntHive  start ------------------------------------------------------------------------");
		return  componentName ;
	}
}
