package hadooptest.gdm.regression.stackIntegration;

import hadooptest.TestSession;
import hadooptest.gdm.regression.stackIntegration.db.DataBaseOperations;
import hadooptest.gdm.regression.stackIntegration.lib.CommonFunctions;

public class StackComponent {
	
	private String stackComponentName;
	private String stackComponentVersion;
	private String hostName;
	private String result;
	private String errorString;
	private String testList;
	private String scriptLocation;
	private String currentTestCaseName;
	private String currentMRJobLink;
	private String componentTestCaseExecutionResultFilePath;
	private String dataSetName;
	private String currentState;
	private boolean health;
	private boolean isScriptCopied;
	
	public String toString() {
		return "stackComponentName = " + stackComponentName 
				+ "  stackComponentVersion  = " + stackComponentVersion 
				+ "  health = " + health
				+ "  hostName = " + this.hostName
				+ "  scriptLocation = " + scriptLocation 
				+ "  dataSetName = " + dataSetName
				+ "  result = "+ result
				+ "  errorString = " + errorString
				;
	}
	
	public StackComponent() { }
	
	public StackComponent(String stackComponentName, boolean health, String stackComponentVersion) {
		this.stackComponentName = stackComponentName;
		this.health = health;
		this.stackComponentVersion = stackComponentVersion;
	}
	
	public String getCurrentState() {
		return currentState;
	}

	public void setCurrentState(String currentState) {
		this.currentState = currentState;
		CommonFunctions commonFunctions = new CommonFunctions();
		String currentDataSetName = commonFunctions.getCurrentHourPath();
		if (this.getStackComponentName().equals("pig") || this.getStackComponentName().equals("tez") ||  this.getStackComponentName().equals("hive")  ||  this.getStackComponentName().equals("hcat") ||  this.getStackComponentName().equals("hbase")) {
			
			// add version to db.
			TestSession.logger.info("-----------------------------------------$$$$$$$$$$$$$$$$$$$$$$$---------------------------------------------------------------------");
			TestSession.logger.info("component Name = " + this.getStackComponentName());
			TestSession.logger.info("version = " + this.getStackComponentVersion());
			TestSession.logger.info("-----------------------------------------$$$$$$$$$$$$$$$$$$$$$$$---------------------------------------------------------------------");
			commonFunctions.updateDB(currentDataSetName, this.getStackComponentName() +  "CurrentState" , this.getCurrentState());
		}
	}
	
	public String getCurrentMRJobLink() {
		return currentMRJobLink;
	}

	public void setCurrentMRJobLink(String currentMRJobLink) {
		this.currentMRJobLink = currentMRJobLink;
		CommonFunctions commonFunctions = new CommonFunctions();
		String currentDataSetName = commonFunctions.getCurrentHourPath();
		if (this.getStackComponentName().equals("pig") || this.getStackComponentName().equals("tez") ||  this.getStackComponentName().equals("hive")  ||  this.getStackComponentName().equals("hcat") ||  this.getStackComponentName().equals("hbase")) {
			
			// add version to db.
			TestSession.logger.info("-----------------------------------------$$$$$$$$$$$$$$$$$$$$$$$---------------------------------------------------------------------");
			TestSession.logger.info("component Name = " + this.getStackComponentName());
			TestSession.logger.info("version = " + this.getStackComponentVersion());
			TestSession.logger.info("-----------------------------------------$$$$$$$$$$$$$$$$$$$$$$$---------------------------------------------------------------------");
			commonFunctions.updateDB(currentDataSetName, this.getStackComponentName() +  "MRJobURL" , this.getCurrentMRJobLink());
		}
	}

	public String getCurrentTestCaseName() {
		return currentTestCaseName;
	}

	public void setCurrentTestCaseName(String currentTestCaseName) {
		this.currentTestCaseName = currentTestCaseName;
	}

	public String getScriptLocation() {
		return scriptLocation;
	}

	public void setScriptLocation(String scriptLocation) {
		this.scriptLocation = scriptLocation;
	}
	
	public void setStackComponentName(String stackComponentName) {
		this.stackComponentName = stackComponentName;
	}

	public void setHealth(boolean health) {
		this.health = health;
	}

	public void setStackComponentVersion(String stackComponentVersion) {
		this.stackComponentVersion = stackComponentVersion;
		CommonFunctions commonFunctions = new CommonFunctions();
		String currentDataSetName = commonFunctions.getCurrentHourPath();
		if (this.getStackComponentName().equals("pig") || this.getStackComponentName().equals("tez") ||  this.getStackComponentName().equals("hive")  ||  this.getStackComponentName().equals("hcat") ||  this.getStackComponentName().equals("hbase")) {
			// add version to db.
			TestSession.logger.info("-----------------------------------------$$$$$$$$$$$$$$$$$$$$$$$---------------------------------------------------------------------");
			TestSession.logger.info("dataSetName = " + currentDataSetName);
			TestSession.logger.info("component Name = " + this.getStackComponentName());
			TestSession.logger.info("version = " + this.getStackComponentVersion());
			TestSession.logger.info("-----------------------------------------$$$$$$$$$$$$$$$$$$$$$$$---------------------------------------------------------------------");
			commonFunctions.updateDB(currentDataSetName, this.getStackComponentName() +  "Version" , this.getStackComponentVersion());
		}
	}

	public void setResult(String result) {
		this.result = result;
		CommonFunctions commonFunctions = new CommonFunctions();
		String currentDataSetName = commonFunctions.getCurrentHourPath();
		if (this.getStackComponentName().equals("pig") || this.getStackComponentName().equals("tez") ||  this.getStackComponentName().equals("hive")  ||  this.getStackComponentName().equals("hcat") ||  this.getStackComponentName().equals("hbase")) {
			// add version to db.
			TestSession.logger.info("-----------------------------------------$$$$$$$$$$$$$$$$$$$$$$$---------------------------------------------------------------------");
			TestSession.logger.info("component Name = " + this.getStackComponentName());
			TestSession.logger.info("version = " + this.getResult());
			TestSession.logger.info("-----------------------------------------$$$$$$$$$$$$$$$$$$$$$$$---------------------------------------------------------------------");
			commonFunctions.updateDB(currentDataSetName, this.getStackComponentName() + "Result" , this.getResult());
		}
	}

	public void setErrorString(String errorString) {
		this.errorString = errorString;
		CommonFunctions commonFunctions = new CommonFunctions();
		String currentDataSetName = commonFunctions.getCurrentHourPath();
		if (this.getStackComponentName().equals("pig") || this.getStackComponentName().equals("tez") ||  this.getStackComponentName().equals("hive")  ||  this.getStackComponentName().equals("hcat") ||  this.getStackComponentName().equals("hbase")) {
			// add version to db.
			TestSession.logger.info("-----------------------------------------$$$$$$$$$$$$$$$$$$$$$$$---------------------------------------------------------------------");
			TestSession.logger.info("dataSetName = " + this.getDataSetName());
			TestSession.logger.info("component Name = " + this.getStackComponentName());
			TestSession.logger.info("version = " + this.getResult());
			TestSession.logger.info("-----------------------------------------$$$$$$$$$$$$$$$$$$$$$$$---------------------------------------------------------------------");
			commonFunctions.updateDB(currentDataSetName, this.getStackComponentName() , this.getErrorString());
		}
	}

	public void setTestList(String testList) {
		this.testList = testList;
	}

	public String getStackComponentName() {
		return this.stackComponentName;
	}

	public boolean getHealth() {
		return health;
	}

	public String getStackComponentVersion() {
		return stackComponentVersion;
	}

	public String getResult() {
		return result;
	}

	public String getErrorString() {
		return errorString;
	}

	public String getTestList() {
		return testList;
	}
	
	public String getHostName() {
		return hostName;
	}

	public void setHostName(String hostName) {
		this.hostName = hostName;
	}
	
	public String getComponentTestCaseExecutionResultFilePath() {
		return componentTestCaseExecutionResultFilePath;
	}

	public void setComponentTestCaseExecutionResultFilePath(String componentTestCaseExecutionResultFilePath) {
		this.componentTestCaseExecutionResultFilePath = componentTestCaseExecutionResultFilePath;
	}
	
	public boolean isScriptCopied() {
		return isScriptCopied;
	}

	public void setScriptCopied(boolean isScriptCopied) {
		this.isScriptCopied = isScriptCopied;
	}
	
	public String getDataSetName() {
		return dataSetName;
	}

	public void setDataSetName(String dataSetName) {
		this.dataSetName = dataSetName;
	}
}
