package hadooptest.gdm.regression.stackIntegration;

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
	
	public String getCurrentMRJobLink() {
		return currentMRJobLink;
	}

	public void setCurrentMRJobLink(String currentMRJobLink) {
		this.currentMRJobLink = currentMRJobLink;
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
	}

	public void setResult(String result) { 
		this.result = result;
	}

	public void setErrorString(String errorString) {
		this.errorString = errorString;
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
