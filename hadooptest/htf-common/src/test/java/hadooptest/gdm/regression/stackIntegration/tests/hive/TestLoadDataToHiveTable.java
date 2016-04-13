package hadooptest.gdm.regression.stackIntegration.tests.hive;

import java.util.Arrays;
import java.util.List;

import hadooptest.TestSession;
import hadooptest.gdm.regression.stackIntegration.StackComponent;
import hadooptest.gdm.regression.stackIntegration.lib.CommonFunctions;

public class TestLoadDataToHiveTable {

	private String initCommand;
	private String dataPath;
	private String hostName;
	private String errorMessage;
	private String currentMinute;
	private String nameNodeName;
	private CommonFunctions commonFunction;
	private StackComponent stackComponent;

	public TestLoadDataToHiveTable( StackComponent stackComponent, String initCommand, String dataPath) {
		this.stackComponent = stackComponent;
		this.setInitCommand(initCommand);
		this.setDataPath(dataPath);
		this.commonFunction = new CommonFunctions();
	}
	
	public String getNameNodeName() {
		return nameNodeName;
	}

	public void setNameNodeName(String nameNodeName) {
		this.nameNodeName = nameNodeName;
	}

	public String getCurrentMinute() {
		return currentMinute;
	}

	public void setCurrentMinute(String currentMinute) {
		this.currentMinute = currentMinute;
	}

	public String getErrorMessage() {
		return errorMessage;
	}

	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}

	public String getHostName() {
		return hostName;
	}

	public void setHostName(String hostName) {
		this.hostName = hostName;
	}

	public String getInitCommand() {
		return initCommand;
	}

	public void setInitCommand(String initCommand) {
		this.initCommand = initCommand;
	}

	public String getDataPath() {
		return this.dataPath;
	}

	public void setDataPath(String dataPath) {
		this.dataPath = dataPath;
	}
	
	public String getCurrentHivePath() {
		StringBuilder currentPathBuilder =  new StringBuilder("hdfs://").append( this.getNameNodeName()).append("/data/daqdev/abf/data/").append(commonFunction.getCurrentHourPath()).append("/20130309/").append(this.getCurrentMinute()).append("/hiveData/");
		return currentPathBuilder.toString(); 
	}

	public boolean execute() {
		TestSession.logger.info("---------------------------------------------------------------TestLoadDataToHiveTable  start ------------------------------------------------------------------------");
		String currentDataSetName = this.commonFunction.getDataSetName();
		this.commonFunction.updateDB(currentDataSetName, "hiveLoadDataToTableCurrentState", "RUNNING");
		boolean  dataLoadedToHive = false;
		this.setCurrentMinute(commonFunction.getCurrentHrMin());	
		String command = this.getInitCommand() + " hive -f " + this.stackComponent.getScriptLocation() + "/LoadDataToHive.hql" +  "\"";
		String result = this.commonFunction.executeCommand(command);
		if (result != null ) {
			List<String> dropTableOutputList = Arrays.asList(result.split("\n"));
			boolean flag = false;
			for (String str : dropTableOutputList ) {
				if (str.startsWith("OK")) {
					dataLoadedToHive = true;
				}
			}
			if (dataLoadedToHive == false) {
				this.setErrorMessage(this.commonFunction.getErrorMessage());
				this.commonFunction.updateDB(currentDataSetName, "hiveLoadDataToTable", "FAIL");
				this.commonFunction.updateDB(currentDataSetName, "hiveLoadDataToTableComment", this.commonFunction.getErrorMessage());
			} else 	if (dataLoadedToHive == true) {
				this.commonFunction.updateDB(currentDataSetName, "hiveLoadDataToTable", "PASS");
			}
		} else {
			this.commonFunction.updateDB(currentDataSetName, "hiveLoadDataToTable", "FAIL");
			this.commonFunction.updateDB(currentDataSetName, "hiveLoadDataToTableComment", this.commonFunction.getErrorMessage());
			this.setErrorMessage(this.commonFunction.getErrorMessage());
		}
		this.commonFunction.updateDB(currentDataSetName, "hiveLoadDataToTableCurrentState", "COMPLETED");
		TestSession.logger.info("---------------------------------------------------------------TestLoadDataToHiveTable  end ------------------------------------------------------------------------");
		return dataLoadedToHive;
	}

}
