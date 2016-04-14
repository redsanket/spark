package hadooptest.gdm.regression.stackIntegration.tests.hive;

import java.util.Arrays;
import java.util.List;

import hadooptest.TestSession;
import hadooptest.gdm.regression.stackIntegration.StackComponent;
import hadooptest.gdm.regression.stackIntegration.lib.CommonFunctions;

public class TestHCatalog {

	private StackComponent stackComponent;
	private CommonFunctions commonFunctions;
	private String errorMessage;
	private String initCommand;
	private String scriptPath;
	public static final String PIG_HOME = "/home/y/share/pig";
	
	public String getErrorMessage() {
		return errorMessage;
	}

	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}
	
	public String getInitCommand() {
		return initCommand;
	}

	public void setInitCommand(String initCommand) {
		this.initCommand = initCommand;
	}
	
	public String getScriptPath() {
		return scriptPath;
	}

	public void setScriptPath(String scriptPath) {
		this.scriptPath = scriptPath;
	}

	public TestHCatalog() {
	}
	
	public TestHCatalog(String initCommand , String scriptPath) {
		this.commonFunctions = new CommonFunctions();
		this.setInitCommand(initCommand);
		this.setScriptPath(scriptPath);
	}
	
	public boolean execute() {
		TestSession.logger.info("---------------------------------------------------------------TestHCatalog  start ------------------------------------------------------------------------");
		String currentDataSetName = this.commonFunctions.getDataSetName();
		this.commonFunctions.updateDB(currentDataSetName, "hcatCurrentState", "RUNNING");
		boolean flag = false;
		String mrJobURL = "";
		String command = this.getInitCommand() + "pig -useHCatalog -x tez " + this.getScriptPath() + "/FetchHiveDataUsingHCatalog_temp.pig" + "\"" ;
		String result = this.commonFunctions.executeCommand(command);
		if (result != null ) {
			List<String> fetchDataOutputList = Arrays.asList(result.split("\n"));
			int count=0;
			String startTime = null , endTime = null;
			TestSession.logger.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~    fetchDataUsingHCat() ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
			for (String str : fetchDataOutputList ) {
				TestSession.logger.info("str = " + str);
				if (str.indexOf("org.apache.hadoop.mapreduce.Job - The url to track the job:") > -1) {	
					List<String>  temp = Arrays.asList(str.split(" "));
					mrJobURL = temp.get(temp.size() - 1);
					TestSession.logger.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~  mrJobURL  = " + mrJobURL);
				}
				if (str.trim().startsWith("Success")) {
					flag = true;
					TestSession.logger.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~  flag  = " + flag);
				}
				if (str.indexOf("HadoopVersion	PigVersion	UserId	StartedAt	FinishedAt	Features") > -1) {
					count++;
					String tempTime = fetchDataOutputList.get(count);
					List<String> tempList = Arrays.asList(tempTime.split("\t"));
					startTime = tempList.get(3);
					endTime = tempList.get(4);
					TestSession.logger.info("startTime = " + startTime);
					TestSession.logger.info("endTime = " + endTime);
				}
				count++;
			}
			if (flag == false) {
				this.commonFunctions.updateDB(currentDataSetName, "hcatResult", "FAIL");
				this.commonFunctions.updateDB(currentDataSetName, "hcatMRJobURL", mrJobURL);
				this.commonFunctions.updateDB(currentDataSetName, "hcatComment", result);
				this.setErrorMessage(this.commonFunctions.getErrorMessage());
			}  else if (flag == true) {
				this.commonFunctions.updateDB(currentDataSetName, "hcatResult", "PASS");
				this.commonFunctions.updateDB(currentDataSetName, "hcatMRJobURL", mrJobURL);
			}
			this.commonFunctions.updateDB(currentDataSetName, "hcatCurrentState", "COMPLETED");
		}
		TestSession.logger.info("---------------------------------------------------------------TestHCatalog  end ------------------------------------------------------------------------");
		return flag;
	}
}
