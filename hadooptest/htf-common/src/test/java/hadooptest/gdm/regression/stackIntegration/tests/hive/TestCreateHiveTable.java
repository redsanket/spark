package hadooptest.gdm.regression.stackIntegration.tests.hive;

import java.util.Arrays;
import java.util.List;

import hadooptest.TestSession;
import hadooptest.gdm.regression.stackIntegration.lib.CommonFunctions;

public class TestCreateHiveTable {
	private String command;
	private String scriptLocation;
	private boolean result;
	private String errorMessage;
	private CommonFunctions commonFunctions;
	private static final String HIVE_CREATE_TABLE_SCRIPT_NAME = "HiveCreateTable.hql";

	public TestCreateHiveTable(String argument) {
		this.commonFunctions = new CommonFunctions();
		List<String> argumentList = Arrays.asList(argument.split(","));
		TestSession.logger.info("size " + argumentList.size() + "  argument = " + argument);
		if (argumentList.size() > 1) {
			this.setCommand(argumentList.get(0).trim());
			this.setScriptLocation(argumentList.get(1).trim());
		}
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
	
	public boolean getResult() {
		return result;
	}

	public void setResult(boolean result) {
		this.result = result;
	}

	public String getCommand() {
		return command;
	}

	public void setCommand(String command) {
		this.command = command;
	}

	public boolean execute() {
		TestSession.logger.info("--------------------------------------------------------------- TestCreateHiveTable  start ------------------------------------------------------------------------");
		String executionCommand = this.getCommand() + " hive -f " + this.getScriptLocation() + "/" + HIVE_CREATE_TABLE_SCRIPT_NAME + "\" ";
		TestSession.logger.info("executionCommand = " + executionCommand);
		String executionResult = this.commonFunctions.executeCommand(executionCommand);
		if (executionResult != null) {
			List<String> createTableOutputList = Arrays.asList(executionResult.split("\n"));
			boolean flag = false;
			for (String str : createTableOutputList ) {
				if (str.startsWith("OK")) {
					flag = true;
					this.setResult(flag);
					break;
				}
			}
			if (flag == false) {
				this.setErrorMessage(this.commonFunctions.getErrorMessage());
			}
		} else {
			this.setResult(false);
			this.setErrorMessage(this.commonFunctions.getErrorMessage());
		}
		TestSession.logger.info("--------------------------------------------------------------- TestCreateHiveTable  start ------------------------------------------------------------------------");
		return this.getResult();	
	}
}
