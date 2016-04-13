package hadooptest.gdm.regression.stackIntegration.tests.hive;

import hadooptest.TestSession;
import hadooptest.gdm.regression.stackIntegration.lib.CommonFunctions;
import java.util.List;
import java.util.Arrays;

public class TestDropHiveTable {
	private String command;
	private String scriptLocation;
	private boolean result;
	private String errorMessage;
	private CommonFunctions commonFunctions;
	private static final String TEST_CASE_DESCRIPTION = "Verify whether user is able to drop the hive table";
	private static final String HIVE_DROP_TABLE_SCRIPT_NAME = "HiveDropTable.hql";

	public TestDropHiveTable(String argument) {
		this.commonFunctions = new CommonFunctions();
		List<String> argumentList = Arrays.asList(argument.split(","));
		TestSession.logger.info("size " + argumentList.size() + "  argument = " + argument);
		if (argumentList.size() > 1) {
			this.setCommand(argumentList.get(0).trim());
			this.setScriptLocation(argumentList.get(1).trim());
		}
	}
	
	public String getTestCaseDescription() {
		return TEST_CASE_DESCRIPTION;
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
		TestSession.logger.info("--------------------------------------------------------------- TestDropHiveTable  start ------------------------------------------------------------------------");
		String currentDataSetName = this.commonFunctions.getDataSetName();
		this.commonFunctions.updateDB(currentDataSetName, "hiveDropTableCurrentState", "RUNNING");
		String executionCommand = this.getCommand() + " hive -f " + this.getScriptLocation() + "/" + HIVE_DROP_TABLE_SCRIPT_NAME + "\" ";
		TestSession.logger.info("executionCommand = " + executionCommand);
		String executionResult = this.commonFunctions.executeCommand(executionCommand);
		TestSession.logger.info("TestDropHiveTable  --------------------------- " + executionResult);
		if (executionResult != null) {
			List<String> dropTableOutputList = Arrays.asList(executionResult.split("\n"));
			boolean flag = false;
			for (String str : dropTableOutputList ) {
				if (str.startsWith("OK")) {
					flag = true;
					this.setResult(flag);
					break;
				}
			}
			if (flag == false) {
				this.setErrorMessage(this.commonFunctions.getErrorMessage());
				this.commonFunctions.updateDB(currentDataSetName, "hiveDropTableComment", executionResult);
			}else if (flag == true) {
				this.commonFunctions.updateDB(currentDataSetName, "hiveDropTable", "PASS");
			}
		} else {
			this.setResult(false);
			this.setErrorMessage(this.commonFunctions.getErrorMessage());
		}
		this.commonFunctions.updateDB(currentDataSetName, "hiveDropTableCurrentState", "COMPLETED");
		TestSession.logger.info("executionResult of drop table result - " + this.getResult());
		TestSession.logger.info("--------------------------------------------------------------- TestDropHiveTable  end  ------------------------------------------------------------------------");
		return this.getResult();
	}
}
