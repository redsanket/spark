package hadooptest.gdm.regression.stackIntegration.tests.hbase;

import java.util.Arrays;
import java.util.List;

import hadooptest.TestSession;
import hadooptest.gdm.regression.stackIntegration.StackComponent;
import hadooptest.gdm.regression.stackIntegration.lib.CommonFunctions;

public class TestHBaseCreateTable {
	private String hostName;
	private String scriptPath;
	private String kinitCommand;
	private String path;
	private String tableName;
	private CommonFunctions commonFunctions;
	private StackComponent stackComponent;
	
	public StackComponent getStackComponent() {
		return stackComponent;
	}

	public void setStackComponent(StackComponent stackComponent) {
		this.stackComponent = stackComponent;
	}
	
	public TestHBaseCreateTable( StackComponent stackComponent ,  String kinitCommand , String path , String tableName ) {
		this.setStackComponent(stackComponent);
		this.setHostName(this.getStackComponent().getHostName());
		this.setScriptPath(this.getStackComponent().getScriptLocation());
		this.setKinitCommand(kinitCommand);
		this.setPath(path);
		this.setTableName(tableName);
		this.commonFunctions = new CommonFunctions();
	}
	
	public String getHostName() {
		return hostName;
	}

	public void setHostName(String hostName) {
		this.hostName = hostName;
	}

	public String getScriptPath() {
		return scriptPath;
	}

	public void setScriptPath(String scriptPath) {
		this.scriptPath = scriptPath;
	}	
	
	public String getKinitCommand() {
		return kinitCommand;
	}

	public void setKinitCommand(String kinitCommand) {
		this.kinitCommand = kinitCommand;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public boolean execute() {
		TestSession.logger.info("---------------------------------------------------------------TestHBaseCreateTable  start ------------------------------------------------------------------------");
		String currentDataSet = this.commonFunctions.getCurrentHourPath();
		TestSession.logger.info("path = " + this.getPath() + "   knit = " + this.getKinitCommand()  + "  script location =   " + this.getScriptPath());
		boolean hbaseTableCreated = false;
		String command = "ssh " + this.getHostName() + "  \"" +  this.getPath() + ";"  + this.getKinitCommand() + ";hbase shell " + this.getScriptPath() + "/createHBaseIntegrationTable_temp.txt\"";
		String output = this.commonFunctions.executeCommand(command);
		if ( output != null ) {
			List<String> creatTableLogOuputList = Arrays.asList(output.split("\n"));
			String createOutput = creatTableLogOuputList.get(creatTableLogOuputList.size() - 2);
			if ( (createOutput.equals(this.getTableName()) == true) && (creatTableLogOuputList.get(creatTableLogOuputList.size() -1).startsWith("1 row(s)")) ) {
				hbaseTableCreated = true;
				this.commonFunctions.updateDB(currentDataSet, "hbaseCreateTable", "PASS");
			} else {
				hbaseTableCreated = false;
				this.commonFunctions.updateDB(currentDataSet, "hbaseCreateTable", "FAIL");
			}	
		}
		TestSession.logger.info("---------------------------------------------------------------TestHBaseCreateTable  end ------------------------------------------------------------------------");
		return hbaseTableCreated;
	}
}
