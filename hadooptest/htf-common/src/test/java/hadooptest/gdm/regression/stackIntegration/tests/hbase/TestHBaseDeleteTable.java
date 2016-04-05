package hadooptest.gdm.regression.stackIntegration.tests.hbase;

import java.util.Arrays;
import java.util.List;

import hadooptest.TestSession;
import hadooptest.gdm.regression.stackIntegration.StackComponent;
import hadooptest.gdm.regression.stackIntegration.lib.CommonFunctions;

public class TestHBaseDeleteTable {
	private String hostName;
	private String scriptPath;
	private String kinitCommand;
	private String path;
	private CommonFunctions commonFunctions;
	private StackComponent stackComponent;
	
	public StackComponent getStackComponent() {
		return stackComponent;
	}

	public void setStackComponent(StackComponent stackComponent) {
		this.stackComponent = stackComponent;
	}
	
	public TestHBaseDeleteTable( StackComponent stackComponent ,  String kinitCommand , String path ) {
		this.setStackComponent(stackComponent);
		this.setHostName(this.getStackComponent().getHostName());
		this.setScriptPath(this.getStackComponent().getScriptLocation());
		this.setKinitCommand(kinitCommand);
		this.setPath(path);
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

	public boolean execute() {
		TestSession.logger.info("---------------------------------------------------------------TestHBaseDeleteTable  start ------------------------------------------------------------------------");
		TestSession.logger.info("path = " + this.getPath() + "   knit = " + this.getKinitCommand()  + "  script location =   " + this.getScriptPath());
		boolean hbaseTableDeleted = false;
		String command = "ssh " + this.getHostName() + "  \"" +  this.getPath() + ";"  + this.getKinitCommand() + ";hbase shell " + this.getScriptPath() + "/deleteHBaseIntegrationTable_temp.txt\"";
		String output = this.commonFunctions.executeCommand(command);
		if ( output != null ) {
			List<String> deleteTableOuputList = Arrays.asList(output.split("\n"));
			if (deleteTableOuputList.get(deleteTableOuputList.size() -1).trim().startsWith("0 row(s)")) {
				hbaseTableDeleted = true;
			} else {
				hbaseTableDeleted = false;
			}
		}
		TestSession.logger.info("---------------------------------------------------------------TestHBaseDeleteTable  end ------------------------------------------------------------------------");
		return hbaseTableDeleted;
	}

}
