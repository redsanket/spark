package hadooptest.gdm.regression.stackIntegration.tests.hbase;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import hadooptest.TestSession;
import hadooptest.gdm.regression.stackIntegration.StackComponent;
import hadooptest.gdm.regression.stackIntegration.lib.CommonFunctions;

public class TestHBaseInsertRecords {
	
	private String hostName;
	private String scriptPath;
	private String kinitCommand;
	private String path;
	private String tableName;
	private String nameNodeName;
	private CommonFunctions commonFunctions;
	private StackComponent stackComponent;
	
	public TestHBaseInsertRecords() {
	}
	
	public TestHBaseInsertRecords( StackComponent stackComponent ,  String kinitCommand , String path , String tableName , String nameNodeName ) {
		this.setStackComponent(stackComponent);
		this.setHostName(this.getStackComponent().getHostName());
		this.setScriptPath(this.getStackComponent().getScriptLocation());
		this.setKinitCommand(kinitCommand);
		this.setPath(path);
		this.setTableName(tableName);
		this.setNameNodeName(nameNodeName);
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

	public String getNameNodeName() {
		return nameNodeName;
	}

	public void setNameNodeName(String nameNodeName) {
		this.nameNodeName = nameNodeName;
	}

	public StackComponent getStackComponent() {
		return stackComponent;
	}

	public void setStackComponent(StackComponent stackComponent) {
		this.stackComponent = stackComponent;
	}

	public boolean execute() {
		TestSession.logger.info("---------------------------------------------------------------TestHBaseInsertRecords  start ------------------------------------------------------------------------");

		boolean insertRecordResult = false;
		String dataSetName = this.commonFunctions.getCurrentHourPath();
		String command = "ssh " + this.getHostName() + "  \"" +  this.getPath() + ";"  + this.getKinitCommand() + ";pig -x mapreduce " 
				+ "-param \"NAMENODE_NAME=" + this.getNameNodeName() + "\""
				+ "  "
				+ "-param \"DATASET_NAME=" + dataSetName + "\""
				+ "  "
				+ "-param \"TABLE_NAME=" + this.getTableName() + "\""
				+ "  "
				+ this.getScriptPath() + "/HBaseInsertRecord_temp.pig\"";
		String output = this.commonFunctions.executeCommand(command );
		if (output != null ) {
			List<String> insertOutputList = Arrays.asList(output.split("\n"));
			String insertResult = insertOutputList.get(insertOutputList.size() - 1);
			TestSession.logger.info("Result - " + insertResult );

			String mrJobURL = null;
			int count = 0;
			String startTime = null , endTime = null;
			for ( String item : insertOutputList ) {
				if (item.indexOf("INFO  org.apache.hadoop.mapreduce.Job - The url to track the job:") > -1) {
					List<String>  temp = Arrays.asList(item.split(" "));
					mrJobURL = temp.get(temp.size() - 1);
				}
				if (item.indexOf("HadoopVersion	PigVersion	UserId	StartedAt	FinishedAt	Features") > -1) {
					count ++;
					String tempTime = insertOutputList.get(count);
					List<String> tempList = Arrays.asList(tempTime.split("\t"));
					startTime = tempList.get(3);
					endTime = tempList.get(4);
				}
				
				if (item.indexOf("Output(s):") > -1) {
					TestSession.logger.info("item = " + item);
					String state = insertOutputList.get(count);
					TestSession.logger.info("state = " + state);
					assertTrue("Expected  Successfully, but got " +  state , state.indexOf("Successfully") > -1 );
					insertRecordResult = true;
				}
				count++;
			}
		}
		TestSession.logger.info("---------------------------------------------------------------TestHBaseInsertRecords  end  ------------------------------------------------------------------------");
		return insertRecordResult;
	}

}
