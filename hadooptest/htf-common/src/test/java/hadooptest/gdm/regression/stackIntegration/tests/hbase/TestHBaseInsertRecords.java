package hadooptest.gdm.regression.stackIntegration.tests.hbase;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import hadooptest.TestSession;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.gdm.regression.stackIntegration.StackComponent;
import hadooptest.gdm.regression.stackIntegration.lib.CommonFunctions;

public class TestHBaseInsertRecords {
	
	private String hostName;
	private String scriptPath;
	private String kinitCommand;
	private String path;
	private String tableName;
	private String nameNodeName;
	private String hbaseGateWayHostName;
	private CommonFunctions commonFunctions;
	private StackComponent stackComponent;
	public static final String PIG_HOME = "/home/y/share/pig";
	public static final String KINIT = "kinit -k -t /etc/grid-keytabs/hbaseqa.dev.service.keytab hbaseqa/";
	
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

	public String getHbaseGateWayHostName() {
		return hbaseGateWayHostName;
	}

	public void setHbaseGateWayHostName(String hbaseGateWayHostName) {
		this.hbaseGateWayHostName = hbaseGateWayHostName;
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
	
	public void getHBaseGateWayHostName() {
		String hbaseClusterName = GdmUtils.getConfiguration("testconfig.TestWatchForDataDrop.hbaseClusterName");
		String command = "yinst range -ir \"(@grid_re.clusters." + hbaseClusterName + ".gateway"+")\"";
		TestSession.logger.info("Command = " + command);
		String hostName = this.commonFunctions.executeCommand(command).trim();	
		this.setHbaseGateWayHostName(hostName);
	}
	
	public void copyScriptFilesToGw() {
		if (this.commonFunctions.isJarFileExist(this.getHbaseGateWayHostName()) == true) {
			boolean flag = this.commonFunctions.copyTestCases("hbase" ,this.commonFunctions.TESTCASE_PATH , this.getHbaseGateWayHostName());
			if (flag == true) {
				TestSession.logger.info("script files copied to hbase gateway - " + this.getHbaseGateWayHostName());
			} else {
				try {
					throw new  Exception("Failed to copy the script files on hbase gateway");
				} catch (Exception e) {
					TestSession.logger.error(e);
					e.printStackTrace();
				}
			}
		}
	}

	public boolean execute() {
		this.getHBaseGateWayHostName();
		this.copyScriptFilesToGw();
		String currentTableName = GdmUtils.getConfiguration("testconfig.TestWatchForDataDrop.pipeLineName") + "_" + this.commonFunctions.getDataSetName();
		TestSession.logger.info("---------------------------------------------------------------TestHBaseInsertRecords  start   "+  currentTableName + "------------------------------------------------------------------------");
		String currentDataSet = this.commonFunctions.getDataSetName();
		this.commonFunctions.updateDB(currentDataSet, "hbaseInsertTableCurrentState", "RUNNING");
		boolean insertRecordResult = false;
		String mrJobURL = null;
		String dataSetName = this.commonFunctions.getCurrentHourPath();
		
		String command = "ssh " + getHbaseGateWayHostName() + "  \"export HBASE_PREFIX=/home/y/libexec/hbase;export PATH=$PATH:/home/y/share/pig:/home/y/libexec/hbase/bin:/tmp/integration_test_files/lib/*.jar;" 
		+ KINIT + getHbaseGateWayHostName() + ";" + "export PIG_HOME=" + PIG_HOME + ";export PATH=$PATH:$PIG_HOME/bin/;pig -x mapreduce  " 
				+ "-param \"NAMENODE_NAME=" + this.getNameNodeName() + "\""
				+ "  "
				+ "-param \"DATASET_NAME=" + dataSetName + "\""
				+ "  "
				+ "-param \"TABLE_NAME=" + currentTableName + "\""
				+ "  "
				+ this.getScriptPath() + "/HBaseInsertRecord_temp.pig\"";
		String output = this.commonFunctions.executeCommand(command );
		if (output != null ) {
			List<String> insertOutputList = Arrays.asList(output.split("\n"));
			String insertResult = insertOutputList.get(insertOutputList.size() - 1);
			TestSession.logger.info("Result - " + insertResult );			int count = 0;
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
		if (insertRecordResult == false) {
			this.commonFunctions.updateDB(currentDataSet, "hbaseInsertRecordTable", "FAIL");
			this.commonFunctions.updateDB(currentDataSet, "hbaseInsertRecordTableMRJobURL", mrJobURL );
		} else if (insertRecordResult == true) {
			this.commonFunctions.updateDB(currentDataSet, "hbaseInsertRecordTable", "PASS");
			this.commonFunctions.updateDB(currentDataSet, "hbaseInsertRecordTableMRJobURL", mrJobURL );
		}
		this.commonFunctions.updateDB(currentDataSet, "hbaseInsertTableCurrentState", "COMPLETED");
		TestSession.logger.info("---------------------------------------------------------------TestHBaseInsertRecords  end  ------------------------------------------------------------------------");
		return insertRecordResult;
	}

}
