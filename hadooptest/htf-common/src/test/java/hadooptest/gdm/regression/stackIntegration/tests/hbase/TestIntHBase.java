package hadooptest.gdm.regression.stackIntegration.tests.hbase;

import hadooptest.TestSession;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.gdm.regression.stackIntegration.StackComponent;
import hadooptest.gdm.regression.stackIntegration.lib.CommonFunctions;

public class TestIntHBase implements java.util.concurrent.Callable<String>{
	
	private String hostName;
	private String nameNodeName;
	private StackComponent stackComponent;
	private CommonFunctions commonFunctions;
	private static final String  COMPONENT_NAME = "hbase";
	public static final String PIG_HOME = "/home/y/share/pig";
	public static final String HBASE_HOME = "/home/y/libexec/hbase";
	public static final String KINIT = "kinit -k -t /etc/grid-keytabs/hbaseqa.dev.service.keytab hbaseqa/";
	public static final String INTEGRATION_JAR="/tmp/integration_test_files/lib/*.jar";
	
	public TestIntHBase() {
	}
	
	public TestIntHBase(StackComponent stackComponent , String nameNodeName) {
		this.setStackComponent(stackComponent);
		this.setHostName(this.stackComponent.getHostName());
		this.setNameNodeName(nameNodeName);
		this.commonFunctions = new CommonFunctions();
	}
	
	public String getNameNodeName() {
		return nameNodeName;
	}

	public void setNameNodeName(String nameNodeName) {
		this.nameNodeName = nameNodeName;
	}

	public StackComponent getStackComponent() {
		return this.stackComponent;
	}

	public void setStackComponent(StackComponent stackComponent) {
		this.stackComponent = stackComponent;
	}

	public String getKinitCommand() {
		String kinitCommand = KINIT + this.getHostName() + "@DEV.YGRID.YAHOO.COM";
		TestSession.logger.info("kinit command - " + kinitCommand);
		return kinitCommand;
	}

	public String getPathCommand() {
		String pathCommand = "export HBASE_PREFIX=/home/y/libexec/hbase;export PATH=$PATH:" + PIG_HOME + ":" + HBASE_HOME + "/bin" + ":" + INTEGRATION_JAR;
		TestSession.logger.info("export path value - " + pathCommand);
		return pathCommand.trim();
	}
	
	public String getHostName() {
		return this.hostName;
	}

	public void setHostName(String hostName) {
		this.hostName = hostName;
	}
	
	@Override
	public String call() throws Exception {
		String result = execute();
		return result;
	}
	
	public String  execute() {
		TestSession.logger.info(" ---------------------------------------------------------------  TestHBASE  start ------------------------------------------------------------------------");
		boolean isHBaseTableCreated = false , isHBaseRecordInserted = false , isTableScanned = false, isTableDeleted = false;
		String currentDataSetName = this.commonFunctions.getCurrentHourPath();
		String tableName = this.commonFunctions.getPipeLineName() + "_" + currentDataSetName;
		this.commonFunctions.updateDB(currentDataSetName, "hbaseCurrentState", "RUNNING");
		TestSession.logger.info("Hbase Table name =-  " + tableName);
		TestHBaseCreateTable testHBaseCreateTable = new TestHBaseCreateTable(this.getStackComponent() , this.getKinitCommand() , this.getPathCommand() , tableName);
		isHBaseTableCreated = testHBaseCreateTable.execute();
		TestSession.logger.info("isTablecreated = " +isHBaseTableCreated );
		
		if (isHBaseTableCreated == true) {
			TestHBaseInsertRecords testHBaseInsertRecords = new TestHBaseInsertRecords(this.getStackComponent() , this.getKinitCommand() , this.getPathCommand() , tableName , this.getNameNodeName());
			isHBaseRecordInserted = testHBaseInsertRecords.execute();
			if (isHBaseRecordInserted == true) {
				TestSession.logger.info("HBase Records inserted successfully into table");
				TestHBaseScanTable testHBaseScanTable = new TestHBaseScanTable(this.getStackComponent() , this.getKinitCommand() , this.getPathCommand() , tableName);
				isTableScanned = testHBaseScanTable.execute();
				if (isTableScanned == true) {
					TestSession.logger.info("HBase Table scanned successfully ");
					TestHBaseDeleteTable testHBaseDeleteTable = new TestHBaseDeleteTable(this.getStackComponent() , this.getKinitCommand() , this.getPathCommand() );
					isTableDeleted = testHBaseDeleteTable.execute();
					if (isTableDeleted == true) {
						TestSession.logger.info("HBase table deleted successfully ");
					} else {
						TestSession.logger.info("Failed to delete HBase table");
						this.commonFunctions.updateDB(currentDataSetName, "hbaseResult", "FAILED");
						this.commonFunctions.updateDB(currentDataSetName, "hbaseCurrentState", "COMPLETED");
					}
				} else {
					TestSession.logger.info("HBase Table scanned failed.. ");
					this.commonFunctions.updateDB(currentDataSetName, "hbaseResult", "FAILED");
					this.commonFunctions.updateDB(currentDataSetName, "hbaseCurrentState", "COMPLETED");
				}
			} else {
				TestSession.logger.info("Failed to insert record into HBasetable");
				this.commonFunctions.updateDB(currentDataSetName, "hbaseResult", "FAILED");
				this.commonFunctions.updateDB(currentDataSetName, "hbaseCurrentState", "COMPLETED");
			}
		} else {
			TestSession.logger.info("Failed to create the hbase table " + tableName + "  hence hbase test is marked as failed.");
			this.commonFunctions.updateDB(currentDataSetName, "hbaseCurrentState", "COMPLETED");
			this.commonFunctions.updateDB(currentDataSetName, "hbaseResult", "FAILED");
		}
		TestSession.logger.info(" ---------------------------------------------------------------  TestHBASE  start ------------------------------------------------------------------------");
		TestSession.logger.info("HbaseTable created = " + isHBaseTableCreated  + "  records inserted - " + isHBaseRecordInserted  + "  table scanned - " + isTableScanned + " hbase deleted - " + isTableDeleted);
		boolean hbaseTestResult = isHBaseTableCreated && isHBaseRecordInserted && isTableScanned && isTableDeleted;
		return COMPONENT_NAME + "-" + hbaseTestResult;
	}

}
