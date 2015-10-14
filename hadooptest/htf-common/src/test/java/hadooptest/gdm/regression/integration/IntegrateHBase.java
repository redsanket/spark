package hadooptest.gdm.regression.integration;

import static org.junit.Assert.assertTrue;
import static java.nio.file.Files.readAllBytes;
import static java.nio.file.Paths.get;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;

import hadooptest.TestSession;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;

/**
 * Integrate HBase 
 */
public class IntegrateHBase {
	private String hbaseMasterHostName;
	private String scriptPath;
	private boolean isRecordsInserted = false;
	private boolean isRecordsScaned = false;
	private String insertRecordResult;
	private String scanRecordResult;
	private String hbaseExecutePigScriptLocation;
	private String dataPath;
	private String currentFeedName;
	private static final String PROCOTOL = "hdfs://";
	public static final String PIG_HOME = "/home/y/share/pig";
	public static final String HBASE_HOME = "/home/y/libexec/hbase";
	public static final String KINIT = "kinit -k -t /etc/grid-keytabs/hbaseqa.dev.service.keytab hbaseqa/";
	public static final String INTEGRATION_JAR="/tmp/integration_test_files/lib/*.jar";

	public IntegrateHBase() { }

	public String getKinitCommand() {
		this.hbaseMasterHostName = GdmUtils.getConfiguration("testconfig.TestWatchForDataDrop.hbaseMasterHostName").trim();
		String kinitCommand = KINIT + this.hbaseMasterHostName + "@DEV.YGRID.YAHOO.COM";
		TestSession.logger.info("kinit command - " + kinitCommand);
		return kinitCommand;
	}

	public String getPathCommand() {
		String pathCommand = "export PATH=$PATH:" + PIG_HOME + ":" + HBASE_HOME + "/bin" + ":" + INTEGRATION_JAR;
		TestSession.logger.info("export path value - " + pathCommand);
		return pathCommand.trim();
	}

	public  boolean isRecordInsertedIntoHBase() {
		return this.isRecordsInserted;
	}
	public boolean isRecordScannedFromHBase() {
		return this.isRecordsScaned;
	}
	
	public void resetHBaseRecordInserted(boolean flag) {
		this.isRecordsInserted = flag;
	}
	public void resetHBaseRecordScanned(boolean flag) {
		this.isRecordsScaned = flag;
	}

	public void setCurrentFeedName(String feedName) {
		this.currentFeedName = feedName;
	}

	public String getCurrentFeedName() {
		return this.currentFeedName;
	}

	public void setDataPath(String dataPath) {
		this.dataPath = dataPath;
	}

	private String getDataPath() {
		return this.dataPath;
	}

	public void executeInsertingRecordsIntoHBase() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		this.isRecordsInserted = false;
		boolean result = false;
		String hbaseHostName = GdmUtils.getConfiguration("testconfig.TestWatchForDataDrop.hbaseMasterHostName").trim();
		String command = "ssh " + hbaseHostName + "  \"" +  this.getPathCommand() + ";"  + this.getKinitCommand() + ";pig -x mapreduce " + this.getHbaseExecutePigScriptLocation() + "/HBaseInsertRecord.pig\"";
		TestSession.logger.info("command = " + command);
		String output = this.executeCommand(command);
		List<String> insertOutputList = Arrays.asList(output.split("\n"));
		String insertResult = insertOutputList.get(insertOutputList.size() - 1);
		TestSession.logger.info("Result - " + insertResult );
		assertTrue("Expected Success! , but got " + insertResult , insertResult.indexOf("Success!") > -1);
		result = true;
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
			count++;
		}
		if (result == true) {
			this.insertRecordResult = "PASS~" +  mrJobURL + "~" + startTime.trim() + "~" + endTime.trim();
		} else if (result == false) {
			this.insertRecordResult = "FAIL~" +  mrJobURL + "~" + startTime.trim() + "~" + endTime.trim();
		}

		// insert hbase insert record into db
		DataBaseOperations dbOperations = new DataBaseOperations();
		Connection con = dbOperations.getConnection();
		dbOperations.updateRecord(con, "hbaseInsert" , this.insertRecordResult , this.getCurrentFeedName());
		dbOperations = null;
		this.isRecordsInserted = true;
	}

	public void executeReadRecordsFromHBaseToPig() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		this.isRecordsScaned = false;
		boolean result = false;
		String hbaseHostName = GdmUtils.getConfiguration("testconfig.TestWatchForDataDrop.hbaseMasterHostName").trim();
		String command = "ssh " + hbaseHostName + "  \"" +  this.getPathCommand() + ";"  + this.getKinitCommand() + ";pig -x mapreduce " + this.getHbaseExecutePigScriptLocation() + "/HBaseScanTable.pig\"";
		TestSession.logger.info("command = " + command);
		String output = this.executeCommand(command);
		List<String> scanOuputList = Arrays.asList(output.split("\n"));
		String scanResult = scanOuputList.get(scanOuputList.size() - 2);
		TestSession.logger.info("Result - " + scanResult );
		assertTrue("Expected Success! , but got " + scanResult , scanResult.indexOf("(4)") > -1);
		result = true;
		String mrJobURL = null;
		int count = 0;
		String startTime = null , endTime = null;
		for ( String item : scanOuputList ) {
			if (item.indexOf("INFO  org.apache.hadoop.mapreduce.Job - The url to track the job:") > -1) {
				List<String>  temp = Arrays.asList(item.split(" "));
				mrJobURL = temp.get(temp.size() - 1);
			}
			if (item.indexOf("HadoopVersion	PigVersion	UserId	StartedAt	FinishedAt	Features") > -1) {
				count ++;
				String tempTime = scanOuputList.get(count);
				List<String> tempList = Arrays.asList(tempTime.split("\t"));
				startTime = tempList.get(3);
				endTime = tempList.get(4);
			}
			count++;
		}
		if (result == true) {
			this.scanRecordResult = "PASS~" +  mrJobURL + "~" + startTime.trim() + "~" + endTime.trim();
		} else if (result == false) {
			this.scanRecordResult = "FAIL~" +  mrJobURL + "~" + startTime.trim() + "~" + endTime.trim();
		}
		// insert hbase scan result into db.
		DataBaseOperations dbOperations = new DataBaseOperations();
		Connection con = dbOperations.getConnection();
		dbOperations.updateRecord(con, "hbaseScan" , this.scanRecordResult , this.getCurrentFeedName());
		dbOperations = null;
		this.isRecordsScaned = true;
	}

	public void setScriptPath(String path) {
		this.scriptPath = path.trim();
	}

	private String getScriptPath() {
		return this.scriptPath;
	}

	/*
	 * set the pig script location
	 */
	private void setHbaseExecutePigScriptLocation(String path) {
		this.hbaseExecutePigScriptLocation = path;
	}

	/**
	 * Return the pig script location
	 * @return
	 */
	private String getHbaseExecutePigScriptLocation() {
		return this.hbaseExecutePigScriptLocation;
	}

	/**
	 * Modify the HBaseInsertRecord_temp.pig file content to actual path.
	 * @throws IOException
	 */
	public void modifyHBasePigFile() throws IOException {
		String  absolutePath = new File("").getAbsolutePath();
		TestSession.logger.info("AbsolutePath = " + absolutePath );
		File filePath = new File(absolutePath + "/resources/stack_integration/");
		if (filePath.exists()) {
			ConsoleHandle consoleHandle = new ConsoleHandle();
			String clusterName = GdmUtils.getConfiguration("testconfig.TestWatchForDataDrop.clusterName");
			String nameNode_Name = consoleHandle.getClusterNameNodeName(clusterName);
			String fileContent = new String(readAllBytes(get(filePath + "/HBaseInsertRecord_temp.pig")));
			fileContent = fileContent.replaceAll("NAME_NODE_NAME", "hdfs://" + nameNode_Name + ":8020");
			fileContent = fileContent.replaceAll("FILEPATH", this.getDataPath());

			String newPigScriptFilePath = filePath + "/HBaseInsertRecord.pig";
			File file = new File(newPigScriptFilePath);
			if (file.exists()) {
				TestSession.logger.info(newPigScriptFilePath + "  already exists.");
				if (file.delete() == true) {
					TestSession.logger.info(newPigScriptFilePath + " file deleted successfully **** ");
					java.nio.file.Files.write(java.nio.file.Paths.get(newPigScriptFilePath), fileContent.getBytes());
					TestSession.logger.info("Successfully " + newPigScriptFilePath + " created. *************");
				} else {
					TestSession.logger.info("Failed to delete " + newPigScriptFilePath);	
				}
			} else {
				TestSession.logger.info(newPigScriptFilePath + " does not exists");
				java.nio.file.Files.write(java.nio.file.Paths.get(newPigScriptFilePath), fileContent.getBytes());
			}
		} else {
			TestSession.logger.info(filePath + " file does not exists.");
		}
	}

	/**
	 * copy hbase pig scripts to hbase host.
	 */
	public void copyHBasePigScriptToHBaseMasterHost() {
		String  absolutePath = new File("").getAbsolutePath();
		File hbaseInsertRecordFilePath = new File(absolutePath + "/resources/stack_integration/HBaseInsertRecord.pig");
		File hbaseScanTableFilePath = new File(absolutePath + "/resources/stack_integration/HBaseScanTable.pig");
		if (hbaseInsertRecordFilePath.exists() && hbaseScanTableFilePath.exists()) {

			// create the folder with timestamp
			SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmm");
			Calendar calendar = Calendar.getInstance();
			Date d = new Date();
			this.setHbaseExecutePigScriptLocation("/tmp/IntegrationTestingHBasePigScript_" + simpleDateFormat.format(d));;
			TestSession.logger.info("hbasePigScriptLocation  = " + this.getHbaseExecutePigScriptLocation());

			// copy the pig script to hbase master
			String hbaseHostName = GdmUtils.getConfiguration("testconfig.TestWatchForDataDrop.hbaseMasterHostName").trim();
			String hbasePigScriptFileName = hbaseInsertRecordFilePath.toString();

			// make directory in hbase master to copy the hbase pig script
			String mkdirCommand = "ssh " + hbaseHostName + " mkdir  " + this.getHbaseExecutePigScriptLocation();
			String mkdirCommandResult = this.executeCommand(mkdirCommand);
			String scpCommand = "scp " + absolutePath +"/resources/stack_integration/*.pig" + "  " +  hbaseHostName + ":" + this.getHbaseExecutePigScriptLocation();
			this.executeCommand(scpCommand);
		} else {
			fail("Either " + hbaseScanTableFilePath.toString()  + "  or " + hbaseScanTableFilePath.toString() + "  file is missing.");
		}
	}

	/**
	 * Execute a given command and return the output of the command.
	 * @param command
	 * @return
	 */
	public String executeCommand(String command) {
		String output = null;
		TestSession.logger.info("command - " + command);
		ImmutablePair<Integer, String> result = SystemCommand.runCommand(command);
		if ((result == null) || (result.getLeft() != 0)) {
			if (result != null) { 
				// save script output to log
				TestSession.logger.info("Command exit value: " + result.getLeft());
				TestSession.logger.info(result.getRight());
			}
			throw new RuntimeException("Exception" );
		} else {
			output = result.getRight();
			TestSession.logger.info("log = " + output);
		}
		return output;
	}
}