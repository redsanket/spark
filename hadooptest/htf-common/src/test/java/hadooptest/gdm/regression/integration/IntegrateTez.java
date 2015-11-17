package hadooptest.gdm.regression.integration;

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

public class IntegrateTez  {

	private String tezScriptPath;
	private String tezVersion;
	private String dataPath;
	private String currentFeedName;
	private String tezScriptLocation;
	private boolean tezTested;
	private String tezHostname;
	private final static String kINIT_COMMAND = "kinit -k -t /homes/dfsload/dfsload.dev.headless.keytab dfsload@DEV.YGRID.YAHOO.COM";
	public static final String INTEGRATION_JAR="/tmp/integration_test_files/lib/*.jar";
	public static final String PIG_HOME = "/home/y/share/pig";
	public static final String TEZ_HOME = "/home/gs/tez/current/";
	public static final String TEZ_CONF_DIR ="/home/gs/conf/tez/current";
	public static final String TEZ_HEALTH_CHECKUP = "tez_health";
	public static final String TEZ_RESULT = "tez_result";
	public static final String HADOOP_HOME="/home/gs/hadoop/current";
	
	public IntegrateTez() {
		String clusterName = GdmUtils.getConfiguration("testconfig.TestWatchForDataDrop.clusterName").trim();
		String command = "yinst range -ir \"(@grid_re.clusters."+ clusterName  +".gateway)\"";
		String tezHName = this.executeCommand(command).trim();
		TestSession.logger.info("Tez hostname -  " + tezHName);
		this.tezHostname = tezHName;
	}
	
	public IntegrateTez(String currentFeedName , String dataPath) {
		this.currentFeedName = currentFeedName;
		this.dataPath = dataPath;
	}
	
	private void setTezScriptPath(String tezScriptPath) {
		this.tezScriptPath = tezScriptPath.trim();
	}
	
	private String getTezScriptPath() {
		return this.tezScriptPath;
	}
	
	public void setTezVersion(String tezVersion) {
		this.tezVersion = tezVersion;
	}
	
	public String getTezVersion() {
		return this.tezVersion;
	}
	
	public void setDataPath(String dataPath) {
		this.dataPath = dataPath;
	}

	private String getDataPath() {
		return this.dataPath;
	}
	
	private void setTezTested(boolean isTezTested) {
		this.tezTested = isTezTested;
	}
	public boolean isTezTested() {
		return this.tezTested;
	}
	
	public String getKinitCommand() {
		String kinitCommand = kINIT_COMMAND;
		TestSession.logger.info("kinit command - " + kinitCommand);
		return kinitCommand;
	}

	public String getPathCommand() {
		String pathCommand = "export PATH=$PATH:" + HADOOP_HOME + ":" +PIG_HOME + ":" + TEZ_HOME + "/bin" + ":" + INTEGRATION_JAR + ":" + TEZ_HOME + ":" +  TEZ_CONF_DIR;
		TestSession.logger.info("export path value - " + pathCommand);
		return pathCommand.trim();
	}

	public boolean getTezHealthCheck() {
		boolean flag = false;
		//String gateWayHostName = GdmUtils.getConfiguration("testconfig.TestWatchForDataDrop.gateWayName").trim();
		String command = "ssh " + this.tezHostname + " ls -t " + TEZ_HOME + "tez-api-*";
		String logOutput = this.executeCommand(command, "Tez_State");
		List<String> logOutputList = Arrays.asList(logOutput.split("\n"));
		for ( String log : logOutputList) {
			if (log.startsWith(TEZ_HOME) == true ) {
				String temp = TEZ_HOME + "/tez-api-";
				String version = log.substring( temp.length() - 1, log.length()).replace(".jar", "");
				this.setTezVersion(version);
				flag = true;
				break;
			}
		}
		return flag;
	}
	
	public void setCurrentFeedName(String feedName) {
		this.currentFeedName = feedName;
	}

	public String getCurrentFeedName() {
		return this.currentFeedName;
	}
	
	public void executeTez() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		boolean result = false;
		//String gateWayHostName = GdmUtils.getConfiguration("testconfig.TestWatchForDataDrop.gateWayName").trim();
		String command = "ssh " + this.tezHostname + "  \"" +  this.getPathCommand() + ";"  + this.getKinitCommand() + ";pig -x tez " + this.getTezScriptPath() + "/TezTestCase.pig\"";
		TestSession.logger.info("command = " + command);
		String output = this.executeCommand(command , "tez");
		List<String> insertOutputList = Arrays.asList(output.split("\n"));
		String mrJobURL = null;
		int count = 0;
		String startTime = null , endTime = null;
		for ( String item : insertOutputList ) {
			if (item.trim().indexOf("INFO  org.apache.tez.client.TezClient - The url to track the Tez Session:") > -1) {
				int startIndex = item.indexOf("The url to track the Tez Session:") + "The url to track the Tez Session:".length() ;
				mrJobURL = item.substring(startIndex , item.length()).trim();
				TestSession.logger.info("mrJobURL = " + mrJobURL);
			}
			if (item.trim().startsWith("StartedAt") == true) {
				startTime = Arrays.asList(item.split("t:")).get(1).trim();
			}
			if (item.trim().startsWith("FinishedAt") == true) {
				endTime = Arrays.asList(item.split("t:")).get(1).trim();
			}
			if (item.trim().startsWith("Success!")) {
				result=true;
			}
		}
		String insertRecordResult = null;
		if (result == true) {
			insertRecordResult = "PASS~" +  mrJobURL + "~" + startTime.trim() + "~" + endTime.trim();
		} else if (result == false) {
			insertRecordResult = "FAIL~" +  mrJobURL + "~" + startTime.trim() + "~" + endTime.trim();
		}
		this.updateHBaseResultIntoDB( "tez" , insertRecordResult , this.getCurrentFeedName());
		this.setTezTested(true);
	}
	
	/**
	 * copy hbase pig scripts to hbase host.
	 */
	public void copyTezScriptToHBaseMasterHost() {
		String  absolutePath = new File("").getAbsolutePath();
		File tezFilePath = new File(absolutePath + "/resources/stack_integration/TezTestCase.pig");
		
		if ( tezFilePath.exists() ) {

			// create the folder with timestamp
			SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmm");
			Calendar calendar = Calendar.getInstance();
			Date d = new Date();
			this.setTezScriptPath("/tmp/IntegrationTestingTezScript_" + simpleDateFormat.format(d));;
			TestSession.logger.info("tezFilePath  = " + this.getTezScriptPath());

			// copy the pig script to hbase master
			//String gateWayHostName = GdmUtils.getConfiguration("testconfig.TestWatchForDataDrop.gateWayName").trim();

			// make directory in hbase master to copy the hbase pig script
			String mkdirCommand = "ssh " + this.tezHostname + " mkdir  " + this.getTezScriptPath();
			String mkdirCommandResult = this.executeCommand(mkdirCommand , "dummyValue");
			String scpCommand = "scp " + absolutePath +"/resources/stack_integration/TezTestCase.pig" + "  " +  this.tezHostname + ":" + this.getTezScriptPath();
			this.executeCommand(scpCommand , "dummyValue");
		} else {
			fail("Either " + tezFilePath.toString()  + "  file is missing.");
		}
	}

	/**
	 * Execute a given command and return the output of the command.
	 * @param command
	 * @return
	 */
	public String executeCommand(String command , String tezType) {
		String output = null;
		TestSession.logger.info("command - " + command);
		ImmutablePair<Integer, String> result = SystemCommand.runCommand(command);
		if ((result == null) || (result.getLeft() != 0)) {
			if (result != null) { 
				// save script output to log
				TestSession.logger.info("Command exit value: " + result.getLeft());
				TestSession.logger.info(result.getRight());
			}
			// if for some reason hbase master is down the operations fails and we need to update the results.
			if (tezType.equals(TEZ_HEALTH_CHECKUP) ) {
			//	this.updateHBaseResultIntoDB(hbaseOperationType, "FAIL~JOB_DID_NOT_STARTED~START_TIME~END_TIME", this.getCurrentFeedName());
				;
			} else if (tezType.equals(TEZ_RESULT)) {
				this.updateHBaseResultIntoDB("tez", "FAIL~JOB_DID_NOT_STARTED~START_TIME~END_TIME", this.getCurrentFeedName());
			}
			throw new RuntimeException("Exception" );
		} else {
			output = result.getRight();
			TestSession.logger.info("log = " + output);
		}
		return output;
	}
	
	/**
	 * update hbase results into database.
	 * @param colunmName
	 * @param result
	 * @param feedName
	 */
	public void updateHBaseResultIntoDB(String colunmName, String result , String feedName) {
		DataBaseOperations dbOperations = new DataBaseOperations();
		try {
			Connection con = dbOperations.getConnection();
			dbOperations.updateRecord(con, colunmName, result , feedName);
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch(IllegalAccessException e) {
			e.printStackTrace();
		}catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch(SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Modify the HBaseInsertRecord_temp.pig file content to actual path.
	 * @throws IOException
	 */
	public void modifyTezFile() throws IOException {
		String  absolutePath = new File("").getAbsolutePath();
		TestSession.logger.info("AbsolutePath = " + absolutePath );
		File filePath = new File(absolutePath + "/resources/stack_integration/");
		if (filePath.exists()) {
			ConsoleHandle consoleHandle = new ConsoleHandle();
			String clusterName = GdmUtils.getConfiguration("testconfig.TestWatchForDataDrop.clusterName");
			String nameNode_Name = consoleHandle.getClusterNameNodeName(clusterName);
			String fileContent = new String(readAllBytes(get(filePath + "/TezTestCase_temp.pig")));
			fileContent = fileContent.replaceAll("NAME_NODE_NAME", "hdfs://" + nameNode_Name + ":8020");
			fileContent = fileContent.replaceAll("FILEPATH", this.getDataPath());

			String newPigScriptFilePath = filePath + "/TezTestCase.pig";
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
