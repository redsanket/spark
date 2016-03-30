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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;

public class IntegrateHive  /*implements Runnable*/ {
	private String hiveHostName;
	private String hiveHealthCheckupStatus;
	private String hiveVersion;
	private String hiveScriptLocation;
	private String hcatVersion;
	private String initialCommand;
	private String dataPath;
	private String pigVersion;
	private ConsoleHandle consoleHandle;
	private boolean tableDropped = false;
	private boolean tableCreated = false;
	private boolean dataLoadedToHive = false;
	private String currentFeedName;
	private String hdfsHivePath;
	private String nameNodeHostName;
	private static final String PROCOTOL = "hdfs://";
	private final static String HADOOP_HOME="export HADOOP_HOME=/home/gs/hadoop/current";
	private final static String JAVA_HOME="export JAVA_HOME=/home/gs/java/jdk64/current/";
	private final static String HADOOP_CONF_DIR="export HADOOP_CONF_DIR=/home/gs/conf/current";
	private final static String KNITI = "kinit -k -t /homes/dfsload/dfsload.dev.headless.keytab dfsload@DEV.YGRID.YAHOO.COM";
	private final static String HADOOPQA_KINIT_COMMAND = "kinit -k -t /homes/hadoopqa/hadoopqa.dev.headless.keytab hadoopqa@DEV.YGRID.YAHOO.COM";
	private final static String HIVE_VERSION_COMMAND = "hive --version";
	public static final String PIG_HOME = "export PIG_HOME=/home/y/share/pig";
	private final static String PATH_COMMAND = "export PATH=$PATH:";

	public IntegrateHive() {
		String clusterName = GdmUtils.getConfiguration("testconfig.TestWatchForDataDrop.clusterName").trim();
		String command = "yinst range -ir \"(@grid_re.clusters."+ clusterName  +".hive)\"";
		String hName = this.executeCommand(command).trim();
		TestSession.logger.info("Hive hostname -  " + hName);
		this.hiveHostName = hName;
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmm");
		Calendar calendar = Calendar.getInstance();
		Date d = new Date();
		this.setHiveScriptLocation("/tmp/IntegrationTestingHiveScript_" + simpleDateFormat.format(d));
		this.initialCommand = "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null  " + this.hiveHostName + "  \"" +HADOOP_HOME + ";" + JAVA_HOME + ";" +  HADOOP_CONF_DIR + ";"  + HADOOPQA_KINIT_COMMAND  + ";" ;
		this.consoleHandle = new ConsoleHandle();
		
		String nameNodeHostNameCommand =  "yinst range -ir \"(@grid_re.clusters." + clusterName + ".namenode)\"";
		String nameNodeName =  this.executeCommand(nameNodeHostNameCommand).trim();
		this.setClusterNameNodeName(nameNodeName);
	}
	
	public IntegrateHive(String currentFeedName , String dataPath ) { 
		this.currentFeedName = currentFeedName;
		this.dataPath = dataPath;
		
		String clusterName = GdmUtils.getConfiguration("testconfig.TestWatchForDataDrop.clusterName").trim();
		String command = "yinst range -ir \"(@grid_re.clusters."+ clusterName  +".hive)\"";
		String hName = this.executeCommand(command).trim();
		TestSession.logger.info("Hive hostname -  " + hName);
		this.hiveHostName = hName;
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmm");
		Calendar calendar = Calendar.getInstance();
		Date d = new Date();
		this.setHiveScriptLocation("/tmp/IntegrationTestingHiveScript_" + simpleDateFormat.format(d));
		this.initialCommand = "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null  " + this.hiveHostName + "  \"" +HADOOP_HOME + ";" + JAVA_HOME + ";" +  HADOOP_CONF_DIR + ";"  + HADOOPQA_KINIT_COMMAND  + ";" ;
		this.consoleHandle = new ConsoleHandle();
		
		String nameNodeHostNameCommand =  "yinst range -ir \"(@grid_re.clusters." + clusterName + ".namenode)\"";
		String nameNodeName =  this.executeCommand(nameNodeHostNameCommand).trim();
		this.setClusterNameNodeName(nameNodeName);
	}
	
	public void setClusterNameNodeName(String nameNodeName) {
		this.nameNodeHostName = nameNodeName.trim();
	}
	
	public String getClusterNameNodeName() {
		return this.nameNodeHostName;
	}
	
	/**
	 * Returns the remote cluster configuration object.
	 * @param aUser  - user
	 * @param nameNode - name of the cluster namenode. 
	 * @return
	 * @throws IOException 
	 */
	public Configuration getConfForRemoteFS() throws IOException {
		Configuration conf = new Configuration(true);
		String namenodeWithChangedSchemaAndPort = this.PROCOTOL + this.getClusterNameNodeName();
		TestSession.logger.info("For HDFS set the namenode to:[" + namenodeWithChangedSchemaAndPort + "]");
		conf.set("fs.defaultFS", namenodeWithChangedSchemaAndPort);
		conf.set("hadoop.security.authentication", "Kerberos");
		UserGroupInformation.setConfiguration(conf);
		UserGroupInformation.loginUserFromKeytab(HadooptestConstants.UserNames.DFSLOAD, HadooptestConstants.Location.Keytab.DFSLOAD);
		TestSession.logger.info(conf);
		return conf;
	}
	
	public void checkForHiveDataFolderAndDelete() throws IOException {
		this.hdfsHivePath = "hdfs://" + this.getClusterNameNodeName() + "/" + this.getDataPath() + "/hiveData" ;
		TestSession.logger.info("this.hdfsHivePath  " + this.hdfsHivePath );
		if (this.isPathExists(this.hdfsHivePath) == true) {
			TestSession.logger.info( this.hdfsHivePath + " path does exists.");
			if (this.deletePath(this.hdfsHivePath) == true) {
				TestSession.logger.info( this.hdfsHivePath + " path is deleted successfully.");
			} else {
				TestSession.logger.info(" failed to delete " + this.hdfsHivePath );
			}
		} else {
			TestSession.logger.info( this.hdfsHivePath + " path does not exists.");
		}
	}
	
	/**
	 * Delete the given path
	 * @param path path to be deleted
	 * @return
	 * @throws IOException
	 */
	public boolean deletePath(String dataPath) throws IOException {
		boolean isPathDeleted = false;
		Configuration configuration = this.getConfForRemoteFS();
		FileSystem hdfsFileSystem = FileSystem.get(configuration);
		if (hdfsFileSystem != null) {
			if (isPathExists(dataPath)) {
				Path path = new Path(dataPath);
				isPathDeleted =  hdfsFileSystem.delete(path, true);
				if (isPathDeleted == true) {
					TestSession.logger.info(dataPath + " is deleted successfully");
				} else {
					TestSession.logger.info("Failed to delete " + dataPath);
				}
			}
		} else {
			TestSession.logger.error("Failed to instance of FileSystem ");
		}
		return isPathDeleted;
	}
	/**
	 * Check whether the given path exists.
	 * @param path
	 * @return
	 * @throws IOException
	 */
	public  boolean isPathExists(String dataPath) throws IOException {
		boolean flag = false;
		Configuration configuration = this.getConfForRemoteFS();
		FileSystem hdfsFileSystem = FileSystem.get(configuration);
		if (hdfsFileSystem != null) {
			Path path = new Path(dataPath);
			if (hdfsFileSystem.exists(path)) {
				flag = true;
			} else {
				TestSession.logger.info(path.toString() + " path does not exists.");
			}
		} else {
			TestSession.logger.error("Failed to create an instance of ");
		}
		return flag;
	}
	
/*	@Override
	public void run() {
		this.getHiveHealthCheckup();
		//integrateHiveObj.setDataPath(this.getCurrentFeedBasePath());
		//integrateHiveObj.setCurrentFeedName(this.currentFeedName);
		TestSession.logger.info("******************************************** started hive and hcat *****************************************************");
		try {
			this.setPigVersion(this.getPigVersion());
			this.modifyPigFile();
			this.modifyLoadDataIntoHiveScript();
			this.modifyFetchDataUsingHCatalogFile();
			this.copyHiveFileToHiveServer();
			this.dropExistingHiveTable();
			this.createHiveTable();
			this.copyDataFromSourceToHiveServer();
			this.loadDataIntoHive();
			this.fetchDataUsingHCat();
			this.cleanUp();
		} catch (IOException e ) {
			e.printStackTrace();
		}
	}*/
	
	public void setCurrentFeedName(String feedName) {
		this.currentFeedName = feedName;
	}

	public String getCurrentFeedName() {
		return this.currentFeedName;
	}
	
	public  void setPigVersion(String pigVersion) {
		this.pigVersion = pigVersion;
	}
	
	public String getPigVersion() {
		return this.pigVersion;
	}
	
	public boolean getHiveHealthCheckup() {
		boolean flag = false;
		this.setHiveHealthCheckupStatu("DOWN");
		this.setHiveVersion("0.0");
		String command =  this.initialCommand + HIVE_VERSION_COMMAND  + "\" ";
		TestSession.logger.info("command = " + command);
		String output = this.executeCommand(command);
		List<String> outputList = Arrays.asList(output.split("\n"));
		for ( String str : outputList) {
			if (str.startsWith("Hive") ) {
				this.hiveVersion = Arrays.asList(str.split(" ")).get(1).trim();
				this.setHiveVersion(this.hiveVersion);
				this.setHiveHealthCheckupStatu("ACTIVE");
				flag = true;
				break;
			}
		}
		return flag;
	}
	
	private void setHiveScriptLocation(String hiveScriptLocation) {
		this.hiveScriptLocation = hiveScriptLocation;
	}
	
	private String getHiveScriptLocation(){
		return this.hiveScriptLocation;
	}
	
	private void setHiveHealthCheckupStatu(String hiveStatus) {
		this.hiveHealthCheckupStatus = hiveStatus;
	}

	public String getHiveHealthCheckupStatus() {
		return this.hiveHealthCheckupStatus;
	}
	
	public void setHiveVersion(String hiveVersion) {
		this.hiveVersion = hiveVersion;
	}
	
	public String getHiveVersion() {
		return this.hiveVersion;
	}
	
	public void copyHiveFileToHiveServer() {
		String  absolutePath = new File("").getAbsolutePath();
		/**
		 * TODO : change the file name hiveCreateTableAndLoadData.hql
		 */
		File hiveDropTableScript = new File(absolutePath + "/resources/stack_integration/hive/HiveDropTable.hql");
		File hiveCreateTableScript = new File(absolutePath + "/resources/stack_integration/hive/HiveCreateTable.hql");
		if (hiveDropTableScript.exists() && hiveCreateTableScript.exists()) {
			
			
			TestSession.logger.info("hive PigScriptLocation  = " + this.getHiveScriptLocation());
			String mkdirCommand = "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null  "  + this.hiveHostName + " mkdir  " + this.getHiveScriptLocation();
			String mkdirOuput = this.executeCommand(mkdirCommand);
			String scpCommand = "scp   -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null  " + absolutePath +"/resources/stack_integration/hive/*.*" + "  " +  this.hiveHostName + ":" + this.getHiveScriptLocation();
			String scpCommandOutput = this.executeCommand(scpCommand);
			String scpCommand1 = "scp   -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null  " + absolutePath +"/resources/stack_integration/lib/*.*" + "  " +  this.hiveHostName + ":" + this.getHiveScriptLocation();
			String scpCommandOutput1 = this.executeCommand(scpCommand1);
 		} else {
			fail(absolutePath + "/resources/stack_integration/hive/hiveCreateTableAndLoadData.hql" + "  file does not exists. Hence Hive Stack Component cannot be tested.");
		}
	}
	
	/**
	 * update hbase results into database.
	 * @param colunmName
	 * @param result
	 * @param feedName
	 */
	public void updateHiveResultIntoDB(String colunmName, String result , String feedName) {
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
	
	public void dropExistingHiveTable() {
		String executeDropTableCommand = this.initialCommand + " hive -f " + this.getHiveScriptLocation() + "/HiveDropTable.hql"  + "\" " ;
		TestSession.logger.info("executeDropTableCommand  = " + executeDropTableCommand);
		String output = this.executeCommand(executeDropTableCommand);
		List<String> dropTableOutputList = Arrays.asList(output.split("\n"));
		boolean flag = false;
		for (String str : dropTableOutputList ) {
			if (str.startsWith("OK")) {
				flag = true;
				break;
			}
		}
		this.tableDropped = flag;
		if (flag == true) {
			this.updateHiveResultIntoDB("hiveTableDeleted", "PASS" , this.getCurrentFeedName());
		} else if ( flag == false) {
			this.updateHiveResultIntoDB("hiveTableDeleted", "FAIL" , this.getCurrentFeedName());
		}
	}
	
	public boolean isTableDropped() {
		return this.tableDropped;
	}
	
	public void cleanUp() {
		String rmWorkingdirCommand = "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null  "  + this.hiveHostName + " rm -rf  " + this.getHiveScriptLocation();
		String scpCommandOutput1 = this.executeCommand(rmWorkingdirCommand);
	}
	
	public void createHiveTable() {
		String executeCreateTableCommand = this.initialCommand + " hive -f " + this.getHiveScriptLocation() + "/HiveCreateTable.hql" + "\" " ;
		TestSession.logger.info("executeDropTableCommand  = " + executeCreateTableCommand);
		String output = this.executeCommand(executeCreateTableCommand);
		List<String> dropTableOutputList = Arrays.asList(output.split("\n"));
		boolean flag = false;
		for (String str : dropTableOutputList ) {
			if (str.startsWith("OK")) {
				flag = true;
			}
		}
		this.tableCreated = flag;
		if (flag == true) {
			this.updateHiveResultIntoDB("hiveTableCreate", "PASS" , this.getCurrentFeedName());
		} else if (flag == true) {
			this.updateHiveResultIntoDB("hiveTableCreate", "FAIL" , this.getCurrentFeedName());
		}
	}
	
	public boolean isTableCreated() {
		return this.tableCreated;
	}
	
	public void modifyPigFile() throws IOException {
		String  absolutePath = new File("").getAbsolutePath();
		File copySourceDataFilePath = new File(absolutePath + "/resources/stack_integration/hive/CopyDataFromSourceToHiveCluster_temp.pig");
		File filePath = new File(absolutePath + "/resources/stack_integration/hive");
		TestSession.logger.info("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ filePath  = " + filePath);
		if (copySourceDataFilePath.exists()) {

			String clusterName = GdmUtils.getConfiguration("testconfig.TestWatchForDataDrop.clusterName");
			String nameNode_Name = this.consoleHandle.getClusterNameNodeName(clusterName);
			String fileContent = new String(readAllBytes(get(filePath + "/CopyDataFromSourceToHiveCluster_temp.pig")));
			fileContent = fileContent.replace("SCRIPT_PATH", this.getHiveScriptLocation());
			fileContent = fileContent.replaceAll("NAME_NODE_NAME", "hdfs://" + nameNode_Name + ":8020");
			fileContent = fileContent.replaceAll("FILEPATH", this.getDataPath());
			TestSession.logger.info("fileContent  = " + fileContent);
			
			String newPigScriptFilePath = filePath + "/CopyDataFromSourceToHiveCluster.pig";
			File file = new File(newPigScriptFilePath);
			if (file.exists()) {
				TestSession.logger.info(newPigScriptFilePath + "  already exists.");
				if (file.delete() == true) {
					TestSession.logger.info(newPigScriptFilePath + " file deleted successfully **** ");
					java.nio.file.Files.write(java.nio.file.Paths.get(newPigScriptFilePath), fileContent.getBytes());
					TestSession.logger.info("Successfully " + newPigScriptFilePath + " created. *************");
				} else {
					TestSession.logger.error("Failed to delete " + newPigScriptFilePath);	
				}
			} else {
				TestSession.logger.info(newPigScriptFilePath + " does not exists");
				java.nio.file.Files.write(java.nio.file.Paths.get(newPigScriptFilePath), fileContent.getBytes());
				File f = new File(newPigScriptFilePath);
				if (f.exists() == true) {
					TestSession.logger.info(newPigScriptFilePath + "  created successfully......................");
				}
			}
			
		} else {
			fail(copySourceDataFilePath.toString() + "  file does not exists. Hence Hive Component cannot be tested.");
		}
	}
	
	
	public synchronized void modifyFetchDataUsingHCatalogFile() throws IOException {
		String absolutePath = new File("").getAbsolutePath();
		File copySourceFilePath = new File(absolutePath + "/resources/stack_integration/hive/FetchHiveDataUsingHCatalog_temp.pig");
		File filePath = new File(absolutePath + "/resources/stack_integration/hive");
		if (copySourceFilePath.exists()) {
			String fileContent = new String(readAllBytes(get(filePath + "/FetchHiveDataUsingHCatalog_temp.pig")));
			fileContent = fileContent.replace("PIG_VERSION", this.getPigVersion().trim());
			TestSession.logger.info("fileContent  = " + fileContent);
			
			String newPigScriptFilePath = filePath + "/FetchHiveDataUsingHCatalog.pig";
			File file = new File(newPigScriptFilePath);
			if (file.exists()) {
				TestSession.logger.info(newPigScriptFilePath + "  already exists.");
				if (file.delete() == true) {
					TestSession.logger.info(newPigScriptFilePath + " file deleted successfully **** ");
					java.nio.file.Files.write(java.nio.file.Paths.get(newPigScriptFilePath), fileContent.getBytes());
					TestSession.logger.info("Successfully " + newPigScriptFilePath + " created. *************");
				} else {
					TestSession.logger.error("Failed to delete " + newPigScriptFilePath);	
				}
			}else {
				TestSession.logger.info(newPigScriptFilePath + " does not exists");
				java.nio.file.Files.write(java.nio.file.Paths.get(newPigScriptFilePath), fileContent.getBytes());
				File f = new File(newPigScriptFilePath);
				if (f.exists() == true) {
					TestSession.logger.info(newPigScriptFilePath + "  created successfully......................");
				}
			}
		} else {
			fail(copySourceFilePath.toString() + "  file does not exists. Hence Hive Component cannot be tested.");
		}
	}

	public synchronized void modifyLoadDataIntoHiveScript() throws IOException {
		String  absolutePath = new File("").getAbsolutePath();
		File loadDataToHiveScriptPath = new File(absolutePath + "/resources/stack_integration/hive/LoadDataToHive_temp.hql");
		if (loadDataToHiveScriptPath.exists()) {
			String clusterName = GdmUtils.getConfiguration("testconfig.TestWatchForDataDrop.clusterName");
			String nameNode_Name = this.consoleHandle.getClusterNameNodeName(clusterName);
			String fileContent = new String(readAllBytes(get(loadDataToHiveScriptPath.toString())));
			
			// Remove the port number after HADOOPPF-10338 is fixed
			//fileContent = fileContent.replaceAll("NAME_NODE_NAME", "hdfs://" + nameNode_Name + ":8020");
			fileContent = fileContent.replaceAll("NAME_NODE_NAME", "hdfs://" + nameNode_Name );
			fileContent = fileContent.replaceAll("FILEPATH", this.getDataPath());
			
			TestSession.logger.info( loadDataToHiveScriptPath.toString() + "  fileContent = " + fileContent);
			String newFilePath = absolutePath + "/resources/stack_integration/hive/LoadDataToHive.hql";
			File file = new File(newFilePath);
			if (file.exists()) {
				TestSession.logger.info(newFilePath + "  already exists.");
				if (file.delete() == true) {
					TestSession.logger.info(newFilePath + " file deleted successfully **** ");
					java.nio.file.Files.write(java.nio.file.Paths.get(newFilePath), fileContent.getBytes());
					TestSession.logger.info("Successfully " + newFilePath + " created. *************");
				} else {
					TestSession.logger.error("Failed to delete " + newFilePath);	
				}
			} else {
				TestSession.logger.info(newFilePath + " does not exists");
				java.nio.file.Files.write(java.nio.file.Paths.get(newFilePath), fileContent.getBytes());
				File f = new File(newFilePath);
				if (f.exists() == true) {
					TestSession.logger.info(newFilePath + "  created successfully......................");
				}
			}
		} else {
			fail(loadDataToHiveScriptPath.toString() + "  file does not exists. Hence Hive Component cannot be tested.");
		}
	}
	
	public synchronized void loadDataIntoHive() {
		String executeLoadDataIntoHiveCommand = this.initialCommand + " hive -f " + this.getHiveScriptLocation() + "/LoadDataToHive.hql" + "\" " ;
		String output = this.executeCommand(executeLoadDataIntoHiveCommand);
		List<String> dropTableOutputList = Arrays.asList(output.split("\n"));
		boolean flag = false;
		for (String str : dropTableOutputList ) {
			if (str.startsWith("OK")) {
				flag = true;
			}
		}
		this.dataLoadedToHive = flag;
		if (flag == true) {
			this.updateHiveResultIntoDB("hiveLoadData", "PASS" , this.getCurrentFeedName());
		} else if ( flag == false) {
			this.updateHiveResultIntoDB("hiveLoadData", "FAIL" , this.getCurrentFeedName());
		}
	}
	
	public synchronized void fetchDataUsingHCat() {
		String command =  this.initialCommand  + this.PIG_HOME + ";"  + PATH_COMMAND + "; pig -useHCatalog -x mapreduce " + this.getHiveScriptLocation() + "/FetchHiveDataUsingHCatalog.pig" + "\" " ;
		TestSession.logger.info("command   = " + command);
		String  absolutePath = new File("").getAbsolutePath();
		File file = new File(absolutePath + "/resources/stack_integration/hive/FetchHiveDataUsingHCatalog.pig");
		if (file.exists() == true) {
			String output = this.executeCommand(command);
			List<String> fetchDataOutputList = Arrays.asList(output.split("\n"));
			int count=0;
			String mrJobURL = "";
			String startTime = null , endTime = null;
			boolean flag = false;
			String result = "";
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
			if (flag == true) {
				result = "PASS~" +  mrJobURL + "~" + startTime.trim() + "~" + endTime.trim();
			} else if (flag == false) {
				result = "FAIL~" +  mrJobURL + "~" + startTime.trim() + "~" + endTime.trim();
			}
			this.updateHiveResultIntoDB("hcat", result , this.getCurrentFeedName());
			TestSession.logger.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
			
		} else {
			TestSession.logger.info(file.toString() + " does not exists..");
		}
	}
	
	public boolean isHCatDeployed() {
		String command = "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null " + this.hiveHostName + "  \" " + "  yinst ls | grep hcat " + "\" " ;
		String output = this.executeCommand(command);
		List<String> outputList = Arrays.asList(output.split("\n"));
		boolean flag = false;
		for (String str : outputList) {
			if (str.startsWith("hcat_server") ) {
				String hVersion = Arrays.asList(str.split("-")).get(1).trim();
				this.setHCatVersion(hVersion);
				flag = true;
			}
		}
		return flag;
	}
	
	public void setHCatVersion(String hcatVersion) {
		this.hcatVersion = hcatVersion;
	}
	
	public String getHCatVersion(){
		return this.hcatVersion;
	}
	
	public void copyDataFromSourceToHiveServer() throws IOException {
		String command = this.initialCommand + this.PIG_HOME + ";" + PATH_COMMAND + "; pig -x mapreduce  "+  this.getHiveScriptLocation() +"/CopyDataFromSourceToHiveCluster.pig" + "\" ";
		TestSession.logger.info("command = " + command);
		String output = this.executeCommand(command);
		TestSession.logger.info(output);
	}
	
	public void setDataPath(String dataPath) {
		this.dataPath= dataPath;
	}
	
	public String getDataPath() {
		return this.dataPath;
	}
	
	private boolean isHiveTableDropped() {
		return this.tableDropped;
	}
	
	private boolean isHiveTableCreated() {
		return this.tableCreated;
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
			this.setHiveHealthCheckupStatu("DOWN");
			throw new RuntimeException("Exception" );
		} else {
			output = result.getRight();

			TestSession.logger.info("log = " + output);
		}
		return output;
	}
}
