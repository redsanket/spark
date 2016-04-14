package hadooptest.gdm.regression.stackIntegration.tests.hive;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import hadooptest.TestSession;
import hadooptest.gdm.regression.stackIntegration.StackComponent;
import hadooptest.gdm.regression.stackIntegration.lib.CommonFunctions;

public class TestCopyDataToHive {
	
	private StackComponent stackComponent;
	private CommonFunctions commonFunctions;
	private String hostName;
	private String nameNodeName;
	private String currentMinute;
	private String errorMessage;
	private String initCommand;
	private String hdfsHivePath;
	private Configuration configuration;
	
	public Configuration getConfiguration() {
		return configuration;
	}

	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	public String getHdfsHivePath() {
		return hdfsHivePath;
	}

	public void setHdfsHivePath(String hdfsHivePath) {
		this.hdfsHivePath = hdfsHivePath;
	}

	public String getInitCommand() {
		return initCommand;
	}

	public void setInitCommand(String initCommand) {
		this.initCommand = initCommand;
	}

	public String getErrorMessage() {
		return errorMessage;
	}

	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}

	public String getCurrentMinute() {
		return currentMinute;
	}

	public void setCurrentMinute(String currentMinute) {
		this.currentMinute = currentMinute;
	}

	public String getNameNodeName() {
		return nameNodeName;
	}

	public void setNameNodeName(String nameNodeName) {
		this.nameNodeName = nameNodeName;
	}

	public String getHostName() {
		return hostName;
	}

	public void setHostName(String hostName) {
		this.hostName = hostName;
	}

	public String getDataSetName() {
		return dataSetName;
	}

	public void setDataSetName(String dataSetName) {
		this.dataSetName = dataSetName;
	}

	private String dataSetName;
	
	public TestCopyDataToHive() {
	}
	
	public TestCopyDataToHive(StackComponent stackComponent , String hostName , String nameNodeName, String dataSetName , String initCommand) {
		this.stackComponent = stackComponent;
		this.commonFunctions = new CommonFunctions();
		this.setNameNodeName(nameNodeName);
		this.setHostName(hostName);
		this.setDataSetName(dataSetName);
		this.setInitCommand(initCommand);
	}
	
	public String getCurrentHivePath() {
		StringBuilder currentPathBuilder =  new StringBuilder("hdfs://").append( this.getNameNodeName()).append("/data/daqdev/abf/data/").append(commonFunctions.getCurrentHourPath()).append("/20130309/").append(this.getCurrentMinute()).append("/hiveData/");
		return currentPathBuilder.toString(); 
	}
	
	public void checkForHiveDataFolderAndDelete() throws IOException {
		 String path = this.getHdfsHivePath() ;
		TestSession.logger.info("this.hdfsHivePath  " + path );
		if (this.isPathExists(path) == true) {
			TestSession.logger.info( path + " path does exists.");
			if (this.deletePath(path) == true) {
				TestSession.logger.info( path + " path is deleted successfully.");
			} else {
				TestSession.logger.info(" failed to delete " + path );
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
		FileSystem hdfsFileSystem = FileSystem.get(this.getConfiguration());
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
		FileSystem hdfsFileSystem = FileSystem.get(this.getConfiguration());
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
	
	public boolean execute() throws IOException {
		TestSession.logger.info("---------------------------------------------------------------TestCopyDataToHive  start ------------------------------------------------------------------------");
		String currentDataSetName = this.commonFunctions.getDataSetName();
		this.commonFunctions.updateDB(currentDataSetName, "hiveCopyDataToHiveCurrentState", "RUNNING");
		StringBuilder currentPathBuilder =  new StringBuilder("hdfs://").append( this.getNameNodeName()).append("/data/daqdev/abf/data/").append(commonFunctions.getCurrentHourPath()).append("/hiveData");
		this.setHdfsHivePath(currentPathBuilder.toString());
		this.setCurrentMinute(commonFunctions.getCurrentHrMin());
		this.setConfiguration(this.commonFunctions.getNameConfForRemoteFS(this.getNameNodeName()));
		checkForHiveDataFolderAndDelete();
		boolean flag = false;
		String mrJobURL = null;
		String command = this.getInitCommand() + "pig -x tez "
				+ "-param \"NAMENODE_NAME=" + this.getNameNodeName() + "\""
				+ "  "
				+ "-param \"DATASET_NAME=" + commonFunctions.getCurrentHourPath() + "\""
				+ "  "
				+ this.stackComponent.getScriptLocation() + "/CopyDataFromSourceToHiveCluster_temp.pig\"";
		
		String executionResult = this.commonFunctions.executeCommand(command);
		if (executionResult != null) {
			List<String> insertOutputList = Arrays.asList(executionResult.split("\n"));
			
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
					flag=true;
				}
			}
			if (flag == false) {
				this.commonFunctions.updateDB(currentDataSetName, "hiveCopyDataToHive", "FAIL");
				this.commonFunctions.updateDB(currentDataSetName, "hiveCopyDataToHiveComment", this.commonFunctions.getErrorMessage());
				this.commonFunctions.updateDB(currentDataSetName, "hiveCopyDataToHiveMRJobURL", mrJobURL);
				this.setCurrentMinute(this.commonFunctions.getErrorMessage());
			} else if (flag == true) {
				this.commonFunctions.updateDB(currentDataSetName, "hiveCopyDataToHive", "PASS");
				this.commonFunctions.updateDB(currentDataSetName, "hiveCopyDataToHiveMRJobURL", mrJobURL);
			}
		} else {
			this.setErrorMessage(this.commonFunctions.getErrorMessage());
			this.commonFunctions.updateDB(currentDataSetName, "hiveCopyDataToHive", "FAIL");
			this.commonFunctions.updateDB(currentDataSetName, "hiveCopyDataToHiveComment", this.commonFunctions.getErrorMessage());
		}
		this.commonFunctions.updateDB(currentDataSetName, "hiveCopyDataToHiveCurrentState", "COMPLETED");
		TestSession.logger.info("-----------------------------------------------------------TestCopyDataToHive end ----------------------------------------------------------------------------");
		return flag;
	}
}
