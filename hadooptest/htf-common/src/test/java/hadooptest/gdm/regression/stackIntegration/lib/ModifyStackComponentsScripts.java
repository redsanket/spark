// Copyright 2016, Yahoo Inc.
package hadooptest.gdm.regression.stackIntegration.lib;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import hadooptest.TestSession;
import hadooptest.cluster.gdm.GdmUtils;

import static java.nio.file.Files.readAllBytes;
import static java.nio.file.Paths.get;

public class ModifyStackComponentsScripts {

	private String resourceAbsolutePath;
	private String nameNodeName;
	private String clusterName;
	private String jobTracker;
	private String oozieHostName;
	private String dataPath;
	private String pipeLineName;
	private CommonFunctions commonFunctiions;
	private static final int THREAD_SIZE = 1;

	public ModifyStackComponentsScripts(String nameNodeName , String dataPath , String clusterName) {
		this.setNameNodeName(nameNodeName);
		this.setDataPath(dataPath);
		this.setClusterName(clusterName);
		this.setResourceAbsolutePath(new File("").getAbsolutePath());
		this.commonFunctiions = new CommonFunctions(this.getClusterName());
	}

	public ModifyStackComponentsScripts(String nameNodeName, String jobTracker, String oozieHostName, String dataPath,String clusterName ) {
		this.setNameNodeName(nameNodeName);
		this.setDataPath(dataPath);
		this.setClusterName(clusterName);
		this.setResourceAbsolutePath(new File("").getAbsolutePath());
		this.commonFunctiions = new CommonFunctions(this.getClusterName());
		this.setJobTracker(jobTracker);
		this.setOozieHostName(oozieHostName);
	}

	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	public String getJobTracker() {
		return jobTracker;
	}

	public void setJobTracker(String jobTracker) {
		this.jobTracker = jobTracker;
	}

	public String getOozieHostName() {
		return oozieHostName;
	}

	public void setOozieHostName(String oozieHostName) {
		this.oozieHostName = oozieHostName;
	}

	public String getPipeLineName() {
		return pipeLineName;
	}

	public void setPipeLineName(String pipeLineName) {
		this.pipeLineName = pipeLineName;
	}

	public String getNameNodeName() {
		return nameNodeName;
	}

	public void setNameNodeName(String nameNodeName) {
		this.nameNodeName = nameNodeName;
	}

	public String getDataPath() {
		return dataPath;
	}

	public void setDataPath(String dataPath) {
		this.dataPath = dataPath;
	}



	public String getResourceAbsolutePath() {
		return resourceAbsolutePath;
	}

	public void setResourceAbsolutePath(String resourceAbsolutePath) {
		this.resourceAbsolutePath = resourceAbsolutePath;
	}

	public void execute() throws InterruptedException, ExecutionException {
		// hive 
		Callable<String> loadDataToHiveScript = ()->{
			boolean loadDataToHiveScriptFlag = false;
			File filePath = new File(this.getResourceAbsolutePath() + "/resources/stack_integration/hive/LoadDataToHive_temp.hql");
			String fileContent = new String(readAllBytes(get(filePath.toString())));
			fileContent = fileContent.replaceAll("DATA_PATH", "hdfs://"  + this.getNameNodeName() + "/" + this.getDataPath() + "/hiveData/");
			TestSession.logger.info(" - " + fileContent);

			// create new file
			File file = new File(this.getResourceAbsolutePath() + "/resources/stack_integration/hive/LoadDataToHive.hql");
			java.nio.file.Files.write(java.nio.file.Paths.get(file.toString()), fileContent.getBytes());
			loadDataToHiveScriptFlag = true;
			TestSession.logger.info("new file ---- " + file.toString());
			return "loadDataToHiveScript-" + loadDataToHiveScriptFlag;
		};

		// hbase create table script
		Callable<String> hbaseCreateTableScript = () -> {
			boolean hbaseCreateTableScriptFlag = false;
			File hbaseCreateTableFilePath = new File(this.getResourceAbsolutePath() + "/resources/stack_integration/hbase/createHBaseIntegrationTable.txt");
			String hbaseCreateTableScriptFileContent = new String(readAllBytes(get(hbaseCreateTableFilePath.toString())));
			hbaseCreateTableScriptFileContent = hbaseCreateTableScriptFileContent.replaceAll("integration_test_table",  this.commonFunctiions.getPipeLineName() + "_" + this.commonFunctiions.getDataSetName());

			// create new file
			File file = new File(this.getResourceAbsolutePath() + "/resources/stack_integration/hbase/createHBaseIntegrationTable_temp.txt");
			java.nio.file.Files.write(java.nio.file.Paths.get(file.toString()), hbaseCreateTableScriptFileContent.getBytes());
			hbaseCreateTableScriptFlag = true;
			TestSession.logger.info("new file ---- " + file.toString());
			return "hbaseCreateTableScript-" + hbaseCreateTableScriptFlag;
		};

		// hbase delete table script
		Callable<String> hbaseDeleteTableScript = () -> {
			boolean hbaseDeleteTableScriptFlag = false;
			File hbaseDeleteTableFilePath = new File(this.getResourceAbsolutePath() + "/resources/stack_integration/hbase/deleteHBaseIntegrationTable.txt");
			String hbaseDeleteTableScriptFileContent = new String(readAllBytes(get(hbaseDeleteTableFilePath.toString())));
			hbaseDeleteTableScriptFileContent = hbaseDeleteTableScriptFileContent.replaceAll("integration_test_table",  this.commonFunctiions.getPipeLineName() + "_" + this.commonFunctiions.getDataSetName());

			// create new file
			File file = new File(this.getResourceAbsolutePath() + "/resources/stack_integration/hbase/deleteHBaseIntegrationTable_temp.txt");
			java.nio.file.Files.write(java.nio.file.Paths.get(file.toString()), hbaseDeleteTableScriptFileContent.getBytes());
			hbaseDeleteTableScriptFlag = true;
			TestSession.logger.info("new file ---- " + file.toString());
			return "hbaseDeleteTableScript-" + hbaseDeleteTableScriptFlag;
		};

		Callable<String> oozieModifyJobProperties = () -> {
			SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHH");
			Calendar calendar = Calendar.getInstance();
			calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
			String currentHR = simpleDateFormat.format(calendar.getTime());
			boolean oozieModifyScriptFlag = false;

			File oozieJobPropertiesFilePath = new File(this.getResourceAbsolutePath() + "/resources/stack_integration/oozie/job.properties.tmp");
			String oozieJobPropertiesFileContent = new String(readAllBytes(get(oozieJobPropertiesFilePath.toString())));
			oozieJobPropertiesFileContent = oozieJobPropertiesFileContent.replaceAll("ADD_INSTANCE_INPUT_PATH", "/data/daqdev/abf/data/" + this.commonFunctiions.getCurrentHourPath() + "/20130309/PAGE/Valid/News/part*");
			oozieJobPropertiesFileContent = oozieJobPropertiesFileContent.replaceAll("ADD_INSTANCE_DATE_TIME", currentHR + "00");
			oozieJobPropertiesFileContent = oozieJobPropertiesFileContent.replaceAll("ADD_INSTANCE_PATH", "/tmp/integration-testing/oozie/" + currentHR);
			oozieJobPropertiesFileContent = oozieJobPropertiesFileContent.replaceAll("ADD_INSTANCE_OUTPUT_PATH", "/tmp/integration-testing/oozie/" + currentHR + "/outputDir/"  );  // TODO create the folder when running the testcase
			oozieJobPropertiesFileContent = oozieJobPropertiesFileContent.replaceAll("HCAT_SERVER_NAME", this.getOozieHostName() );
			oozieJobPropertiesFileContent = oozieJobPropertiesFileContent.replaceAll("JOB_TRACKER_HOST_NAME", this.getJobTracker() );
			oozieJobPropertiesFileContent = oozieJobPropertiesFileContent.replaceAll("NAMENODE_HOST_NAME", this.getNameNodeName() );

			TestSession.logger.info("!!!!!!!!!!");
			TestSession.logger.info("oozieJobPropertiesFileContent  = " + oozieJobPropertiesFileContent);
			TestSession.logger.info("!!!!!!!!!!");

			// create new file
			File file = new File(this.getResourceAbsolutePath() + "/resources/stack_integration/oozie/job.properties");
			java.nio.file.Files.write(java.nio.file.Paths.get(file.toString()), oozieJobPropertiesFileContent.getBytes());
			oozieModifyScriptFlag = true;
			TestSession.logger.info("new file ---- " + file.toString());
			return "oozieJobProperties-" + oozieModifyScriptFlag;
		};

		Callable<String> oozieModifyWorkFlowScript = () -> {
			boolean oozieModifyWorkFlowFlag = false;
			String currentJobName = GdmUtils.getConfiguration("testconfig.TestWatchForDataDrop.pipeLineName") + "_" + this.commonFunctiions.getDataSetName();
			File oozieWorkFlowFilePath = new File(this.getResourceAbsolutePath() + "/resources/stack_integration/oozie/workflow.xml.tmp");
			String oozieWorkFlowFileContent = new String(readAllBytes(get(oozieWorkFlowFilePath.toString())));
			oozieWorkFlowFileContent = oozieWorkFlowFileContent.replaceAll("stackint_oozie_RawInputETL",  currentJobName );

			TestSession.logger.info("!!!!!!!!!!");
			TestSession.logger.info("oozieWorkFlowFileContent  = " + oozieWorkFlowFileContent);
			TestSession.logger.info("!!!!!!!!!!");

			// create new file
			File file = new File(this.getResourceAbsolutePath() + "/resources/stack_integration/oozie/workflow.xml");
			java.nio.file.Files.write(java.nio.file.Paths.get(file.toString()), oozieWorkFlowFileContent.getBytes());
			oozieModifyWorkFlowFlag = true;
			TestSession.logger.info("new file ---- " + file.toString());

			return "oozieWorkFlow-" + oozieModifyWorkFlowFlag;
		};

		ExecutorService executors = Executors.newFixedThreadPool(THREAD_SIZE);
		List<Callable<String>> list = new java.util.ArrayList<Callable<String>>();
		list.add(loadDataToHiveScript);
		list.add(hbaseCreateTableScript);
		list.add(hbaseDeleteTableScript);
		list.add(oozieModifyJobProperties);
		list.add(oozieModifyWorkFlowScript);

		List<Future<String>> testExecutionList = executors.invokeAll(list);
		for ( Future<String> result : testExecutionList) {
			TestSession.logger.info("-----------------------------------------");
			TestSession.logger.info("result - " + result.get());
		}
		executors.shutdown();
	}

}
