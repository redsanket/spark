package hadooptest.gdm.regression.stackIntegration.lib;

import java.io.File;
import java.util.List;
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
	private String dataPath;
	private String pipeLineName;
	private CommonFunctions commonFunctiions;
	private static final int THREAD_SIZE = 1;

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

	public ModifyStackComponentsScripts(String nameNodeName , String dataPath) {
		this.setNameNodeName(nameNodeName);
		this.setDataPath(dataPath);
		this.setResourceAbsolutePath(new File("").getAbsolutePath());
		this.commonFunctiions = new CommonFunctions();
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
			hbaseCreateTableScriptFileContent = hbaseCreateTableScriptFileContent.replaceAll("integration_test_table",  this.commonFunctiions.getPipeLineName() + "_" + this.commonFunctiions.getCurrentHourPath());

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
			hbaseDeleteTableScriptFileContent = hbaseDeleteTableScriptFileContent.replaceAll("integration_test_table",  this.commonFunctiions.getPipeLineName() + "_" + this.commonFunctiions.getCurrentHourPath());

			// create new file
			File file = new File(this.getResourceAbsolutePath() + "/resources/stack_integration/hbase/deleteHBaseIntegrationTable_temp.txt");
			java.nio.file.Files.write(java.nio.file.Paths.get(file.toString()), hbaseDeleteTableScriptFileContent.getBytes());
			hbaseDeleteTableScriptFlag = true;
			TestSession.logger.info("new file ---- " + file.toString());
			return "hbaseDeleteTableScript-" + hbaseDeleteTableScriptFlag;
		};
		
		ExecutorService executors = Executors.newFixedThreadPool(THREAD_SIZE);
		List<Callable<String>> list = new java.util.ArrayList<Callable<String>>();
		list.add(loadDataToHiveScript);
		list.add(hbaseCreateTableScript);
		list.add(hbaseDeleteTableScript);

		List<Future<String>> testExecutionList = executors.invokeAll(list);
		for ( Future<String> result : testExecutionList) {
			System.out.println("-----------------------------------------");
			System.out.println("result - " + result.get());
		}
		executors.shutdown();
	}
}
