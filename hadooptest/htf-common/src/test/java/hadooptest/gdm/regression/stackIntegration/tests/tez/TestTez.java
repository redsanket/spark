package hadooptest.gdm.regression.stackIntegration.tests.tez;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import hadooptest.TestSession;
import hadooptest.gdm.regression.stackIntegration.StackComponent;
import hadooptest.gdm.regression.stackIntegration.lib.CommonFunctions;

public class TestTez implements Callable<String> {
	
	private StackComponent stackComponent;
	private CommonFunctions commonFunctions;
	private String hostName;
	private String nameNodeName;
	private String initCommand;
	private String mrJobURL;
	private final static String HADOOP_HOME="export HADOOP_HOME=/home/gs/hadoop/current";
	private final static String JAVA_HOME="export JAVA_HOME=/home/gs/java/jdk64/current/";
	private final static String HADOOP_CONF_DIR="export HADOOP_CONF_DIR=/home/gs/conf/current";
	private final static String DFSLOAD_KNITI = "kinit -k -t /homes/dfsload/dfsload.dev.headless.keytab dfsload@DEV.YGRID.YAHOO.COM";
	private final static String HADOOPQA_KNITI = "kinit -k -t /homes/hadoopqa/hadoopqa.dev.headless.keytab hadoopqa@DEV.YGRID.YAHOO.COM";
	private final static String HIVE_VERSION_COMMAND = "hive --version";
	public static final String PIG_HOME = "export PIG_HOME=/home/y/share/pig";
	private final static String PATH_COMMAND = "export PATH=$PATH:";
	
	public void constructCommand() {
		this.initCommand = "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null  " +  this.getHostName() + "  \"" +HADOOP_HOME + ";" + JAVA_HOME + ";" +  HADOOP_CONF_DIR + ";"  + HADOOPQA_KNITI  + ";" ;
	}
	
	public String getMrJobURL() {
		return mrJobURL;
	}

	public void setMrJobURL(String mrJobURL) {
		this.mrJobURL = mrJobURL;
	}

	public String getInitCommand() {
		return initCommand;
	}

	public void setInitCommand(String initCommand) {
		this.initCommand = initCommand;
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
	
	public TestTez() {
	}
	
	public TestTez(StackComponent stackComponent , String hostName , String nameNodeName, String dataSetName) {
		this.stackComponent = stackComponent;
		this.commonFunctions = new CommonFunctions();
		this.setNameNodeName(nameNodeName);
		this.setHostName(hostName);
		this.setDataSetName(dataSetName);
		this.stackComponent.setDataSetName(this.commonFunctions.getDataSetName());
		this.stackComponent.setCurrentState("STARTED");
	}
	
	public String execute() {
		TestSession.logger.info("---------------------------------------------------------------TestTez  start ------------------------------------------------------------------------");
		this.stackComponent.setCurrentState("RUNNING");
		this.constructCommand();
		boolean flag = false;
		String testResult = null;
		String mrJobURL = null;
		String command = this.getInitCommand() + "pig -x tez "
				+ "-param \"NAMENODE_NAME=" + this.getNameNodeName() + "\""
				+ "  "
				+ "-param \"DATASET_NAME=" + this.getDataSetName() + "\""
				+ "  "
				+ this.stackComponent.getScriptLocation() + "/TezTestCase_temp.pig\"";
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
			String insertRecordResult = null;
			if (flag == true) {
				testResult = "PASS";
			} else if (flag == false) {
				testResult = "FAIL";
				//this.stackComponent.setErrorString(this.commonFunctions.getErrorMessage());
			}
			this.stackComponent.setCurrentState("COMPLETED");
			this.stackComponent.setResult(testResult);
			this.stackComponent.setCurrentMRJobLink(mrJobURL);
		}
		TestSession.logger.info("---------------------------------------------------------------TestTez  end ------------------------------------------------------------------------");
		return this.stackComponent.getStackComponentName() + "-" + flag;
	}

	@Override
	public String call() throws Exception {
		String testResult = execute();
		return testResult;
	}
	
}
