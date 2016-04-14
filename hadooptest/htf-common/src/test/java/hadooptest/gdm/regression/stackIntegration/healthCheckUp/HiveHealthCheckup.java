package hadooptest.gdm.regression.stackIntegration.healthCheckUp;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import hadooptest.TestSession;
import hadooptest.gdm.regression.stackIntegration.StackComponent;
import hadooptest.gdm.regression.stackIntegration.lib.CommonFunctions;
import hadooptest.gdm.regression.stackIntegration.lib.IntegrationConstants;;

public class HiveHealthCheckup implements Callable<StackComponent>{

	private String hostName;
	private CommonFunctions commonFunctions;
	private final String COMPONENT_NAME = "hive";
	private StackComponent stackComponent;
	private final String HIVE_VERSION_COMMAND = "hive --version";

	public HiveHealthCheckup(String hostName) {
		this.hostName = hostName;
		this.commonFunctions = new CommonFunctions();
		this.stackComponent = new StackComponent();
	}

	public String getHostName() {
		return this.hostName;
	}

	@Override
	public StackComponent call() throws Exception {
		this.stackComponent.setStackComponentName(COMPONENT_NAME);
		this.stackComponent.setDataSetName(this.commonFunctions.getDataSetName());
		this.stackComponent.setHostName(this.getHostName());
		String command = "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null  " + this.getHostName() + "  \"" + IntegrationConstants.HADOOP_HOME + ";" + IntegrationConstants.JAVA_HOME + ";" +  IntegrationConstants.HADOOP_CONF_DIR + ";"  + IntegrationConstants.kINIT_COMMAND   + ";" + HIVE_VERSION_COMMAND  + "\" ";
		TestSession.logger.info("command -" + command);
		String result = this.commonFunctions.executeCommand(command.trim());
		this.getHiveVersion(result);
		return this.stackComponent;
	}
	
	public void getHiveVersion(String result) {
		String currentDataSet = this.stackComponent.getDataSetName();
		if (result != null) {
			List<String> outputList = Arrays.asList(result.split("\n"));
			for ( String str : outputList) {
				if (str.startsWith("Hive") ) {
					String version = Arrays.asList(str.split(" ")).get(1).trim();
					this.stackComponent.setStackComponentVersion(version);
					this.stackComponent.setHealth(true);
					break;
				}
			}	
		} else if (result == null) {
			this.stackComponent.setStackComponentVersion("0.0");
			this.stackComponent.setHealth(false);
			this.commonFunctions.updateDB(currentDataSet, "hiveResult", "FAIL");
			this.commonFunctions.updateDB(currentDataSet, "hiveCurrentState", "COMPLETED");
			this.commonFunctions.updateDB(currentDataSet, "hiveComments", result);
			this.stackComponent.setErrorString("Check whether hive server is up or " + this.commonFunctions.getErrorMessage());
		}
	}
}
