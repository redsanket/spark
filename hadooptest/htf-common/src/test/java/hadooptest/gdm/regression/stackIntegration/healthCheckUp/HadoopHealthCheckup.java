package hadooptest.gdm.regression.stackIntegration.healthCheckUp;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import hadooptest.TestSession;
import hadooptest.gdm.regression.stackIntegration.StackComponent;
import hadooptest.gdm.regression.stackIntegration.lib.CommonFunctions;

public class HadoopHealthCheckup implements Callable<StackComponent>{
	private String hostName;
	private CommonFunctions commonFunctions;
	private final String COMPONENT_NAME = "hadoop";
	public final String TEZ_HOME = "/home/gs/tez/current/";	
	private final static String HADOOPQA_KINIT_COMMAND = "kinit -k -t /homes/hadoopqa/hadoopqa.dev.headless.keytab hadoopqa@DEV.YGRID.YAHOO.COM";

	private StackComponent stackComponent;
	
	public HadoopHealthCheckup(String hostName) {
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
		this.stackComponent.setDataSetName(this.commonFunctions.getCurrentHourPath());
		this.stackComponent.setHostName(this.getHostName());
		String command = "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null  " + this.getHostName() + " \"ls -t " + TEZ_HOME + "tez-api-*\"";
		TestSession.logger.info("command - " + command);
		String result = this.commonFunctions.executeCommand(command);
		this.getHadoopVersion(result);
		return this.stackComponent;
	}
	
	public void getHadoopVersion(String result) {
		String currentDataSet = this.commonFunctions.getCurrentHourPath();
		if (result != null) {
			List<String> logOutputList = Arrays.asList(result.split("\n"));
			for ( String log : logOutputList) {
				if (log.startsWith(TEZ_HOME) == true ) {
					String temp = TEZ_HOME + "/tez-api-";
					String version = log.substring( temp.length() - 1, log.length()).replace(".jar", "").trim();
					TestSession.logger.info("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%   " + version);
					if (version != null) {
						this.stackComponent.setStackComponentVersion(version);
						this.stackComponent.setHealth(true);
						break;	
					} else if (version == null) {
						this.stackComponent.setStackComponentVersion("0.0");
						this.stackComponent.setHealth(false);
						this.commonFunctions.updateDB(currentDataSet, "hadoopResult", "FAIL");
						this.commonFunctions.updateDB(currentDataSet, "hadoopeCurrentState", "COMPLETED");
						this.commonFunctions.updateDB(currentDataSet, "hadoopComments", "Hadoop failed.");
						break;
					}
					
				}
			}	
		} else if (result == null) {
			this.stackComponent.setStackComponentVersion("0.0");
			this.stackComponent.setHealth(false);
			this.stackComponent.setErrorString("Tez is not installed. Check whether tez-api-* exists under " + TEZ_HOME + " or " + this.commonFunctions.getErrorMessage());
		}
	}
}
