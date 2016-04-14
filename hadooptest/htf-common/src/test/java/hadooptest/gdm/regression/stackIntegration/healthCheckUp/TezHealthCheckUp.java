package hadooptest.gdm.regression.stackIntegration.healthCheckUp;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import hadooptest.TestSession;
import hadooptest.gdm.regression.stackIntegration.StackComponent;
import hadooptest.gdm.regression.stackIntegration.lib.CommonFunctions;

public class TezHealthCheckUp implements Callable<StackComponent>{

	private String hostName;
	private CommonFunctions commonFunctions;
	private final String COMPONENT_NAME = "tez";
	public final String TEZ_HOME = "/home/gs/tez/current/";
	private StackComponent stackComponent;
	
	public TezHealthCheckUp(String hostName) {
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
		String command = "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null  " + this.getHostName() + " \"ls -t " + TEZ_HOME + "tez-api-*\"";
		TestSession.logger.info("command - " + command);
		String result = this.commonFunctions.executeCommand(command);
		this.getTezVersion(result);
		return this.stackComponent;
	}
	
	public void getTezVersion(String result) {
		String currentDataSet = this.stackComponent.getDataSetName();
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
						this.commonFunctions.updateDB(currentDataSet, "tezResult", "FAIL");
						this.commonFunctions.updateDB(currentDataSet, "tezCurrentState", "COMPLETED");
						this.commonFunctions.updateDB(currentDataSet, "tezComments", result);
						this.stackComponent.setErrorString("Tez is not installed. Check whether tez-api-* exists under " + TEZ_HOME + " or " + this.commonFunctions.getErrorMessage());
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
