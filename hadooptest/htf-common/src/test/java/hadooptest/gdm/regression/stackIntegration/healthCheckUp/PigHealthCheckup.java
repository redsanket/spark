package hadooptest.gdm.regression.stackIntegration.healthCheckUp;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import hadooptest.TestSession;
import hadooptest.gdm.regression.stackIntegration.StackComponent;
import hadooptest.gdm.regression.stackIntegration.lib.CommonFunctions;

public class PigHealthCheckup implements Callable<StackComponent>{
	
	private String hostName;
	private CommonFunctions commonFunctionsObj;
	private final String COMPONENT_NAME = "pig";
	private final String kINIT_COMMAND = "kinit -k -t /homes/dfsload/dfsload.dev.headless.keytab dfsload@DEV.YGRID.YAHOO.COM";
	private StackComponent stackComponent;
	
	public PigHealthCheckup(String hostName) {
		this.hostName = hostName;
		this.commonFunctionsObj = new CommonFunctions();
		this.stackComponent = new StackComponent();
	}

	public String getHostName() {
		return this.hostName;
	}
	
	@Override
	public StackComponent call() throws Exception {
		this.stackComponent.setStackComponentName(COMPONENT_NAME);
		this.stackComponent.setHostName(this.getHostName());
		String command = "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null  " + this.getHostName()  + " \"" + kINIT_COMMAND + ";" + "export PIG_HOME=/home/y/share/pig;export PATH=$PATH:$PIG_HOME/bin/;pig -version\"";
		TestSession.logger.info("command - " + command);
		String result = this.commonFunctionsObj.executeCommand(command);
		this.getPigVersion(result);
		return this.stackComponent;
	}
	
	public void getPigVersion(String result) {
		if (result != null) {
			java.util.List<String>outputList = Arrays.asList(result.split("\n"));
			for ( String str : outputList) {
				TestSession.logger.info(str);
				int index = str.indexOf("Pig");
				if ( index > 0) {
					String tempStr = str.substring(index, str.indexOf("(rexported)"));
					List<String> tempList = Arrays.asList(tempStr.split(" "));
					String version = tempList.get(tempList.size() - 1);
					this.stackComponent.setStackComponentVersion(version);
					this.stackComponent.setHealth(true);
					break;
				}
			}	
		}else if (result == null) {
			this.stackComponent.setStackComponentVersion("0.0");
			this.stackComponent.setHealth(false);
			this.stackComponent.setErrorString("Pig is not installed, check whether for /home/y/share/pig"  + " or " + this.commonFunctionsObj.getErrorMessage());
		}
	}

}
