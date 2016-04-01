package hadooptest.gdm.regression.stackIntegration.healthCheckUp;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import hadooptest.TestSession;
import hadooptest.gdm.regression.stackIntegration.lib.CommonFunctions;

public class GetStackComponentHostName implements Callable<String> {
	
	private String clusterName;
	private String componentName;
	private CommonFunctions commaonFunctionObject;
	
	public GetStackComponentHostName(String clusterName, String componentName ) {
		this.clusterName = clusterName;
		this.componentName = componentName;
		this.commaonFunctionObject = new CommonFunctions();
	}
	
	public String getClusterName() {
		return this.clusterName;
	}
	
	public String getComponentName() {
		return this.componentName;
	}

	@Override
	public String call() throws Exception {
		String command = "yinst range -ir \"(@grid_re.clusters." + this.getClusterName() + "." + this.getComponentName() +")\"";
		TestSession.logger.info("Command = " + command);
		String hostName = this.commaonFunctionObject.executeCommand(command).trim();
		Thread.sleep(100);
		return this.getComponentName()  + "~" + hostName;
	}
}
