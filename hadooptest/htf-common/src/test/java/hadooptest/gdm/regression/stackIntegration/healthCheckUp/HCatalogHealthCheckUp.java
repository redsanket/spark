package hadooptest.gdm.regression.stackIntegration.healthCheckUp;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import hadooptest.TestSession;
import hadooptest.gdm.regression.stackIntegration.StackComponent;
import hadooptest.gdm.regression.stackIntegration.lib.CommonFunctions;

public class HCatalogHealthCheckUp  implements Callable<StackComponent>{

	private String hostName;
	private CommonFunctions commonFunctions;
	private final String COMPONENT_NAME = "hcat";
	private StackComponent stackComponent;

	public HCatalogHealthCheckUp(String hostName) {
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
		this.getHCatVersion();
		return this.stackComponent;
	}
	
	public void getHCatVersion() {
		boolean flag = false;
		String hVersion = null;
		String command = "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null " + this.getHostName() + "  \" " + "  yinst ls | grep hcat " + "\" " ;
		String output = this.commonFunctions.executeCommand(command);
		if ( output != null ) {
			List<String> outputList = Arrays.asList(output.split("\n"));
			for (String str : outputList) {
				if (str.startsWith("hcat_server") ) {
					hVersion = Arrays.asList(str.split("-")).get(1).trim();
					flag = true;
				}
			}	
		}
		if ( flag == false || output == null) {
			this.stackComponent.setStackComponentVersion("0.0");
			this.stackComponent.setHealth(false);
			this.stackComponent.setErrorString("Check whether hcat server is up or " + this.commonFunctions.getErrorMessage());
		} else if (flag == true) {
			this.stackComponent.setStackComponentVersion(hVersion);
			this.stackComponent.setHealth(true);
		}
	}
	
	public void getHiveVersion(String result) {
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
			this.stackComponent.setErrorString("Check whether hive server is up or " + this.commonFunctions.getErrorMessage());
		}
	}
}
