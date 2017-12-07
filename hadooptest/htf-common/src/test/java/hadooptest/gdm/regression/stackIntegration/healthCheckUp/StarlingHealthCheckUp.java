package hadooptest.gdm.regression.stackIntegration.healthCheckUp;

import java.util.Arrays;
import java.util.concurrent.Callable;

import hadooptest.TestSession;
import hadooptest.gdm.regression.stackIntegration.StackComponent;
import hadooptest.gdm.regression.stackIntegration.lib.CommonFunctions;

/**
 * Class that get starling version, which is assumed as healthcheckup.
 *
 */
public class StarlingHealthCheckUp implements Callable<StackComponent> {

    private String hostName;
    private CommonFunctions commonFunctions;
    private StackComponent stackComponent;
    private final String COMPONENT_NAME = "starling";
    private final String STARLING_VERSION_COMMAND = "yinst ls | grep starling_proc";

    public StarlingHealthCheckUp(String hostName) {
	this.hostName = hostName;
	this.commonFunctions = new CommonFunctions();
	this.stackComponent = new StackComponent();
    }

    @Override
    public StackComponent call() throws Exception {
	this.stackComponent.setStackComponentName(COMPONENT_NAME);
	this.stackComponent.setDataSetName(this.commonFunctions.getDataSetName());
	this.stackComponent.setHostName(this.getHostName().trim());
	String command = "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null  " + this.getHostName()  + "  \"" + STARLING_VERSION_COMMAND  + "\" ";
	TestSession.logger.info("command -" + command);
	String result = this.commonFunctions.executeCommand(command.trim()).trim();
	this.getVersion(result);
	return this.stackComponent;
    }

    private void getVersion(String result) {
	java.util.List<String>outputList = Arrays.asList(result.split("\n"));
	String starlingVersion = null;
	boolean flag = false;
	for ( String str : outputList) {
	    TestSession.logger.info(str);
	    if ( str.startsWith("starling_proc-") == true ) {
		starlingVersion = Arrays.asList(str.split("-")).get(1).trim();
		flag = true;
		break;
	    }
	}
	TestSession.logger.info("starlingVersion - " + starlingVersion);
	if (flag) {
	    this.stackComponent.setStackComponentVersion(starlingVersion.trim());
	    this.stackComponent.setHealth(true);
	} else {
	    this.stackComponent.setStackComponentVersion("0.0");
	    this.stackComponent.setHealth(false);
	    this.stackComponent.setErrorString("Failed to get the starling"); 
	}
    }

    public String getHostName() {
	return hostName;
    }

    public void setHostName(String hostName) {
	this.hostName = hostName;
    }
}