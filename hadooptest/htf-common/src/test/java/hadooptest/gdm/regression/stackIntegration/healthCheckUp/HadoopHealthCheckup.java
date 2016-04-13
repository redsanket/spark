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
		this.stackComponent.setDataSetName(this.commonFunctions.getDataSetName());
		this.stackComponent.setHostName(this.getHostName());
		String command = "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null  " + this.getHostName() +  "  \"" + "hadoop version\"";
		TestSession.logger.info("command - " + command);
		String result = this.commonFunctions.executeCommand(command);
		this.getHadoopVersion(result);
		return this.stackComponent;
	}
	
	public void getHadoopVersion(String result) {
		String currentDataSet = this.stackComponent.getDataSetName();
		String hadoopVersion = null;
		boolean flag = false;
		if (result != null) {
			TestSession.logger.info("hadoop version result = " + result);
			java.util.List<String>outputList = Arrays.asList(result.split("\n"));
			for ( String str : outputList) {
				TestSession.logger.info(str);
				if ( str.startsWith("Hadoop") == true ) {
					hadoopVersion = Arrays.asList(str.split(" ")).get(1);
					flag = true;
					break;
				}
			}
			TestSession.logger.info("Hadoop Version - " + hadoopVersion);	
		} else if (result == null || flag == false) {
			this.stackComponent.setStackComponentVersion("0.0");
			this.stackComponent.setHealth(false);
			this.stackComponent.setErrorString("Failed to get the hadoop Vesion - reason " + this.commonFunctions.getErrorMessage());
		}
		if (flag== true) {
			this.stackComponent.setStackComponentVersion(hadoopVersion);
			this.stackComponent.setHealth(true);
		}
	}
}
