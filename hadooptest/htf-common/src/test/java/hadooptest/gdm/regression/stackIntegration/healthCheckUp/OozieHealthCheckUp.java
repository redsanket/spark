package hadooptest.gdm.regression.stackIntegration.healthCheckUp;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import hadooptest.TestSession;
import hadooptest.gdm.regression.stackIntegration.StackComponent;
import hadooptest.gdm.regression.stackIntegration.lib.CommonFunctions;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

public class OozieHealthCheckUp implements Callable<StackComponent>{

	private String hostName;
	private StackComponent stackComponent;
	private CommonFunctions commonFunctionsObj;
	private final String COMPONENT_NAME = "oozie";
	private final static String OOZIE_ENV_EXPORT_COMMAND = "export OOZIE_SSL_ENABLE=true;export OOZIE_SSL_CLIENT_CERT=/home/y/conf/ygrid_cacert/certstore.jks";
	private final static String QUERY = ":4443/oozie/v1/admin/build-version";

	public OozieHealthCheckUp(String hostName) {
		this.setHostName(hostName);
		this.commonFunctionsObj = new CommonFunctions();
		this.stackComponent = new StackComponent();
	}

	public String getHostName() {
		return hostName;
	}

	public void setHostName(String hostName) {
		this.hostName = hostName;
	}

	@Override
	public StackComponent call() throws Exception {
		this.stackComponent.setStackComponentName(COMPONENT_NAME);
		this.stackComponent.setHostName(this.getHostName());
		this.stackComponent.setDataSetName(this.commonFunctionsObj.getDataSetName());
		String query = "https://" + this.getHostName() + QUERY;
		TestSession.logger.info("query = " + query);
		this.executeRestQuery(query);
		return this.stackComponent;
	}
	
    public String getJSONResponse(String stringUrl) {
    	String curlCommand = "curl --insecure -sb -H \"Accept: application/json\" --negotiate -u : --cacert /home/y/conf/ygrid_cacert/ca-cert.pem " + stringUrl;
    	String cmd = OOZIE_ENV_EXPORT_COMMAND + ";" + curlCommand;
    	String output = this.commonFunctionsObj.executeCommand(cmd);
    	return output;
    }
	
	public void executeRestQuery(String query) {
		String currentDataSet = this.stackComponent.getDataSetName();
		TestSession.logger.info("____________________________________________________________________________________________________");
		try {
			String responseString = this.getJSONResponse(query);
			JSONObject obj = (JSONObject) JSONSerializer.toJSON(responseString);
			if (obj != null) {
				String oozieVersion = obj.getString("buildVersion");
				this.stackComponent.setHealth(true);

				// since webservice dn't give the full version (timestamp is
				// missing ).
				String command = "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null  " + this.getHostName() + "   \"yinst ls | grep oozie | head -1 | cut -d\'-\' -f2 \"";
				TestSession.logger.info("command - " + command);
				String commandOutput = this.commonFunctionsObj.executeCommand(command);
				List<String> logOutputList = Arrays.asList(commandOutput.split("\n"));
				boolean flag = false;
				for (String log : logOutputList) {
					if (log.startsWith(oozieVersion) == true) {
						TestSession.logger.info("oozie version = " + log.trim());
						this.stackComponent.setStackComponentVersion(log.trim());
						flag = true;
						break;
					}
				}
				if (flag == false) {
					this.stackComponent.setStackComponentVersion(oozieVersion.trim());
				}
			} else {
				this.stackComponent.setHealth(false);
				this.stackComponent.setStackComponentVersion("0.0");
				this.commonFunctionsObj.updateDB(currentDataSet, "oozieResult", "FAIL");
				this.commonFunctionsObj.updateDB(currentDataSet, "oozieCurrentState", "COMPLETED");
				this.commonFunctionsObj.updateDB(currentDataSet, "oozieComments", "check whether oozie server is down");
			}
		} catch (Exception e) {
			this.stackComponent.setHealth(false);
			this.stackComponent.setStackComponentVersion("0.0");
			this.commonFunctionsObj.updateDB(currentDataSet, "oozieResult", "FAIL");
			this.commonFunctionsObj.updateDB(currentDataSet, "oozieCurrentState", "COMPLETED");
			this.commonFunctionsObj.updateDB(currentDataSet, "oozieComments", "check whether oozie server is down");
			this.stackComponent.setErrorString(e.getMessage() + "check whether oozie server is down..");
			e.printStackTrace();
		}
		TestSession.logger.info("____________________________________________________________________________________________________");
	}
}
