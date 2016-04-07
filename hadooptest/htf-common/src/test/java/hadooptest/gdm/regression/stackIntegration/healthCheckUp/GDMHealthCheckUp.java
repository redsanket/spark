package hadooptest.gdm.regression.stackIntegration.healthCheckUp;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import com.jayway.restassured.path.json.JsonPath;
import com.jayway.restassured.response.Response;

import hadooptest.TestSession;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.cluster.gdm.HTTPHandle;
import hadooptest.gdm.regression.stackIntegration.StackComponent;
import hadooptest.gdm.regression.stackIntegration.lib.CommonFunctions;

public class GDMHealthCheckUp implements Callable<StackComponent>{

	private String hostName;
	private String cookie;
	private StackComponent stackComponent;
	private CommonFunctions commonFunctionsObj;
	private final String COMPONENT_NAME = "gdm";
	private static final String VERSION = "console/query/hadoop/versions";
	
	public GDMHealthCheckUp( ) {
		this.commonFunctionsObj = new CommonFunctions();
		this.stackComponent = new StackComponent();
	}
	
	public String getHostName() {
		return hostName;
	}

	public void setHostName(String hostName) {
		this.hostName = hostName;
	}

	public String getCookie() {
		return cookie;
	}

	public void setCookie(String cookie) {
		this.cookie = cookie;
	}
	
	public void checkGDMHealthCheckup() {

		String currentDataSet = this.commonFunctionsObj.getCurrentHourPath();
		String gdmVersion = "0.0";
		boolean flag = false;
		ConsoleHandle consoleHandle = new ConsoleHandle();
		String cookie  = consoleHandle.httpHandle.getBouncerCookie();
		this.stackComponent.setHostName(consoleHandle.getConsoleURL());
		String	consoleHealthCheckUpTestURL = consoleHandle.getConsoleURL()+ "/console/api/proxy/health?colo=gq1&facet=console";
		TestSession.logger.info("consoleHealthCheckUpTestURL = " +consoleHealthCheckUpTestURL );
		com.jayway.restassured.response.Response response = given().cookie(cookie).get(consoleHealthCheckUpTestURL);
		assertTrue("Failed to get the response for " + consoleHealthCheckUpTestURL , (response != null) );
		
		if (response != null) {
			String resString = response.asString();
			TestSession.logger.info("response = " + resString);
			JsonPath jsonPath = new JsonPath(resString);
			Map<String , String>applicationSummary = new HashMap<String, String>();
			List<String> keys = jsonPath.get("ApplicationSummary.Parameter");
			List<String> values = jsonPath.get("ApplicationSummary.Value");
			for(int i = 0;i<keys.size() ; i++){
				applicationSummary.put(keys.get(i), values.get(i));
			}
			flag = applicationSummary.containsKey("build.version");
			if (flag == true) {
				gdmVersion = applicationSummary.get("build.version");
				this.stackComponent.setHealth(true);
				this.stackComponent.setStackComponentVersion(gdmVersion);			
			}	
		}
		if (flag == false) {
			this.stackComponent.setHealth(false);
			this.stackComponent.setStackComponentVersion(gdmVersion);
			this.commonFunctionsObj.updateDB(currentDataSet, "gdmResult", "FAIL");
			this.commonFunctionsObj.updateDB(currentDataSet, "gdmCurrentState", "COMPLETED");
			this.commonFunctionsObj.updateDB(currentDataSet, "gdmComments", "check whether gdm console is down");
		}
	}

	@Override
	public StackComponent call() throws Exception {
		this.stackComponent.setStackComponentName(COMPONENT_NAME);
		checkGDMHealthCheckup();
		return this.stackComponent;
	}
	
}
