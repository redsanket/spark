package hadooptest.gdm.regression.nonAdmin;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;

import hadooptest.TestSession;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.HTTPHandle;
import hadooptest.cluster.gdm.JSONUtil;
import hadooptest.cluster.gdm.Response;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.jayway.restassured.path.json.JsonPath;

/**
 * Test Scenario : Verify whether Non Admin user allowed to use GDM HTTP Get methods, not the POST methods.
 *
 */
public class TestNonAdminGetAndPostMethod extends TestSession {

	private ConsoleHandle consoleHandle;
	private String cookie;
	private String url;
	private HTTPHandle httpHandle = null;
	private JSONUtil jsonUtil;
	private String baseDataSetName = "VerifyAcqRepRetWorkFlowExecutionDateRange" ;
	private  String nonAdminUserName; 
	private  String nonAdminPassWord;

	@BeforeClass
	public static void startTestSession() throws Exception {
		TestSession.start();
	}

	@Before
	public void setUp() throws NumberFormatException, Exception {
		this.consoleHandle = new ConsoleHandle();
		this.nonAdminUserName = this.consoleHandle.getConf().getString("auth.nonAdminUser");
		this.nonAdminPassWord = this.consoleHandle.getConf().getString("auth.nonAdminPassWord");
		
		TestSession.logger.info("nonAdminUserName  = " + this.nonAdminUserName  + "    nonAdminPassWord =   " + this.nonAdminPassWord);
		
		httpHandle = new HTTPHandle();
		httpHandle.logonToBouncer(this.nonAdminUserName , this.nonAdminPassWord);
		
		this.cookie = httpHandle.getBouncerCookie();
		jsonUtil = new JSONUtil();
		this.url = this.consoleHandle.getConsoleURL();
		TestSession.logger.info("url = " + url);
	}

	@Test
	public void testNonAdmin() {
		testCreatingDataSetForNonAdmin();
		testDeActivatingDataSet();
		testActivatingDataSet();
		testDisablePauseRetentionForNonAdmin();
		testEnablePauseRetentionForNonAdmin();
		testDeActiveTarget();
	}
	
	/**
	 * Test Scenario : Verify whether non-admin user is not able to INACTIVIATE  the dataset
	 * Expected Result : Non-Admin should be able to INACTIVIATE the dataset.
	 */
	public void testDeActivatingDataSet() {
		List<String> inactiveDataSet = given().cookie(this.cookie).get(this.url + "/console/api/datasets/view?checkverify=true&status=ACTIVE").getBody().jsonPath().getList("DatasetsResult.DatasetName");
		if (inactiveDataSet == null) {
			fail(" Failed to get INACTIVE datasets.");
		}
		TestSession.logger.info("size = " + inactiveDataSet.size());
		String resource = this.jsonUtil.constructResourceNamesParameter(Arrays.asList(inactiveDataSet.get(0)));
		String testURL = this.url + "/console/rest/config/dataset/actions";
		com.jayway.restassured.response.Response response = given().cookie(this.cookie).param("resourceNames", resource).param("command","unterminate")
				.post(testURL);
		assertTrue("Failed to get the response for " + testURL , (response != null || response.toString() != "") );
		assertTrue("Non-Admin should not be able to Activate the dataset,  Http Response code  = " + response.getStatusCode() , response.getStatusCode() == 401);
	}
	
	/**
	 * Test Scenario : Verify whether non-admin user is not able to ACTIVIATE  the dataset
	 * Expected Result : Non-Admin should be able to INACTIVATE the dataset.
	 */
	public void testActivatingDataSet() {
		List<String> inactiveDataSet = given().cookie(this.cookie).get(this.url + "/console/api/datasets/view?checkverify=true&status=INACTIVE").getBody().jsonPath().getList("DatasetsResult.DatasetName");
		if (inactiveDataSet == null) {
			fail(" Failed to get ACTIVE datasets.");
		}
		String resource = this.jsonUtil.constructResourceNamesParameter(Arrays.asList(inactiveDataSet.get(0)));
		String testURL = this.url + "/console/rest/config/dataset/actions";
		com.jayway.restassured.response.Response response = given().cookie(this.cookie).param("resourceNames", resource).param("command","terminate")
				.post(testURL);
		assertTrue("Failed to get the response for " + testURL , (response != null || response.toString() != "") );
		assertTrue("Non-Admin should not be able to Activate the dataset,  Http Response code  = " + response.getStatusCode() , response.getStatusCode() == 401);
	}
	
	/**
	 * Test Scenario : Verify whether non-admin user is not possible to create a dataset.
	 * Expected Result : Non-Admin user should not be possible to create a dataset & he/she should get HTTP code 401.
	 */
	public void testCreatingDataSetForNonAdmin() { 
		
		httpHandle.logonToBouncer(this.nonAdminUserName, this.nonAdminPassWord);
		String dataSetName = "NonAdmin_Create_DataSet_"  + System.currentTimeMillis();

		// Read dataset and replace source and target values
		String dataSetXml = this.consoleHandle.getDataSetXml(this.baseDataSetName);
		dataSetXml = dataSetXml.replaceAll(baseDataSetName, dataSetName);

		Response response = this.consoleHandle.createDataSet(dataSetName, dataSetXml);
		TestSession.logger.info("Response code = " + response.getStatusCode());
		assertTrue("Non-Admin should not able to create a dataset " + dataSetName , response.getStatusCode() != 200);
	}
	
	/**
	 * Test Scenario  : Verify whether non-admin user is not able to disable the pause Retention for a given dataset
	 * Expected Result : Non-admin user  should not be possible to disable the pause retention.
	 */
	public void testDisablePauseRetentionForNonAdmin() {
		List<String> inactiveDataSet = given().cookie(this.cookie).get(this.url + "/console/api/datasets/view?checkverify=true&status=INACTIVE").getBody().jsonPath().getList("DatasetsResult.DatasetName");
		if (inactiveDataSet == null) {
			fail(" Failed to get ACTIVE datasets.");
		}
		String resource = this.jsonUtil.constructResourceNamesParameter(Arrays.asList(inactiveDataSet.get(0)));
		String testURL = this.url + "/console/rest/config/dataset/actions";
		com.jayway.restassured.response.Response response = given().cookie(this.cookie).param("resourceNames", resource).param("command","disableRetention")
				.post(testURL);
		assertTrue("Failed to get the response for " + testURL , (response != null || response.toString() != "") );
		JsonPath jsonPath = response.jsonPath();
		TestSession.logger.info("DataSet = " + jsonPath.prettyPrint());
		assertTrue("Non-Admin should not be able to disable pause retention the dataset,  Http Response code  = " + jsonPath.getString("Response.ResponseId") , jsonPath.getString("Response.ResponseId").equals("-1"));
	}
	
	/**
	 * Test Scenario  : Verify whether non-admin user is not able to enable the pause Retention for a given dataset
	 * Expected Result : Non-admin user  should not be possible to enable the pause retention.
	 */
	public void testEnablePauseRetentionForNonAdmin() {
		List<String> inactiveDataSet = given().cookie(this.cookie).get(this.url + "/console/api/datasets/view?checkverify=true&status=INACTIVE").getBody().jsonPath().getList("DatasetsResult.DatasetName");
		if (inactiveDataSet == null) {
			fail(" Failed to get INACTIVE datasets.");
		}
		String resource = this.jsonUtil.constructResourceNamesParameter(Arrays.asList(inactiveDataSet.get(0)));
		String testURL = this.url + "/console/rest/config/dataset/actions";
		com.jayway.restassured.response.Response response = given().cookie(this.cookie).param("resourceNames", resource).param("command","enableRetention")
				.post(testURL);
		assertTrue("Failed to get the response for " + testURL , (response != null || response.toString() != "") );
		JsonPath jsonPath = response.jsonPath();
		TestSession.logger.info("DataSet = " + jsonPath.prettyPrint());
		assertTrue("Non-Admin should not be able to enable pause retention the dataset,  Http Response code  = " + jsonPath.getString("Response.ResponseId") , jsonPath.getString("Response.ResponseId").equals("-1"));
	}
	
	/**
	 * Test Scenario : Verify whether non-admin should not be able to deactivate the target in the dataset
	 */
	public void testDeActiveTarget() {
		List<String> inactiveDataSet = given().cookie(this.cookie).get(this.url + "/console/api/datasets/view?checkverify=true&status=INACTIVE").getBody().jsonPath().getList("DatasetsResult.DatasetName");
		if (inactiveDataSet == null) {
			fail(" Failed to get INACTIVE datasets.");
		}
		String dataSetName  = inactiveDataSet.get(0);
		String resource = this.jsonUtil.constructResourceNamesParameter(Arrays.asList(dataSetName));
		List<String>dataSetTargetList = this.consoleHandle.getDataSource(dataSetName, "target" ,"name");
		String args = this.jsonUtil.constructArgumentParameter(dataSetTargetList,"deactivateTarget");

		String testURL = this.url + "/console/rest/config/dataset/actions";
		com.jayway.restassured.response.Response response = given().cookie(this.cookie).param("resourceNames", resource).param("command","update").param("args", args)
				.post(testURL);
		assertTrue("Failed to get the response for " + testURL , (response != null || response.toString() != "") );
		JsonPath jsonPath = response.jsonPath();
		TestSession.logger.info("DataSet = " + jsonPath.prettyPrint());
		assertTrue("Non-Admin should not be able to deactivate the target in the dataset,  Http Response code  = " + jsonPath.getString("Response.ResponseId") , jsonPath.getString("Response.ResponseId").equals("-1"));
		String message = jsonPath.getString("Response.ResponseMessage");
		boolean flag = message.contains("not a GDM admin and doesn't have permission to remove/deactivate targets");
		assertTrue("Got message = "  + message  , flag == true);
	}
}
