package hadooptest.gdm.regression.nonAdmin;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import hadooptest.TestSession;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.HTTPHandle;
import hadooptest.cluster.gdm.JSONUtil;
import hadooptest.cluster.gdm.Response;

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.jayway.restassured.path.json.JsonPath;


public class TestNonAdminChangeRetentionValues extends TestSession {

	private ConsoleHandle consoleHandle;
	private String cookie;
	private String url;
	private HTTPHandle httpHandle = null;
	private JSONUtil jsonUtil;
	private  String nonAdminUserName; 
	private  String nonAdminPassWord;
	private String nonAdminUserNameInGroup = "hitusr_1";
	private String nonAdminPassWordInGroup = "New2@password";

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
		testDisableRetentionForNonAdminWithUserNotInGroup();
		testDisableRetentionForNonAdminWithUserInGroup();
	}
	
	/**
	 * Test Scenario  : Verify whether non-admin user is not able to disable the  Retention for a given dataset
	 * Expected Result : Non-admin user  should not be possible to disable the pause retention.
	 */
	public void testDisableRetentionForNonAdminWithUserNotInGroup() {
		List<String> inactiveDataSet = given().cookie(this.cookie).get(this.url + "/console/api/datasets/view?checkverify=true&status=INACTIVE").getBody().jsonPath().getList("DatasetsResult.DatasetName");
		if (inactiveDataSet == null) {
			fail(" Failed to get ACTIVE datasets.");
		}
		
		String dataSetName = inactiveDataSet.get(0);
		TestSession.logger.info("dataSetName = " + dataSetName);
		String resource = this.jsonUtil.constructResourceNamesParameter(Arrays.asList(dataSetName));
		String testURL = this.url + "/console/rest/config/dataset/actions";
		com.jayway.restassured.response.Response response = given().cookie(this.cookie).param("resourceNames", resource).param("command","disableRetention")
				.post(testURL);
		assertTrue("Failed to get the response for " + testURL , (response != null || response.toString() != "") );
		JsonPath jsonPath = response.jsonPath();
		TestSession.logger.info("DataSet = " + jsonPath.prettyPrint());
		String message =  jsonPath.getString("Response.ResponseMessage");
		boolean flag = message.contains("failed")  && message.contains("Error")  && message.contains("not allowed");
		assertTrue("Non-Admin should not be able to disable pause retention the dataset,  Http Response code  = " + jsonPath.getString("Response.ResponseId") , jsonPath.getString("Response.ResponseId").equals("-1"));
		assertTrue("Expected the message to contain words like failed , Error & not allowed, but got " + message , flag == true);
	}
	
	/**
	 * Verify whether Non admin user is able to set the disable the dataset retention value.
	 * Note : Non-Admin user should be in the group
	 */
	public void testDisableRetentionForNonAdminWithUserInGroup() {
		List<String> inactiveDataSet = given().cookie(this.cookie).get(this.url + "/console/api/datasets/view?checkverify=true&status=INACTIVE").getBody().jsonPath().getList("DatasetsResult.DatasetName");
		if (inactiveDataSet == null) {
			fail(" Failed to get ACTIVE datasets.");
		}
		
		String dataSetName = inactiveDataSet.get(0);
		TestSession.logger.info("dataSetName = " + dataSetName);
		String resource = this.jsonUtil.constructResourceNamesParameter(Arrays.asList(dataSetName));
		String testURL = this.url + "/console/rest/config/dataset/actions";
		
		// create a new cookie for non-admin in the group
		httpHandle.logonToBouncer(this.nonAdminUserNameInGroup , this.nonAdminPassWordInGroup);
		this.cookie = httpHandle.getBouncerCookie();
		com.jayway.restassured.response.Response response = given().cookie(this.cookie).param("resourceNames", resource).param("command","disableRetention")
				.post(testURL);
		assertTrue("Failed to get the response for " + testURL , (response != null || response.toString() != "") );
		JsonPath jsonPath = response.jsonPath();
		TestSession.logger.info("DataSet = " + jsonPath.prettyPrint());
		assertTrue("Non-Admin user failed to disable the retention value for the given " + dataSetName , jsonPath.getString("Response.ResponseId").equals("0"));
		String message =  jsonPath.getString("Response.ResponseMessage");
		assertTrue("Expected successful message, but got  " + message , message.contains("successful"));
	}
	
}
