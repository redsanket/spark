package hadooptest.gdm.regression.nonAdmin;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertTrue;
import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.HTTPHandle;
import hadooptest.cluster.gdm.JSONUtil;
import hadooptest.cluster.gdm.Response;

import java.util.Arrays;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.jayway.restassured.path.json.JsonPath;

/**
 * TestCase : Verify whether Non-Admin user is a able to change the retention value.
 *
 */
public class TestNonAdminChangeRetentionValues extends TestSession {

	private ConsoleHandle consoleHandle;
	private String cookie;
	private String url;
	private HTTPHandle httpHandle = null;
	private JSONUtil jsonUtil;
	private String dataSetName;
	private static final String nonAdminUserName = "hitusr_2"; 
	private static String nonAdminPassWord; 
	private static final String baseDataSetName = "VerifyAcqRepRetWorkFlowExecutionSingleDate";
	private static final String OPS_DB_GROUP = "ygrid_group_gdmtest";
	private static final int SUCCESS = 200;

	@BeforeClass
	public static void startTestSession() throws Exception {
		TestSession.start();
	}

	@Before
	public void setUp() throws NumberFormatException, Exception {
		this.consoleHandle = new ConsoleHandle();
		this.httpHandle = new HTTPHandle();
		this.jsonUtil = new JSONUtil();
		this.url = this.consoleHandle.getConsoleURL();
		TestSession.logger.info("url = " + url);
		this.cookie = this.httpHandle.getBouncerCookie();
		nonAdminPassWord = Util.getTestUserPasswordFromYkeykey("headless_user_hitusr_2");
	}

	@Test
	public void testNonAdmin() {
		
		this.dataSetName = "TestNonAdminChangingRetentionByRestAPI_"  + System.currentTimeMillis();
		createDataSet();
		
		testDisableRetentionForNonAdminWithUserNotInGroup();
		testDisableRetentionForNonAdminWithUserInGroup();
	}

	/**
	 * Test Scenario  : Verify whether non-admin user is able to disable the  Retention for a given dataset
	 * Expected Result : Non-admin user  should not be possible to disable the pause retention.
	 */
	public void testDisableRetentionForNonAdminWithUserNotInGroup() {
		String nonAdminUserNameNotInGroup = this.consoleHandle.getConf().getString("auth.nonAdminUser");
		String nonAdminPassWordNotInGroup = this.consoleHandle.getConf().getString("auth.nonAdminPassWord");
		
		// create theh cookie for the non-admin user
		TestSession.logger.info("nonAdminUserNameNotInGroup  = " + nonAdminUserNameNotInGroup  + "    nonAdminPassWordNotInGroup =   " + nonAdminPassWordNotInGroup);
		httpHandle.logonToBouncer(nonAdminUserNameNotInGroup , nonAdminPassWordNotInGroup);
		this.cookie = httpHandle.getBouncerCookie();
		
		String resource = this.jsonUtil.constructResourceNamesParameter(Arrays.asList(this.dataSetName));
		String testURL = this.url + "/console/rest/config/dataset/actions";
		com.jayway.restassured.response.Response response = given().cookie(this.cookie).param("resourceNames", resource).param("command","disableRetention")
				.post(testURL);
		assertTrue("Failed to get the response for " + testURL , (response != null || response.toString() != "") );
		JsonPath jsonPath = response.jsonPath();
		TestSession.logger.info("DataSet = " + jsonPath.prettyPrint());
		String message =  jsonPath.getString("Response.ResponseMessage");
		boolean flag = message.contains("successful");
		assertTrue("Non-admin user should be able to pause (disable) retention of the dataset, Http Response code = " + jsonPath.getString("Response.ResponseId") , jsonPath.getString("Response.ResponseId").equals("0"));
		assertTrue("Expected the message to contain words like failed , Error & not allowed, but got " + message , flag == true);
	}

	/**
	 * Verify whether Non admin user is able to set the disable the dataset retention value.
	 * Note : Non-Admin user should be in the group
	 */
	public void testDisableRetentionForNonAdminWithUserInGroup() throws Exception {
		
		String nonAdminUserNameInGroup = "hitusr_2"; 
		String nonAdminPassWordInGroup = Util.getTestUserPasswordFromYkeykey("headless_user_hitusr_2"); 
		this.httpHandle.logonToBouncer(this.nonAdminUserName, this.nonAdminPassWord);
		this.cookie = this.httpHandle.getBouncerCookie();
		
		String resource = this.jsonUtil.constructResourceNamesParameter(Arrays.asList(this.dataSetName));
		String testURL = this.url + "/console/rest/config/dataset/actions";
		com.jayway.restassured.response.Response response = given().cookie(this.cookie).param("resourceNames", resource).param("command","disableRetention")
				.post(testURL);
		assertTrue("Failed to get the response for " + testURL , (response != null || response.toString() != "") );
		JsonPath jsonPath = response.jsonPath();
		TestSession.logger.info("DataSet = " + jsonPath.prettyPrint());
		assertTrue("Non-Admin user failed to disable the retention value for the given " + dataSetName , jsonPath.getString("Response.ResponseId").equals("0"));
		String message =  jsonPath.getString("Response.ResponseMessage");
		assertTrue("Expected successful message, but got  " + message , message.contains("successful"));
	}

	/**
	 * Create a dataset and activate it as GDM Admin.
	 */
	private void createDataSet() {

		// create a dataset.
		String dataSetXml = this.consoleHandle.getDataSetXml(this.baseDataSetName);
		dataSetXml = dataSetXml.replaceAll(this.baseDataSetName, this.dataSetName);

		StringBuffer dataSetXmlStrBuffer = new StringBuffer(dataSetXml);
		int index = dataSetXmlStrBuffer.indexOf("</Targets>") + "</Targets>".length() + 1;

		// inserting SelfServe tag, ExtendedState ,History, Project and Tag.
		dataSetXmlStrBuffer.insert(index , "<SelfServe><OpsdbGroup>"+ OPS_DB_GROUP +"</OpsdbGroup><RequireGroupAdmin>FALSE</RequireGroupAdmin><SelfServeEnabled>TRUE</SelfServeEnabled><UseOpsdbGroup>TRUE</UseOpsdbGroup></SelfServe><ExtendedState><History/></ExtendedState><Project/><Tags/>");

		String datasetXML = dataSetXmlStrBuffer.toString();
		Response response = this.consoleHandle.createDataSet(this.dataSetName, datasetXML);
		assertTrue("Failed to create a dataset " +this.dataSetName , response.getStatusCode() == 200);
		
		this.consoleHandle.sleep(5000);
	}	
}
