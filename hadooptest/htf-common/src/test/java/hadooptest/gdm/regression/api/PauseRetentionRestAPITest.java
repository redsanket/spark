package hadooptest.gdm.regression.api;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.HTTPHandle;
import hadooptest.cluster.gdm.JSONUtil;

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.jayway.restassured.path.json.JsonPath;
import com.jayway.restassured.response.Response;

import hadooptest.SerialTests;

/**
 * 
 * TestCase : PauseRetention REST API
 *
 */
@Category(SerialTests.class)
public class PauseRetentionRestAPITest extends TestSession {

	private String cookie;
	private String url;
	private ConsoleHandle consoleHandle;
	private JSONUtil jsonUtil;
	public static final String DataSetPath = "/console/query/config/dataset/getDatasets";
	private static final String PauseRetentionPath ="/console/rest/config/dataset/actions";
	private List<String> datasetsResultList;
	private String hostName;
	public static final int SUCCESS = 200;

	@BeforeClass
	public static void startTestSession() throws Exception {
		TestSession.start();
	}

	@Before
	public void setUp() throws NumberFormatException, Exception {
		HTTPHandle httpHandle = new HTTPHandle();
		consoleHandle = new ConsoleHandle();
		jsonUtil = new JSONUtil();
		cookie = httpHandle.getBouncerCookie();
		this.url = this.consoleHandle.getConsoleURL();
		TestSession.logger.info("url = " + url);

		// Invoke "/console/query/config/dataset/getDatasets" GDM REST API and select DatasetName element(s) from the response & store them in List
		datasetsResultList = getDataSetListing(cookie , this.url + this.DataSetPath).getBody().jsonPath().getList("DatasetsResult.DatasetName");
		if (datasetsResultList == null) {
			fail("Failed to get the datasets");
		}
	}

	/**
	 * Verify whether setting disableRetention value to false for a given dataset is successful.
	 */
	@Test	
	public void testPauseRetentionDisabledOnSingleDataset() {
		String datasetName = datasetsResultList.get(0);
		String resource = jsonUtil.constructResourceNamesParameter(Arrays.asList(datasetName));
		Response response = given().cookie(cookie).param("resourceNames", resource).param("command","disableRetention").post(this.url + PauseRetentionPath);
		assertTrue(this.hostName + this.PauseRetentionPath + " failed and got http response " + response.getStatusCode() , response.getStatusCode() == SUCCESS);
		JsonPath jsonPath = response.jsonPath();
		TestSession.logger.info("Response = " + jsonPath.prettyPrint());
		assertTrue("Expected disableretention but got " + jsonPath.getString("Response.ActionName") , jsonPath.getString("Response.ActionName").equals("disableretention"));
		assertTrue("Expected 0 but got " + jsonPath.getString("Response.ResponseId") , jsonPath.getString("Response.ResponseId").equals("0"));
		assertTrue("Expected successful but got " + jsonPath.getString("Response.ResponseMessage") , jsonPath.getString("Response.ResponseMessage").contains("successful"));
	}

	/**
	 * Verify whether setting disableRetention value to true for a given dataset is successful.
	 */
	@Test
	public void testPauseRetentionEnabledOnSingleDataset() {
		String datasetName = datasetsResultList.get(0);
		String resource = jsonUtil.constructResourceNamesParameter(Arrays.asList(datasetName));
		Response response = given().cookie(cookie).param("resourceNames", resource).param("command","enableRetention").post(this.url + PauseRetentionPath);
		assertTrue(this.hostName + this.PauseRetentionPath + " failed and got http response " + response.getStatusCode() , response.getStatusCode() == SUCCESS);
		JsonPath jsonPath = response.jsonPath();
		TestSession.logger.info("Response = " + jsonPath.prettyPrint());
		assertTrue("Expected enableRetention but got " + jsonPath.getString("Response.ActionName") , jsonPath.getString("Response.ActionName").equals("enableretention"));
		assertTrue("Expected 0 but got " + jsonPath.getString("Response.ResponseId") , jsonPath.getString("Response.ResponseId").equals("0"));
		assertTrue("Expected successful but got " + jsonPath.getString("Response.ResponseMessage") , jsonPath.getString("Response.ResponseMessage").contains("successful"));
	}

	/**
	 * Verify whether setting a set of dataset 's disableRetention value to false is successful.
	 */
	@Test
	public void testPauseRetentionDisabledOnSetOfDataset() {
		String datasetName1 = datasetsResultList.get(0);
		String datasetName2 = datasetsResultList.get(1);
		List<String> dataNameList = Arrays.asList(datasetName1 , datasetName2);
		String resource = jsonUtil.constructResourceNamesParameter(dataNameList);
		Response response = given().cookie(cookie).param("resourceNames", resource).param("command","disableRetention").post(this.url + PauseRetentionPath);
		assertTrue(this.hostName + this.PauseRetentionPath + " failed and got http response " + response.getStatusCode() , response.getStatusCode() == SUCCESS);
		JsonPath jsonPath = response.jsonPath();
		TestSession.logger.info("Response = " + jsonPath.prettyPrint());
		assertTrue("Expected disableretention but got " + jsonPath.getString("Response.ActionName") , jsonPath.getString("Response.ActionName").equals("disableretention"));
		assertTrue("Expected 0 but got " + jsonPath.getString("Response.ResponseId") , jsonPath.getString("Response.ResponseId").equals("0"));
		List<String> datsetNames = Arrays.asList(jsonPath.getString("Response.ResponseMessage").split(","));
		for (String dsn : datsetNames) {
			assertEquals("Expected successful but got " + jsonPath.getString("Response.ResponseMessage") , dsn.contains("successful"), true );
		}
	}

	/**
	 * Verify whether setting a set of dataset 's disableRetention value to true is successful.
	 */
	@Test
	public void testPauseRetentionEnabledOnSetOfDataset() {
		String datasetName1 = datasetsResultList.get(0);
		String datasetName2 = datasetsResultList.get(1);
		List<String> dataNameList = Arrays.asList(datasetName1 , datasetName2);
		String resource = jsonUtil.constructResourceNamesParameter(dataNameList);
		Response response = given().cookie(cookie).param("resourceNames", resource).param("command","enableRetention").post(this.url + PauseRetentionPath);
		assertTrue(this.hostName + this.PauseRetentionPath + " failed and got http response " + response.getStatusCode() , response.getStatusCode() == SUCCESS);
		JsonPath jsonPath = response.jsonPath();
		TestSession.logger.info("Response = " + jsonPath.prettyPrint());
		assertTrue("Expected enableRetention but got " + jsonPath.getString("Response.ActionName") , jsonPath.getString("Response.ActionName").equals("enableretention"));
		assertTrue("Expected 0 but got " + jsonPath.getString("Response.ResponseId") , jsonPath.getString("Response.ResponseId").equals("0"));
		List<String> datsetNames = Arrays.asList(jsonPath.getString("Response.ResponseMessage").split(","));
		for (String dsn : datsetNames) {
			assertEquals("Expected successful but got " + jsonPath.getString("Response.ResponseMessage") , dsn.contains("successful"), true );
		}
	}

	/*
	 * Verify whether setting disableRetention value to true for a UNKNOWN dataset fails
	 */
	public void testPauseRetentionWithUNKNOWNDataset() {
		String resource = jsonUtil.constructResourceNamesParameter(Arrays.asList("UNKNOWN-DATASET"));
		Response response = given().cookie(cookie).param("resourceNames", resource).param("command","enableRetention").post(this.url + PauseRetentionPath);
		assertTrue(this.hostName + this.PauseRetentionPath + " failed and got http response " + response.getStatusCode() , response.getStatusCode() == SUCCESS);
		JsonPath jsonPath = response.jsonPath();
		TestSession.logger.info("Response = " + jsonPath.prettyPrint());
		assertTrue("Expected -1 but got " + jsonPath.getString("Response.ResponseId") , jsonPath.getString("Response.ResponseId").equals("-1"));
		assertTrue("Expected failed. Error: ConfigStore Exception. But got " +jsonPath.getString("Response.ResponseMessage") , jsonPath.getString("Response.ResponseMessage").contains("failed. Error: ConfigStore Exception.") );
	}
	
	private com.jayway.restassured.response.Response getDataSetListing(String cookie , String url)  {
		com.jayway.restassured.response.Response response = given().cookie(cookie).get(url );
		return response;
	}
}
