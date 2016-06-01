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

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.jayway.restassured.path.json.JsonPath;
import com.jayway.restassured.response.Response;

import hadooptest.SerialTests;
import hadooptest.Util;

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
	private String sourceGrid;
	private String target;
	private String newDataSetName =  "TestDataSet_" + System.currentTimeMillis();
	private static final String INSTANCE1 = "20151201";
	public static final String DATA_SET_PATH = "/console/query/config/dataset/getDatasets";
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

		datasetsResultList = getDataSetListing(cookie , this.url + this.DATA_SET_PATH).getBody().jsonPath().getList("DatasetsResult.DatasetName");
		if (datasetsResultList == null) {
			fail("Failed to get the datasets");
		}

		if (datasetsResultList.size() == 0) {
			// create the dataset.

			List<String> grids = this.consoleHandle.getUniqueGrids();
			if (grids.size() < 2) {
				Assert.fail("Only " + grids.size() + " of 2 required grids exist");
			}
			this.sourceGrid = grids.get(0);
			this.target = grids.get(1);
			createDataset();

			datasetsResultList = getDataSetListing(cookie , this.url + this.DATA_SET_PATH).getBody().jsonPath().getList("DatasetsResult.DatasetName");
			assertTrue("Failed to get the newly created dataset name" , datasetsResultList.size() > 0);
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
		List<String> dataNameList = new java.util.ArrayList<String>();
		if (datasetsResultList.size() == 1) {
			dataNameList.add(datasetsResultList.get(0));
		} else if (datasetsResultList.size() >= 1) {
			dataNameList.add(datasetsResultList.get(0));
			dataNameList.add(datasetsResultList.get(1));
		}
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
		List<String> dataNameList = new java.util.ArrayList<String>();
		if (datasetsResultList.size() == 1) {
			dataNameList.add(datasetsResultList.get(0));
		} else if (datasetsResultList.size() >= 1) {
			dataNameList.add(datasetsResultList.get(0));
			dataNameList.add(datasetsResultList.get(1));
		}
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

	private void createDataset() {
		String basePath = "/projects/" + this.newDataSetName + "/data/%{date}";
		String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/BasicReplDataSet1Target.xml");
		String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.newDataSetName, dataSetConfigFile);
		dataSetXml = dataSetXml.replaceAll("TARGET", this.target);
		dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", this.newDataSetName);
		dataSetXml = dataSetXml.replaceAll("SOURCE", this.sourceGrid );
		dataSetXml = dataSetXml.replaceAll("START_TYPE", "fixed" );
		dataSetXml = dataSetXml.replaceAll("START_DATE", INSTANCE1);
		dataSetXml = dataSetXml.replaceAll("END_TYPE", "offset");
		dataSetXml = dataSetXml.replaceAll("END_DATE", "0");
		dataSetXml = dataSetXml.replaceAll("REPLICATION_DATA_PATH", basePath);
		dataSetXml = dataSetXml.replaceAll("CUSTOM_PATH_DATA", basePath);
		hadooptest.cluster.gdm.Response response = this.consoleHandle.createDataSet(this.newDataSetName, dataSetXml);
		if (response.getStatusCode() != org.apache.commons.httpclient.HttpStatus.SC_OK) {
			Assert.fail("Response status code is " + response.getStatusCode() + ", expected 200.");
		}

		this.consoleHandle.sleep(30000);
	}
}
