package hadooptest.gdm.regression.api;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.jayway.restassured.path.json.JsonPath;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.HTTPHandle;
import hadooptest.cluster.gdm.JSONUtil;
import hadooptest.cluster.gdm.report.GDMGenerateReport;
import hadooptest.SerialTests;

import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;
import net.sf.json.JSONArray;

/**
 * Release : 5.6.0
 * Feature : "Allow searching for datasets by data path"
 * Bug id : http://bug.corp.yahoo.com/show_bug.cgi?id=6602431
 * TestCase twiki : http://twiki.corp.yahoo.com/view/Grid/GDM_6602431_SearchingOfDataSet_By_DataPath_TestCases
 *
 */

@Category(SerialTests.class)
public class SearchDataSetByPathRestAPITest extends TestSession {

	public static final String dataSetPath = "/console/query/config/dataset/getDatasets";
	private static final String dataSetRestAPIPath = "/console/api/datasets/view";
	private String sourceGrid;
	private String target;
	private String newDataSetName =  "TestDataSet_" + System.currentTimeMillis();
	private static final String INSTANCE1 = "20151201";
	private String cookie;
	private String url;
	private ConsoleHandle consoleHandle;
	private JSONUtil jsonUtil;
	private List<String>datasetsResultList;
	private com.jayway.restassured.response.Response response;

	@BeforeClass
	public static void startTestSession() throws Exception {
		TestSession.start();
	}

	@Before
	public void setUp() throws NumberFormatException, Exception {
		HTTPHandle httpHandle = new HTTPHandle();
		this.consoleHandle = new ConsoleHandle();
		this.jsonUtil = new JSONUtil();
		this.cookie = httpHandle.getBouncerCookie();
		this.url = this.consoleHandle.getConsoleURL();

		datasetsResultList = given().cookie(cookie).get(this.url + dataSetPath ).getBody().jsonPath().getList("DatasetsResult.DatasetName");
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

			datasetsResultList = getDataSetListing(cookie , this.url + this.dataSetPath).getBody().jsonPath().getList("DatasetsResult.DatasetName");
			assertTrue("Failed to get the newly created dataset name" , datasetsResultList.size() > 0);
			TestSession.logger.info("dataSetsResultList  = " + datasetsResultList.toString());
		}
	}

	@Test
	public void testSearchDataSetByPathRestAPI() {
		testSearchDataSetByPartialPath();
		testSearchDataSetByPathUsingRegEx();
		testSearchDataSetByCompleteDataSetPath();
		testSearchDataSetByNonExistanceDataPath();
		testSearchDataSetByDataPathAsRegularExpression();
	}

	/**
	 * Test Scenario : Verify whether dataset gets selected for a given basepath of the dataset. 
	 * atleast  we should get one dataset from the given path.
	 */
	public void testSearchDataSetByPartialPath() {
		String dataSetPath = "/data/";
		response = given().cookie(cookie).get(this.url + dataSetRestAPIPath + "?prefix=" + dataSetPath);
		JsonPath jsonPath = response.jsonPath();
		TestSession.logger.info("url = " + url + dataSetRestAPIPath + "?prefix=" + dataSetPath);
		TestSession.logger.info("Response = " + jsonPath.prettyPrint());
		List<String> dataSetNamesList = jsonPath.getList("DatasetsResult.DatasetName");
		TestSession.logger.info("reponse = "+dataSetNamesList);

		// since we are specifying the dataset path, i assume that atlease one dataset gets selected.
		assertTrue("Expected atleast one dataSet get selected , but got " + dataSetNamesList.toString() , !dataSetNamesList.isEmpty());
	}

	/**
	 * Test Scenario : Get all the dataset that matches the regular expression of the dataset path
	 */
	public void testSearchDataSetByPathUsingRegEx() {
		String dataSetName = datasetsResultList.get(0).trim();
		String searchString  = dataSetName.substring(0, 4);
		String regEx = searchString + "*";
		String dataSetPath = "/data/daqdev/data/" + regEx;
		String testURL = this.url + dataSetRestAPIPath + "?prefix=" + dataSetPath;
		TestSession.logger.info("testURL = " + testURL);
		response = given().cookie(cookie).get(testURL);
		JsonPath jsonPath = response.jsonPath();
		TestSession.logger.info("Response = " + jsonPath.prettyPrint());
		List<String> dataSetNameList = jsonPath.getList("DatasetsResult.DatasetName");
		TestSession.logger.info("reponse = "+dataSetNameList);
		for (String dSetName : dataSetNameList ) {
			assertTrue("Expected that dataset starts with " + regEx  +" but got " + dSetName , dSetName.startsWith(searchString) == true);
		}
	}

	/**
	 * Test Scenario : Verify whether specifying the complete data path of the dataset returns the same dataset as the result.
	 *  i,e /data/daqdev/data/<dataSetName>
	 * 
	 */
	public void testSearchDataSetByCompleteDataSetPath() {
		
		List<String> dataSourceList = this.consoleHandle.getUniqueGrids();
		List<String> dataSetNameList;
		String dataSetName=null;
		
		// loop through the datastore until you get one dataset
		for (String dataStore : dataSourceList) {
			String testURL =  this.url + "/console/api/datasets/view?source=" + dataStore.trim();
			TestSession.logger.info("testURL = " + testURL);
			response = given().cookie(cookie).get(testURL);
			if (response != null) {
				dataSetNameList = response.jsonPath().getList("DatasetsResult.DatasetName");
				if (dataSetNameList.size() > 0) {
					dataSetName = dataSetNameList.get(0);
					
					//get SourcePaths for in dataset
					String getDataSetByNameURL = this.url + "/console/query/config/dataset/v1/" + dataSetName + "?format=json";
					TestSession.logger.info("getDataSetByNameURL - " + getDataSetByNameURL);
					response = given().cookie(cookie).get(getDataSetByNameURL);
					String responseString = response.getBody().asString();
					TestSession.logger.info("responseString = "  +  responseString);
					JSONObject jsonObject = (JSONObject) JSONSerializer.toJSON(responseString);
					
					// check whether response contains DataSet field
					assertTrue(jsonObject.toString() + " does not contain DataSet field " , jsonObject.containsKey("DataSet") == true);
					jsonObject = jsonObject.getJSONObject("DataSet");
					assertTrue("DataSetName key does not exists in reponse." , jsonObject.containsKey("SourcePaths") == true);
					
					JSONArray jsonArray = jsonObject.getJSONArray("SourcePaths");
					String dataSetPath = (String)jsonArray.get(0);

					testURL = this.url + dataSetRestAPIPath + "?prefix=" + dataSetPath;
					TestSession.logger.info("testURL = " + testURL);
					response = given().cookie(cookie).get(testURL);
					JsonPath jsonPath = response.jsonPath();
					TestSession.logger.info("Response = " + jsonPath.prettyPrint());
					List<String> dataSetNamesList = jsonPath.getList("DatasetsResult.DatasetName");
					assertTrue("Expected " + dataSetName + " dataset but found " + dataSetNamesList.toString() , dataSetNamesList.get(0).trim().equals(dataSetName));
					
					break;
				}
			}
		}		
	}

	/**
	 * Test Scenario : Verify whether searching for a non-existence data path dn't return any dataset. 
	 */
	public void testSearchDataSetByNonExistanceDataPath() {
		String dataSetPath = "/data/daqdev/UNKNOWN/";
		String testURL = this.url + dataSetRestAPIPath + "?prefix=" + dataSetPath;
		TestSession.logger.info("testURL = " + testURL);
		response = given().cookie(cookie).get(testURL);
		JsonPath jsonPath = response.jsonPath();
		TestSession.logger.info("Response = " + jsonPath.prettyPrint());
		List<String> dataSetNamesList = jsonPath.getList("DatasetsResult.DatasetName");
		assertTrue("Expected no dataset gets selected but found " + dataSetNamesList.toString() , dataSetNamesList.isEmpty() );
	}


	/**
	 * Test Scenario : Verify whether datasets are selected when user specifies the data path as regular expression.
	 */
	public void testSearchDataSetByDataPathAsRegularExpression() {
		String dataSetPath = "/data/da*";
		String testURL = this.url + dataSetRestAPIPath + "?prefix=" + dataSetPath;
		TestSession.logger.info("testURL = " + testURL);
		response = given().cookie(cookie).get(testURL);
		JsonPath jsonPath = response.jsonPath();
		TestSession.logger.info("Response = " + jsonPath.prettyPrint());
		List<String> dataSetNamesList = jsonPath.getList("DatasetsResult.DatasetName");

		// Assuming that atleast one dataset will get selected
		assertTrue("Expected not null dataset but got " +dataSetNamesList.toString() , !datasetsResultList.isEmpty());
	}

	private void createDataset() {
		String basePath = "/data/daqdev/" + this.newDataSetName + "/data/%{date}";
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

	private com.jayway.restassured.response.Response getDataSetListing(String cookie , String url)  {
		com.jayway.restassured.response.Response response = given().cookie(cookie).get(url );
		return response;
	}

}
