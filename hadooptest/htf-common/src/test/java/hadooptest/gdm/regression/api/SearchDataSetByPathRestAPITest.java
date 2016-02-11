package hadooptest.gdm.regression.api;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.jayway.restassured.path.json.JsonPath;

import hadooptest.SerialTests;
import hadooptest.TestSession;
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
//@RunWith(GDMGenerateReport.class)
public class SearchDataSetByPathRestAPITest extends TestSession {

	public static final String dataSetPath = "/console/query/config/dataset/getDatasets";
	private static final String dataSetRestAPIPath = "/console/api/datasets/view";
	private String cookie;
	private String url;
	private ConsoleHandle consoleHandle;
	private JSONUtil jsonUtil;
	private List<String>dataSetsResultList;
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

		dataSetsResultList = given().cookie(cookie).get(this.url + dataSetPath ).getBody().jsonPath().getList("DatasetsResult.DatasetName");
		TestSession.logger.info("dataSetsResultList  = " + dataSetsResultList.toString());
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
		String dataSetPath = "/data/daqdev/data/";
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
		String dataSetName = dataSetsResultList.get(0).trim();
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
		String dataSetName = dataSetsResultList.get(0).trim();
		// get path for this dataset
		String getDataSetByNameURL = this.url + "/console/query/config/dataset/v1/" + dataSetName + "?format=json";
		response = given().cookie(cookie).get(getDataSetByNameURL);
                String responseString = response.getBody().asString();
                JSONObject jsonObject = (JSONObject) JSONSerializer.toJSON(responseString);
                jsonObject = jsonObject.getJSONObject("DataSet");
		JSONArray jsonArray = jsonObject.getJSONArray("SourcePaths");
		String dataSetPath = (String)jsonArray.get(0);
                
		String testURL = this.url + dataSetRestAPIPath + "?prefix=" + dataSetPath;
		TestSession.logger.info("testURL = " + testURL);
		response = given().cookie(cookie).get(testURL);
		JsonPath jsonPath = response.jsonPath();
		TestSession.logger.info("Response = " + jsonPath.prettyPrint());
		List<String> dataSetNamesList = jsonPath.getList("DatasetsResult.DatasetName");
		assertTrue("Expected " + dataSetName + " dataset but found " + dataSetNamesList.toString() , dataSetNamesList.get(0).trim().equals(dataSetName));
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
		assertTrue("Expected not null dataset but got " +dataSetNamesList.toString() , !dataSetsResultList.isEmpty());
	}

}
