package hadooptest.gdm.regression;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import hadooptest.TestSession;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.cluster.gdm.HTTPHandle;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Iterator;
import java.util.TimeZone;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * Test Case : Verify whether size-limit alert is created, when max and min size is specified in the dataset. 
 *
 */
public class TestCreateSizeLimitAlert extends TestSession {

	private ConsoleHandle consoleHandle;
	private String cookie;
	private String dataSetName;
	private String dsActivationTime; 
	private WorkFlowHelper workFlowHelperObj = null;
	private static String baseDataSetName = "VerifyAcqRepRetWorkFlowExecutionDateRange";
	private static final int SUCCESS = 200;
	private static String ALERT_API = "/console/api/alerts?"; 

	@BeforeClass
	public static void startTestSession() {
		TestSession.start();
	}

	@Before
	public void setup() throws Exception {
		this.consoleHandle = new ConsoleHandle();
		HTTPHandle httpHandle = new HTTPHandle();
		this.cookie = httpHandle.getBouncerCookie();
		this.dataSetName = "Testing_SizeLimitAlert_" + System.currentTimeMillis();
		this.workFlowHelperObj = new WorkFlowHelper();
	}

	@Test
	public void testSizeLimitAlert() throws Exception {

		// create the dataset.
		createTestDataSet();

		// activate the dataset
		this.consoleHandle.checkAndActivateDataSet(this.dataSetName);
		this.dsActivationTime = GdmUtils.getCalendarAsString();

		// check for acquisition workflow
		this.workFlowHelperObj.checkWorkFlow(this.dataSetName , "acquisition", this.dsActivationTime);

		// invoke rest api to check whether alert is generated for acquisition facet.
		checkSizeLimitAlertIsProduced(this.dataSetName , "acquisition");

		// check for replication workflow
		this.workFlowHelperObj.checkWorkFlow(this.dataSetName , "replication", this.dsActivationTime);

		// invoke rest api to check whether alert is generated for replication facet.
		checkSizeLimitAlertIsProduced(this.dataSetName , "replication");
	}

	/**
	 * Create a dataset
	 */
	public void createTestDataSet() {

		StringBuilder dataSetBuilder = new StringBuilder(this.consoleHandle.getDataSetXml(this.baseDataSetName));
		int indexOf = dataSetBuilder.indexOf("</DiscoveryInterface>") + "</DiscoveryInterface>".length();
		dataSetBuilder.insert(indexOf, "\n<SmallestExpectedInstanceSize>100</SmallestExpectedInstanceSize>\n<LargestExpectedInstanceSize>1000</LargestExpectedInstanceSize>");

		String dataSetXml = dataSetBuilder.toString();
		TestSession.logger.info("dataSetXml  = " + dataSetXml);

		// replace basedatasetName with the new datasetname
		dataSetXml = dataSetXml.replaceAll(this.baseDataSetName, this.dataSetName);
		dataSetXml = dataSetXml.replaceAll("<SmallestExpectedInstanceSize/>", "");
		dataSetXml = dataSetXml.replaceAll("<LargestExpectedInstanceSize/>", "");
		
		TestSession.logger.info("after changing the dataset name    = " + dataSetXml);

		// Create a new dataset
		Response response = this.consoleHandle.createDataSet(this.dataSetName, dataSetXml);
		assertTrue("Failed to create the dataset " + this.dataSetName ,  response.getStatusCode() == SUCCESS);
		this.consoleHandle.sleep(5000);
	}

	/**
	 * Check whether newly created size limit alert is created for the created dataset on the specified facet.
	 * @param dataSetName
	 */
	public void checkSizeLimitAlertIsProduced(String dataSetName , String facetName) {

		Calendar cal = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
		String endDate = sdf.format(cal.getTime());
		cal.add(Calendar.DAY_OF_MONTH, -1);
		String startDate = sdf.format(cal.getTime());
		TestSession.logger.info("start date = " + startDate  + "   end date = " + endDate);

		String severityType = "Warning";
		String facetType = "replication";
		String alertType = "size-limit";

		String testURL = this.consoleHandle.getConsoleURL() + this.ALERT_API +  "severity=" + severityType + "&starttime=" + startDate + "&endtime=" + endDate  
				+ "&type=" + alertType +"&facet=" + facetName;

		TestSession.logger.info("queueAbortURL  = " + testURL);
		com.jayway.restassured.response.Response response = given().cookie(this.cookie).get(testURL);

		String res = response.getBody().asString();
		TestSession.logger.info("response = " + res);
		JSONObject alertObj =  (JSONObject) JSONSerializer.toJSON(res.toString());
		Object obj = alertObj.get("AlertEvents");
		if (obj instanceof JSONArray) {
			System.out.println("its an array");
			JSONArray sizeLimitAlertArray = alertObj.getJSONArray("AlertEvents");
			Iterator iterator = sizeLimitAlertArray.iterator();
			while (iterator.hasNext()) {
				JSONObject jsonObject = (JSONObject) iterator.next();
				String shortDescription = jsonObject.getString("shortDescription").trim();
				boolean flag = shortDescription.contains(dataSetName);
				if(flag == true) {
					assertTrue("" , flag == true);
					String alterTypeName = jsonObject.getString("name").trim();	
					assertTrue("Expected alter name to be size-limit, but got  " + alterTypeName , alterTypeName.equals("size-limit"));
					String longDescription = jsonObject.getString("longDescription").trim();
					assertTrue("Expected longDescription to contain " + this.dataSetName + "  but got  " +longDescription   , longDescription.contains(dataSetName));
				}			
			}
		} else if ( obj instanceof JSONObject ) { // observed that if there is only one alert, then array is not created & testcase fails.
			System.out.println("its an object");
			JSONObject AlertEventsObj = alertObj.getJSONObject("AlertEvents");
			String alterTypeName = AlertEventsObj.getString("name").trim();	
			assertTrue("Expected alter name to be size-limit, but got  " + alterTypeName , alterTypeName.equals("size-limit"));
			String longDescription = AlertEventsObj.getString("longDescription").trim();
			assertTrue("Expected longDescription to contain " + this.dataSetName + "  but got  " + longDescription   , longDescription.indexOf(dataSetName) > 0);
		}
	}

	@After
	public void tearDown() throws Exception {
		// deactivate the dataset
		Response response = this.consoleHandle.deactivateDataSet(this.dataSetName);
		assertEquals("ResponseCode - Deactivate DataSet", 200, response.getStatusCode());
		assertEquals("ActionName.", "terminate", response.getElementAtPath("/Response/ActionName").toString());
		assertEquals("ResponseId", "0", response.getElementAtPath("/Response/ResponseId").toString());
		assertEquals("ResponseMessage.", "Operation on " + this.dataSetName + " was successful.", response.getElementAtPath("/Response/ResponseMessage/[0]").toString());
	}
}
