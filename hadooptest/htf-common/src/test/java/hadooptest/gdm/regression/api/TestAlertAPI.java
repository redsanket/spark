package hadooptest.gdm.regression.api;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertTrue;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;

import hadooptest.TestSession;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.HTTPHandle;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.jayway.restassured.response.Response;

/**
 * 
 * Test Scenario : Test Alert REST API.
 *
 */
public class TestAlertAPI extends TestSession {

	private String cookie;
	private ConsoleHandle consoleHandle;
	private HTTPHandle httpHandle ;
	private static String ALERT_API = "/console/api/alerts?"; 
	private List<String>dataSetNameList = new ArrayList<String>();
	
	@BeforeClass
	public static void startTestSession() throws Exception {
		TestSession.start();
	}

	@Before
	public void setUp() throws NumberFormatException, Exception {
		this.httpHandle = new HTTPHandle();
		this.consoleHandle = new ConsoleHandle();
		this.cookie = httpHandle.getBouncerCookie();
	}

	@Test
	public void testAlertRESTAPI() throws ParseException {
		testAlertWithWarningServerityAndTypeAvailabilityForAcquisitionFacet();
		testAlertWithWarningServerityAndTypeSlaForAcquisitionFacet();
		testAlertWithWarningServerityAndTypeSystemWarningForAcquisitionFacet();
		testAlertWithWarningServerityByDataSetName();
	}
	
	/**
	 * TestCase : Verify whether alertEvent response contains the Warning severity and type name is availability for acquisition facet
	 * Query parameters : severity = Warning , facet = acquisition , startDate = current date - 2 , endDate = current date , type = availability
	 */
	public void testAlertWithWarningServerityAndTypeAvailabilityForAcquisitionFacet() {
		
		Calendar cal = Calendar.getInstance();
	    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	    sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
	    String endDate = sdf.format(cal.getTime());
	    cal.add(Calendar.DAY_OF_MONTH, -5);
	    String startDate = sdf.format(cal.getTime());
	    TestSession.logger.info("start date = " + startDate  + "   end date = " + endDate);
	    
		String severityType = "Warning";
		String facetType = "acquisition";
		String type = "availability";
		String testURL = this.consoleHandle.getConsoleURL() + this.ALERT_API +  "severity=" + severityType + "&facet=" + facetType + "&starttime=" + startDate + "&endtime=" + endDate + "&type=" + type;
		TestSession.logger.info("testURL  = " + testURL);
		Response response = given().cookie(this.cookie).get(testURL);
		String responseString = response.getBody().asString();
		TestSession.logger.info("Response = " + responseString );
		assertTrue("Expected response not equal to null , but got " + responseString , responseString != null);
		
		// convert responseString to json
		JSONObject obj =  (JSONObject) JSONSerializer.toJSON(responseString.toString());
		JSONArray jsonArray = null;
		jsonArray = obj.getJSONArray("AlertEvents");
		
		if (jsonArray.size() > 0 && jsonArray != null) {
			Iterator iterator = jsonArray.iterator();
			while (iterator.hasNext()) {
				JSONObject jsonObject = (JSONObject) iterator.next();
				TestSession.logger.info("AlertEvents  = " + jsonObject.toString());
				
				// check for facet
				String facetName = jsonObject.getString("facetName");
				assertTrue("Expected " + facetType + " but got " + facetName , facetType.equals(facetName) );
				
				// check serverity type
				String serverity = jsonObject.getString("severity");
				assertTrue("Expected " + severityType + " but got " + serverity , severityType.equals(serverity));
				
				String name = jsonObject.getString("name");
				assertTrue("Expected availability, but got " + name  , name.equals("availability"));
				
				String context = jsonObject.getString("context");
				assertTrue("Expected Dataset unavailable , but got " + context , context.equals("Dataset unavailable"));
								
				JSONObject dataSetInstanceObject = jsonObject.getJSONObject("dataSetInstance");
				String dsName = dataSetInstanceObject.getString("@DataSetName");
				TestSession.logger.info("dataSetName = " + dsName);
				
				// this is useful for other testcase.
				dataSetNameList.add(dsName);
				
				String numOfFiles = dataSetInstanceObject.getString("@NumOfFiles");
				assertTrue("Expected to be 0 files, since the context is " + context , numOfFiles.equals("0"));
			}
		} else {
			TestSession.logger.info("Empty AlertEvents array");
		}
	}
	
	/**
	 * TestCase : Verify whether alertEvent response contains the Warning severity verify and type name is availability for acquisition facet
	 * Query parameters : severity = Warning , facet = acquisition , startDate = current date - 2 , endDate = current date , type = sla
	 */
	public void testAlertWithWarningServerityAndTypeSlaForAcquisitionFacet() {
		
		Calendar cal = Calendar.getInstance();
	    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	    sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
	    String endDate = sdf.format(cal.getTime());
	    cal.add(Calendar.DAY_OF_MONTH, -5);
	    String startDate = sdf.format(cal.getTime());
	    TestSession.logger.info("start date = " + startDate  + "   end date = " + endDate);
	    
		String severityType = "Warning";
		String facetType = "acquisition";
		String type = "sla";
		String testURL = this.consoleHandle.getConsoleURL() + this.ALERT_API +  "severity=" + severityType + "&facet=" + facetType + "&starttime=" + startDate + "&endtime=" + endDate + "&type=" + type;
		TestSession.logger.info("testURL  = " + testURL);
		Response response = given().cookie(this.cookie).get(testURL);
		String responseString = response.getBody().asString();
		TestSession.logger.info("Response = " + responseString );
		assertTrue("Expected response not equal to null , but got " + responseString , responseString != null);
		
		// convert responseString to json
		JSONObject obj =  (JSONObject) JSONSerializer.toJSON(responseString.toString());
		JSONArray jsonArray = null;
		jsonArray = obj.getJSONArray("AlertEvents");
		
		if (jsonArray.size() > 0 && jsonArray != null) {
			Iterator iterator = jsonArray.iterator();
			while (iterator.hasNext()) {
				JSONObject jsonObject = (JSONObject) iterator.next();
				TestSession.logger.info("AlertEvents  = " + jsonObject.toString());
				
				// check for facet
				String facetName = jsonObject.getString("facetName");
				assertTrue("Expected " + facetType + " but got " + facetName , facetType.equals(facetName) );
				
				// check serverity type
				String serverity = jsonObject.getString("severity");
				assertTrue("Expected " + severityType + " but got " + serverity , severityType.equals(serverity));
				
				String name = jsonObject.getString("name");
				assertTrue("Expected availability, but got " + name  , name.equals("sla"));
				
				String context = jsonObject.getString("context");
				assertTrue("Expected SLA_BREACH_LIKELY , but got " + context , context.equals("SLA_BREACH_LIKELY"));								
			}
		} else {
			TestSession.logger.info("Empty AlertEvents array");
		}
	}
	
	/**
	 * TestCase : Verify whether alertEvent response contains the Warning severity and type name is availability for acquisition facet
	 * Query parameters : severity = Warning , facet = acquisition , startDate = current date - 2 , endDate = current date , type = sla
	 */
	public void testAlertWithWarningServerityAndTypeSystemWarningForAcquisitionFacet() {
		
		Calendar cal = Calendar.getInstance();
	    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	    sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
	    String endDate = sdf.format(cal.getTime());
	    cal.add(Calendar.DAY_OF_MONTH, -5);
	    String startDate = sdf.format(cal.getTime());
	    TestSession.logger.info("start date = " + startDate  + "   end date = " + endDate);
	    
		String severityType = "Warning";
		String facetType = "acquisition";
		String type = "system-warning";
		String testURL = this.consoleHandle.getConsoleURL() + this.ALERT_API +  "severity=" + severityType + "&facet=" + facetType + "&starttime=" + startDate + "&endtime=" + endDate + "&type=" + type;
		TestSession.logger.info("testURL  = " + testURL);
		Response response = given().cookie(this.cookie).get(testURL);
		String responseString = response.getBody().asString();
		TestSession.logger.info("Response = " + responseString );
		assertTrue("Expected response not equal to null , but got " + responseString , responseString != null);
		
		// convert responseString to json
		JSONObject obj =  (JSONObject) JSONSerializer.toJSON(responseString.toString());
		JSONArray jsonArray = null;
		jsonArray = obj.getJSONArray("AlertEvents");
		
		if (jsonArray.size() > 0 && jsonArray != null) {
			Iterator iterator = jsonArray.iterator();
			while (iterator.hasNext()) {
				JSONObject jsonObject = (JSONObject) iterator.next();
				TestSession.logger.info("AlertEvents  = " + jsonObject.toString());
				
				// check for facet
				String facetName = jsonObject.getString("facetName");
				assertTrue("Expected " + facetType + " but got " + facetName , facetType.equals(facetName) );
				
				// check serverity type
				String serverity = jsonObject.getString("severity");
				assertTrue("Expected " + severityType + " but got " + serverity , severityType.equals(serverity));
				
				String name = jsonObject.getString("name");
				assertTrue("Expected availability, but got " + name  , name.equals("system-warning"));
				
				String context = jsonObject.getString("context");
				assertTrue("Expected Memory Threshold Exceeded , but got " + context , context.equals("Memory Threshold Exceeded"));								
			}
		} else {
			TestSession.logger.info("Empty AlertEvents array");
		}
	}
	
	
	
	/**
	 * TestCase : Verify whether alertEvent response contains the Warning severity , type name is availability and by dataset namefor acquisition facet
	 * Query parameters : severity = Warning , facet = acquisition , startDate = current date - 2 , endDate = current date , type = sla , datasetName
	 */
	public void testAlertWithWarningServerityByDataSetName() {
		
		Calendar cal = Calendar.getInstance();
	    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	    sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
	    String endDate = sdf.format(cal.getTime());
	    cal.add(Calendar.DAY_OF_MONTH, -5);
	    String startDate = sdf.format(cal.getTime());
	    TestSession.logger.info("start date = " + startDate  + "   end date = " + endDate);
	    
		String severityType = "Warning";
		String facetType = "acquisition";
		String type = "availability";
		
		assertTrue("Expected there is atleast one dataset, but got " +  this.dataSetNameList.size() , this.dataSetNameList.size() > 0 );
		String dataSetName = this.dataSetNameList.get(0);
		
		String testURL = this.consoleHandle.getConsoleURL() + this.ALERT_API +  "severity=" + severityType + "&facet=" + facetType + "&starttime=" + startDate + "&endtime=" + endDate + "&type=" + type + "&datasetName=" + dataSetName;
		TestSession.logger.info("testURL  = " + testURL);
		Response response = given().cookie(this.cookie).get(testURL);
		String responseString = response.getBody().asString();
		TestSession.logger.info("Response = " + responseString );
		assertTrue("Expected response not equal to null , but got " + responseString , responseString != null);
		
		// convert responseString to json
		JSONObject obj =  (JSONObject) JSONSerializer.toJSON(responseString.toString());
		JSONArray jsonArray = null;
		jsonArray = obj.getJSONArray("AlertEvents");
		
		if (jsonArray.size() > 0 && jsonArray != null) {
			Iterator iterator = jsonArray.iterator();
			while (iterator.hasNext()) {
				JSONObject jsonObject = (JSONObject) iterator.next();
				TestSession.logger.info("AlertEvents  = " + jsonObject.toString());
				
				// check for facet
				String facetName = jsonObject.getString("facetName");
				assertTrue("Expected " + facetType + " but got " + facetName , facetType.equals(facetName) );
				
				// check serverity type
				String serverity = jsonObject.getString("severity");
				assertTrue("Expected " + severityType + " but got " + serverity , severityType.equals(serverity));
				
				String name = jsonObject.getString("name");
				assertTrue("Expected availability, but got " + name  , name.equals(type));
				
				String context = jsonObject.getString("context");
				assertTrue("Expected Dataset unavailable , but got " + context , context.equals("Dataset unavailable"));								
			}
		} else {
			TestSession.logger.info("Empty AlertEvents array");
		}
	}
	
	
}
