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
	}

	/**
	 * TestCase : Verify whether alertEvent response contains the Warning severity and type name is availability for acquisition facet
	 * Query parameters : severity = Warning , facet = acquisition , startDate = current date - 2 , endDate = current date , type = availability
	 */
	public void testAlertWithWarningServerityAndTypeAvailabilityForAcquisitionFacet() {
		String severityType = "Warning";
		String facetType = "acquisition";
		String alertType = "availability";
		this.executeAlertQuery(facetType , alertType , severityType );
	}

	/**
	 * TestCase : Verify whether alertEvent response contains the Warning severity verify and type name is availability for acquisition facet
	 * Query parameters : severity = Warning , facet = acquisition , startDate = current date - 2 , endDate = current date , type = sla
	 */
	public void testAlertWithWarningServerityAndTypeSlaForAcquisitionFacet() {
		String severityType = "Warning";
		String facetType = "acquisition";
		String alertType = "sla-miss";
		this.executeAlertQuery(facetType , alertType ,severityType );
	}

	/**
	 * TestCase : Verify whether alertEvent response contains the Warning severity and type name is availability for acquisition facet
	 * Query parameters : severity = Warning , facet = acquisition , startDate = current date - 2 , endDate = current date , type = sla
	 */
	public void testAlertWithWarningServerityAndTypeSystemWarningForAcquisitionFacet() {

		String severityType = "Warning";
		String facetType = "acquisition";
		String alertType = "system-warning";
		this.executeAlertQuery(facetType , alertType ,severityType );
	}

	/**
	 * method that executes the alert query for the given parameters and checks the results by navigating the response based on the alert type.
	 * 
	 */
	public void executeAlertQuery(String... args) {
	    String facetType = args[0];
	    String alertType = args[1];
	    String severityType = args[2];

	    Calendar cal = Calendar.getInstance();
	    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	    sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
	    String endDate = sdf.format(cal.getTime());
	    cal.add(Calendar.DAY_OF_MONTH, -5);
	    String startDate = sdf.format(cal.getTime());
	    TestSession.logger.info("start date = " + startDate  + "   end date = " + endDate);

	    String testURL = null;
	    if ( (args.length - 1 ) > 2) {
		String dataSetName = args[3];
		testURL = this.consoleHandle.getConsoleURL() + this.ALERT_API +  "severity=" + severityType + "&facet=" + facetType + "&starttime=" + startDate + "&endtime=" + endDate + "&type=" + alertType + "&datasetName=" + dataSetName;
		TestSession.logger.info("testURL  = " + testURL);
	    } else {
		testURL = this.consoleHandle.getConsoleURL() + this.ALERT_API +  "severity=" + severityType + "&facet=" + facetType + "&starttime=" + startDate + "&endtime=" + endDate + "&type=" + alertType ;
		TestSession.logger.info("testURL  = " + testURL);
	    }
	    Response response = given().cookie(this.cookie).get(testURL);
	    String responseString;
	    if ( response != null && !((responseString=response.getBody().asString()).equals("null")) ) {

		TestSession.logger.info("Response = " + responseString );

		// convert responseString to json
		JSONObject obj =  (JSONObject) JSONSerializer.toJSON(responseString.toString());
		Object tObj = obj.get("AlertEvents");
		if ( tObj instanceof JSONArray) {
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

			    if(alertType.equals("sla-miss")) {
				String name = jsonObject.getString("name");
				assertTrue("Expected availability, but got " + name  , name.equals("sla-miss"));
			    } else if (alertType.equals("system-warning")) {
				String name = jsonObject.getString("name");
				assertTrue("Expected availability, but got " + name  , name.equals("system-warning"));

				String context = jsonObject.getString("context");
				assertTrue("Expected Memory Threshold Exceeded , but got " + context , context.equals("Memory Threshold Exceeded"));

			    } else if (alertType.equals("availability")) {
				String name = jsonObject.getString("name");
				assertTrue("Expected availability, but got " + name  , name.equals("availability"));
			    }
			}
		    }else {
			TestSession.logger.info("Empty AlertEvents array, no alert were found for " + testURL  + "  query.");
		    }
		} else if ( tObj instanceof JSONObject) {
		    TestSession.logger.info("***************** not a json array, its  a simple json Object*************");
		    JSONObject jObj = obj.getJSONObject("AlertEvents");
		    String facetName = jObj.getString("facetName");
		    assertTrue("Expected " + facetType + " but got " + facetName , facetType.equals(facetName) );
		}
	    } else {
		// TODO : this else block will be removed once the response returning null is fixed.
		TestSession.logger.info("Didn't get any response for " + testURL  + "  query.");
	    }
	}
}
