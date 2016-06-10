package hadooptest.gdm.regression.api;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertTrue;
import hadooptest.TestSession;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.HTTPHandle;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.jayway.restassured.path.json.JsonPath;

/**
 *  Test Case : To test GDM HealthCheckup on facets.
 */
public class TestGDMHealthCheckup extends TestSession {

	private ConsoleHandle consoleHandle;
	private String cookie;
	private String targetGrid1;
	private List<String> hcatSupportedGrid;
	private static final String HCAT_ENABLED = "TRUE";
	private static final String HCAT_DISABLED = "FALSE";

	@BeforeClass
	public static void startTestSession() {
		TestSession.start();
	}

	@Before
	public void setup() throws Exception {
		this.consoleHandle = new ConsoleHandle();
		HTTPHandle httpHandle = new HTTPHandle();
		this.cookie = httpHandle.getBouncerCookie();

		// get all the hcat supported clusters
		hcatSupportedGrid = this.consoleHandle.getHCatEnabledGrid();

		// check whether we have two hcat cluster one for acquisition and replication
		if (hcatSupportedGrid.size() == 0 ) {
			TestSession.logger.info("There is not hive installed on any cluster.");
		} else if (hcatSupportedGrid.size() >= 1) {
			this.targetGrid1 = hcatSupportedGrid.get(0).trim();
			TestSession.logger.info("Using grids " + this.targetGrid1 );
			
			// initially disable hcat 
			disableHCatOnDataSource(this.targetGrid1);
		}
	}

	@Test
	public void testHealthCheckUp() {
		
		if (hcatSupportedGrid.size() >= 1) {
			testAcquisitionHealthCheckup();
			testReplicationHealthCheckup();
			testRetentionHealthCheckup();

			// enable hcat for the given datasource
			enableHCatOnDataSource(this.targetGrid1);

			testHCatEnabledOnAcquisitionFacet();
			testHCatEnabledOnReplicationFacet();

			// disable hcat for a given datasource
			disableHCatOnDataSource(this.targetGrid1);

			testHCatDisabledOnAcquisitionFacet();
			testHCatDisabledOnReplicationFacet();
		} else {
			TestSession.logger.info("There is no hive installed on any cluster, please install hive and run the testcase.");
		}
		
		
	}

	/**
	 * Test Scenario : Verify whether acquisition health checkup services are up and running 
	 */
	public void testAcquisitionHealthCheckup() {
		testFacetHealthCheckUp( "acquisition");
	}

	/**
	 * Test Scenario : Verify whether replication health checkup services are up and running 
	 */
	public void testReplicationHealthCheckup() {
		testFacetHealthCheckUp("replication");
	}

	/**
	 * Test Scenario : Verify whether retention health checkup services are up and running 
	 */
	public void testRetentionHealthCheckup() {
		testFacetHealthCheckUp("retention");
	}

	/**
	 * Test Scenario : Verify whether HCAT enabled cluster is seen in acquisition health checkup
	 */
	public void testHCatEnabledOnAcquisitionFacet() {
		Map<String , String> applicationSummary = this.getHealthCheckDetails("acquisition");
		boolean isHcatEnabled = applicationSummary.containsKey("HCat connection - "+this.targetGrid1);
		assertTrue("Expected that Hcat is enabled on " + this.targetGrid1 + " but failed to get the hcat details " , isHcatEnabled == true);
		assertTrue("Expected HCat connection - "+this.targetGrid1 + "  " + applicationSummary.get("HCat connection - "+this.targetGrid1) , applicationSummary.get("HCat connection - "+this.targetGrid1).equals("valid") );
	}

	/**
	 * Test Scenario : Verify whether HCAT enabled cluster is seen in replication health checkup
	 */
	public void testHCatEnabledOnReplicationFacet() {
		Map<String , String> applicationSummary = this.getHealthCheckDetails("replication");
		boolean isHcatEnabled = applicationSummary.containsKey("HCat connection - "+this.targetGrid1);
		assertTrue("Expected that Hcat is enabled on " + this.targetGrid1 + " but failed to get the hcat details " , isHcatEnabled == true);
		assertTrue("Expected HCat connection - "+this.targetGrid1 + "  " + applicationSummary.get("HCat connection - "+this.targetGrid1) , applicationSummary.get("HCat connection - "+this.targetGrid1).equals("valid") );
	}

	/*
	 * Test Scenario : Verify whether HCAT information is not seen for acquisition facet, when HCAT is disabled.
	 */
	public void testHCatDisabledOnAcquisitionFacet() {
		Map<String , String> applicationSummary = this.getHealthCheckDetails("acquisition");
		boolean isHcatEnabled = applicationSummary.containsKey("HCat connection - "+this.targetGrid1);
		assertTrue("Expected that Hcat is enabled on " + this.targetGrid1 + " but failed to get the hcat details " , isHcatEnabled == false);
	}

	/*
	 * Test Scenario : Verify whether HCAT information is not seen for replication facet, when HCAT is disabled.
	 */
	public void testHCatDisabledOnReplicationFacet() {
		Map<String , String> applicationSummary = this.getHealthCheckDetails("acquisition");
		boolean isHcatEnabled = applicationSummary.containsKey("HCat connection - "+this.targetGrid1);
		assertTrue("Expected that Hcat is enabled on " + this.targetGrid1 + " but failed to get the hcat details " , isHcatEnabled == false);
	}

	/**
	 * Enable HCAT for a given dataSource
	 * @param dataSource - DataSource for which HCAT has to be enabled.
	 */
	private void enableHCatOnDataSource(String dataSource) {
		this.consoleHandle.modifyDataSource(dataSource, "HCatSupported", HCAT_DISABLED, HCAT_ENABLED);
		this.consoleHandle.sleep(40000);
	}

	/**
	 * Disable HCAT for a given dataSource
	 * @param dataSource - DataSource for which HCAT has to be disabled.
	 */
	private void disableHCatOnDataSource(String dataSource) {
		this.consoleHandle.modifyDataSource(dataSource, "HCatSupported", HCAT_ENABLED , HCAT_DISABLED);
		this.consoleHandle.sleep(40000);
	}

	/**
	 * Common method that just assert the values of the health check up for different facets. 
	 * @param facetName
	 */
	private void testFacetHealthCheckUp(String facetName) {
		Map<String , String> applicationSummary = this.getHealthCheckDetails(facetName);
		assertTrue("Expected ApplicationStatus to be running but got " + applicationSummary.get("ApplicationStatus") , applicationSummary.get("ApplicationStatus").equals("Running") );
		assertTrue("Expected Current State to be Active but got " + applicationSummary.get("Current State") ,  applicationSummary.get("Current State").endsWith("Active"));
	}

	/**
	 * Request the facet health checkup and returns the key and value.
	 * @param facetName
	 * @return
	 */
	private Map<String , String> getHealthCheckDetails(String facetName) {
		String	consoleHealthCheckUpTestURL = this.consoleHandle.getConsoleURL()+ "/console/api/proxy/health?colo=gq1&facet=" + facetName;
		TestSession.logger.info("consoleHealthCheckUpTestURL = " +consoleHealthCheckUpTestURL );
		com.jayway.restassured.response.Response response = given().cookie(this.cookie).get(consoleHealthCheckUpTestURL);
		assertTrue("Failed to get the response for " + consoleHealthCheckUpTestURL , (response != null) );
		String resString = response.asString();
		TestSession.logger.info("response = " + resString);
		JsonPath jsonPath = new JsonPath(resString);
		Map<String , String>applicationSummary = new HashMap<String, String>();
		List<String> keys = jsonPath.get("ApplicationSummary.Parameter");
		List<String> values = jsonPath.get("ApplicationSummary.Value");
		for(int i = 0;i<keys.size() ; i++){
			applicationSummary.put(keys.get(i), values.get(i));
		}
		return applicationSummary;
	}
	
	@AfterClass
	public void tearDown() {
		// since the test disabled the hcat on the target hcat testcases were failing. so enabling the hcat since this testcase disabled it.
		enableHCatOnDataSource(this.targetGrid1);		
	}
}
