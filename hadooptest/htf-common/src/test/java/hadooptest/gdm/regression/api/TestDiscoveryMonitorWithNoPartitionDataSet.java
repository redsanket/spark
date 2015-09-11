package hadooptest.gdm.regression.api;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertTrue;
import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.cluster.gdm.HTTPHandle;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;

import java.util.Iterator;
import java.util.List;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;


/**
 * Test Case : To Verify whether discovery monitor return the correct discovered value for non DSD partition.
 *
 */
@Category(SerialTests.class)
public class TestDiscoveryMonitorWithNoPartitionDataSet extends TestSession {

	private ConsoleHandle consoleHandle;
	private String dataSetName;
	private WorkFlowHelper workFlowHelper;
	private String datasetActivationTime;
	private String newDataSourceName ;
	private String dataSetWithErrMessage;
	private HTTPHandle httpHandle ;
	private String cookie;
	private List<String> grids = null;
	private String targetGrid1;
	private String targetGrid2;
	private String url;
	public static final String DISCOVERY_MONITOR = "/api/discovery/monitor";
	private static final String baseDataSetName = "VerifyAcqRepRetWorkFlowExecutionSingleDate";
	public static final int SUCCESS = 200;
	private static final String HCAT_ENABLED = "FALSE";
	private static final String customPathString = "<Paths><Path location=\"CUSTOM_COUNT_PATH\" type=\"count\"/><Path location=\"CUSTOM_DATA_PATH\" type=\"data\"/><Path location=\"CUSTOM_SCHEMA_PATH\" type=\"schema\"/></Paths>";

	@BeforeClass
	public static void startTestSession() {
		TestSession.start();
	}

	@Before
	public void setup() throws Exception {
		this.consoleHandle = new ConsoleHandle();
		HTTPHandle httpHandle = new HTTPHandle();
		this.cookie = httpHandle.getBouncerCookie();
		this.url = this.consoleHandle.getConsoleURL();
		TestSession.logger.info("url = " + this.url);
		this.dataSetName = "TestDiscoverMonitoringWithNonDataSet_"  + System.currentTimeMillis();
		this.workFlowHelper = new WorkFlowHelper();

		// Get all the grid with the current deployment and select first two grid as targets for datasest.  
		grids = this.consoleHandle.getAllInstalledGridName();
		if (grids.size() > 2) {
			this.targetGrid1 = grids.get(0);
			this.targetGrid2 = grids.get(1);

			TestSession.logger.info("target1 = " + this.targetGrid1); 
			TestSession.logger.info("target2 = " + this.targetGrid2);
		}
	}

	@Test
	public void testDiscoveryMonitorWithNoPartitionDataSet() {

		// create a dataset
		createDataSet(this.dataSetName);

		//  Monitor for dataset discovery
		JSONArray jsonArray = this.workFlowHelper.isDiscoveryMonitoringStarted("acquisition", this.dataSetName);
		if (jsonArray.size() > 0) {
			Iterator iterator = jsonArray.iterator();
			while (iterator.hasNext()) {
				JSONObject jsonObject = (JSONObject) iterator.next();
				String dataSet = jsonObject.getString("Dataset");
				String partition = jsonObject.getString("Partition");
				String status = jsonObject.getString("Status");
				String priority = jsonObject.getString("Priority");
				String discoverInterface = jsonObject.getString("Discovery Interface");
				String feedFrequency = jsonObject.getString("Feed Frequency");
				String poolFrequency = jsonObject.getString("Polling Frequency");
				assertTrue("Failed to get the  " + dataSet  , jsonObject.getString("Dataset").equals(this.dataSetName));
				assertTrue("Failed to get the srcid & partition id " + partition  , partition.equalsIgnoreCase("None") );
				assertTrue("Looks like there is some issue while discovery " + status  , status.equalsIgnoreCase("OK") );
				assertTrue("Failed expected  HIGHEST prioroty, but got " + priority  , priority.equalsIgnoreCase("HIGHEST") );
				assertTrue("Failed FDI discover interface , but got " + discoverInterface  , discoverInterface.equalsIgnoreCase("fdi") );
				assertTrue("Failed daily feedFrequency , but got " + feedFrequency  , feedFrequency.equalsIgnoreCase("daily") );
				assertTrue("Failed 180 sec for poolFrequency , but got " + poolFrequency  , poolFrequency.equalsIgnoreCase("180") );
			}
		}
		assertTrue("Failed to get discovery monitoring on acquisition for " + this.dataSetName , jsonArray.size() > 0);
	}

	/**
	 * Test Case : Verify whether passing falling behind to false returns all the dataset with status equal to OK
	 */
	@Test
	public void testDiscoverMonitorWithFallBehindParameterFalse() {
		String discoveryMonitoringURL = this.consoleHandle.getConsoleURL().replace("9999", this.consoleHandle.getFacetPortNo("acquisition")) + "/" + "acquisition" + this.DISCOVERY_MONITOR + "?fallingbehind=false";
		TestSession.logger.info("discoveryMonitoringURL = " + discoveryMonitoringURL);
		com.jayway.restassured.response.Response response = given().cookie(this.cookie).get(discoveryMonitoringURL);
		JSONArray jsonArray = this.consoleHandle.convertResponseToJSONArray(response, "Results");
		if (jsonArray.size() > 0) {
			Iterator iterator = jsonArray.iterator();
			while (iterator.hasNext()) {
				JSONObject jsonObject = (JSONObject) iterator.next();
				String status = jsonObject.getString("Status");
				String priority = jsonObject.getString("Priority");
				String discoverInterface = jsonObject.getString("Discovery Interface");
				String feedFrequency = jsonObject.getString("Feed Frequency");
				String poolFrequency = jsonObject.getString("Polling Frequency");
				assertTrue("Looks like there is some issue while discovery " + status  , status.equalsIgnoreCase("OK") );
				assertTrue("Failed expected  HIGHEST prioroty, but got " + priority  , priority.equalsIgnoreCase("HIGHEST") );
				assertTrue("Failed daily feedFrequency , but got " + feedFrequency  , feedFrequency.equalsIgnoreCase("daily") || feedFrequency.equalsIgnoreCase("hourly"));
			}
		}
	}

	@Test
	public void testDiscoveryMonitorWithDSDPartitionDataSet() throws Exception {
		String dsdDataSetName = "testDiscoveryMonitorWithDSDPartitionDataSet"  + System.currentTimeMillis();
		createDSDataSetAndActivate(dsdDataSetName);

		//  Monitor for dataset discovery
		JSONArray jsonArray = this.workFlowHelper.isDiscoveryMonitoringStarted("acquisition", dsdDataSetName);
		if (jsonArray.size() > 0) {
			Iterator iterator = jsonArray.iterator();
			while (iterator.hasNext()) {
				JSONObject jsonObject = (JSONObject) iterator.next();
				String dataSet = jsonObject.getString("Dataset");
				String partition = jsonObject.getString("Partition");
				String status = jsonObject.getString("Status");
				String priority = jsonObject.getString("Priority");
				String discoverInterface = jsonObject.getString("Discovery Interface");
				String feedFrequency = jsonObject.getString("Feed Frequency");
				String poolFrequency = jsonObject.getString("Polling Frequency");
				assertTrue("Failed to get the  " + dataSet  , jsonObject.getString("Dataset").equals(dsdDataSetName));
				assertTrue("Failed to get the srcid & partition id " + partition  , partition.equalsIgnoreCase("srcid_1780") || partition.equalsIgnoreCase("srcid_23440") );
				assertTrue("Looks like there is some issue while discovery " + status  , status.equalsIgnoreCase("OK") );
				assertTrue("Failed expected  HIGHEST prioroty, but got " + priority  , priority.equalsIgnoreCase("HIGHEST") );
				//assertTrue("Failed FDI discover interface , but got " + discoverInterface  , discoverInterface.equalsIgnoreCase("fdi") );
				assertTrue("Failed daily feedFrequency , but got " + feedFrequency  , feedFrequency.equalsIgnoreCase("daily") );
				assertTrue("Failed 100 sec for poolFrequency , but got " + poolFrequency  , poolFrequency.equalsIgnoreCase("100") );
			}
		}
		assertTrue("Failed to get discovery monitoring on acquisition for " + this.dataSetName , jsonArray.size() > 0);
	}

	/**
	 * Method to create a dataset for acquisition and replication
	 * @param baseDataSet
	 * @throws Exception 
	 */
	public void createDSDataSetAndActivate(String dsdDataSetName ) throws Exception {
		String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/" + "DSDPartitionDataSet.xml");
		String tempdataSetXml = this.consoleHandle.createDataSetXmlFromConfig(dsdDataSetName, dataSetConfigFile);
		String feedName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Parameters" , "value");
		String sourceName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Sources" , "name");

		StringBuffer newDataSetXml = new StringBuffer(tempdataSetXml);

		// insert custom path for first target ( acquisition ) 
		int firstIndexValue =  newDataSetXml.indexOf("</DateRange>") + "</DateRange>".length();
		newDataSetXml.insert(firstIndexValue, customPathString);

		// insert custom path for second target ( replication ) 
		int secondIndexValue = newDataSetXml.lastIndexOf("</DateRange>") + "</DateRange>".length();
		newDataSetXml.insert(secondIndexValue, customPathString);

		String dataSetXml = newDataSetXml.toString();
		dataSetXml = dataSetXml.replaceAll("FEED_NAME", feedName );
		dataSetXml = dataSetXml.replaceAll("FEED_STATS", feedName + "_stats" );
		dataSetXml = dataSetXml.replaceAll("FDI_SERVER_NAME", sourceName );
		dataSetXml = dataSetXml.replaceAll("TARGET1", this.targetGrid1 );
		dataSetXml = dataSetXml.replaceAll("TARGET2", this.targetGrid2 );
		dataSetXml = dataSetXml.replaceAll("HCAT_ENABLED", this.HCAT_ENABLED );
		dataSetXml = dataSetXml.replaceAll("CUSTOM_DATA_PATH", this.getCustomPath("data" , dsdDataSetName  ) );
		dataSetXml = dataSetXml.replaceAll("CUSTOM_COUNT_PATH",  this.getCustomPath("count" , dsdDataSetName ) );
		dataSetXml = dataSetXml.replaceAll("CUSTOM_SCHEMA_PATH",  this.getCustomPath("schema" , dsdDataSetName ) );
		hadooptest.cluster.gdm.Response response = this.consoleHandle.createDataSet(dsdDataSetName, dataSetXml);
		if (response.getStatusCode() != SUCCESS) {
			try {
				throw new Exception("Response status code is " + response.getStatusCode() + ", expected 200.");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		// wait for some time so that specification files are created
		this.consoleHandle.sleep(40000);

		// activate the dataset
		try {
			this.consoleHandle.checkAndActivateDataSet(dsdDataSetName);
			this.datasetActivationTime = GdmUtils.getCalendarAsString();
		} catch (Exception e) { 
			e.printStackTrace();
		}

		this.consoleHandle.sleep(4000);
	}

	/**
	 * Method to create a custom path without date in path.
	 * @param pathType - its a string type which represents either data/count/schema
	 * @param dataSet - dataset name
	 * @return
	 */
	private String getCustomPath(String pathType , String dataSet ) {
		return  "/data/daqdev/"+ pathType +"/"+ dataSet + "/%{srcid}/%{date}"; 

	}

	/**
	 * Create a dataset and activate it.
	 */
	private void createDataSet(String datasetName) {
		// Read dataset and replace source and target values
		String dataSetXml = this.consoleHandle.getDataSetXml(this.baseDataSetName);

		// Replace the dataset name
		dataSetXml = dataSetXml.replaceAll(this.baseDataSetName, datasetName);

		// Create a new dataset
		Response response = this.consoleHandle.createDataSet(datasetName, dataSetXml);
		assertTrue("Failed to create a dataset " + datasetName , response.getStatusCode() == 200);

		// wait for some time so that specification files are created
		this.consoleHandle.sleep(40000);

		// activate the dataset
		try {
			this.consoleHandle.checkAndActivateDataSet(datasetName);
			this.datasetActivationTime = GdmUtils.getCalendarAsString();
		} catch (Exception e) { 
			e.printStackTrace();
		}
		this.consoleHandle.sleep(4000);
	}

}
