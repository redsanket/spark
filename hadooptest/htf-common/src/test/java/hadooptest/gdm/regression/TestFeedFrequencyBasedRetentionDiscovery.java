package hadooptest.gdm.regression;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Iterator;
import java.util.List;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.commons.configuration.XMLConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.cluster.gdm.HTTPHandle;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedExecutor;


/**
 * 
 * Test Scenario : Verify whether discovery happens when setting yinst for the following on retention facets.
 *	retention_discovery_frequency_beyondhourly 86400      
 *  retention_discovery_frequency_hourly 7200      
 *  retention_discovery_frequency_subhourly 1200
 *  
 *  Note : For this testcase, just setting for  retention_discovery_frequency_hourly yinst setting. 
 *  Expected Result : Discovery should happen.
 *
 */
public class TestFeedFrequencyBasedRetentionDiscovery extends TestSession {

	private ConsoleHandle consoleHandle;
	private String dataSetName;
	private XMLConfiguration conf;
	private String deploymentSuffixName;
	private WorkFlowHelper workFlowHelper;
	private String datasetActivationTime;
	private String cookie;
	private String url;
	private List<String>datasetsResultList;
	private String hourlyBaseDataSetName;
	private FullyDistributedExecutor executor = new FullyDistributedExecutor();
	public static final String DISCOVERY_MONITOR = "/api/discovery/monitor";
	public static final String dataSetPath = "/console/query/config/dataset/getDatasets";
	public static final int SUCCESS = 200;

	@BeforeClass
	public static void startTestSession() {
		TestSession.start();
	}

	@Before
	public void setup() throws Exception {
		String configPath = Util.getResourceFullPath("gdm/conf/config.xml");
		this.conf = new org.apache.commons.configuration.XMLConfiguration(configPath);
		this.deploymentSuffixName = this.conf.getString("hostconfig.console.deployment-suffix-name").trim();
		this.consoleHandle = new ConsoleHandle();
		HTTPHandle httpHandle = new HTTPHandle();
		this.cookie = httpHandle.getBouncerCookie();
		this.url = this.consoleHandle.getConsoleURL();
		TestSession.logger.info("url = " + this.url);

		this.dataSetName = "TestFeedFrequencyBasedRetentionDiscovery_"  + System.currentTimeMillis();
		this.workFlowHelper = new WorkFlowHelper();
		com.jayway.restassured.response.Response response = given().cookie(this.cookie).get(this.url + this.dataSetPath);
		datasetsResultList = response.getBody().jsonPath().getList("DatasetsResult.DatasetName");
		if(datasetsResultList == null){
			fail("Failed to get the datasets");
		}
		this.hourlyBaseDataSetName = "gdm-dataset-" + this.deploymentSuffixName + "-hourly";
		assertTrue(this.hourlyBaseDataSetName  + "  dataset does not exist, hence testcase cannot be executed." , this.datasetsResultList.contains(this.hourlyBaseDataSetName));

	}

	@Test
	public void testDiscoveryMonitorWithNoPartitionDataSet() throws Exception {

		// create a dataset
		createDataSet(this.dataSetName , this.hourlyBaseDataSetName);

		// check for acquisition workflow
		this.workFlowHelper.checkWorkFlow(this.dataSetName, "acquisition", this.datasetActivationTime);

		//check for replication workflow
		this.workFlowHelper.checkWorkFlow(this.dataSetName, "replication", this.datasetActivationTime);

		// set the yinst setting 
		String []yinst  = executor.runProcBuilder(new String [] {"./resources/gdm/restart/gdm_yinst_set.sh" , "retention" , "ygrid_gdm_retention_server.retention_discovery_frequency_subhourly" , "120"});
		for ( String  s : yinst ) {
			TestSession.logger.info(s);
		}
		
		// stop the retention facet
		String stop[] =  executor.runProcBuilder(new String [] {"./resources/gdm/restart/StopStartFacet.sh" , "retention" , "stop" });
		for ( String  s : stop ) {
			TestSession.logger.info(s);
		}
		
		// start the retention facet
		String start[] =  executor.runProcBuilder(new String [] {"./resources/gdm/restart/StopStartFacet.sh" , "retention" , "start" });
		for ( String  s : start ) {
			TestSession.logger.info(s);
		}
		
		// wait for some time, so that classes gets loaded successfully
		TestSession.logger.info("Please wait for a minutes, so that discovery can start...! ");
		this.consoleHandle.sleep(60000);

		//  Monitor for dataset discovery
		JSONArray jsonArray = this.workFlowHelper.isDiscoveryMonitoringStarted("retention", this.dataSetName);
		if (jsonArray.size() > 0) {
			Iterator iterator = jsonArray.iterator();
			while (iterator.hasNext()) {
				JSONObject jsonObject = (JSONObject) iterator.next();
				String dataSet = jsonObject.getString("Dataset");
				String status = jsonObject.getString("Status");
				assertTrue("Failed to get the  " + dataSet  , jsonObject.getString("Dataset").equals(this.dataSetName));
				assertTrue("Looks like there is some issue while discovery " + status  , status.equals("OK") );
			}
		}
		assertTrue("Failed to get discovery monitoring on retention for " + this.dataSetName , jsonArray.size() > 0);
	}

	/**
	 * Create a hourly dataset and activate it.
	 */
	private void createDataSet(String datasetName , String baseDataSetName) {
		// Read dataset and replace source and target values
		String dataSetXml = this.consoleHandle.getDataSetXml(baseDataSetName);

		// Replace the dataset name
		//dataSetXml = dataSetXml.replaceAll(baseDataSetName, datasetName);
		dataSetXml = dataSetXml.replaceFirst(baseDataSetName, datasetName);
		dataSetXml = dataSetXml.replaceFirst(baseDataSetName, datasetName);

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

	/**
	 * Method to deactivate the dataset(s)
	 */
	@After
	public void tearDown() throws Exception {
		Response response = consoleHandle.deactivateDataSet(this.dataSetName);
		assertEquals("ResponseCode - Deactivate DataSet", 200, response.getStatusCode());
		assertEquals("ActionName.", "terminate", response.getElementAtPath("/Response/ActionName").toString());
		assertEquals("ResponseId", "0", response.getElementAtPath("/Response/ResponseId").toString());
		assertEquals("ResponseMessage.", "Operation on " + this.dataSetName + " was successful.", response.getElementAtPath("/Response/ResponseMessage/[0]").toString());
	}
}
