package hadooptest.gdm.regression.archival;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.cluster.gdm.HTTPHandle;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

import org.apache.commons.httpclient.HttpStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * TestCase : Test whether archival workflow is successful.
 *
 */
public class TestArchivalWorkFlow extends TestSession {

	private String cookie;
	private String consoleURL;
	private String datasetActivationTime;
	private WorkFlowHelper workFlowHelper;
	private HTTPHandle httpHandle ;
	private static final String baseDataSetName = "AcqReplArchivalDataSet.xml";
	private static String INSTANCE1 = "20130725";
	private static String INSTANCE2 = "20130726";
	private ConsoleHandle consoleHandle = new ConsoleHandle();
	private String dataSetName = "AcqRepArchivalTest_" + System.currentTimeMillis();
	private String sourceFDI;
	private String targetGrid1;
	private String targetGrid2;
	private String archivalTargetName;
	private boolean eligibleForDelete = false;

	@BeforeClass
	public static void startTestSession() throws Exception {
		TestSession.start();
	}

	@Before
	public void setUp() throws NumberFormatException, Exception {
		this.workFlowHelper = new WorkFlowHelper();
		this.dataSetName = "TestArchival_WorkFlow_DataSet_" + System.currentTimeMillis();

		List<String> datastores = this.consoleHandle.getUniqueGrids();
		if (datastores.size() < 2) {
			Assert.fail("Only " + datastores.size() + " of 2 required grids exist");
		}
		this.targetGrid1 = datastores.get(0);
		this.targetGrid2 = datastores.get(1);

		datastores = this.consoleHandle.getWarehouseDatastores();
		if (datastores.size() < 1) {
			Assert.fail("No warehouse datastores");
		}
		this.sourceFDI = datastores.get(0);

		datastores = this.consoleHandle.getArchivalDataStores();
		if (datastores.size() < 1) {
			Assert.fail("No Archival warehouse datastore(s)");
		}
		this.archivalTargetName = datastores.get(0);
	}

	@Test
	public void testArchival() {
		// create dataset
		createArchivalDataSetAndActivate();

		// check for acquisition workflow
		workFlowHelper.checkWorkFlow(this.dataSetName, "acquisition", this.datasetActivationTime);

		// check for replication workflow
		workFlowHelper.checkWorkFlow(this.dataSetName, "replication", this.datasetActivationTime);

		// check for Archival workflow.
		testArchivalWorkFlow(this.dataSetName , "replication");
		
		eligibleForDelete = true;
	}

	/**
	 * 	create a archival dataset.
	 */
	private void createArchivalDataSetAndActivate() {
		String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/AcqReplArchivalDataSet.xml");
		String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.dataSetName, dataSetConfigFile);
		dataSetXml = dataSetXml.replaceAll("DATASET_NAME", dataSetName);
		dataSetXml = dataSetXml.replaceAll("SOURCE_NAME", this.sourceFDI);
		dataSetXml = dataSetXml.replaceAll("TARGET_1", this.targetGrid1);
		dataSetXml = dataSetXml.replaceAll("TARGET_2", this.targetGrid2);
		dataSetXml = dataSetXml.replaceAll("ARCHIVAL_TARGET_NAME", this.archivalTargetName);

		Response response = this.consoleHandle.createDataSet(this.dataSetName, dataSetXml);
		if (response.getStatusCode() != HttpStatus.SC_OK) {
			Assert.fail("Response status code is " + response.getStatusCode() + ", expected 200.");
		}
	}

	/**
	 * Test whether archival workflow is completed.
	 * @param datasetName -  dataset name 
	 * @param facetName - replication workflow, since archival workflow is tightly coupled with replication workflow.
	 */
	public void testArchivalWorkFlow(String datasetName , String facetName) {
		boolean isArchivalWorkFlowCompletd = false;
		Map<String,String> jobs = new HashMap<String,String>();
		String completedWorkFlowURL = this.consoleHandle.getConsoleURL() + "/console/api/workflows/completed?datasetname=" + this.dataSetName + "&instancessince=F&joinType=innerJoin&facet=" + facetName ;
		TestSession.logger.info("completedWorkFlowURL  = " + completedWorkFlowURL);
		this.consoleHandle.sleep(30000);

		HTTPHandle httpHandle = new HTTPHandle();
		this.cookie = httpHandle.getBouncerCookie();

		com.jayway.restassured.response.Response completedResponse =  given().cookie(this.cookie).get(completedWorkFlowURL);
		JSONArray jsonArray = this.consoleHandle.convertResponseToJSONArray(completedResponse, "completedWorkflows");
		if (jsonArray.size() > 0) {
			for (int i = 0 ; jsonArray.size() > 0 ; i++) {
				JSONObject completedJsonObject = (JSONObject) jsonArray.get(i);
				String executionId = completedJsonObject.getString("ExecutionID");
				String colo = completedJsonObject.getString("FacetColo");
				String fName = completedJsonObject.getString("FacetName");
				String wflowName = completedJsonObject.getString("WorkflowName");
				List<String> workFlowInstance = Arrays.asList(wflowName.split("/"));
				String dsName = workFlowInstance.get(0);
				String instanceId = workFlowInstance.get(1);

				this.consoleHandle.sleep(30000);

				// Get details information about the completed workflow i,e get step details and fetch only map reduce url
				String testDetailInfoOnDataSetURL = this.consoleHandle.getConsoleURL() + "/console/api/workflows/" + executionId + "/view?facet=" + fName + "&colo=" + colo;
				TestSession.logger.info("testDetailInfoOnDataSetURL   - " + testDetailInfoOnDataSetURL);
				com.jayway.restassured.response.Response response =  given().cookie(this.cookie).get(testDetailInfoOnDataSetURL);
				String str = response.getBody().asString();
				JSONObject obj =  (JSONObject) JSONSerializer.toJSON(str.toString());
				TestSession.logger.info("obj ===== " + obj.toString());
				JSONObject jsonObject =  obj.getJSONObject("WorkflowExecution");
				TestSession.logger.info("workflow name = " + jsonObject.getString("Workflow Name"));
				jsonArray = jsonObject.getJSONArray("Step Executions");
				Iterator iterator = jsonArray.iterator();
				String jobId = null;

				// build the notification string that has to be searched in facet application.log file.
				while (iterator.hasNext()) {
					JSONObject tempJsonObj = (JSONObject) iterator.next();
					String stepName = tempJsonObj.getString("Step Name");
					String exitStatus = tempJsonObj.getString("ExitStatus");

					// archival workflow is completed. 
					if (exitStatus.equals("COMPLETED") && stepName.contains("dataout.") ) {
						TestSession.logger.info("Achrival workflow is successful..................");
						isArchivalWorkFlowCompletd = true;
						break;
					}
				}
				if ( isArchivalWorkFlowCompletd ) {
					break;
				}
			}
			assertTrue("Failed : Archival workflow failed or Dn't run." , isArchivalWorkFlowCompletd == true);
		}
	}
	
	@After
	public void tearDown() {
	    if (eligibleForDelete) {
		this.consoleHandle.deActivateAndRemoveDataSet(this.dataSetName);
	    }
	}
}
