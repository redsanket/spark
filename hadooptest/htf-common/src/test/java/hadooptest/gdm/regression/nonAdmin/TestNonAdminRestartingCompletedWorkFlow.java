package hadooptest.gdm.regression.nonAdmin;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import hadooptest.TestSession;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.cluster.gdm.HTTPHandle;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.jayway.restassured.path.json.JsonPath;

/**
 *  Test Scenario : Verify whether non admin user is not able to restart the completed workflow.
 *
 */
public class TestNonAdminRestartingCompletedWorkFlow  extends TestSession {

	private ConsoleHandle consoleHandle;
	private String dataSetName;
	private String cookie;
	private String hostName;
	private String datasetActivationTime;
	private String baseDataSetName = "VerifyAcqRepRetWorkFlowExecutionSingleDate";
	public static final int SUCCESS = 200;
	private HTTPHandle httpHandle = null; 
	private WorkFlowHelper workFlowHelperObj = null;
	private String nonAdminUserName; 
	private String nonAdminPassWord;

	@BeforeClass
	public static void startTestSession() {
		TestSession.start();
	}

	@Before
	public void setup() throws Exception {
		this.consoleHandle = new ConsoleHandle();
		this.httpHandle = new HTTPHandle();
		this.nonAdminUserName = this.consoleHandle.getConf().getString("auth.nonAdminUser");
		this.nonAdminPassWord = this.consoleHandle.getConf().getString("auth.nonAdminPassWord");
		this.httpHandle.logonToBouncer(this.nonAdminUserName, nonAdminPassWord);
		this.hostName = this.consoleHandle.getConsoleURL();
		this.cookie = this.httpHandle.getBouncerCookie();
		workFlowHelperObj = new WorkFlowHelper();
	}

	@Test
	public void testNonAdminAsWorkFlowWatcher() {
		startWorkFlowAsAdmin();
		testWatchWorkFlowAsNonAdmin();
		restartCompletedWorkFlowAsNonAdminUser();
	}

	/**
	 * Create a dataset and activate it as GDM Admin.
	 */
	private void startWorkFlowAsAdmin() {
		this.dataSetName = "Test_NonAdmin_Restarting_Workflow_" + System.currentTimeMillis();

		// create a dataset.
		String dataSetXml = this.consoleHandle.getDataSetXml(this.baseDataSetName);
		dataSetXml = dataSetXml.replaceAll(this.baseDataSetName, this.dataSetName);
		Response response = this.consoleHandle.createDataSet(this.dataSetName, dataSetXml);
		assertTrue("Failed to create a dataset " +this.dataSetName , response.getStatusCode() == 200);

		// activate the dataset.
		response = this.consoleHandle.activateDataSet(dataSetName);
		assertTrue("Failed to activate dataset " + dataSetName , response.getStatusCode() == SUCCESS);
		this.consoleHandle.sleep(30000);
		this.datasetActivationTime = GdmUtils.getCalendarAsString();
	}

	/**
	 *  Login as non-admin user & watch for the workflow.
	 */
	private void testWatchWorkFlowAsNonAdmin() {
	
		// check for acquisition workflow completed successfully
		this.workFlowHelperObj.checkWorkFlow(this.dataSetName, "acquisition", this.datasetActivationTime);
	}

	/**
	 * As a Non-Admin user try to restart the completed  acquisition workflow. Non-admin user should be able to restart the workflow.
	 */
	private void restartCompletedWorkFlowAsNonAdminUser() {
		JSONArray jsonArray = null;
		com.jayway.restassured.response.Response response = null;
		boolean didDataSetIsInCompletedState = false;
		int i = 1;
		while (i <= 10)  {
			String testURL =  this.hostName + "/console/api/workflows/completed?datasetname="+ this.dataSetName +"&instancessince=F&joinType=innerJoin";
			TestSession.logger.info("testURL = " + testURL);
			response = given().cookie(this.cookie).get(testURL);

			assertTrue("Failed to get the response for " + testURL , (response != null || response.toString() != "") );

			jsonArray = this.consoleHandle.convertResponseToJSONArray(response , "completedWorkflows");
			TestSession.logger.info("size = " + jsonArray.size());
			if ( jsonArray.size() > 0 ) {
				JSONArray resourceArray = new JSONArray();
				Iterator iterator = jsonArray.iterator();
				while (iterator.hasNext()) {
					JSONObject runningJsonObject = (JSONObject) iterator.next();
					String exitStatus = runningJsonObject.getString("ExitStatus");
					if (exitStatus.equals("COMPLETED")) {
						didDataSetIsInCompletedState = true;
						String executionId = runningJsonObject.getString("ExecutionID");
						String facetName = runningJsonObject.getString("FacetName");
						String facetColo = runningJsonObject.getString("FacetColo");
						
						resourceArray.add(new JSONObject().element("ExecutionID",executionId).element("FacetName", facetName).element("FacetColo", facetColo));
						String url = this.hostName + "/console/api/admin/proxy/workflows";
						com.jayway.restassured.response.Response jobKilledResponse = given().cookie(this.cookie).param("command", "restart")
								.param("workflowIds" , resourceArray.toString()).post(url);

						assertTrue("Failed to get the response for " + url , (jobKilledResponse != null || jobKilledResponse.toString() != "") );

						TestSession.logger.info("Non-Admin Restarting the completed workflow Response code = " + jobKilledResponse.getStatusCode());
						JsonPath jsonPath = jobKilledResponse.getBody().jsonPath();
						TestSession.logger.info("** response = " + jsonPath.prettyPrint());
						String responseId = jsonPath.getString("Response.ResponseId");
						String responseMessage = jsonPath.getString("Response.ResponseMessage");
						boolean flag = responseMessage.contains("Unauthorized access");
						assertTrue("Expected ResponseId is 0 , but got " +responseId  , responseId.equals("-3"));
						assertTrue("Expected response message as Unauthorized access , but got " + responseMessage  , flag == true);
					}
				}
			}
			i++;
			this.consoleHandle.sleep(15000);
		}
		// check whether did dataset was to completed state or not.
		assertTrue("Failed : " + this.dataSetName + "  dataSet dn't came to completed state, may be the dataset is still in running state. ", didDataSetIsInCompletedState == true);
	}

	@After
	public void tearDown() throws Exception {
		// make dataset inactive
		Response response = this.consoleHandle.deactivateDataSet(this.dataSetName);
		assertEquals("ResponseCode - Deactivate DataSet", 200, response.getStatusCode());
		assertEquals("ActionName.", "terminate", response.getElementAtPath("/Response/ActionName").toString());
		assertEquals("ResponseId", "0", response.getElementAtPath("/Response/ResponseId").toString());
		assertEquals("ResponseMessage.", "Operation on " + this.dataSetName + " was successful.", response.getElementAtPath("/Response/ResponseMessage/[0]").toString());
	}
}
