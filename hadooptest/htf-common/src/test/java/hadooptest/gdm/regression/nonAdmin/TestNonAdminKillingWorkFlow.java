package hadooptest.gdm.regression.nonAdmin;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import hadooptest.TestSession;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.cluster.gdm.HTTPHandle;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.jayway.restassured.path.json.JsonPath;

/**
 * Test Scenario : Verify whether non admin user is able to kill the running workflow.
 *
 */
public class TestNonAdminKillingWorkFlow extends TestSession {

	private ConsoleHandle console;
	private HTTPHandle httpHandle;	
	private String dataSetName;
	private String hostName;
	private String cookie;
	private String baseDataSetName = "VerifyAcqRepRetWorkFlowExecutionDateRange";
	private static final int SUCCESS = 200;
	private String datasetActivationTime = null;
	private Response response;
	private List<String> workFlowNames;
	private WorkFlowHelper workFlowHelperObj;
	private  String nonAdminUserName; 
	private  String nonAdminPassWord;

	@BeforeClass
	public static void startTestSession() throws Exception {
		TestSession.start();
	}

	@Before
	public void setUp() throws NumberFormatException, Exception {
		this.console = new ConsoleHandle();
		this.httpHandle = new HTTPHandle();
		this.dataSetName = "Test_NonAdmin_Trying_to_Kill_RunningWorkFlow_" + System.currentTimeMillis();
		this.nonAdminUserName = this.console.getConf().getString("auth.nonAdminUser");
		this.nonAdminPassWord = this.console.getConf().getString("auth.nonAdminPassWord");
		this.httpHandle.logonToBouncer(this.nonAdminUserName, nonAdminPassWord);
		this.hostName = this.console.getConsoleURL();
		this.cookie = this.httpHandle.getBouncerCookie();
		this.workFlowNames = new ArrayList<String>();
		this.workFlowHelperObj = new WorkFlowHelper();
	}

	@Test
	public void testKillRunningWorkFlowAsNonAdminUser() {
		createDataSet();

		this.response = this.console.activateDataSet(this.dataSetName);
		assertTrue("Failed to activate dataset " + dataSetName , response.getStatusCode() == SUCCESS);

		this.datasetActivationTime = GdmUtils.getCalendarAsString();
		TestSession.logger.info("DataSet Activation Time: " + datasetActivationTime);

		// wait for some time so that discovery starts and workflow comes to running state.
		this.console.sleep(50000);

		// check whether workflow is in acquisition running state , if yes, kill
		getRunningDataSetAndKillWorkFlowAsNonAdmin();
		
		// check for acquisition workflow completed successfully
		this.workFlowHelperObj.checkWorkFlow(this.dataSetName, "acquisition", this.datasetActivationTime);
	}

	/**
	 * Non-Admin user trying to kills the running dataset. 
	 */
	private void getRunningDataSetAndKillWorkFlowAsNonAdmin() {
		JSONArray jsonArray = null;
		com.jayway.restassured.response.Response response = null;
		boolean didDataSetInRunningState = false;
		int i = 1;
		while (i <= 10)  {
			String testURL =  this.hostName + "/console/api/workflows/running?datasetname="+ this.dataSetName +"&instancessince=F&joinType=innerJoin";
			TestSession.logger.info("testURL = " + testURL);
			response = given().cookie(this.cookie).get(testURL);
			assertTrue("Failed to get the response for " + testURL , (response != null || response.toString() != "") );
			
			jsonArray = this.console.convertResponseToJSONArray(response , "runningWorkflows");
			TestSession.logger.info("size = " + jsonArray.size());
			if ( jsonArray.size() > 0 ) {
				JSONArray resourceArray = new JSONArray();
				Iterator iterator = jsonArray.iterator();
				while (iterator.hasNext()) {
					JSONObject runningJsonObject = (JSONObject) iterator.next();
					String exitStatus = runningJsonObject.getString("ExitStatus");
					
					// kill dataset only if its in running state.
					if (exitStatus.equals("EXECUTING")) {
						didDataSetInRunningState = true;
						
						String executionId = runningJsonObject.getString("ExecutionID");
						String facetName = runningJsonObject.getString("FacetName");
						String facetColo = runningJsonObject.getString("FacetColo");
						
						resourceArray.add(new JSONObject().element("ExecutionID",executionId).element("FacetName", facetName).element("FacetColo", facetColo));
						String url = this.hostName + "/console/api/admin/proxy/workflows";
						com.jayway.restassured.response.Response jobKilledResponse = given().cookie(this.cookie).param("command", "kill").param("workflowIds" , resourceArray.toString())
								.post(url);
						assertTrue("Failed to get the response for " + url , (response != null || response.toString() != "") );
						
						TestSession.logger.info("Non-Admin Killing Response code = " + jobKilledResponse.getStatusCode());
						JsonPath jsonPath = jobKilledResponse.getBody().jsonPath();
						TestSession.logger.info("response = " + jsonPath.prettyPrint());
						String responseId = jsonPath.getString("Response.ResponseId");
						String responseMessage = jsonPath.getString("Response.ResponseMessage");
						boolean flag = responseMessage.contains("Unauthorized access");
						assertTrue("Expected ResponseId is 0 , but got " +responseId  , responseId.equals("-3"));
						assertTrue("Expected response message as Unauthorized access , but got " + responseMessage  , flag == true);
					}
				}
			}
			i++;
			this.console.sleep(15000);
		}
		// check whether dataset was in running state.
		assertTrue("Failed : " + this.dataSetName + "  dataSet dn't came to running state. ", didDataSetInRunningState == true);
	}

	/**
	 * Create a dataset
	 */
	private void createDataSet() {
		String dataSetXml = this.console.getDataSetXml(this.baseDataSetName);
		dataSetXml = dataSetXml.replaceAll(this.baseDataSetName, this.dataSetName);

		Response response = this.console.createDataSet(this.dataSetName, dataSetXml);
		assertTrue("Failed to create a dataset " + dataSetName , response.getStatusCode() == SUCCESS);
		this.console.sleep(50000);
	}
	
	/**
	 *   Deactivate the dataset
	 * 
	 */
	@After
	public void tearDown() throws Exception {
		Response response = this.console.deactivateDataSet(this.dataSetName);
		assertEquals("ResponseCode - Deactivate DataSet", 200, response.getStatusCode());
		assertEquals("ActionName.", "terminate", response.getElementAtPath("/Response/ActionName").toString());
		assertEquals("ResponseId", "0", response.getElementAtPath("/Response/ResponseId").toString());
		assertEquals("ResponseMessage.", "Operation on " + this.dataSetName + " was successful.", response.getElementAtPath("/Response/ResponseMessage/[0]").toString());
	}
}
