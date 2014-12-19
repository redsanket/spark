package hadooptest.gdm.regression.nonAdmin;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import hadooptest.TestSession;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.HTTPHandle;
import hadooptest.cluster.gdm.JSONUtil;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;

import java.util.Arrays;
import java.util.List;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.jayway.restassured.path.json.JsonPath;

public class TestNonAdminChangingTargetRetentionPolicyThroughRESTAPI extends TestSession {

	private ConsoleHandle consoleHandle;
	private String dataSetName;
	private String datasetActivationTime;
	private JSONUtil jsonUtil;
	private String cookie;
	private  String nonAdminUserName; 
	private  String nonAdminPassWord;
	private String baseDataSetName = "VerifyAcqRepRetWorkFlowExecutionSingleDate";
	public static final int SUCCESS = 200;
	private HTTPHandle httpHandle = null; 
	private WorkFlowHelper workFlowHelperObj = null;
	private static final String OPS_DB_GROUP = "ygrid_group_gdmtest";
	private List<String> grids;

	@BeforeClass
	public static void startTestSession() {
		TestSession.start();
	}

	@Before
	public void setup() throws Exception {
		this.consoleHandle = new ConsoleHandle();
		this.nonAdminUserName = this.consoleHandle.getConf().getString("auth.nonAdminUser");
		this.nonAdminPassWord = this.consoleHandle.getConf().getString("auth.nonAdminPassWord");
		this.httpHandle = new HTTPHandle();
		this.workFlowHelperObj = new WorkFlowHelper();
		this.cookie = httpHandle.getBouncerCookie(); 
		 this.jsonUtil = new JSONUtil();
		
		this.dataSetName = "TestNonAdminChangingRetentionPolicyByDataSet_" + System.currentTimeMillis();
		
		this.grids = this.consoleHandle.getUniqueGrids();
	}

	@Test
	public void testNonAdminAsWorkFlowWatcher() {
		createDataSet();
		testModifySourceRetentionPolicy(this.dataSetName);
	}

	/**
	 * Create a dataset and activate it as GDM Admin.
	 */
	private void createDataSet() {
		
		// create a dataset.
		String dataSetXml = this.consoleHandle.getDataSetXml(this.baseDataSetName);
		dataSetXml = dataSetXml.replaceAll(this.baseDataSetName, this.dataSetName);
		String sourceName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Sources" , "name");
		
		TestSession.logger.info("********** sourceName = " + sourceName);
		StringBuffer dataSetXmlStrBuffer = new StringBuffer(dataSetXml);
		int index = dataSetXmlStrBuffer.indexOf("latency=")  - 1 ;
		dataSetXmlStrBuffer.insert(index , "  switchovertype=\"Standard\"  ");
		index = 0;
		index = dataSetXmlStrBuffer.indexOf("</Targets>") + "</Targets>".length() + 1;
		
		// inserting SelfServe tag, ExtendedState ,History, Project and Tag.
		dataSetXmlStrBuffer.insert(index , "<SelfServe><OpsdbGroup>"+ OPS_DB_GROUP +"</OpsdbGroup><RequireGroupAdmin>FALSE</RequireGroupAdmin><SelfServeEnabled>TRUE</SelfServeEnabled><UseOpsdbGroup>TRUE</UseOpsdbGroup></SelfServe><ExtendedState><History/></ExtendedState><Project/><Tags/>");
		
		String datasetXML = dataSetXmlStrBuffer.toString();
		Response response = this.consoleHandle.createDataSet(this.dataSetName, datasetXML);
		assertTrue("Failed to create a dataset " +this.dataSetName , response.getStatusCode() == 200);
		
		this.consoleHandle.sleep(5000);
	}
	
	/*
	 * Non-Admin user is trying to update retention policy through REST API. Non-Admin should be able to do.
	 */
	public void testModifySourceRetentionPolicy(String datasetName) {
		this.httpHandle.logonToBouncer(this.nonAdminUserName, this.nonAdminPassWord);
		String newCookie = this.httpHandle.getBouncerCookie();
		TestSession.logger.info("newCookie   = "  + newCookie );
		
		// get datastore name from the given dataset
		String resourceName = this.jsonUtil.constructResourceNamesParameter(Arrays.asList(datasetName));
        TestSession.logger.info("resourceName  = "  + resourceName);
        
        List<String> sources = this.consoleHandle.getDataSource(this.dataSetName , "target" , "name");
        String dataSourceName = sources.get(0);
        TestSession.logger.info("dataSourceName =  " + dataSourceName);
        
        // set the policy for the source
        String dataStoreResourceName = this.jsonUtil.constructResourceNamesParameter(Arrays.asList(dataSourceName));
        
        JSONObject actionObject = new JSONObject().element("action", "updateRetention");
        JSONArray resourceArray = new JSONArray();
        JSONObject policy = new JSONObject().element("policyType", "numberOfInstances").element("days", "100").element("target" , dataSourceName);
        resourceArray.add(policy);
        actionObject.put("policies", resourceArray);
        String args = actionObject.toString();
        
        TestSession.logger.info("args =  "  + args);
        
        // update retention policy for source and make sure the request is success.
        com.jayway.restassured.response.Response res = given().cookie(newCookie).param("resourceNames", resourceName).param("command","update").param("args", args)
				.post(this.consoleHandle.getConsoleURL() + "/console/rest/config/dataset/actions");
		
		JsonPath jsonPath = res.jsonPath();
		TestSession.logger.info("DataSet = " + jsonPath.prettyPrint());
		
		assertTrue("Non-Admin user failed to update the retention policy to " + dataSetName , jsonPath.getString("Response.ResponseId").equals("0"));
	}
	
	
	/**
	 * Construct a policyType for the given grids or targets
	 * @param policiesArguments
	 * @param action
	 * @return
	 */
	private String constructPoliciesArguments(List<String>policiesArguments , String action , String dataStoreType) {
		JSONObject actionObject = new JSONObject().element("action", action);
		String args = null;
		JSONArray resourceArray = new JSONArray();
		for ( String policies : policiesArguments) {
			List<String> values = Arrays.asList(policies.split(":"));
			JSONObject policy = new JSONObject().element("policyType", values.get(0)).element("days", values.get(1)).element(dataStoreType, values.get(2));
			resourceArray.add(policy);
		}
		actionObject.put("policies", resourceArray);
		args = actionObject.toString();
		return args;
	}

	// deactivate the dataset
	@After
	public void tearDown() throws Exception {
		Response response = this.consoleHandle.deactivateDataSet(this.dataSetName);
		assertEquals("ResponseCode - Deactivate DataSet", 200, response.getStatusCode());
		assertEquals("ActionName.", "terminate", response.getElementAtPath("/Response/ActionName").toString());
		assertEquals("ResponseId", "0", response.getElementAtPath("/Response/ResponseId").toString());
		assertEquals("ResponseMessage.", "Operation on " + this.dataSetName + " was successful.", response.getElementAtPath("/Response/ResponseMessage/[0]").toString());
	}

}
