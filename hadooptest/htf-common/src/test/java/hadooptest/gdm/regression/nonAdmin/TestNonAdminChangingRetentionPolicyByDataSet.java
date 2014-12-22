package hadooptest.gdm.regression.nonAdmin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import hadooptest.TestSession;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.HTTPHandle;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * TestCase : Verify whether Non-Admin user is able to modify the retention policy for a given dataset.
 * Steps : 
 * 			1) Create a dataset by a admin user.
 * 			2) Modify the retention policy by the non-admin ( should be able to modify)
 * 
 * 
 * Note : TestCase could have modified retention policy by invoking the REST API Modify, but want to perform a similar action like doing it on the console.
 *
 */
public class TestNonAdminChangingRetentionPolicyByDataSet  extends TestSession {

	private ConsoleHandle consoleHandle;
	private String dataSetName;
	private String datasetActivationTime;
	private String cookie;
	private  String nonAdminUserName; 
	private  String nonAdminPassWord;
	private String baseDataSetName = "VerifyAcqRepRetWorkFlowExecutionSingleDate";
	public static final int SUCCESS = 200;
	private HTTPHandle httpHandle = null; 
	private WorkFlowHelper workFlowHelperObj = null;
	private static final String OPS_DB_GROUP = "ygrid_group_gdmtest";

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
		
		this.dataSetName = "TestNonAdminChangingRetentionPolicyByDataSet_" + System.currentTimeMillis();
	}

	@Test
	public void testNonAdminAsWorkFlowWatcher() {
		createDataSet();
		testModifyRetentionPolicy(this.dataSetName);
	}

	/**
	 * Create a dataset and activate it as GDM Admin.
	 */
	private void createDataSet() {
		
		// create a dataset.
		String dataSetXml = this.consoleHandle.getDataSetXml(this.baseDataSetName);
		dataSetXml = dataSetXml.replaceAll(this.baseDataSetName, this.dataSetName);
		
		StringBuffer dataSetXmlStrBuffer = new StringBuffer(dataSetXml);
		int index = dataSetXmlStrBuffer.indexOf("</Targets>") + "</Targets>".length() + 1;
		
		// inserting SelfServe tag, ExtendedState ,History, Project and Tag.
		dataSetXmlStrBuffer.insert(index , "<SelfServe><OpsdbGroup>"+ OPS_DB_GROUP +"</OpsdbGroup><RequireGroupAdmin>FALSE</RequireGroupAdmin><SelfServeEnabled>TRUE</SelfServeEnabled><UseOpsdbGroup>TRUE</UseOpsdbGroup></SelfServe><ExtendedState><History/></ExtendedState><Project/><Tags/>");
		
		String datasetXML = dataSetXmlStrBuffer.toString();
		Response response = this.consoleHandle.createDataSet(this.dataSetName, datasetXML);
		assertTrue("Failed to create a dataset " +this.dataSetName , response.getStatusCode() == 200);
	}
	
	/*
	 * Non-Admin user is trying to modify the retention policy and he should be able to modify.
	 */
	public void testModifyRetentionPolicy(String datasetName) {
		this.httpHandle.logonToBouncer(this.nonAdminUserName, this.nonAdminPassWord);
		String dataSetXML = this.consoleHandle.getDataSetXml(datasetName);
		dataSetXML = dataSetXML.replaceAll("numberOfInstances", "instanceDate");
		Response response = this.consoleHandle.modifyDataSet(datasetName, dataSetXML);
		TestSession.logger.info("status code = " + response.getStatusCode());
		assertTrue("Failed to modify the dataset " +this.dataSetName , response.getStatusCode() == 200);
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
