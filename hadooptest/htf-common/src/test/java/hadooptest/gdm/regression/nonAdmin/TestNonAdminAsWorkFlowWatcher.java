package hadooptest.gdm.regression.nonAdmin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import hadooptest.TestSession;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.cluster.gdm.HTTPHandle;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test Scenario : Verify whether Non-Admin user can watch for the workflows
 *
 */
public class TestNonAdminAsWorkFlowWatcher  extends TestSession {

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
	}

	@Test
	public void testNonAdminAsWorkFlowWatcher() {
		startWorkFlowAsAdmin() ;
		testWatchWorkFlowAsNonAdmin();
	}

	/**
	 * Create a dataset and activate it as GDM Admin.
	 */
	private void startWorkFlowAsAdmin() {
		this.dataSetName = "TestNonAdmin_As_Workflow_Watcher_" + System.currentTimeMillis();

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
	 *  Login as non-admin user, and watch for the workflow 
	 */
	private void testWatchWorkFlowAsNonAdmin() {
		this.httpHandle.logonToBouncer(this.nonAdminUserName, this.nonAdminPassWord);
		
		this.workFlowHelperObj.checkWorkFlow(this.dataSetName, "acquisition", this.datasetActivationTime);

		// check for replication workflow completed successfully
		this.workFlowHelperObj.checkWorkFlow(this.dataSetName, "replication", this.datasetActivationTime);
		
	}

	@After
	public void tearDown() throws Exception {
		// deactivate the dataset
		Response response = this.consoleHandle.deactivateDataSet(this.dataSetName);
		assertEquals("ResponseCode - Deactivate DataSet", 200, response.getStatusCode());
		assertEquals("ActionName.", "terminate", response.getElementAtPath("/Response/ActionName").toString());
		assertEquals("ResponseId", "0", response.getElementAtPath("/Response/ResponseId").toString());
		assertEquals("ResponseMessage.", "Operation on " + this.dataSetName + " was successful.", response.getElementAtPath("/Response/ResponseMessage/[0]").toString());
	}
}
