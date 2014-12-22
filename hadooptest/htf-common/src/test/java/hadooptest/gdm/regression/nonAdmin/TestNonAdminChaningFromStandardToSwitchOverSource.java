package hadooptest.gdm.regression.nonAdmin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

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
 * Test Scenario : Verify whether non-admin user is able to modify the source type i,e from standard to switchover.
 * Step : 
 * 			Admin user creates the dataset
 * 			Non-Admin user modifies from standard to switchover type.
 * 			
 */
public class TestNonAdminChaningFromStandardToSwitchOverSource extends TestSession {

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
		
		this.dataSetName = "TestNonAdminChangingRetentionPolicyByDataSet_" + System.currentTimeMillis();
		
		this.grids = this.consoleHandle.getUniqueGrids();
	}

	@Test
	public void testNonAdminAsWorkFlowWatcher() {
		createDataSet();
		testModifySourceFromStandardToSwitchOver(this.dataSetName);
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
		datasetXML = datasetXML.replaceAll(sourceName, this.grids.get(0));
		
		Response response = this.consoleHandle.createDataSet(this.dataSetName, datasetXML);
		assertTrue("Failed to create a dataset " +this.dataSetName , response.getStatusCode() == 200);
		
		this.consoleHandle.sleep(5000);
	}
	
	/*
	 * Non-Admin user is trying to modify the to switch over. Non-Admin should be able to do.
	 */
	public void testModifySourceFromStandardToSwitchOver(String datasetName) {
		this.httpHandle.logonToBouncer(this.nonAdminUserName, this.nonAdminPassWord);
		String dataSetXML = this.consoleHandle.getDataSetXml(datasetName);
		
		// switch over will work when discovery is of HDFS type.
		dataSetXML = dataSetXML.replaceAll("FDI", "HDFS");
		dataSetXML = dataSetXML.replaceAll("Standard", "Switchover");
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
