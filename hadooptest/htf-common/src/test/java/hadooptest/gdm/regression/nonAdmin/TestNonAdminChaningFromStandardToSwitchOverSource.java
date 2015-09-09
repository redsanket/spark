package hadooptest.gdm.regression.nonAdmin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
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
	private String nonAdminUserName; 
	private String nonAdminPassWord;
	private String baseDataSetName = "VerifyAcqRepRetWorkFlowExecutionSingleDate";
	public static final int SUCCESS = 200;
	private HTTPHandle httpHandle = null; 
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
		this.cookie = httpHandle.getBouncerCookie();
		this.dataSetName = "TestNonAdminChangingRetentionPolicyByDataSet_" + System.currentTimeMillis();
		this.grids = this.consoleHandle.getUniqueGrids();
	}

	@Test
	public void testNonAdminAsWorkFlowWatcher() {
		if (this.createDataSet() ) {
			this.testModifySourceFromStandardToSwitchOver(this.dataSetName);
		}  else {
			TestSession.logger.info("there is no enough targets.");
			fail("there is no enough targets.");
		}
	}

	/**
	 * Create a dataset and activate it as GDM Admin.
	 */
	private boolean createDataSet() {
		boolean flag = false;
		String sourceName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Sources" , "name").trim();
		List<String> targetList = this.consoleHandle.getDataSource(this.baseDataSetName , "target" , "name");

		// create a dataset.
		String dataSetXml = this.consoleHandle.getDataSetXml(this.baseDataSetName);
		dataSetXml = dataSetXml.replaceAll(this.baseDataSetName, this.dataSetName);		
		dataSetXml = dataSetXml.replaceAll("<SelfServeEnabled>FALSE</SelfServeEnabled>", "<SelfServeEnabled>TRUE</SelfServeEnabled>");
		dataSetXml = dataSetXml.replaceAll("<UseOpsdbGroup>FALSE</UseOpsdbGroup>", "<UseOpsdbGroup>TRUE</UseOpsdbGroup>");
		StringBuffer dataSetXmlStrBuffer = new StringBuffer(dataSetXml);
		int index = dataSetXmlStrBuffer.indexOf("<SelfServe>") + "<SelfServe>".length() + 1;
		dataSetXmlStrBuffer.insert(index , "<OpsdbGroup>ygrid_group_gdmtest</OpsdbGroup>");
		List<String> finalSourceList = this.getUniqueItemList(targetList, this.grids);
		TestSession.logger.info("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!  finalSourceList  =  " + finalSourceList);
		String datasetXML = dataSetXmlStrBuffer.toString();
		if (finalSourceList.size() >= 0 ) {
			datasetXML = datasetXML.replaceAll(sourceName, finalSourceList.get(0));
			Response response = this.consoleHandle.createDataSet(this.dataSetName, datasetXML);
			assertTrue("Failed to create a dataset " +this.dataSetName , response.getStatusCode() == 200);
			flag = true;
			this.consoleHandle.sleep(5000);	
		}
		return flag;
	}

	/**
	 * Remove all the similar item from l2 matching in l1.
	 * @param l1
	 * @param l2
	 * @return
	 */
	public List<String> getUniqueItemList(List<String> l1 , List<String> l2) {
		List<String> result = new ArrayList(l2);
		result.removeAll(l1);
		return result;
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
