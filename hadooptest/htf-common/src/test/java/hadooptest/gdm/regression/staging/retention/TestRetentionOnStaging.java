package hadooptest.gdm.regression.staging.retention;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.commons.httpclient.HttpStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.automation.utils.http.HTTPHandle;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;

/**
 * Test Retention workflow is success and removes the replicated instance files  
 *
 */
public class TestRetentionOnStaging extends TestSession {
	private String cookie;
	private String consoleURL;
	private String datasetActivationTime;
	private WorkFlowHelper workFlowHelper;
	private HTTPHandle httpHandle ;
	private ConsoleHandle consoleHandle = new ConsoleHandle();
	private final static String SOURCE_CLUSTER_NAME = "AxoniteRed";
	private final static String TARGET_CLUSTER_NAME = "KryptoniteRed";
	private final String dataSetName = "TestRetentionOnStaging_" + System.currentTimeMillis();

	@BeforeClass
	public static void startTestSession() throws Exception {
		TestSession.start();
	}

	@Before
	public void setUp() throws NumberFormatException, Exception {
		this.workFlowHelper = new WorkFlowHelper();
		List<String> datastores = this.consoleHandle.getUniqueGrids();
		if (datastores.size() < 2) {
			Assert.fail("Only " + datastores.size() + " of 2 required grids exist");
		}

		if (! datastores.contains(SOURCE_CLUSTER_NAME)) {
			Assert.fail( SOURCE_CLUSTER_NAME + " cluster does not exists.");
		}

		if (! datastores.contains(TARGET_CLUSTER_NAME)) {
			Assert.fail( TARGET_CLUSTER_NAME + " cluster does not exists.");
		}
	}

	@Test
	public void  testReplication() throws Exception {

		// create a new dataset
		this.createDataSet();

		// check for replication workflow
		workFlowHelper.checkWorkFlow(this.dataSetName, "replication", this.datasetActivationTime);

		// deactivate the dataset before applying retention to dataset
		this.deActivateDataSet();
		
		// set retention policy
		this.consoleHandle.setRetentionPolicyToAllDataSets(this.dataSetName , "0");

		// check for retention workflow
		workFlowHelper.checkWorkFlow(this.dataSetName , "retention", this.datasetActivationTime);
	}

	/**
	 * 	create a archival dataset.
	 * @throws Exception 
	 */
	private void createDataSet() throws Exception {
		String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/stagingDataSets/ReplicationDataSet.xml");
		String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.dataSetName, dataSetConfigFile);
		dataSetXml = dataSetXml.replaceAll("DATASET_NAME", this.dataSetName);
		dataSetXml = dataSetXml.replaceAll("SOURCE", SOURCE_CLUSTER_NAME);
		dataSetXml = dataSetXml.replaceAll("TARGET", TARGET_CLUSTER_NAME);
		dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", this.dataSetName);
		TestSession.logger.info("dataSetXml  - " + dataSetXml);

		Response response = this.consoleHandle.createDataSet(this.dataSetName, dataSetXml);
		if (response.getStatusCode() != HttpStatus.SC_OK) {
			Assert.fail("Response status code is " + response.getStatusCode() + ", expected 200.");
		}

		TestSession.logger.info("Wait for some time, so that version file gets created.");
		this.activateDataSet();
	}

	/**
	 * Activate dataset.
	 * @throws Exception
	 */
	private void activateDataSet() throws Exception {
		this.consoleHandle.sleep(5000);

		// activate the dataset
		this.consoleHandle.checkAndActivateDataSet(this.dataSetName);
		this.datasetActivationTime = GdmUtils.getCalendarAsString();
	}
	
	/**
	 * Deactivate dataset
	 */
	private void deActivateDataSet() {
		Response response = consoleHandle.deactivateDataSet(this.dataSetName);
		assertEquals("ResponseCode - Deactivate DataSet", 200, response.getStatusCode());
		assertEquals("ActionName.", "terminate", response.getElementAtPath("/Response/ActionName").toString());
		assertEquals("ResponseMessage.", "Operation on " + this.dataSetName + " was successful.", response.getElementAtPath("/Response/ResponseMessage/[0]").toString());
	}

	@After
	public void tearDown() {
		deActivateDataSet();
	}
}
