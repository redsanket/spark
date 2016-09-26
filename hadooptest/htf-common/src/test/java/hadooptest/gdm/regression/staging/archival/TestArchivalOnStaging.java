package hadooptest.gdm.regression.staging.archival;

import java.io.IOException;
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
import hadooptest.cluster.gdm.DataSetTarget;
import hadooptest.cluster.gdm.DataSetXmlGenerator;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;
import hadooptest.gdm.regression.HadoopFileSystemHelper;

/**
 * Test case : Test archival on staging.
 *
 */
public class TestArchivalOnStaging  extends TestSession {
	private String datasetActivationTime;
	private WorkFlowHelper workFlowHelper;
	private ConsoleHandle consoleHandle = new ConsoleHandle();
	private final static String SOURCE_CLUSTER_NAME = "AxoniteRed";
	private final static String TARGET_CLUSTER_NAME = "KryptoniteRed";
	private final static String ARCHIVAL_CLUSTER_NAME = "ArchivalAdapter_Red";
		
	private final String dataSetName = "TestArchivalOnStaging_" + System.currentTimeMillis();
	private final static String INSTANCE = "20160720";
	private final static String SOURCE_DATA_PATH = "/user/hitusr_1/test/";
	
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

		// check whether source cluster exists in the current console data source configuration
		if (! datastores.contains(SOURCE_CLUSTER_NAME)) {
			Assert.fail( SOURCE_CLUSTER_NAME + " cluster does not exists.");
		}

		// check whether target cluster exists in the current console data source configuration
		if (! datastores.contains(TARGET_CLUSTER_NAME)) {
			Assert.fail( TARGET_CLUSTER_NAME + " cluster does not exists.");
		}
		
		// check whether archival cluster exists in the current console data source configuration
		List<String> archivalStores = this.consoleHandle.getArchivalDataStores();
		if (! archivalStores.contains(ARCHIVAL_CLUSTER_NAME)) {
			Assert.fail( ARCHIVAL_CLUSTER_NAME + " archival cluster does not exists.");
		}
		
		if ( this.consoleHandle.checkFacetRunning("replication", "red", "bf1") == false ) {
		    Assert.fail("Looks like Red replication facet in bf1 color is down.");
		}
	}

	@Test
	public void  testArchivalOnStaging() throws IOException, InterruptedException {

		// create a new dataset
		createArchivalDataSet();

		// wait till dataset get activated
		this.consoleHandle.sleep(2000);
		
		// check for replication workflow
		workFlowHelper.checkWorkFlow(this.dataSetName, "replication", this.datasetActivationTime);
	}
	
	/**
	 * 	create a archival dataset.
	 */
	private void createArchivalDataSet() {
		String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/staging/ArchivalDataSet.xml");
		String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.dataSetName, dataSetConfigFile);
		dataSetXml = dataSetXml.replaceAll("DATASET_NAME", dataSetName);
		dataSetXml = dataSetXml.replaceAll("SOURCE_NAME", SOURCE_CLUSTER_NAME);
		dataSetXml = dataSetXml.replaceAll("TARGET", TARGET_CLUSTER_NAME);
		dataSetXml = dataSetXml.replaceAll("ARCHIVAL_CLUSTER_NAME", ARCHIVAL_CLUSTER_NAME);
		Response response = this.consoleHandle.createDataSet(this.dataSetName, dataSetXml);
		if (response.getStatusCode() != HttpStatus.SC_OK) {
			Assert.fail("Response status code is " + response.getStatusCode() + ", expected 200.");
		}
	}

	@After
	public void tearDown() {
		Response response = this.consoleHandle.deactivateDataSet(this.dataSetName);
		Assert.assertEquals("ResponseCode - Deactivate DataSet failed", HttpStatus.SC_OK, response.getStatusCode());
		
		this.consoleHandle.removeDataSet(this.dataSetName);
	}

}
