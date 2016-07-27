package hadooptest.gdm.regression.staging.replication;

import java.io.IOException;
import java.util.List;
import static org.junit.Assert.assertEquals;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import hadooptest.TestSession;
import hadooptest.automation.utils.http.HTTPHandle;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.DataSetTarget;
import hadooptest.cluster.gdm.DataSetXmlGenerator;
import hadooptest.cluster.gdm.WorkFlowHelper;
import hadooptest.gdm.regression.HadoopFileSystemHelper;
import hadooptest.Util;
import hadooptest.cluster.gdm.Response;
import org.apache.commons.httpclient.HttpStatus;
import hadooptest.cluster.gdm.GdmUtils;

/**
 * Test Scenario : Test replication and retention workflow on staging.
 *
 */
public class TestReplicationOnStaging extends TestSession {

	private String cookie;
	private String consoleURL;
	private String datasetActivationTime;
	private WorkFlowHelper workFlowHelper;
	private HTTPHandle httpHandle ;
	private ConsoleHandle consoleHandle = new ConsoleHandle();
	private final static String SOURCE_CLUSTER_NAME = "AxoniteRed";
	private final static String TARGET_CLUSTER_NAME = "KryptoniteRed";
	private final String dataSetName = "TestReplicationOnStaging_" + System.currentTimeMillis();
	private final static String START_INSTANCE_RANGE = "20160719";
	private final static String END_INSTANCE_RANGE = "20160721";
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

		if (! datastores.contains(SOURCE_CLUSTER_NAME)) {
			Assert.fail( SOURCE_CLUSTER_NAME + " cluster does not exists.");
		}

		if (! datastores.contains(TARGET_CLUSTER_NAME)) {
			Assert.fail( TARGET_CLUSTER_NAME + " cluster does not exists.");
		}
	}

	@Test
	public void  testReplication() throws IOException, InterruptedException {

		// check whether the instanc file(s) exists
		HadoopFileSystemHelper sourceHelper = new HadoopFileSystemHelper(SOURCE_CLUSTER_NAME);
		if (! sourceHelper.exists(SOURCE_DATA_PATH + INSTANCE ) ) {
			TestSession.logger.info(SOURCE_DATA_PATH + INSTANCE + " does not exists creating new instance file.");
			sourceHelper.createFile(SOURCE_DATA_PATH + INSTANCE + "/testfile");
		}

		// create a new dataset
		this.createDataset();

		// check for replication workflow
		workFlowHelper.checkWorkFlow(this.dataSetName, "replication", this.datasetActivationTime);

		// check whether instance file(s) exists on target cluster
		HadoopFileSystemHelper targetHelper = new HadoopFileSystemHelper(TARGET_CLUSTER_NAME);
		if (! targetHelper.exists("/data/daqdev/data/" + this.dataSetName + "/" + INSTANCE)) {
			Assert.fail("/data/daqdev/data/" + this.dataSetName + "/" + INSTANCE + " instance files does not exists on " + TARGET_CLUSTER_NAME);
		}
		
		// deactivate the dataset before applying retention to dataset
		Response response = this.consoleHandle.deactivateDataSet(this.dataSetName);
		Assert.assertEquals("ResponseCode - Deactivate DataSet failed", HttpStatus.SC_OK, response.getStatusCode());

		// set retention policy
		this.consoleHandle.setRetentionPolicyToAllDataSets(this.dataSetName , "0");

		// check for retention workflow
		workFlowHelper.checkWorkFlow(this.dataSetName , "retention", this.datasetActivationTime);

		// check whether instance files gets deleted after retention workflow
		Assert.assertFalse(targetHelper.exists("/data/daqdev/data/" + this.dataSetName + "/" + INSTANCE));
	}

	/**
	 * Create a replication dataset
	 */
	private void createDataset() {
		DataSetXmlGenerator generator = new DataSetXmlGenerator();
		generator.setName(this.dataSetName);
		generator.setDescription(this.getClass().getSimpleName());
		generator.setCatalog(this.getClass().getSimpleName());
		generator.setActive("TRUE");
		generator.setRetentionEnabled("TRUE");
		generator.setPriority("NORMAL");
		generator.setFrequency("daily");
		generator.setDiscoveryFrequency("10");
		generator.setDiscoveryInterface("HDFS");
		generator.addSourcePath("data", SOURCE_DATA_PATH + "%{date}");
		generator.setSource(SOURCE_CLUSTER_NAME);

		DataSetTarget target = new DataSetTarget();
		target.setName(TARGET_CLUSTER_NAME);
		target.setDateRangeStart(true, START_INSTANCE_RANGE);
		target.setDateRangeEnd(true, END_INSTANCE_RANGE);
		target.setHCatType("DataOnly");
		target.addPath("data", "/data/daqdev/data/" + this.dataSetName + "/%{date}");
		target.setNumInstances("5");
		generator.setTarget(target);

		String dataSetXml = generator.getXml();

		Response response = this.consoleHandle.createDataSet(this.dataSetName, dataSetXml);
		if (response.getStatusCode() != HttpStatus.SC_OK) {
			TestSession.logger.error("Failed to create dataset, xml: " + dataSetXml);
			Assert.fail("Response status code is " + response.getStatusCode() + ", expected 200.");
		}
	}

	@After
	public void tearDown() {
		Response response = this.consoleHandle.deactivateDataSet(this.dataSetName);
		Assert.assertEquals("ResponseCode - Deactivate DataSet failed", HttpStatus.SC_OK, response.getStatusCode());
	}
}
