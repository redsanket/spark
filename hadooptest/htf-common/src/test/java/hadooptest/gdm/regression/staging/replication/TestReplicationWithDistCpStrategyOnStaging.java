package hadooptest.gdm.regression.staging.replication;

import java.io.IOException;

import org.apache.commons.httpclient.HttpStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import hadooptest.TestSession;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.DataSetTarget;
import hadooptest.cluster.gdm.DataSetXmlGenerator;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;

public class TestReplicationWithDistCpStrategyOnStaging extends TestSession {
    private String datasetActivationTime;
    private WorkFlowHelper workFlowHelper;
    private ConsoleHandle consoleHandle = new ConsoleHandle();
    private final static String SOURCE_CLUSTER_NAME = "AxoniteRed";
    private final static String TARGET_CLUSTER_NAME = "KryptoniteRed";
    private final String dataSetName = "TestReplicationWithDistCpStrategyOnStaging_" + System.currentTimeMillis();
    private final static String START_INSTANCE_RANGE = "20160719";
    private final static String END_INSTANCE_RANGE = "20160721";
    private final static String SOURCE_DATA_PATH = "/user/hitusr_1/test/";
    private boolean testCasePassedFlag = false;

    @BeforeClass
    public static void startTestSession() throws Exception {
	TestSession.start();
    }

    @Before
    public void setUp() throws NumberFormatException, Exception {
	
	if ( this.consoleHandle.isFacetRunning("replication", "red", "bf1") == false ) {
	    Assert.fail("Looks like Red replication facet in bf1 colo is down.");
	}
	
	this.workFlowHelper = new WorkFlowHelper();
    }

    @Test
    public void  testReplication() throws IOException, InterruptedException {

	// create a new dataset
	this.createDataset();

	// check for replication workflow
	workFlowHelper.checkWorkFlow(this.dataSetName, "replication", this.datasetActivationTime);

	// deactivate the dataset before applying retention to dataset
	Response response = this.consoleHandle.deactivateDataSet(this.dataSetName);
	Assert.assertEquals("ResponseCode - Deactivate DataSet failed", HttpStatus.SC_OK, response.getStatusCode());

	// set retention policy to zero
	this.consoleHandle.setRetentionPolicyToAllDataSets(this.dataSetName , "0");

	// check for retention workflow
	workFlowHelper.checkWorkFlow(this.dataSetName , "retention", this.datasetActivationTime);
	
	// set to true if both replication and retention are successful
	testCasePassedFlag = true;
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
	target.setReplicationStrategy("DistCp");
	generator.setTarget(target);
	String dataSetXml = generator.getXml();

	Response response = this.consoleHandle.createDataSet(this.dataSetName, dataSetXml);
	if (response.getStatusCode() != HttpStatus.SC_OK) {
	    TestSession.logger.error("Failed to create dataset, xml: " + dataSetXml);
	    Assert.fail("Response status code is " + response.getStatusCode() + ", expected 200.");
	}
	
	TestSession.logger.info("Wait for some time, so that dataset gets created, activated and ready for replication.");
	this.consoleHandle.sleep(5000);
    }

    @After
    public void tearDown() {
	Response response = this.consoleHandle.deactivateDataSet(this.dataSetName);
	Assert.assertEquals("ResponseCode - Deactivate DataSet failed", HttpStatus.SC_OK, response.getStatusCode());
	
	if (testCasePassedFlag) {
	    this.consoleHandle.removeDataSet(this.dataSetName);
	}
    }
}