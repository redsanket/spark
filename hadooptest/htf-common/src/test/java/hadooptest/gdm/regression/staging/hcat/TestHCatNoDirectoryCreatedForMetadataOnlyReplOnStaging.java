package hadooptest.gdm.regression.staging.hcat;

import org.apache.commons.httpclient.HttpStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import hadooptest.TestSession;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.DataSetTarget;
import hadooptest.cluster.gdm.DataSetXmlGenerator;
import hadooptest.cluster.gdm.HCatHelper;
import hadooptest.cluster.gdm.HTTPHandle;

import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;

import static org.junit.Assert.assertTrue;

/**
 * TestCase : To test whether hcat metadata only works on staging
 *
 */
public class TestHCatNoDirectoryCreatedForMetadataOnlyReplOnStaging extends TestSession {

    private ConsoleHandle consoleHandle;
    private WorkFlowHelper workFlowHelper;
    private HCatHelper hCatHelper;
    private String dataSetName;
    private String datasetActivationTime;
    private final static String SOURCE_CLUSTER_NAME = "AxoniteRed";
    private final static String TARGET_CLUSTER_NAME = "KryptoniteRed";
    private static final String DBNAME = "gdmstgtesting";
    private static final String TABLE_NAME = "metadata_only";
    private static final String DATA_PATH = "/user/hitusr_1/metadata_only";
    private static int SLEEP_TIME = 50000;
    private static final String START_INSTANCE_RANGE = "20150101";
    private static final String END_INSTANCE_RANGE = "20160721";

    @BeforeClass
    public static void startTestSession() {
	TestSession.start();
    }

    @Before
    public void setup() throws Exception {
	this.consoleHandle = new ConsoleHandle();
	dataSetName = "TestHCatMetadataOnlyReplOnStg_" + System.currentTimeMillis();
	workFlowHelper = new WorkFlowHelper();
	hCatHelper = new HCatHelper();
	
	if ( this.consoleHandle.isFacetRunning("replication", "red", "bf1") == false ) {
	    Assert.fail("Looks like Red replication facet in bf1 colo is down.");
	}
    }

    @Test
    public void test() {
	String hostName = this.consoleHandle.getFacetHostName("replication" , "red" , "ne1");
	if (hostName != "") {
	    TestSession.logger.info("replication hostName - " + hostName);
	    if ( hCatHelper.checkTableAndDataPathExists(hostName, "replication", SOURCE_CLUSTER_NAME, DBNAME, TABLE_NAME, DATA_PATH) == true ) {
		TestSession.logger.info(this.TABLE_NAME + " exists on " + this.SOURCE_CLUSTER_NAME);

		List<String> dataSetNames = this.consoleHandle.checkDataSetExistForGivenPath(DATA_PATH, SOURCE_CLUSTER_NAME);
		if (dataSetNames.size() > 0) {
		    for ( String dsName : dataSetNames) {

			// check if partition exits for the specified table, if partition exists apply retention to the existing dataset remove partition and run the test.
			if (this.hCatHelper.doPartitionExist(hostName, "replication", TARGET_CLUSTER_NAME, DBNAME, TABLE_NAME) == true) {
			    TestSession.logger.info("Partition exits.");

			    // set retention policy to zero
			    this.consoleHandle.setRetentionPolicyToAllDataSets(dsName , "0");

			    TestSession.logger.info("wait for some time, so that dataset changes are applied.");
			    this.consoleHandle.sleep(SLEEP_TIME);
			    
			    // check for retention workflow
			    workFlowHelper.checkWorkFlow(dsName, "retention", this.datasetActivationTime);
			}
			TestSession.logger.info("dataSetName - " + dsName);
			this.consoleHandle.deActivateAndRemoveDataSet(dsName);
		    }
		}

		// create new dataset
		createDataset();

		this.consoleHandle.activateDataSet(this.dataSetName);
		TestSession.logger.info("wait for some time, so that dataset can get activated.");
		this.consoleHandle.sleep(SLEEP_TIME);

		// check for replication workflow
		workFlowHelper.checkWorkFlow(this.dataSetName, "replication", this.datasetActivationTime);

		// check whether partitions exists on the target after replication workflow is success
		List<String> partitionList = this.hCatHelper.getHCatTablePartitions(hostName, "replication", TARGET_CLUSTER_NAME, DBNAME, TABLE_NAME);
		Assert.assertTrue("Partition does not exists on " + TARGET_CLUSTER_NAME  + "  after replication workflow is completed.", partitionList.size() > 0);

		// deactivate the dataset before applying retention to dataset
		Response response = this.consoleHandle.deactivateDataSet(this.dataSetName);
		Assert.assertEquals("ResponseCode - Deactivate DataSet failed", HttpStatus.SC_OK, response.getStatusCode());

		// set retention policy to zero
		this.consoleHandle.setRetentionPolicyToAllDataSets(this.dataSetName , "0");

		TestSession.logger.info("wait for some time, so that dataset changes are applied.");
		this.consoleHandle.sleep(SLEEP_TIME);

		// check for retention workflow
		workFlowHelper.checkWorkFlow(this.dataSetName, "retention", this.datasetActivationTime);

		// check parition does not exists after retention is successfull
		Assert.assertTrue("Expected there is no partitions exists on " + TARGET_CLUSTER_NAME  + "  but looks like it exists after retention " , this.hCatHelper.doPartitionExist(hostName, "replication", TARGET_CLUSTER_NAME, DBNAME, TABLE_NAME) == false);
	    } else {
		Assert.fail( this.TABLE_NAME + " does not exists on " + this.SOURCE_CLUSTER_NAME + "  , please check whether " + this.DBNAME + "  &  " + this.TABLE_NAME +  " exists on " + this.SOURCE_CLUSTER_NAME );
	    }
	} else {
	    Assert.fail("No replication host configured for console - " + this.consoleHandle.getConsoleURL());
	}
    }

    /**
     * Create a replication dataset for metadata only
     */
    private void createDataset() {
	DataSetXmlGenerator generator = new DataSetXmlGenerator();
	generator.setName(this.dataSetName);
	generator.setDescription(this.dataSetName);
	generator.setCatalog(this.dataSetName);
	generator.setActive("FALSE");
	generator.setOwner("dfsload");
	generator.setGroup("users");
	generator.setPermission("750");
	generator.setRetentionEnabled("TRUE");
	generator.setPriority("NORMAL");
	generator.setFrequency("daily");
	generator.setDiscoveryFrequency("30");
	generator.setDiscoveryInterface("HCAT");
	generator.addSourcePath("data", DATA_PATH + "/instancedate=%{date}");
	generator.setSource(SOURCE_CLUSTER_NAME);

	// hcat specifics 
	generator.setHcatDbName(DBNAME);
	generator.setHcatForceExternalTables("FALSE");
	generator.setHcatInstanceKey("instancedate");
	generator.setHcatRunTargetFilter("FALSE");
	generator.setHcatTableName(TABLE_NAME);
	generator.setHcatTablePropagationEnabled("TRUE");

	DataSetTarget target = new DataSetTarget();
	target.setName(TARGET_CLUSTER_NAME);
	target.setDateRangeStart(true, START_INSTANCE_RANGE);
	target.setDateRangeEnd(true, END_INSTANCE_RANGE);
	target.setHCatType("HCatOnly");
	target.setNumInstances("5");
	generator.setTarget(target);
	String dataSetXml = generator.getXml();

	Response response = this.consoleHandle.createDataSet(this.dataSetName, dataSetXml);
	if (response.getStatusCode() != HttpStatus.SC_OK) {
	    TestSession.logger.error("Failed to create dataset, xml: " + dataSetXml);
	    Assert.fail("Response status code is " + response.getStatusCode() + ", expected 200.");
	}
    }

}
