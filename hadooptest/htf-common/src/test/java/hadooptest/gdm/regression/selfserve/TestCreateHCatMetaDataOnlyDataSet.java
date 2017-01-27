package hadooptest.gdm.regression.selfserve;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedExecutor;

import java.util.List;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * TestCases to create HCat MetaData Only dataset.
 *
 */
public class TestCreateHCatMetaDataOnlyDataSet extends TestSession {

	private ConsoleHandle consoleHandle;
	private List<String> hcatSupportedGrid;
	private String targetGrid1;
	private String targetGrid2;
	private FullyDistributedExecutor executor = new FullyDistributedExecutor();
	private String baseDataSetName = "VerifyAcqRepRetWorkFlowExecutionSingleDate";
	private static final int SUCCESS = 200;
	public static final int FAILED = 500;
	private static final String DATABASE_NAME = "gdm";

	@BeforeClass
	public static void startTestSession() throws Exception {
		TestSession.start();
	}

	@Before
	public void setUp() throws NumberFormatException, Exception {

		this.consoleHandle = new ConsoleHandle("hitusr_2"  , "NOT_VALID");

		// get all the hcat supported clusters
		hcatSupportedGrid = this.consoleHandle.getHCatEnabledGrid();

		// check whether we have two hcat cluster one for acquisition and replication
		if (hcatSupportedGrid.size() < 2) {
			throw new Exception("Need atleast 2 hcat enabled  datasources to run the testcase, right now available hcat enabled datasource are " + hcatSupportedGrid.size() );
		}

		this.targetGrid1 = hcatSupportedGrid.get(0).trim();
		this.targetGrid2 = hcatSupportedGrid.get(1).trim();
		TestSession.logger.info("Using grids " + this.targetGrid1  + "  & " +  this.targetGrid2);

		// set the yinst setting 
		String []yinst  = executor.runProcBuilder(new String [] {"./resources/gdm/restart/gdm_yinst_set.sh" , "console" , "ygrid_gdm_console_server.bouncer_selfserve_create_hcatMetaDataOnly_role" , "B"});
		for ( String  s : yinst ) {
			TestSession.logger.info(s);
		}

		// stop the retention facet
		String stop[] =  executor.runProcBuilder(new String [] {"./resources/gdm/restart/StopStartFacet.sh" , "console" , "stop" });
		for ( String  s : stop ) {
			TestSession.logger.info(s);
		}

		// start the retention facet
		String start[] =  executor.runProcBuilder(new String [] {"./resources/gdm/restart/StopStartFacet.sh" , "console" , "start" });
		for ( String  s : start ) {
			TestSession.logger.info(s);
		}

		// wait for some time, so that classes gets loaded successfully
		TestSession.logger.info("Please wait for a minutes, so that discovery can start...! ");
		this.consoleHandle.sleep(60000);

	}

	/**
	 * TestCase : Verify whether non-admin user is not able to create metadata only dataset when SelfServe is not enabled.
	 */
	@Test
	public void testCreatingMetaDataOnlyDataSetWithOutSelfServeEnabled() {
		String dataSetName = "testCreatingMetaDataOnlyDataSetWithOutSelfServeEnabled_" + System.currentTimeMillis();
		String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/TablePropagationDataSet.xml");
		String feedName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Parameters" , "value");
		String dataSetXml  = this.consoleHandle.createDataSetXmlFromConfig(dataSetName, dataSetConfigFile);
		dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", dataSetName);
		dataSetXml = dataSetXml.replaceAll("GROUP_NAME", "jaggrp");
		dataSetXml = dataSetXml.replaceAll("DATA_OWNER", "jagpip");
		dataSetXml = dataSetXml.replaceAll("FACET", "replication,retention");
		dataSetXml = dataSetXml.replaceAll("CUSTOM_DATA_PATH", "/data/daqdev/data");
		dataSetXml = dataSetXml.replaceAll("CUSTOM_SCHEMA_PATH", "/data/daqdev/schema");
		dataSetXml = dataSetXml.replaceAll("FEED_NAME", feedName );
		dataSetXml = dataSetXml.replaceAll("FEED_STATS", feedName + "_stats" );
		dataSetXml = dataSetXml.replaceAll("TARGET1", this.targetGrid1);
		dataSetXml = dataSetXml.replaceAll("TARGET2", this.targetGrid2);
		dataSetXml = dataSetXml.replaceAll("TABLE_NAME", dataSetName);
		dataSetXml = dataSetXml.replaceAll("DATABASE", DATABASE_NAME);

		TestSession.logger.info("**************   dataSetXml  ******************* " + dataSetXml);
		Response res = this.consoleHandle.createDataSet(dataSetName, dataSetXml); 
		if (res.getStatusCode() != SUCCESS) {
			TestSession.logger.info("code = " + res.getStatusCode() );
			TestSession.logger.info("response = " + res.getStatus() );
			TestSession.logger.info("message  = " + res.getResponseBodyAsString() );
			String errorMessage = "validateDataSetFields; Self-Serve must be enabled";
			int index = res.getResponseBodyAsString().indexOf(errorMessage);
			assertTrue(" Expected to contain " + errorMessage + "  but got "+ res.getResponseBodyAsString(), index > 0);
		}
	}

	/**
	 * Test Scenario : Verify whether non-admin user is not able to create the dataset, when doAs is not enabled.
	 */
	@Test
	public void testCreatingMetaDataOnlyDataSetWithOutEnablingDoAs() {
		String dataSetName = "testCreatingMetaDataOnlyDataSetWithOutEnablingDoAs_" + System.currentTimeMillis();
		String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/TablePropagationDataSet.xml");
		String feedName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Parameters" , "value");
		String dataSetXml  = this.consoleHandle.createDataSetXmlFromConfig(dataSetName, dataSetConfigFile);
		dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", dataSetName);
		dataSetXml = dataSetXml.replaceAll("GROUP_NAME", "jaggrp");
		dataSetXml = dataSetXml.replaceAll("DATA_OWNER", "jagpip");
		dataSetXml = dataSetXml.replaceAll("<RunAsOwner>FACET</RunAsOwner>", "");
		dataSetXml = dataSetXml.replaceAll("CUSTOM_DATA_PATH", "/data/daqdev/data");
		dataSetXml = dataSetXml.replaceAll("CUSTOM_SCHEMA_PATH", "/data/daqdev/schema");
		dataSetXml = dataSetXml.replaceAll("FEED_NAME", feedName );
		dataSetXml = dataSetXml.replaceAll("FEED_STATS", feedName + "_stats" );
		dataSetXml = dataSetXml.replaceAll("TARGET1", this.targetGrid1);
		dataSetXml = dataSetXml.replaceAll("TARGET2", this.targetGrid2);
		dataSetXml = dataSetXml.replaceAll("TABLE_NAME", dataSetName);
		dataSetXml = dataSetXml.replaceAll("DATABASE", DATABASE_NAME);
		dataSetXml = dataSetXml.replaceAll("<SelfServeEnabled>FALSE</SelfServeEnabled>", "<SelfServeEnabled>TRUE</SelfServeEnabled>");

		TestSession.logger.info("**************   dataSetXml  ******************* " + dataSetXml);
		Response res = this.consoleHandle.createDataSet(dataSetName, dataSetXml); 
		if (res.getStatusCode() != SUCCESS) {
			TestSession.logger.info("code = " + res.getStatusCode() );
			TestSession.logger.info("response = " + res.getStatus() );
			TestSession.logger.info("message  = " + res.getResponseBodyAsString() );
			String errorMessage = "validateDataSetFields; DFS Permissions - Run As Owner must be set for replication and retention";
			int index = res.getResponseBodyAsString().indexOf(errorMessage);
			assertTrue(" Expected to contain " + errorMessage + "  but got "+ res.getResponseBodyAsString(), index > 0);
		}
	}

	/**
	 * Test Scenario : Verify whether non-admin user is not able to create the dataset, when Replication doAs is not enabled.
	 */
	@Test
	public void testCreatingMetaDataOnlyDataSetWithOutEnablingReplicationDoAs() {
		String dataSetName = "testCreatingMetaDataOnlyDataSetWithOutEnablingReplicationDoAs_" + System.currentTimeMillis();
		String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/TablePropagationDataSet.xml");
		String feedName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Parameters" , "value");
		String dataSetXml  = this.consoleHandle.createDataSetXmlFromConfig(dataSetName, dataSetConfigFile);
		dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", dataSetName);
		dataSetXml = dataSetXml.replaceAll("GROUP_NAME", "jaggrp");
		dataSetXml = dataSetXml.replaceAll("DATA_OWNER", "jagpip");
		dataSetXml = dataSetXml.replaceAll("FACET", "replication");
		dataSetXml = dataSetXml.replaceAll("CUSTOM_DATA_PATH", "/data/daqdev/data");
		dataSetXml = dataSetXml.replaceAll("CUSTOM_SCHEMA_PATH", "/data/daqdev/schema");
		dataSetXml = dataSetXml.replaceAll("FEED_NAME", feedName );
		dataSetXml = dataSetXml.replaceAll("FEED_STATS", feedName + "_stats" );
		dataSetXml = dataSetXml.replaceAll("TARGET1", this.targetGrid1);
		dataSetXml = dataSetXml.replaceAll("TARGET2", this.targetGrid2);
		dataSetXml = dataSetXml.replaceAll("TABLE_NAME", dataSetName);
		dataSetXml = dataSetXml.replaceAll("DATABASE", DATABASE_NAME);
		dataSetXml = dataSetXml.replaceAll("<SelfServeEnabled>FALSE</SelfServeEnabled>", "<SelfServeEnabled>TRUE</SelfServeEnabled>");

		TestSession.logger.info("**************   dataSetXml  ******************* " + dataSetXml);
		Response res = this.consoleHandle.createDataSet(dataSetName, dataSetXml); 
		if (res.getStatusCode() != SUCCESS) {
			TestSession.logger.info("code = " + res.getStatusCode() );
			TestSession.logger.info("response = " + res.getStatus() );
			TestSession.logger.info("message  = " + res.getResponseBodyAsString() );
			String errorMessage = "validateDataSetFields; DFS Permissions - Run As Owner must be set for replication and retention";
			int index = res.getResponseBodyAsString().indexOf(errorMessage);
			assertTrue(" Expected to contain " + errorMessage + "  but got "+ res.getResponseBodyAsString(), index > 0);
		}
	}

	/**
	 * Verify whether non-admin user is not able to create the dataset, when retention doAs is not enabled.
	 */
	@Test
	public void testCreatingMetaDataOnlyDataSetWithOutEnablingRetentionDoAs() {
		String dataSetName = "testCreatingMetaDataOnlyDataSetWithOutEnablingRetentionDoAs_" + System.currentTimeMillis();
		String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/TablePropagationDataSet.xml");
		String feedName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Parameters" , "value");
		String dataSetXml  = this.consoleHandle.createDataSetXmlFromConfig(dataSetName, dataSetConfigFile);
		dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", dataSetName);
		dataSetXml = dataSetXml.replaceAll("GROUP_NAME", "jaggrp");
		dataSetXml = dataSetXml.replaceAll("DATA_OWNER", "jagpip");
		dataSetXml = dataSetXml.replaceAll("FACET", "retention");
		dataSetXml = dataSetXml.replaceAll("CUSTOM_DATA_PATH", "/data/daqdev/data");
		dataSetXml = dataSetXml.replaceAll("CUSTOM_SCHEMA_PATH", "/data/daqdev/schema");
		dataSetXml = dataSetXml.replaceAll("FEED_NAME", feedName );
		dataSetXml = dataSetXml.replaceAll("FEED_STATS", feedName + "_stats" );
		dataSetXml = dataSetXml.replaceAll("TARGET1", this.targetGrid1);
		dataSetXml = dataSetXml.replaceAll("TARGET2", this.targetGrid2);
		dataSetXml = dataSetXml.replaceAll("TABLE_NAME", dataSetName);
		dataSetXml = dataSetXml.replaceAll("DATABASE", DATABASE_NAME);
		dataSetXml = dataSetXml.replaceAll("<SelfServeEnabled>FALSE</SelfServeEnabled>", "<SelfServeEnabled>TRUE</SelfServeEnabled>");

		TestSession.logger.info("**************   dataSetXml  ******************* " + dataSetXml);
		Response res = this.consoleHandle.createDataSet(dataSetName, dataSetXml); 
		if (res.getStatusCode() != SUCCESS) {
			TestSession.logger.info("code = " + res.getStatusCode() );
			TestSession.logger.info("response = " + res.getStatus() );
			TestSession.logger.info("message  = " + res.getResponseBodyAsString() );
			String errorMessage = "validateDataSetFields; DFS Permissions - Run As Owner must be set for replication and retention";
			int index = res.getResponseBodyAsString().indexOf(errorMessage);
			assertTrue(" Expected to contain " + errorMessage + "  but got "+ res.getResponseBodyAsString(), index > 0);
		}
	}

	/**
	 * Verify whether non-admin user is not able to create the dataset, when wrong discovery type is specified ( other than HCAT )
	 * 
	 */
	@Test
	public void testCreatingMetaDataOnlyDataSetWithWrongDiscoveryType(){
		String dataSetName = "testCreatingMetaDataOnlyDataSetWithWrongDiscoveryType_" + System.currentTimeMillis();
		String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/TablePropagationDataSet.xml");
		String feedName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Parameters" , "value");
		String dataSetXml  = this.consoleHandle.createDataSetXmlFromConfig(dataSetName, dataSetConfigFile);
		dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", dataSetName);
		dataSetXml = dataSetXml.replaceAll("GROUP_NAME", "jaggrp");
		dataSetXml = dataSetXml.replaceAll("DATA_OWNER", "jagpip");
		dataSetXml = dataSetXml.replaceAll("FACET", "replication,retention");
		dataSetXml = dataSetXml.replaceAll("CUSTOM_DATA_PATH", "/data/daqdev/data");
		dataSetXml = dataSetXml.replaceAll("CUSTOM_SCHEMA_PATH", "/data/daqdev/schema");
		dataSetXml = dataSetXml.replaceAll("FEED_NAME", feedName );
		dataSetXml = dataSetXml.replaceAll("FEED_STATS", feedName + "_stats" );
		dataSetXml = dataSetXml.replaceAll("TARGET1", this.targetGrid1);
		dataSetXml = dataSetXml.replaceAll("TARGET2", this.targetGrid2);
		dataSetXml = dataSetXml.replaceAll("TABLE_NAME", dataSetName);
		dataSetXml = dataSetXml.replaceAll("DATABASE", DATABASE_NAME);
		dataSetXml = dataSetXml.replaceAll("<SelfServeEnabled>FALSE</SelfServeEnabled>", "<SelfServeEnabled>TRUE</SelfServeEnabled>");
		dataSetXml = dataSetXml.replaceAll("<DiscoveryInterface>HCAT</DiscoveryInterface>", "<DiscoveryInterface>HDFS</DiscoveryInterface>");

		TestSession.logger.info("**************   dataSetXml  ******************* " + dataSetXml);
		Response res = this.consoleHandle.createDataSet(dataSetName, dataSetXml); 
		if (res.getStatusCode() != SUCCESS) {
			TestSession.logger.info("code = " + res.getStatusCode() );
			TestSession.logger.info("response = " + res.getStatus() );
			TestSession.logger.info("message  = " + res.getResponseBodyAsString() );
			String errorMessage = "Discovery Interface must be set to HCAT";
			int index = res.getResponseBodyAsString().indexOf(errorMessage);
			assertTrue(" Expected to contain " + errorMessage + "  but got "+ res.getResponseBodyAsString(), index > 0);
		}
	}

	/**
	 * Verify whether non-admin user is able to create a dataset, when doAs (replication and retention) and self serve is enabled. 
	 * @throws Exception 
	 */
	@Test
	public void testCreatingRetentionOnlyDataSuccessfully() throws Exception {
		String dataSetName = "testCreatingRetentionOnlyDataSuccessfully_" + System.currentTimeMillis();
		String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/TablePropagationDataSet.xml");
		String feedName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Parameters" , "value");
		String dataSetXml  = this.consoleHandle.createDataSetXmlFromConfig(dataSetName, dataSetConfigFile);
		dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", dataSetName);
		dataSetXml = dataSetXml.replaceAll("GROUP_NAME", "jaggrp");
		dataSetXml = dataSetXml.replaceAll("DATA_OWNER", "jagpip");
		dataSetXml = dataSetXml.replaceAll("FACET", "replication,retention");
		dataSetXml = dataSetXml.replaceAll("CUSTOM_DATA_PATH", "/data/daqdev/data");
		dataSetXml = dataSetXml.replaceAll("CUSTOM_SCHEMA_PATH", "/data/daqdev/schema");
		dataSetXml = dataSetXml.replaceAll("FEED_NAME", feedName );
		dataSetXml = dataSetXml.replaceAll("FEED_STATS", feedName + "_stats" );
		dataSetXml = dataSetXml.replaceAll("TARGET1", this.targetGrid1);
		dataSetXml = dataSetXml.replaceAll("TARGET2", this.targetGrid2);
		dataSetXml = dataSetXml.replaceAll("TABLE_NAME", dataSetName);
		dataSetXml = dataSetXml.replaceAll("DATABASE", DATABASE_NAME);
		dataSetXml = dataSetXml.replaceAll("<SelfServeEnabled>FALSE</SelfServeEnabled>", "<SelfServeEnabled>TRUE</SelfServeEnabled>");

		TestSession.logger.info("**************   dataSetXml  ******************* " + dataSetXml);
		Response res = this.consoleHandle.createDataSet(dataSetName, dataSetXml); 
		if (res.getStatusCode() != SUCCESS) {
			TestSession.logger.info("code = " + res.getStatusCode() );
			TestSession.logger.info("response = " + res.getStatus() );
			TestSession.logger.info("message  = " + res.getResponseBodyAsString() );
			fail("Failed to create the dataset, when all the requirement are available");
		}

	}

}
