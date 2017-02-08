package hadooptest.gdm.regression.selfserve;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.HTTPHandle;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.XMLConfiguration;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestCreateRetentionOnlyDataSet extends TestSession {

	private ConsoleHandle consoleHandle;
	private String target1;
	private String target2;
	private List<String> grids = new ArrayList<String>();
	private WorkFlowHelper workFlowHelperObj = null;
	private String hostName;
	private String environmentType;
	private Configuration conf;
	private String baseDataSetName = "VerifyAcqRepRetWorkFlowExecutionSingleDate";
	public static final int FAILED = 500;

	@BeforeClass
	public static void startTestSession() {
		TestSession.start();
	}

	@Before
	public void setup() throws Exception {
		String HITUSR2_PASSWD = Util.getTestUserPasswordFromYkeykey("headless_user_hitusr_2");
		this.consoleHandle = new ConsoleHandle("hitusr_2", HITUSR2_PASSWD);
		this.grids = this.consoleHandle.getAllGridNames();
		TestSession.logger.info("Grids = " + grids);
		assertTrue("Expected atleast two targets." , this.grids.size() > 2);

		this.target1 = this.grids.get(0);
		this.target2 = this.grids.get(1);
		TestSession.logger.info("Using grids " + this.target1  + "  & " +  this.target2);

		workFlowHelperObj = new WorkFlowHelper();
		WorkFlowHelper workFlowHelperObj  = new WorkFlowHelper();
		String configPath = Util.getResourceFullPath("gdm/conf/config.xml");
		this.conf = new XMLConfiguration(configPath);
		this.environmentType = this.conf.getString("hostconfig.console.test_environment_type");
		if (environmentType.equals("oneNode")) {
			TestSession.logger.info("Test can't  be executed on oneNode");
		} else if (environmentType.equals("staging")) {
			TestSession.logger.info("**  staging **");
			String consoleURL = this.conf.getString("hostconfig.console.staging_console_url");
			this.hostName = Arrays.asList(consoleURL.split(":")).get(1).replaceAll("//", "").trim();
			String YINST_COMMAND = "ssh " + this.hostName + "  \"yinst set ygrid_gdm_console_server.bouncer_selfserve_create_retentionOnly_role=B\""  ;
			String output = workFlowHelperObj.executeCommand(YINST_COMMAND);
			this.consoleHandle.sleep(30000);			
			YINST_COMMAND = "ssh " + this.hostName + "  yinst restart ygrid_gdm_console_server";
			output = workFlowHelperObj.executeCommand(YINST_COMMAND);
			this.consoleHandle.sleep(60000);
		} else  {
			TestSession.logger.info("****** Specified invalid test environment ******** ");
			fail("Unknown test environment specified.");
		}
	}

	@Test
	public void test() throws Exception {
		testCreatingRetentionDataSetWithOutEnablingSelfServe();
		testCreatingRetentionDataSetWithOutEnablingDoAs();
		testCreatingRetentionDataSetWithOutEnablingReplicationDoAs();
		testCreatingRetentionDataSetWithOutEnablingRetentionDoAs();
		testCreatingRetentionOnlyDataSuccessfully();
	}

	/**
	 * Test Scenario : Verify whether non-admin user is not able to create the dataset, when self serve is not enabled.
	 */
	public  void testCreatingRetentionDataSetWithOutEnablingSelfServe() {
		String dataSetName = "CreateDataSetWithOutSelfServerEnabled_" + System.currentTimeMillis();
		String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/DoAsRetentionDataSet.xml");
		String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(dataSetName, dataSetConfigFile);
		String feedName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Parameters" , "value");
		dataSetXml = dataSetXml.replaceAll("TARGET1", this.target1);
		dataSetXml = dataSetXml.replaceAll("TARGET2", this.target2 );
		dataSetXml = dataSetXml.replaceAll("GROUP_NAME", "users");
		dataSetXml = dataSetXml.replaceAll("DATA_OWNER", "jagpip");
		dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", dataSetName);
		dataSetXml = dataSetXml.replaceAll("FEED_NAME", feedName );
		dataSetXml = dataSetXml.replaceAll("FEED_STATS", feedName + "_stats" );
		dataSetXml = dataSetXml.replace("HCAT_TYPE", "Mixed");
		dataSetXml = dataSetXml.replaceAll("HCAT_TABLE_NAME", dataSetName);
		dataSetXml = dataSetXml.replaceAll("ACQ_CUSTOM_DATA_PATH", getCustomPath("data" , dataSetName) );
		dataSetXml = dataSetXml.replaceAll("ACQ_CUSTOM_COUNT_PATH", getCustomPath("count" , dataSetName) );
		dataSetXml = dataSetXml.replaceAll("ACQ_CUSTOM_SCHEMA_PATH", getCustomPath("schema", dataSetName) );
		dataSetXml = dataSetXml.replaceAll("REP_CUSTOM_DATA_PATH", getCustomPath("data", dataSetName) ); 
		dataSetXml = dataSetXml.replaceAll("REP_CUSTOM_COUNT_PATH", getCustomPath("count", dataSetName) ); 
		dataSetXml = dataSetXml.replaceAll("REP_CUSTOM_SCHEMA_PATH", getCustomPath("schema", dataSetName) );
		dataSetXml = dataSetXml.replaceAll("DATABASE_NAME" , "gdm" );
		Response res = this.consoleHandle.createDataSet(dataSetName, dataSetXml);
		if (res.getStatusCode() == FAILED) {
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
	public void testCreatingRetentionDataSetWithOutEnablingDoAs() {
		String dataSetName = "CreateDataSetWithOutDoAsEnabled_" + System.currentTimeMillis();
		String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/DoAsRetentionDataSet.xml");
		StringBuffer buffer  = new StringBuffer(this.consoleHandle.createDataSetXmlFromConfig(dataSetName, dataSetConfigFile));
		int index = buffer.indexOf("</HCat>") + "</HCat>".length();
		buffer.insert(index, "<SelfServe><RequireGroupAdmin>FALSE</RequireGroupAdmin><SelfServeEnabled>TRUE</SelfServeEnabled><UseOpsdbGroup>FALSE</UseOpsdbGroup></SelfServe>");
		String dataSetXml = buffer.toString();
		String feedName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Parameters" , "value");
		dataSetXml = dataSetXml.replaceAll("TARGET1", this.target1);
		dataSetXml = dataSetXml.replaceAll("TARGET2", this.target2 );
		dataSetXml = dataSetXml.replaceAll("GROUP_NAME", "users");
		dataSetXml = dataSetXml.replaceAll("DATA_OWNER", "dfsload");
		dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", dataSetName);
		dataSetXml = dataSetXml.replaceAll("FEED_NAME", feedName );
		dataSetXml = dataSetXml.replaceAll("FEED_STATS", feedName + "_stats" );
		dataSetXml = dataSetXml.replace("HCAT_TYPE", "Mixed");
		dataSetXml = dataSetXml.replace("<RunAsOwner>retention</RunAsOwner>", "");
		dataSetXml = dataSetXml.replaceAll("HCAT_TABLE_NAME", dataSetName);
		dataSetXml = dataSetXml.replaceAll("ACQ_CUSTOM_DATA_PATH", getCustomPath("data" , dataSetName) );
		dataSetXml = dataSetXml.replaceAll("ACQ_CUSTOM_COUNT_PATH", getCustomPath("count" , dataSetName) );
		dataSetXml = dataSetXml.replaceAll("ACQ_CUSTOM_SCHEMA_PATH", getCustomPath("schema", dataSetName) );
		dataSetXml = dataSetXml.replaceAll("REP_CUSTOM_DATA_PATH", getCustomPath("data", dataSetName) ); 
		dataSetXml = dataSetXml.replaceAll("REP_CUSTOM_COUNT_PATH", getCustomPath("count", dataSetName) ); 
		dataSetXml = dataSetXml.replaceAll("REP_CUSTOM_SCHEMA_PATH", getCustomPath("schema", dataSetName) );
		dataSetXml = dataSetXml.replaceAll("DATABASE_NAME" , "gdm" );
		Response res = this.consoleHandle.createDataSet(dataSetName, dataSetXml);
		if (res.getStatusCode() == FAILED) {
			TestSession.logger.info("code = " + res.getStatusCode() );
			TestSession.logger.info("response = " + res.getStatus() );
			TestSession.logger.info("message  = " + res.getResponseBodyAsString() );
			String errorMessage = "validateDataSetFields; DFS Permissions - Run As Owner must be set for replication and retention";
			int indexOf = res.getResponseBodyAsString().indexOf(errorMessage);
			assertTrue(" Expected to contain " + errorMessage + "  but got "+ res.getResponseBodyAsString(), indexOf > 0);
		}
	}


	/**
	 * Test Scenario : Verify whether non-admin user is not able to create the dataset, when Replication doAs is not enabled.
	 */
	public void testCreatingRetentionDataSetWithOutEnablingReplicationDoAs() {

		//Note : <RunAsOwner>retention</RunAsOwner> tag exists in  DoAsRetentionDataSet.xml file.
		String dataSetName = "CreateDataSetWithOutDoAsReplicationEnable_" + System.currentTimeMillis();
		String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/DoAsRetentionDataSet.xml");
		StringBuffer buffer  = new StringBuffer(this.consoleHandle.createDataSetXmlFromConfig(dataSetName, dataSetConfigFile));
		int index = buffer.indexOf("</HCat>") + "</HCat>".length();
		buffer.insert(index, "<SelfServe><RequireGroupAdmin>FALSE</RequireGroupAdmin><SelfServeEnabled>TRUE</SelfServeEnabled><UseOpsdbGroup>FALSE</UseOpsdbGroup></SelfServe>");
		String dataSetXml = buffer.toString();
		String feedName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Parameters" , "value");
		dataSetXml = dataSetXml.replaceAll("TARGET1", this.target1);
		dataSetXml = dataSetXml.replaceAll("TARGET2", this.target2 );
		dataSetXml = dataSetXml.replaceAll("GROUP_NAME", "users");
		dataSetXml = dataSetXml.replaceAll("DATA_OWNER", "dfsload");
		dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", dataSetName);
		dataSetXml = dataSetXml.replaceAll("FEED_NAME", feedName );
		dataSetXml = dataSetXml.replaceAll("FEED_STATS", feedName + "_stats" );
		dataSetXml = dataSetXml.replace("HCAT_TYPE", "Mixed");
		dataSetXml = dataSetXml.replaceAll("HCAT_TABLE_NAME", dataSetName);
		dataSetXml = dataSetXml.replaceAll("ACQ_CUSTOM_DATA_PATH", getCustomPath("data" , dataSetName) );
		dataSetXml = dataSetXml.replaceAll("ACQ_CUSTOM_COUNT_PATH", getCustomPath("count" , dataSetName) );
		dataSetXml = dataSetXml.replaceAll("ACQ_CUSTOM_SCHEMA_PATH", getCustomPath("schema", dataSetName) );
		dataSetXml = dataSetXml.replaceAll("REP_CUSTOM_DATA_PATH", getCustomPath("data", dataSetName) ); 
		dataSetXml = dataSetXml.replaceAll("REP_CUSTOM_COUNT_PATH", getCustomPath("count", dataSetName) ); 
		dataSetXml = dataSetXml.replaceAll("REP_CUSTOM_SCHEMA_PATH", getCustomPath("schema", dataSetName) );
		dataSetXml = dataSetXml.replaceAll("DATABASE_NAME" , "gdm" );
		Response res = this.consoleHandle.createDataSet(dataSetName, dataSetXml);
		if (res.getStatusCode() == FAILED) {
			TestSession.logger.info("code = " + res.getStatusCode() );
			TestSession.logger.info("response = " + res.getStatus() );
			String errorMessage = "validateDataSetFields; DFS Permissions - Run As Owner must be set for replication and retention";
			int indexOf = res.getResponseBodyAsString().indexOf(errorMessage);
			assertTrue(" Expected to contain " + errorMessage + "  but got "+ res.getResponseBodyAsString(), indexOf > 0);
		}
	}


	/**
	 * Verify whether non-admin user is not able to create the dataset, when retention doAs is not enabled.
	 */
	public void testCreatingRetentionDataSetWithOutEnablingRetentionDoAs() {
		String dataSetName = "CreateDataSetWithOutDoAsRetentionEnabled_" + System.currentTimeMillis();
		String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/DoAsRetentionDataSet.xml");
		StringBuffer buffer  = new StringBuffer(this.consoleHandle.createDataSetXmlFromConfig(dataSetName, dataSetConfigFile));
		int index = buffer.indexOf("</HCat>") + "</HCat>".length();
		buffer.insert(index, "<SelfServe><RequireGroupAdmin>FALSE</RequireGroupAdmin><SelfServeEnabled>TRUE</SelfServeEnabled><UseOpsdbGroup>FALSE</UseOpsdbGroup></SelfServe>");
		String dataSetXml = buffer.toString();
		String feedName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Parameters" , "value");
		dataSetXml = dataSetXml.replaceAll("TARGET1", this.target1);
		dataSetXml = dataSetXml.replaceAll("TARGET2", this.target2 );
		dataSetXml = dataSetXml.replaceAll("GROUP_NAME", "users");
		dataSetXml = dataSetXml.replaceAll("DATA_OWNER", "dfsload");
		dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", dataSetName);
		dataSetXml = dataSetXml.replaceAll("FEED_NAME", feedName );
		dataSetXml = dataSetXml.replaceAll("FEED_STATS", feedName + "_stats" );
		dataSetXml = dataSetXml.replace("HCAT_TYPE", "Mixed");
		dataSetXml = dataSetXml.replace("<RunAsOwner>retention</RunAsOwner>", "<RunAsOwner>replication</RunAsOwner>");
		dataSetXml = dataSetXml.replaceAll("HCAT_TABLE_NAME", dataSetName);
		dataSetXml = dataSetXml.replaceAll("ACQ_CUSTOM_DATA_PATH", getCustomPath("data" , dataSetName) );
		dataSetXml = dataSetXml.replaceAll("ACQ_CUSTOM_COUNT_PATH", getCustomPath("count" , dataSetName) );
		dataSetXml = dataSetXml.replaceAll("ACQ_CUSTOM_SCHEMA_PATH", getCustomPath("schema", dataSetName) );
		dataSetXml = dataSetXml.replaceAll("REP_CUSTOM_DATA_PATH", getCustomPath("data", dataSetName) ); 
		dataSetXml = dataSetXml.replaceAll("REP_CUSTOM_COUNT_PATH", getCustomPath("count", dataSetName) ); 
		dataSetXml = dataSetXml.replaceAll("REP_CUSTOM_SCHEMA_PATH", getCustomPath("schema", dataSetName) );
		dataSetXml = dataSetXml.replaceAll("DATABASE_NAME" , "gdm" );
		Response res = this.consoleHandle.createDataSet(dataSetName, dataSetXml);
		if (res.getStatusCode() == FAILED) {
			TestSession.logger.info("code = " + res.getStatusCode() );
			TestSession.logger.info("response = " + res.getStatus() );
			String errorMessage = "validateDataSetFields; DFS Permissions - Run As Owner must be set for replication and retention";
			int indexOf = res.getResponseBodyAsString().indexOf(errorMessage);
			assertTrue(" Expected to contain " + errorMessage + "  but got "+ res.getResponseBodyAsString(), indexOf > 0);
		}
	}

	/**
	 * Verify whether non-admin user is able to create a dataset, when doAs (replication and retention) and self serve is enabled. 
	 * @throws Exception 
	 */
	public void testCreatingRetentionOnlyDataSuccessfully() throws Exception {
		String dataSetName = "CreateRetentionOnlyDataSet_NonAdmin_" + System.currentTimeMillis();
		String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/DoAsRetentionDataSet.xml");
		StringBuffer buffer  = new StringBuffer(this.consoleHandle.createDataSetXmlFromConfig(dataSetName, dataSetConfigFile));
		int index = buffer.indexOf("</HCat>") + "</HCat>".length();
		buffer.insert(index, "<SelfServe><RequireGroupAdmin>FALSE</RequireGroupAdmin><SelfServeEnabled>TRUE</SelfServeEnabled><UseOpsdbGroup>FALSE</UseOpsdbGroup></SelfServe>");
		String dataSetXml = buffer.toString();
		String feedName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Parameters" , "value");
		dataSetXml = dataSetXml.replaceAll("TARGET1", this.target1);
		dataSetXml = dataSetXml.replaceAll("TARGET2", this.target2 );
		dataSetXml = dataSetXml.replaceAll("GROUP_NAME", "jaggrp");
		dataSetXml = dataSetXml.replaceAll("DATA_OWNER", "jagpip");
		dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", dataSetName);
		dataSetXml = dataSetXml.replaceAll("FEED_NAME", feedName );
		dataSetXml = dataSetXml.replaceAll("FEED_STATS", feedName + "_stats" );
		dataSetXml = dataSetXml.replace("HCAT_TYPE", "Mixed");
		dataSetXml = dataSetXml.replace("<Active>TRUE</Active>", "<Active>FALSE</Active>");
		dataSetXml = dataSetXml.replace("<RunAsOwner>retention</RunAsOwner>", "<RunAsOwner>retention,replication</RunAsOwner>");
		dataSetXml = dataSetXml.replaceAll("HCAT_TABLE_NAME", dataSetName);
		dataSetXml = dataSetXml.replaceAll("ACQ_CUSTOM_DATA_PATH", getCustomPath("data" , dataSetName) );
		dataSetXml = dataSetXml.replaceAll("ACQ_CUSTOM_COUNT_PATH", getCustomPath("count" , dataSetName) );
		dataSetXml = dataSetXml.replaceAll("ACQ_CUSTOM_SCHEMA_PATH", getCustomPath("schema", dataSetName) );
		dataSetXml = dataSetXml.replaceAll("REP_CUSTOM_DATA_PATH", getCustomPath("data", dataSetName) ); 
		dataSetXml = dataSetXml.replaceAll("REP_CUSTOM_COUNT_PATH", getCustomPath("count", dataSetName) ); 
		dataSetXml = dataSetXml.replaceAll("REP_CUSTOM_SCHEMA_PATH", getCustomPath("schema", dataSetName) );
		dataSetXml = dataSetXml.replaceAll("DATABASE_NAME" , "gdm" );
		Response res = this.consoleHandle.createDataSet(dataSetName, dataSetXml);
		if (res.getStatusCode() == 200) {
			TestSession.logger.info("********");
			TestSession.logger.info("code = " + res.getStatusCode() );
			TestSession.logger.info("response = " + res.getStatus() );
			TestSession.logger.info("Body = " + res.getResponseBodyAsString());
		} else {
			TestSession.logger.info("********  - " + res.getStatusCode()  );
		}
	}

	/**
	 * Method to create a custom path for the dataset.
	 * @param pathType - its a string type which represents either data/count/schema
	 * @param dataSet - dataset name
	 * @return
	 */
	private String getCustomPath(String pathType , String dataSetName) {
		return  "/data/daqdev/"+ pathType +"/" + dataSetName + "/%{date}";
	}	
}
