package hadooptest.gdm.regression;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.cluster.gdm.HTTPHandle;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/*
 * TestCase : Verify whether replication workflow success with the specified path after date and retention workflow is success.
 * example :  data/daqdev/data/%{date}/PAGE/ 
 *   
 */
public class TestDiscoveryPathSpecifiedAfterDate extends TestSession {

	private ConsoleHandle consoleHandle;
	private String dataSetName;
	private String baseDataSetName = "VerifyAcqRepRetWorkFlowExecutionDateRange";
	private String targetGrid1;
	private String targetGrid2;
	private String datasetActivationTime;
	private String cookie;
	private WorkFlowHelper workflowHelper = null;
	private List<String> grids = new ArrayList<String>();
	private List<String> instanceDates;
	private static final int FAILURE = 500;
	private static final int SUCCESS = 200;
	private final static String HADOOP_LS_PATH = "/console/api/admin/hadoopls?dataSource=";
	private final static String ABF_DATA_PATH = "/data/SOURCE_ABF/ABF_Daily/";
	private static final String GROUP_NAME = "jaggrp";
	private static final String DATA_OWNER = "jagpip";
	private static final String HCAT_TYPE = "DataOnly";
	private static final String SOURCE_CLUSTER = "qe9blue";
	private static final String CUSTOM_PATH = "/data/daqdev/abf-discovery/data/";

	@BeforeClass
	public static void startTestSession() {
		TestSession.start();
	}

	@Before
	public void setup() throws Exception {
		this.consoleHandle = new ConsoleHandle();
		HTTPHandle httpHandle = new HTTPHandle();
		this.cookie = httpHandle.getBouncerCookie();
		List<String> tempGrids = this.consoleHandle.getAllInstalledGridName();

		// remove target that starting with gdm-target
		for ( String gridName : tempGrids) {
			if ( !gridName.startsWith("gdm-target") )
				grids.add(gridName);
		}

		// check whether SOURCE_CLUSTER exists
		if (! this.grids.contains(SOURCE_CLUSTER)) {
			fail(SOURCE_CLUSTER + " does not exist, and testcase can't be executed since data does not exists on other cluster.");
		} else {
			if (grids.size() >= 2 ) {

				// check whether first element is a source cluster , if its equal to 
				this.targetGrid1 = grids.get(0);
				if (this.targetGrid1.equals(SOURCE_CLUSTER)) {
					this.targetGrid1 = grids.get(1);
				}
			} else {
				fail("There are only " + grids.size() + " grids; need at least two to run tests. ");
			}	
		}
		this.workflowHelper = new WorkFlowHelper();
	}

	@Test
	public void testDoAcqAndRep() throws Exception {

		instanceDates = this.getInstanceFiles();
		assertTrue("ABF data is missing so testing can't be done, make sure whether the ABF data path is correct...!" , instanceDates != null);

		// dataset name
		this.dataSetName =  "TestDiscoveryPathSpecifiedAfterDate_"  + System.currentTimeMillis();

		// create replication dataset
		createDataSetWithOutDateInPath("DoAsReplicationDataSet.xml");

		// activate the dataset
		this.consoleHandle.checkAndActivateDataSet(this.dataSetName);
		this.datasetActivationTime = GdmUtils.getCalendarAsString();

		// check for replication workflow
		this.workflowHelper.checkWorkFlow(this.dataSetName, "replication", this.datasetActivationTime);

		List<String> pathList = this.getInstanceFileAfterWorkflow(this.targetGrid1, dataSetName);

		String path = this.CUSTOM_PATH + this.dataSetName + "/" + instanceDates.get(0) + "/PAGE";
		TestSession.logger.info("custom path = " + path);

		for ( String pathValue : pathList) {
			assertTrue("Expected to have " + path + " but got " + pathValue ,  pathValue.indexOf(path) > 0 );
		}

		this.consoleHandle.setRetentionPolicyToAllDataSets(this.dataSetName, "0");
		this.workflowHelper.checkWorkFlow(this.dataSetName, "retention", this.datasetActivationTime);
	}

	/**
	 * Create a dataset without %date specified in the datapath, creating of dataset will fail with HTTP error code 500
	 * @param dataSetFileName - name of the replication dataset
	 */
	private void createDataSetWithOutDateInPath(String dataSetFileName) {
		String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/" + dataSetFileName);
		String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.dataSetName, dataSetConfigFile);
		String feedName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Parameters" , "value");

		dataSetXml = dataSetXml.replaceAll("TARGET1", this.targetGrid1 );
		dataSetXml = dataSetXml.replaceAll("GROUP_NAME", this.GROUP_NAME);
		dataSetXml = dataSetXml.replaceAll("DATA_OWNER", this.DATA_OWNER);
		dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", this.dataSetName);
		dataSetXml = dataSetXml.replaceAll("FEED_NAME", feedName );
		dataSetXml = dataSetXml.replaceAll("FEED_STATS", feedName + "_stats" );
		dataSetXml = dataSetXml.replaceAll("ACQUISITION_SOURCE_NAME", SOURCE_CLUSTER );
		dataSetXml = dataSetXml.replace("HCAT_TYPE", this.HCAT_TYPE);
		dataSetXml = dataSetXml.replaceAll("HCAT_TABLE_NAME", this.dataSetName);
		dataSetXml = dataSetXml.replaceAll("REPLICATION_SOURCE_DATA_PATH", this.getReplicationPath());
		dataSetXml = dataSetXml.replaceAll("REPLICATION_SOURCE_SCHEMA_PATH", this.getReplicationPath());
		dataSetXml = dataSetXml.replaceAll("REPLICATION_SOURCE_COUNT_PATH", this.getReplicationPath());
		dataSetXml = dataSetXml.replaceAll("CUSTOM_DATA_PATH", this.getCustomPath());
		dataSetXml = dataSetXml.replaceAll("CUSTOM_COUNT_PATH", this.getCustomPath());
		dataSetXml = dataSetXml.replaceAll("CUSTOM_SCHEMA_PATH", this.getCustomPath());
		dataSetXml = dataSetXml.replaceAll("20130725" , instanceDates.get(0));

		Response response = this.consoleHandle.createDataSet(this.dataSetName, dataSetXml);
		assertTrue("Response status code is " + response.getStatusCode() + ", expected 200." , response.getStatusCode() == SUCCESS);
	}

	/**
	 * Method to create a custom path with date in path.
	 * @param pathType - its a string type which represents either data/count/schema
	 * @param dataSet - dataset name
	 * @return
	 */
	private String getCustomPath() {
		return  CUSTOM_PATH + this.dataSetName + "/%{date}/PAGE/";
	}

	private String getReplicationPath() {
		return ABF_DATA_PATH + "/%{date}/PAGE";
	}

	/**
	 * Return the datapath of the specified dataset on the datasource
	 * @param dataSourceName
	 * @param dataSetName
	 * @return
	 */
	public List<String> getInstanceFileAfterWorkflow(String dataSourceName , String dataSetName) {
		JSONArray jsonArray = null;
		List<String> dataPath = new ArrayList<String>();
		String testURL = this.consoleHandle.getConsoleURL() + this.HADOOP_LS_PATH + dataSourceName + "&dataSet=" + dataSetName + "&format=json";
		TestSession.logger.info("Test url = " + testURL);
		com.jayway.restassured.response.Response res = given().cookie(this.cookie).get(testURL);
		assertTrue("Failed to get the respons  " + res , (res != null ) );
		jsonArray = this.consoleHandle.convertResponseToJSONArray(res , "Files");
		if ( jsonArray.size() > 0 ) {
			Iterator iterator = jsonArray.iterator();
			while (iterator.hasNext()) {
				JSONObject dSObject = (JSONObject) iterator.next();
				String  directory = dSObject.getString("Directory");
				if (directory.equals("yes")) {
					String path = dSObject.getString("Path");
					dataPath.add(path);
				}
			}
		}
		return dataPath;
	}

	/**
	 * First checks whether ABF data exists on the grid for a given path, if exists returns instance date(s) 
	 * @return
	 */
	public List<String> getInstanceFiles() {
		JSONArray jsonArray = null;
		List<String>instanceDate = new ArrayList<String>();
		String testURL = this.consoleHandle.getConsoleURL() + this.HADOOP_LS_PATH + this.SOURCE_CLUSTER + "&path=" + ABF_DATA_PATH + "&format=json";
		TestSession.logger.info("Test url = " + testURL);
		com.jayway.restassured.response.Response res = given().cookie(this.cookie).get(testURL);
		assertTrue("Failed to get the respons  " + res , (res != null ) );

		jsonArray = this.consoleHandle.convertResponseToJSONArray(res , "Files");
		TestSession.logger.info("********size = " + jsonArray.size());
		if ( jsonArray.size() > 0 ) {
			Iterator iterator = jsonArray.iterator();
			while (iterator.hasNext()) {
				JSONObject dSObject = (JSONObject) iterator.next();
				String  directory = dSObject.getString("Directory");
				TestSession.logger.info("######directory == " + directory);
				if (directory.equals("yes")) {
					String path = dSObject.getString("Path");
					List<String>instanceFile = Arrays.asList(path.split("/"));
					if (instanceFile != null ) {
						String dt = instanceFile.get(instanceFile.size() - 1);
						TestSession.logger.info("^^^^^^ date = " + dt);
						instanceDate.add(dt);
					}	
				}
			}
			return instanceDate;
		}
		return null;
	}

	// make dataset inactive
	@After
	public  void tearDown() {
		Response response = this.consoleHandle.deactivateDataSet(this.dataSetName);
		assertEquals("ResponseCode - Deactivate DataSet", 200, response.getStatusCode());
		assertEquals("ActionName.", "terminate", response.getElementAtPath("/Response/ActionName").toString());
		assertEquals("ResponseId", "0", response.getElementAtPath("/Response/ResponseId").toString());
		assertEquals("ResponseMessage.", "Operation on " + this.dataSetName + " was successful.", response.getElementAtPath("/Response/ResponseMessage/[0]").toString());
	}
}
