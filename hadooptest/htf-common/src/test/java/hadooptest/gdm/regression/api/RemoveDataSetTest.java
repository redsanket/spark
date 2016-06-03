// Copyright 2016, Yahoo Inc.
package hadooptest.gdm.regression.api;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import hadooptest.Util;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.HTTPHandle;
import hadooptest.cluster.gdm.JSONUtil;
import hadooptest.cluster.gdm.Response;

import org.junit.Assert;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.jayway.restassured.path.json.JsonPath;


/**
 * 
 * Testcase : RemoveDataset , RemoveDatasource REST API 
 *
 */
@Category(SerialTests.class)
public class RemoveDataSetTest extends TestSession {

	private ConsoleHandle consoleHandle;
	private String cookie;
	private String url;
	public static final String DATA_SET_PATH = "/console/query/config/dataset/getDatasets";
	public static final String DataSourcePath = "/console/query/config/datasource";
	private String originalDataSetName = "VerifyAcqRepRetWorkFlowExecutionDateRange" ;
	private List<String> dataSourceList;
	private List<String> dataTargetList;
	private List<String> datasetsResultList;
	private static List<String>newDataSourceList = new ArrayList<String>();
	private static List<String>newDataTargetList = new ArrayList<String>();
	private static String newDataSetName  =  "TestDataSet_" + System.currentTimeMillis();
	private JSONUtil jsonUtil;
	private static int SUCCESS = 200;
	private static int SLEEP_TIME = 50000; 
	private String sourceGrid;
	private String target;
	private static final String INSTANCE1 = "20151201";

    @BeforeClass
    public static void startTestSession() throws Exception {
        TestSession.start();
    }

    @Before
    public void setUp() throws NumberFormatException, Exception {
    	HTTPHandle httpHandle = new HTTPHandle();
    	jsonUtil = new JSONUtil();
    	consoleHandle = new ConsoleHandle();
    	cookie = httpHandle.getBouncerCookie();
    	this.url = this.consoleHandle.getConsoleURL();
    	TestSession.logger.info("url = " + url);


    	datasetsResultList = given().cookie(cookie).get(this.url + DATA_SET_PATH ).getBody().jsonPath().getList("DatasetsResult.DatasetName");
    	if (datasetsResultList == null) {
    		fail("Failed to get the datasets");
    	}

    	if (datasetsResultList.size() == 0) {
    		// create the dataset.

    		List<String> grids = this.consoleHandle.getUniqueGrids();
    		if (grids.size() < 2) {
    			Assert.fail("Only " + grids.size() + " of 2 required grids exist");
    		}
    		this.sourceGrid = grids.get(0);
    		this.target = grids.get(1);
    		createDataset();

    		originalDataSetName = newDataSetName;

    		datasetsResultList = given().cookie(cookie).get(this.url + DATA_SET_PATH ).getBody().jsonPath().getList("DatasetsResult.DatasetName");
    		assertTrue("Failed to get the newly created dataset name" , datasetsResultList.size() > 0);
    		TestSession.logger.info("dataSetsResultList  = " + datasetsResultList.toString());
    	} else if (datasetsResultList.size() > 0) {
    		originalDataSetName = datasetsResultList.get(0);
    	}
    }

    @Test
    public void testRemoveDataSet() {
        testRemovingActiveDataSet();
        testDeativateAllTargetsInDataSet();
        testRemoveTargetsFromDataSet();
        testDeactivateAndRemoveDataSet();
        testRemoveDataSource();
        testRemoveDataTargets();
    }

    /**
     * Test Scenario : Verify whether user trying to remove the ACTIVE dataset fails
     * @throws Exception
     */
    public void testRemovingActiveDataSet()  {
        createTestData();

        // activate newly created dataset
        try {
            this.consoleHandle.checkAndActivateDataSet(this.newDataSetName);
        } catch (Exception e) { 
            e.printStackTrace();
        }

        // Try removing the activate dataset
        String resource = this.jsonUtil.constructResourceNamesParameter(Arrays.asList(this.newDataSetName));
        com.jayway.restassured.response.Response res = given().cookie(cookie).param("resourceNames", resource).param("command","remove").post(this.url + "/console/rest/config/dataset/actions");

        String resString = res.asString();
        TestSession.logger.info("response after trying to remove the active dataset *****"+this.jsonUtil.formatString(resString));

        JsonPath jsonPath = new JsonPath(resString);
        String actionName = jsonPath.getString("Response.ActionName");
        String responseId = jsonPath.getString("Response.ResponseId");
        String responseMessage = jsonPath.getString("Response.ResponseMessage");
        TestSession.logger.info("actionName = "+actionName);
        TestSession.logger.info("ResponseId = "+responseId);
        TestSession.logger.info("responseMessage = "+responseMessage);
        assertTrue("Expected remove action name , but found " + actionName , actionName.equals("remove"));
        assertTrue("Expected -1, but found " + responseId , responseId.equals("-1"));
        boolean flag = responseMessage.contains(this.newDataSetName) && responseMessage.contains("failed") &&
                responseMessage.contains("Error: Configuration is active") && responseMessage.contains("Not Removed:");
        assertTrue("failed to get the correct message, but found " + responseMessage , flag == true);
    }

    /**
     * Test Scenario : Deactivate all the targets & verify whether target status are set to inactive
     */
    public void testDeativateAllTargetsInDataSet() {
        // create resource parameter
        String resource = this.jsonUtil.constructResourceNamesParameter(Arrays.asList(this.newDataSetName));
        TestSession.logger.info("resource = "+this.jsonUtil.formatString(resource));

        // create args parameter
        String args = this.jsonUtil.constructArgumentParameter(this.newDataTargetList,"deactivateTarget");
        TestSession.logger.info("args *****"+this.jsonUtil.formatString(args));

        // wait for some time, so that changes are reflected in the dataset i,e TARGETS gets INACTIVE
        this.consoleHandle.sleep(SLEEP_TIME);

        com.jayway.restassured.response.Response res = given().cookie(cookie).param("resourceNames", resource).param("command","update").param("args", args).post(this.url + "/console/rest/config/dataset/actions");   
        String resString = res.asString();
        TestSession.logger.info("response after deactivating the targets *****"+this.jsonUtil.formatString(resString));

        // check for response values
        JsonPath jsonPath = new JsonPath(resString);
        String actionName = jsonPath.getString("Response.ActionName");
        String responseId = jsonPath.getString("Response.ResponseId");
        assertTrue("Expected update action name , but found " + actionName , actionName.equals("update"));
        assertTrue("Expected 0, but found " + responseId , responseId.equals("0"));
        String responseMessage = jsonPath.getString("Response.ResponseMessage");
        boolean flag = responseMessage.contains(this.newDataSetName) && responseMessage.contains("successful");
        assertTrue("failed to get the correct message, but found " + responseMessage , flag == true);

        // wait for some time, so that changes are reflected in the dataset i,e TARGETS gets INACTIVE
        this.consoleHandle.sleep(SLEEP_TIME);

        // Check whether targets in dataset are set to INACTIVE state
        List<String>targetsStatus = this.consoleHandle.getDataSource(this.newDataSetName , "target" , "status");
        TestSession.logger.info("************ testDeativateAllTargetsInDataSet ******** = " +targetsStatus.toString() );
        for(String tarStatus : targetsStatus){
            String status[] = tarStatus.split(":");
            assertTrue("Expected that targets are inactive , but got " + tarStatus , status[1].trim().equals("inactive"));
        }

        TestSession.logger.info("******** datasetName = " + this.newDataSetName);
    }

    /*
     * Test Scenario : Verify whether user is able to remove all the targets from the dataset, except the last one
     */
    public void testRemoveTargetsFromDataSet() {

        String resource = this.jsonUtil.constructResourceNamesParameter(Arrays.asList(this.newDataSetName));
        TestSession.logger.info("resource = "+this.jsonUtil.formatString(resource));
        int size = newDataTargetList.size();

        // Since last target in the dataset can't be removed, so removing all the targets expect the last one.
        List<String>targets = newDataTargetList.subList(0, (size - 1));
        String args = this.jsonUtil.constructArgumentParameter(targets,"removeTarget");
        TestSession.logger.info("args *****"+this.jsonUtil.formatString(args));
        this.consoleHandle.sleep(SLEEP_TIME);

        com.jayway.restassured.response.Response res = given().cookie(cookie).param("resourceNames", resource).param("command","update").param("args", args)
                .post(this.url + "/console/rest/config/dataset/actions");
        String resString = res.asString();
        TestSession.logger.info("response after removing the targets *****"+this.jsonUtil.formatString(resString));

        // Check for Response values
        JsonPath jsonPath = new JsonPath(resString);
        String actionName = jsonPath.getString("Response.ActionName");
        String responseId = jsonPath.getString("Response.ResponseId");
        assertTrue("Expected update action name , but found " + actionName , actionName.equals("update"));
        
        // since last target can't be removed from dataset, trying to remove will fail.
        assertTrue("Expected -1, but found " + responseId , responseId.equals("-1"));
        String responseMessage = jsonPath.getString("Response.ResponseMessage");
        boolean flag = responseMessage.contains(this.newDataSetName);
        assertTrue("failed to get the correct message, but found " + responseMessage , flag == true);
    }

    /**
     * Test Scenario : Deactivate & remove dataset
     */
    public void testDeactivateAndRemoveDataSet() {

        // deactivate dataset
        Response response = this.consoleHandle.deactivateDataSet(this.newDataSetName);
        assertTrue("Failed to deactivate the dataset " +this.newDataSetName , response.getStatusCode() == SUCCESS);

        // wait for some time, so that changes are reflected in the dataset specification file i,e active to inactive
        this.consoleHandle.sleep(SLEEP_TIME);

        String resource = this.jsonUtil.constructResourceNamesParameter(Arrays.asList(this.newDataSetName));
        TestSession.logger.info("resource = "+this.jsonUtil.formatString(resource));

        // remove the dataset
        com.jayway.restassured.response.Response res = given().cookie(this.cookie).param("resourceNames", resource).param("command","remove")
                .post(this.url + "/console/rest/config/dataset/actions");

        String resString = res.asString();
        TestSession.logger.info("response *****"+this.jsonUtil.formatString(resString));

        // Check for Response
        JsonPath jsonPath = new JsonPath(resString);
        String actionName = jsonPath.getString("Response.ActionName");
        String responseId = jsonPath.getString("Response.ResponseId");
        assertTrue("Expected remove action, but got " + actionName , actionName.equals("remove"));
        assertTrue("Expected 0, but found " + responseId , responseId.equals("0"));
        String responseMessage = jsonPath.getString("Response.ResponseMessage");
        boolean flag = responseMessage.contains(this.newDataSetName) && responseMessage.contains("successful");
        assertTrue("failed to get the correct message, but found " + responseMessage , flag == true);
    }

    /**
     * Test Scenario : Remove datasource, if there is no dataset reference for this datasource
     */
    public void testRemoveDataSource() {

        String resource = this.jsonUtil.constructResourceNamesParameter(this.newDataSourceList);

        // Deactivate datasource
        com.jayway.restassured.response.Response res = given().cookie(this.cookie).param("resourceNames", resource).param("command","terminate")
                .post(this.url + "/console/rest/config/datasource/actions");

        String resString = res.asString();
        TestSession.logger.info("response *****"+this.jsonUtil.formatString(resString));

        // Check for Response
        JsonPath jsonPath = new JsonPath(resString);
        String actionName = jsonPath.getString("Response.ActionName");
        String responseId = jsonPath.getString("Response.ResponseId");
        assertTrue("Expected terminate keyword, but got " + actionName , actionName.equals("terminate"));
        assertTrue("Expected 0, but found " + responseId , responseId.equals("0"));
        String responseMessage = jsonPath.getString("Response.ResponseMessage");
        boolean flag = responseMessage.contains(this.newDataSourceList.get(0).trim()) && responseMessage.contains("successful");
        assertTrue("failed to get the correct message, but found " + responseMessage , flag == true);

        // wait for some time, so that changes are reflected to the datasource specification file i,e active to inactive
        this.consoleHandle.sleep(SLEEP_TIME);

        // remove datasource
        res = given().cookie(this.cookie).param("resourceNames", resource).param("command","remove")
                .post(this.url + "/console/rest/config/datasource/actions");

        resString = res.asString();
        TestSession.logger.info("response *****"+this.jsonUtil.formatString(resString));

        // Check for Response
        jsonPath = new JsonPath(resString);
        actionName = jsonPath.getString("Response.ActionName");
        responseId = jsonPath.getString("Response.ResponseId");
        assertTrue("Expected terminate keyword, but got " + actionName , actionName.equals("remove"));
        assertTrue("Expected 0, but found " + responseId , responseId.equals("0"));
        responseMessage = jsonPath.getString("Response.ResponseMessage");
        flag = responseMessage.contains(this.newDataSourceList.get(0).trim()) && responseMessage.contains("successful");
        assertTrue("failed to get the correct message, but found " + responseMessage , flag == true);
    }

    public void testRemoveDataTargets() {

        String resource = this.jsonUtil.constructResourceNamesParameter(newDataTargetList);
        TestSession.logger.info("response *****"+this.jsonUtil.formatString(resource));

        // deactivate datasource
        com.jayway.restassured.response.Response res = given().cookie(this.cookie).param("resourceNames", resource).param("command","terminate")
                .post(this.url + "/console/rest/config/datasource/actions");
        String resString = res.asString();
        TestSession.logger.info("response *****"+this.jsonUtil.formatString(resString));

        // wait for some time, so that changes are reflected to the datasource specification file i,e active to inactive
        this.consoleHandle.sleep(SLEEP_TIME);

        // remove datasource
        res = given().cookie(this.cookie).param("resourceNames", resource).param("command","remove")
                .post(this.url + "/console/rest/config/datasource/actions");
        resString = res.asString();
        TestSession.logger.info("response *****"+this.jsonUtil.formatString(resString));

        // Check for Response
        JsonPath jsonPath = new JsonPath(resString);
        String actionName = jsonPath.getString("Response.ActionName");
        String responseId = jsonPath.getString("Response.ResponseId");
        assertTrue("Expected terminate keyword, but got " + actionName , actionName.equals("remove"));
        assertTrue("Expected 0, but found " + responseId , responseId.equals("0"));
    }

    /**
     * Clone a dataset and the data source
     * @param dataSourceName : datasource name
     * @param sourceType - either the target or source
     */
    private void cloneDataSource(String dataSourceName , String sourceType) {
        String newSourceName = "gdm_source_" + System.currentTimeMillis();
        String newTargetName = "gdm_target_" + System.currentTimeMillis();

        if (sourceType.equals("source")) {
            String xml = this.consoleHandle.getDataSourceXml(dataSourceName);
            hadooptest.cluster.gdm.Response response = this.consoleHandle.createDataSource(dataSourceName , newSourceName , xml );
            assertTrue("Failed to create a DataSource specification " + newSourceName , response.getStatusCode() == SUCCESS);

            this.consoleHandle.sleep(SLEEP_TIME);

            // If new source is created successfully, add the name of the source to the list
            newDataSourceList.add(newSourceName);

        } else if (sourceType.equals("target")) {
            String xml = this.consoleHandle.getDataSourceXml(dataSourceName);
            hadooptest.cluster.gdm.Response response = this.consoleHandle.createDataSource(dataSourceName , newTargetName , xml );
            assertTrue("Failed to create a DataSource specification " + newTargetName , response.getStatusCode() == SUCCESS);

            this.consoleHandle.sleep(SLEEP_TIME);

            // If new target is created successfully add the name of the target to the list
            newDataTargetList.add(newTargetName);
        }
    }

    /*
     * Method to create a dataset, actually this method should be part of setup method, but since
     * setUp method is annoated with @Before and it will be executed each time for when a new test method is
     * invoked.
     */
    private void createTestData() {
        dataSourceList = this.consoleHandle.getDataSource(originalDataSetName , "source" , "name" );
        TestSession.logger.info("sourceName = "+dataSourceList.get(0));
        dataTargetList = this.consoleHandle.getDataSource(originalDataSetName, "target" ,"name");
        for (String s : dataTargetList) {
            TestSession.logger.info(s);
        }

        // clone data source
        String oldSourceName = dataSourceList.get(0);
        cloneDataSource(oldSourceName, "source");

        // clone targets
        for (int i=0; i< dataTargetList.size(); i++) {
            String oldTargetName = dataTargetList.get(i).trim();
            cloneDataSource(oldTargetName, "target");
            this.consoleHandle.sleep(SLEEP_TIME);
        }

        // Read dataset and replace source and target values
        String dataSetXml = this.consoleHandle.getDataSetXml(originalDataSetName);

        // Replace the dataset name
        this.newDataSetName = "Test-Remove-Dataset_" + System.currentTimeMillis();
        dataSetXml = dataSetXml.replaceAll(originalDataSetName, this.newDataSetName);

        // Replace the original datasource name with the new datasource name
        dataSetXml = dataSetXml.replaceAll(oldSourceName, newDataSourceList.get(0));

        // replace all the old target name with the new target name
        for (int i=0; i< dataTargetList.size() ; i++) {
            String oldTargetName = dataTargetList.get(i).trim();
            dataSetXml = dataSetXml.replaceAll(oldTargetName, newDataTargetList.get(i));
        }
        TestSession.logger.info("newDataSetXml = "+dataSetXml);
        
        this.consoleHandle.sleep(SLEEP_TIME);

        // Create a new dataset
        Response response = this.consoleHandle.createDataSet(this.newDataSetName, dataSetXml);
        assertTrue("Failed to create a dataset " +this. newDataSetName , response.getStatusCode() == SUCCESS);
    }
    
    /**
     * Create a dataset.
     */
    private void createDataset() {
    	String basePath = "/data/daqdev/" + this.newDataSetName + "/data/%{date}";
    	String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/BasicReplDataSet1Target.xml");
    	String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.newDataSetName, dataSetConfigFile);
    	dataSetXml = dataSetXml.replaceAll("TARGET", this.target);
    	dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", this.newDataSetName);
    	dataSetXml = dataSetXml.replaceAll("SOURCE", this.sourceGrid );
    	dataSetXml = dataSetXml.replaceAll("START_TYPE", "fixed" );
    	dataSetXml = dataSetXml.replaceAll("START_DATE", INSTANCE1);
    	dataSetXml = dataSetXml.replaceAll("END_TYPE", "offset");
    	dataSetXml = dataSetXml.replaceAll("END_DATE", "0");
    	dataSetXml = dataSetXml.replaceAll("REPLICATION_DATA_PATH", basePath);
    	dataSetXml = dataSetXml.replaceAll("CUSTOM_PATH_DATA", basePath);
    	hadooptest.cluster.gdm.Response response = this.consoleHandle.createDataSet(this.newDataSetName, dataSetXml);
    	if (response.getStatusCode() != org.apache.commons.httpclient.HttpStatus.SC_OK) {
    		Assert.fail("Response status code is " + response.getStatusCode() + ", expected 200.");
    	}

    	this.consoleHandle.sleep(30000);
    }

}
