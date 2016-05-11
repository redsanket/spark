// Copyright 2016, Yahoo Inc.
package hadooptest.gdm.regression.api;

import static org.junit.Assert.assertEquals;
import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertTrue;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.cluster.gdm.HTTPHandle;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;


/**
 * Test Scenario :  Verify whether user can search for the dataset based on the data owner.
 * Bug id : http://bug.corp.yahoo.com/show_bug.cgi?id=7149819
 * Bug description :  Provide user the ability to search dataset by owner
 *
 */
public class TestSearchDataSetByOwner extends TestSession {

    private ConsoleHandle consoleHandle;
    private List<String> hcatSupportedGrid;
    private String dataSetName;
    private String targetGrid1;
    private String cookie;
    private String datasetActivationTime;
    private String baseDataSetName = "VerifyAcqRepRetWorkFlowExecutionSingleDate";
    private WorkFlowHelper workFlowHelper;
    private static final String ACQ_HCAT_TYPE = "DataOnly";

    private static final String GROUP_NAME = "aporeport";
    private static final String DATA_OWNER = "apollog";
    private static final int SUCCESS = 200;

    @BeforeClass
    public static void startTestSession() throws Exception {
        TestSession.start();
    }

    @Before
    public void setUp() { 
        this.consoleHandle = new ConsoleHandle();
        this.workFlowHelper = new WorkFlowHelper();
        List<String> grids = this.consoleHandle.getUniqueGrids();
        boolean flag =  (grids != null && grids.size() > 0);
        if ( flag == true) {
            this.targetGrid1 = grids.get(0);
        } else {
            assertTrue("Expected atleast one grid to be installed. " , flag == true);   
        }   
        
        HTTPHandle httpHandle = new HTTPHandle();
        this.cookie = httpHandle.getBouncerCookie();
    }

    @Test
    public void testSearchDataSetByOwner() throws Exception {
        this.dataSetName = "Test_SearchDataSetByOwner_" + System.currentTimeMillis();;
        createAquisitionDataSet("DoAsAcquisitionDataSet.xml");
        
        // activate the dataset
        this.consoleHandle.checkAndActivateDataSet(this.dataSetName);
        this.datasetActivationTime = GdmUtils.getCalendarAsString();

        // check for acquisition workflow 
        this.workFlowHelper.checkWorkFlow(this.dataSetName , "acquisition" , this.datasetActivationTime );
        
        // construct the REST API url
        String url = this.consoleHandle.getConsoleURL() + "/console/api/datasets/view?checkverify=true&owner=" + this.DATA_OWNER;
        TestSession.logger.info("Test URL = " + url);
        
        // invoke the REST API
        com.jayway.restassured.response.Response response = given().cookie(this.cookie).get(url);
        List<String> dataSetNames = response.getBody().jsonPath().getList("DatasetsResult.DatasetName");
        assertTrue("Expected to contain " + this.dataSetName + " dataset, but looks like it d't exist in " + dataSetNames , dataSetNames.contains(this.dataSetName) == true);
    }

    /**
     * creates a acquisition dataset to test DataOnly HCatTargetType
     * @param dataSetFileName - name of the acquisition dataset
     */
    private void createAquisitionDataSet( String dataSetFileName) {
        String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/" + dataSetFileName);
        String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.dataSetName, dataSetConfigFile);
        String feedName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Parameters" , "value");
        String sourceName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Sources" , "name");

        dataSetXml = dataSetXml.replaceAll("TARGET1", this.targetGrid1 );
        dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", this.dataSetName);
        dataSetXml = dataSetXml.replaceAll("FEED_NAME", feedName );
        dataSetXml = dataSetXml.replaceAll("FEED_STATS", feedName + "_stats" );
        dataSetXml = dataSetXml.replaceAll("FDI_SERVER_NAME", sourceName );
        dataSetXml = dataSetXml.replace("HCAT_TYPE", this.ACQ_HCAT_TYPE);
        dataSetXml = dataSetXml.replace("GROUP_NAME" , this.GROUP_NAME);
        dataSetXml = dataSetXml.replace("DATA_OWNER" , this.DATA_OWNER);
        dataSetXml = dataSetXml.replaceAll("CUSTOM_DATA_PATH", getCustomPath("data", this.dataSetName) );
        dataSetXml = dataSetXml.replaceAll("CUSTOM_COUNT_PATH", getCustomPath("count", this.dataSetName) );
        dataSetXml = dataSetXml.replaceAll("CUSTOM_SCHEMA_PATH", getCustomPath("schema", this.dataSetName));
        dataSetXml = dataSetXml.replaceAll("HCAT_TABLE_NAME", this.dataSetName);

        Response response = this.consoleHandle.createDataSet(this.dataSetName, dataSetXml);
        if (response.getStatusCode() != SUCCESS) {
            try {
                throw new Exception("Response status code is " + response.getStatusCode() + ", expected 200.");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        this.consoleHandle.sleep(5000);
    }

    /**
     * Method to create a custom path for the dataset.
     * @param pathType - its a string type which represents either data/count/schema
     * @param dataSet - dataset name
     * @return
     */
    private String getCustomPath(String pathType , String dataSet) {
        return  "/data/daqdev/test-hcat/"+ pathType +"/"+ dataSet + "/%{date}";
    }
    
    /**
     * Method to deactivate the dataset(s)
     */
    @After
    public void tearDown() throws Exception {
            Response response = consoleHandle.deactivateDataSet(this.dataSetName);
            assertEquals("ResponseCode - Deactivate DataSet", 200, response.getStatusCode());
            assertEquals("ActionName.", "terminate", response.getElementAtPath("/Response/ActionName").toString());
            assertEquals("ResponseId", "0", response.getElementAtPath("/Response/ResponseId").toString());
            assertEquals("ResponseMessage.", "Operation on " + this.dataSetName + " was successful.", response.getElementAtPath("/Response/ResponseMessage/[0]").toString());
    }
}
