// Copyright 2016, Yahoo Inc.
package hadooptest.gdm.regression.doAs;

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
import java.util.Iterator;
import java.util.List;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test Scenario : Verify whether CUSTOM working directory and custom eviction is created.
 */
public class TestDoAsFacetWorkFlowWithCustomWorkingAndCustomEvictionDirectory extends TestSession {

    private ConsoleHandle consoleHandle;
    private HTTPHandle httpHandle ;
    private String acqDataSetName;
    private String retDataSetName;

    private String cookie;
    private String url;
    private String datasetActivationTime;
    private String baseDataSetName = "VerifyAcqRepRetWorkFlowExecutionSingleDate";

    private String targetGrid1;
    private String targetGrid2;
    private List<String> dataSetNames;
    private List<String> dataSourceNames;
    private String acqDataSourceName;

    private static final int SUCCESS = 200;
    private static final String GROUP_NAME = "jaggrp";
    private static final String DATA_OWNER = "jagpip";
    private String workingDirPath;
    private String evictionDirPath;
    private static final String CUSTOM_WORKING_PATH = "/user/daqload/GDM-CUSTOM-Working-Dir/";
    private static final String CUSTOM_EVICTION_PATH  = "/user/daqload/GDM-CUSTOM-Eviction-DIR/";
    private static final String HCAT_TYPE = "DataOnly";
    private WorkFlowHelper helper = null;
    private static final String DATABASE_NAME = "gdm";
    private static final String PATH = "/data/daqdev/";
    private String targetGrid1_NameNode;
    private String targetGrid2_NameNode;
    private List<String> grids = new ArrayList<String>();

    @BeforeClass
    public static void startTestSession() {
        TestSession.start();
    }

    @Before
    public void setup() throws Exception {
        this.consoleHandle = new ConsoleHandle();
        this.httpHandle = new HTTPHandle();
        this.url = this.consoleHandle.getConsoleURL();
        dataSetNames = new ArrayList<String>();
        dataSourceNames = new ArrayList<String>();
        helper = new WorkFlowHelper();
        this.cookie = httpHandle.getBouncerCookie();
        
        this.grids = this.consoleHandle.getAllGridNames();
        TestSession.logger.info("Grids = " + grids);
        
        // check whether secure cluster exists
        if (this.grids.size() > 2 ) {
            this.targetGrid1 = this.grids.get(0);
            this.targetGrid2 = this.grids.get(1);
        } else {
            fail("There are only " + grids.size() + " grids; need at least two to run tests.");
        }

        // Get namenode name of target cluster
        this.targetGrid1_NameNode = this.consoleHandle.getClusterNameNodeName(this.targetGrid1);
        this.targetGrid2_NameNode = this.consoleHandle.getClusterNameNodeName(this.targetGrid2);
        
        // check and change the group, owner and permission if they did n't meet the following requirement
        // Permission should be 777 for the destination path, group = users and owner = dfsload
        this.helper.checkAndSetPermision(this.targetGrid1_NameNode, this.PATH); 
        this.helper.checkAndSetPermision(this.targetGrid2_NameNode, this.PATH);
    }

    @Test
    public void testCustomWorkingAndEvictionDirectory() throws Exception {

            // Acquisition workflow
            {
                this.workingDirPath = CUSTOM_WORKING_PATH + "workingDir" + "_" + System.currentTimeMillis();
                this.evictionDirPath = CUSTOM_EVICTION_PATH + "evictionDir" + "_" + System.currentTimeMillis();
                this.acqDataSourceName = this.targetGrid1 + "_AcqCustomWorkingDir_" + System.currentTimeMillis();
                this.acqDataSetName = "TestDoAsAcquisition_CustomWorkingDir_" + System.currentTimeMillis();
                
                this.createDataSource(this.targetGrid1 , this.acqDataSourceName);
                this.dataSourceNames.add(this.acqDataSourceName);

                // create doAsAcquisition dataset.
                createDoAsAquisitionDataSet("DoAsAcquisitionDataSet.xml");
                
                // activate the dataset
                this.consoleHandle.checkAndActivateDataSet(this.acqDataSetName);
                this.datasetActivationTime = GdmUtils.getCalendarAsString();

                // add acquisition dataset name to list
                dataSetNames.add(this.acqDataSetName);

                // check for acquisition workflow
                this.helper.checkWorkFlow(this.acqDataSetName, "acquisition", this.datasetActivationTime);
                
                // Acquisition workflow to check whether custom working directory is created or not.
                checkForCustomPath(this.workingDirPath);
            }
            
            // Retention workflow 
            {
                this.retDataSetName = "TestDoAsRetention_CustomEvictionDir_" + System.currentTimeMillis();

                // create a datasource for each target
                String rentionDataSourceForTarget1 = this.targetGrid1 +"_DoAsCustomEvicDirDataSource_"  + System.currentTimeMillis();
                TestSession.logger.info("*****************  rentionDataSourceForTarget1  = " + rentionDataSourceForTarget1);
                
                createDataSourceForEachRetentionJob(this.targetGrid1 , rentionDataSourceForTarget1);
                this.dataSourceNames.add(rentionDataSourceForTarget1);
                
                this.consoleHandle.sleep(50000);
                createDoAsRetentionDataSet("DoAsRetentionDataSet.xml" , rentionDataSourceForTarget1 );

                // activate the dataset
                this.consoleHandle.checkAndActivateDataSet(this.retDataSetName);
                this.datasetActivationTime = GdmUtils.getCalendarAsString();

                // add retention dataset name to list
                dataSetNames.add(this.retDataSetName);

                // check for replication workflow
                this.helper.checkWorkFlow(this.retDataSetName, "retention", this.datasetActivationTime);
                
                // Retention workflow to check whether custom eviction directory is created or not
                checkForCustomPath(this.evictionDirPath);
            }
    }
    
    /**
     * Create DataSource for each target, in order to avoid target collision
     * @param DataSourceName existing target datasource
     * @param newDataSourceName - new datasource name
     */
    public void createDataSourceForEachRetentionJob(String dataSourceName , String newDataSourceName) {
        String dataSourceXMLContent = this.consoleHandle.getDataSourceXml(this.targetGrid1);
        
        String xml = this.consoleHandle.getDataSourceXml(dataSourceName);
        xml = xml.replaceFirst(dataSourceName,newDataSourceName);
        
        // change the default eviction directory to custom eviction directory
        xml = xml.replaceFirst("/user/daqload/daqtest/todelete1", this.evictionDirPath);
        boolean isDataSourceCreated = this.consoleHandle.createDataSource(xml);
        assertTrue("Failed to create a DataSource specification " + newDataSourceName , isDataSourceCreated == true);
    }
    
    /**
     * Check whether working and eviction directory are created after the workflow.
     * working directory is created after acquisition or replication workflow.
     * eviction directory is created after retention workflow.
     * @param path - string representing either working or eviction directory.
     */
    public void checkForCustomPath(String customPath) {
        boolean isCustomWorkingDirExists = false ,  owner = false, group = false, permission = false;
        String path = null;

        // check whether custom working directory is really created & check for group and owner.
        String hadoopLSURL =  this.url + "/console/api/admin/hadoopls?dataSource=" + this.targetGrid1 + "&path=" + customPath + "&format=json";
        TestSession.logger.info("CUSTOM_WORKING_DIR  = " + customPath);
        TestSession.logger.info("testURL = " + hadoopLSURL);
        com.jayway.restassured.response.Response response = given().cookie( this.cookie).get(hadoopLSURL);
        assertTrue("Failed to get the respons = " + hadoopLSURL , (response != null || response.toString() != "") );
        
        JSONArray hadoopLSJsonArray = this.consoleHandle.convertResponseToJSONArray(response, "Files");
        if (hadoopLSJsonArray.size() > 0) {
            Iterator iterator = hadoopLSJsonArray.iterator();
            while (iterator.hasNext()) {
                JSONObject hadoopLSJsonObject = (JSONObject) iterator.next();
                path = hadoopLSJsonObject.getString("Path");
                boolean flag = path.contains(customPath);
                if (flag == true ) {
                    isCustomWorkingDirExists = true;
                    if (hadoopLSJsonObject.getString("Owner").equals("dfsload") ) {
                        owner = true;
                    }
                    if (hadoopLSJsonObject.getString("Group").equals("users") ) {
                        group = true;
                    }
                    break;
                }
            }
            // check for path created, owner & group  values.
            assertTrue("Failed to create the working directory, got " + path , isCustomWorkingDirExists == true );
            assertTrue("Failed to create the working directory with specified owner , expected "+ this.DATA_OWNER + " but got " + owner , owner == true );
            assertTrue("Failed to create the working directory with specified group , expected "+ this.GROUP_NAME + " but got " + group , group == true );
        } else if (hadoopLSJsonArray.size() == 0) {
            fail("Failed to get response for hadoopLS REST API : " + hadoopLSURL);
        }
    }

    /**
     * Create DataSource for each target, in order to avoid target collision
     * @param DataSourceName existing target datasource
     * @param newDataSourceName - new datasource name
     */
    public void createDataSource(String dataSourceName , String newDataSourceName) {        
        String xml = this.consoleHandle.getDataSourceXml(dataSourceName);
        xml = xml.replaceFirst(dataSourceName,newDataSourceName);
        
        //change the default eviction directory to custom eviction directory
        xml = xml.replaceFirst("/user/daqload/daqtest/tmp1", this.workingDirPath );
        
        boolean isDataSourceCreated = this.consoleHandle.createDataSource(xml);
        assertTrue("Failed to create a DataSource specification " + newDataSourceName , isDataSourceCreated == true);
        this.consoleHandle.sleep(40000);
    }

    /**
     * Method that creates a acquisition dataset
     * @param dataSetFileName - name of the acquisition dataset
     */
    private void createDoAsAquisitionDataSet( String dataSetFileName) {
        String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/" + dataSetFileName);
        String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.acqDataSetName, dataSetConfigFile);
        String feedName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Parameters" , "value");
        String sourceName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Sources" , "name");

        dataSetXml = dataSetXml.replaceAll("TARGET1", this.acqDataSourceName );
        dataSetXml = dataSetXml.replaceAll("GROUP_NAME", this.GROUP_NAME);
        dataSetXml = dataSetXml.replaceAll("DATA_OWNER", this.DATA_OWNER);
        dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", this.acqDataSetName);
        dataSetXml = dataSetXml.replaceAll("FEED_NAME", feedName );
        dataSetXml = dataSetXml.replaceAll("FEED_STATS", feedName + "_stats" );
        dataSetXml = dataSetXml.replaceAll("FDI_SERVER_NAME", sourceName );
        dataSetXml = dataSetXml.replace("HCAT_TYPE", this.HCAT_TYPE);
        dataSetXml = dataSetXml.replaceAll("CUSTOM_DATA_PATH", getCustomPath("data", this.acqDataSetName) );
        dataSetXml = dataSetXml.replaceAll("CUSTOM_COUNT_PATH", getCustomPath("count", this.acqDataSetName) );
        dataSetXml = dataSetXml.replaceAll("CUSTOM_SCHEMA_PATH", getCustomPath("schema", this.acqDataSetName));
        dataSetXml = dataSetXml.replaceAll("HCAT_TABLE_NAME", this.acqDataSetName);
        dataSetXml = dataSetXml.replaceAll("DATABASE_NAME" , this.DATABASE_NAME );
        
        Response response = this.consoleHandle.createDataSet(this.acqDataSetName, dataSetXml);
        if (response.getStatusCode() != SUCCESS) {
            try {
                fail("Failed to create dataset - " + this.acqDataSetName);
                throw new Exception("Response status code is " + response.getStatusCode() + ", expected 200.");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        this.consoleHandle.sleep(5000);
    }

    /**
     * Method that creates a retention dataset
     * @param dataSetFileName - name of the retention dataset
     */
    private void createDoAsRetentionDataSet(String dataSetFileName , String newTargetName1 ) {
        
        String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/" + dataSetFileName);
        String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.retDataSetName, dataSetConfigFile);
        String feedName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Parameters" , "value");

        dataSetXml = dataSetXml.replaceAll("TARGET1", newTargetName1 );
        dataSetXml = dataSetXml.replaceAll("TARGET2", this.targetGrid2 );
        dataSetXml = dataSetXml.replaceAll("GROUP_NAME", this.GROUP_NAME);
        dataSetXml = dataSetXml.replaceAll("DATA_OWNER", this.DATA_OWNER);
        dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", this.retDataSetName);
        dataSetXml = dataSetXml.replaceAll("FEED_NAME", feedName );
        dataSetXml = dataSetXml.replaceAll("FEED_STATS", feedName + "_stats" );
        dataSetXml = dataSetXml.replace("HCAT_TYPE", this.HCAT_TYPE);
        dataSetXml = dataSetXml.replaceAll("HCAT_TABLE_NAME", this.acqDataSetName);

        dataSetXml = dataSetXml.replaceAll("ACQ_CUSTOM_DATA_PATH", getCustomPath("data" , this.acqDataSetName) );
        dataSetXml = dataSetXml.replaceAll("ACQ_CUSTOM_COUNT_PATH", getCustomPath("count" , this.acqDataSetName) );
        dataSetXml = dataSetXml.replaceAll("ACQ_CUSTOM_SCHEMA_PATH", getCustomPath("schema" , this.acqDataSetName));
        
        dataSetXml = dataSetXml.replaceAll("REP_CUSTOM_DATA_PATH", getCustomPath("data", this.acqDataSetName) ); 
        dataSetXml = dataSetXml.replaceAll("REP_CUSTOM_COUNT_PATH", getCustomPath("count", this.acqDataSetName) ); 
        dataSetXml = dataSetXml.replaceAll("REP_CUSTOM_SCHEMA_PATH", getCustomPath("schema", this.acqDataSetName) ); 
        
        dataSetXml = dataSetXml.replaceAll("DATABASE_NAME" , this.DATABASE_NAME );

        Response response = this.consoleHandle.createDataSet(this.retDataSetName, dataSetXml);
        if (response.getStatusCode() != SUCCESS) {
            try {
                fail("Failed to create dataset - " + this.retDataSetName);
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
        return  "/data/daqdev/"+ pathType +"/"+ dataSet + "/%{date}";
    }

    /**
     * Method to deactivate the dataset(s)
     */
    @After
    public void tearDown() throws Exception {
        List<String> datasetList =  consoleHandle.getAllDataSetName();

        // Deactivate all three facet datasets
        for ( String datasetName : dataSetNames)  {
            if (datasetList.contains(datasetName)) {
                Response response = consoleHandle.deactivateDataSet(datasetName);
                assertEquals("ResponseCode - Deactivate DataSet", 200, response.getStatusCode());
                assertEquals("ActionName.", "terminate", response.getElementAtPath("/Response/ActionName").toString());
                assertEquals("ResponseId", "0", response.getElementAtPath("/Response/ResponseId").toString());
                assertEquals("ResponseMessage.", "Operation on " + datasetName + " was successful.", response.getElementAtPath("/Response/ResponseMessage/[0]").toString());
                
                // remove dataset
                this.consoleHandle.removeDataSet(datasetName);
            } else {
                TestSession.logger.info(datasetName + " does not exist.");
            }
        }
        
        for( String dataSourceName : this.dataSourceNames) {
            this.consoleHandle.removeDataSource(dataSourceName);
        }
    }
}
