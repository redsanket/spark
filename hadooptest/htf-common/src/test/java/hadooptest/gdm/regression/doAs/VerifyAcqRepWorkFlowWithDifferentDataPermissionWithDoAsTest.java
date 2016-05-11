// Copyright 2016, Yahoo Inc.
package hadooptest.gdm.regression.doAs;

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
import java.util.List;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * Test Scenario : Verify whether workflow on all facets works as expected when 
 *                 dataset is created with different permissions.
 *
 */
public class VerifyAcqRepWorkFlowWithDifferentDataPermissionWithDoAsTest extends TestSession {

    private ConsoleHandle consoleHandle;
    private HTTPHandle httpHandle ;
    private String acqDataSetName;
    private String repDataSetName;
    private String retDataSetName;
    private String cookie;
    private String datasetActivationTime;
    private String baseDataSetName = "VerifyAcqRepRetWorkFlowExecutionSingleDate";
    private String targetGrid1;
    private String targetGrid2;
    private WorkFlowHelper helper = null;
    private List<String> permissionList;
    private List<String> dataSetNames;
    private List<String> dataSourceNames;
    private List<String> grids = new ArrayList<String>();
    private String targetGrid1_NameNode;
    private String targetGrid2_NameNode;
    
    private static final int SUCCESS = 200;
    private static final String GROUP_NAME = "jaggrp";
    private static final String DATA_OWNER = "jagpip";
    private static final String HCAT_TYPE = "DataOnly";
    private static final String HCAT_ENABLED = "FALSE";
    private static final String PATH = "/data/daqdev/";

    @BeforeClass
    public static void startTestSession() {
        TestSession.start();
    }

    @Before
    public void setup() throws Exception {
        this.consoleHandle = new ConsoleHandle();
        this.httpHandle = new HTTPHandle();
        this.cookie = this.httpHandle.getBouncerCookie();

        List<String> tempGrids = this.consoleHandle.getUniqueGrids();
        // remove target that starting with gdm-target
        for ( String gridName : tempGrids) {
            if ( !gridName.startsWith("gdm-target") )
                grids.add(gridName);
        }

        // check whether secure cluster exists
        if (grids.size() >= 2 ) {
            this.targetGrid1 = grids.get(0);
            this.targetGrid2 = grids.get(1);
        } else {
            fail("There are only " + grids.size() + " grids; need at least two to run tests. ");
        }
        dataSetNames = new ArrayList<String>();
        helper = new WorkFlowHelper();
        dataSourceNames = new ArrayList<String>();

        permissionList = new ArrayList<String>();
        permissionList.add("755");
        permissionList.add("750");
        permissionList.add("700");
        
        // Get namenode name of target cluster 
        this.targetGrid1_NameNode = this.consoleHandle.getClusterNameNodeName(this.targetGrid1);
        this.targetGrid2_NameNode = this.consoleHandle.getClusterNameNodeName(this.targetGrid2);
        
        // check and change the group, owner and permission if they did n't meet the following requirement
        // Permission should be 777 for the destination path, group = users and owner = dfsload
        this.helper.checkAndSetPermision(this.targetGrid1_NameNode, this.PATH); 
        this.helper.checkAndSetPermision(this.targetGrid2_NameNode, this.PATH);
    }
    
    @Test
    public void testDoAcqAndRep() throws Exception {

        for (String permission : permissionList) {

            // Acquisition 
            {
                this.acqDataSetName = "TestDoAsAcquisitionWorkFlow_Permission_" + permission + "_" + System.currentTimeMillis();

                // create doAsAcquisition dataset.
                createDoAsAquisitionDataSet("DoAsAcquisitionDataSet.xml" , permission);

                // activate the dataset
                this.consoleHandle.checkAndActivateDataSet(this.acqDataSetName);
                this.datasetActivationTime = GdmUtils.getCalendarAsString();

                // add acquisition dataset name to list
                dataSetNames.add(this.acqDataSetName);

                // check for acquisition workflow
                this.helper.checkWorkFlow(this.acqDataSetName, "acquisition", this.datasetActivationTime);

                // check for instance files, owner and groupd on HDFS after completion of acquisition workflow 
                checkInstanceFilesAndItsOwnerAndGroup(this.targetGrid1 , "/data/daqdev/data/" + this.acqDataSetName);

            }

            // Replication
            {
                this.repDataSetName =  "TestDoAsReplicationWorkFlow_Permission_" + permission + "_" + System.currentTimeMillis();

                // create replication dataset
                createDoAsReplicationDataSet("DoAsReplicationDataSet.xml");

                // activate the dataset
                this.consoleHandle.checkAndActivateDataSet(this.repDataSetName);
                this.datasetActivationTime = GdmUtils.getCalendarAsString();

                // add replication dataset name to list
                dataSetNames.add(this.repDataSetName);

                // check for replication workflow
                this.helper.checkWorkFlow(this.repDataSetName, "replication", this.datasetActivationTime);

                // check for instance files, owner and groupd on HDFS after completion of acquisition workflow 
                checkInstanceFilesAndItsOwnerAndGroup(this.targetGrid2 , "/data/daqdev/data/" + this.repDataSetName);
            }

            // Retention
            {               
                this.consoleHandle.setRetentionPolicyToAllDataSets(this.acqDataSetName, "0");
                this.consoleHandle.setRetentionPolicyToAllDataSets(this.repDataSetName, "0");
                this.consoleHandle.checkAndActivateDataSet(this.acqDataSetName);
                this.consoleHandle.checkAndActivateDataSet(this.repDataSetName);
                this.datasetActivationTime = GdmUtils.getCalendarAsString();
                
                // check for replication workflow
                this.helper.checkWorkFlow(this.acqDataSetName, "retention", this.datasetActivationTime);
                this.helper.checkWorkFlow(this.repDataSetName, "retention", this.datasetActivationTime);
                
            }
        }
    }

    /**
     * Check whether instance files exists  for a given path on a given datasource
     * @param dataSourceName  - name of the target where the instance files exists
     * @param dataPath - path of the instance files
     */
    public void checkInstanceFilesAndItsOwnerAndGroup(String dataSourceName, String dataPath) {
        //  check for instance files owner and group name
        JSONArray filesJSONArray = this.consoleHandle.getDataSetInstanceFilesDetailsByPath(dataSourceName, dataPath);
        
        if (filesJSONArray.size() > 0) {
            for (int i = 0 ; i <filesJSONArray.size() ; i++) {
                JSONObject jsonObject1 = filesJSONArray.getJSONObject(i);
                String path = jsonObject1.getString("Path");
                TestSession.logger.info(" Path = " + path);

                // check for dataset is the part of the path
                assertTrue("expected " +  dataPath + " to be the part  "+ path +" , but found " + path , path.contains(dataPath) == true);

                String owner = jsonObject1.getString("Owner").trim();
                assertTrue("Expected " + this.DATA_OWNER + " but got  " + owner , owner.equals(this.DATA_OWNER));

                String group = jsonObject1.getString("Group").trim();
                assertTrue("Expected " + this.GROUP_NAME + " but got  " + group , group.equals(this.GROUP_NAME));
            }
        } else {
            fail("Failed to get instance files details on " + this.targetGrid1 + " with path = " + dataPath + " got size = " + filesJSONArray.size() );
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
        boolean isDataSourceCreated = this.consoleHandle.createDataSource(xml);
        assertTrue("Failed to create a DataSource specification " + newDataSourceName , isDataSourceCreated == true);
    }

    /**
     * Method that creates a acquisition dataset
     * @param dataSetFileName - name of the acquisition dataset
     */
    private void createDoAsAquisitionDataSet( String dataSetFileName , String permission ) {
        String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/" + dataSetFileName);
        String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.acqDataSetName, dataSetConfigFile);
        String feedName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Parameters" , "value");
        String sourceName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Sources" , "name");

        // set permission
        dataSetXml = dataSetXml.replaceAll("755", permission );

        dataSetXml = dataSetXml.replaceAll("TARGET1", this.targetGrid1 );
        //dataSetXml = dataSetXml.replaceAll("TARGET2", this.targetGrid2 );
        dataSetXml = dataSetXml.replaceAll("GROUP_NAME", this.GROUP_NAME);
        dataSetXml = dataSetXml.replaceAll("DATA_OWNER", this.DATA_OWNER);
        dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", this.acqDataSetName);
        dataSetXml = dataSetXml.replaceAll("FEED_NAME", feedName );
        dataSetXml = dataSetXml.replaceAll("FEED_STATS", feedName + "_stats" );
        dataSetXml = dataSetXml.replaceAll("FDI_SERVER_NAME", sourceName );
        dataSetXml = dataSetXml.replace("HCAT_TYPE", this.HCAT_TYPE);
        dataSetXml = dataSetXml.replaceAll("HCAT_TABLE_NAME", this.acqDataSetName);
        dataSetXml = dataSetXml.replaceAll("CUSTOM_DATA_PATH", getCustomPath("data", this.acqDataSetName) );
        dataSetXml = dataSetXml.replaceAll("CUSTOM_COUNT_PATH", getCustomPath("count", this.acqDataSetName) );
        dataSetXml = dataSetXml.replaceAll("CUSTOM_SCHEMA_PATH", getCustomPath("schema", this.acqDataSetName));

        Response response = this.consoleHandle.createDataSet(this.acqDataSetName, dataSetXml);
        if (response.getStatusCode() != SUCCESS) {
            try {
                fail("Failed to create the acquisition dataset - " + this.acqDataSetName);
                throw new Exception("Response status code is " + response.getStatusCode() + ", expected 200.");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        this.consoleHandle.sleep(5000);
    }

    /**
     * Method that creates a replication dataset
     * @param dataSetFileName - name of the replication dataset
     */
    private void createDoAsReplicationDataSet(String dataSetFileName) {
        String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/" + dataSetFileName);
        String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.repDataSetName, dataSetConfigFile);
        String feedName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Parameters" , "value");
        String sourceName =  this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Sources" , "name");

        dataSetXml = dataSetXml.replaceAll("TARGET1", this.targetGrid2 );
        dataSetXml = dataSetXml.replaceAll("GROUP_NAME", this.GROUP_NAME);
        dataSetXml = dataSetXml.replaceAll("DATA_OWNER", this.DATA_OWNER);
        dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", this.repDataSetName);
        dataSetXml = dataSetXml.replaceAll("FEED_NAME", feedName );
        dataSetXml = dataSetXml.replaceAll("FEED_STATS", feedName + "_stats" );
        dataSetXml = dataSetXml.replaceAll("ACQUISITION_SOURCE_NAME", this.targetGrid1 );
        dataSetXml = dataSetXml.replace("HCAT_TYPE", this.HCAT_TYPE);
        dataSetXml = dataSetXml.replaceAll("HCAT_TABLE_NAME", this.acqDataSetName);
        dataSetXml = dataSetXml.replaceAll("REPLICATION_SOURCE_DATA_PATH", getCustomPath("data" , this.acqDataSetName) );
        dataSetXml = dataSetXml.replaceAll("REPLICATION_SOURCE_COUNT_PATH", getCustomPath("count" , this.acqDataSetName) );
        dataSetXml = dataSetXml.replaceAll("REPLICATION_SOURCE_SCHEMA_PATH", getCustomPath("schema" , this.acqDataSetName));

        dataSetXml = dataSetXml.replaceAll("CUSTOM_DATA_PATH", getCustomPath("data" , this.repDataSetName));
        dataSetXml = dataSetXml.replaceAll("CUSTOM_COUNT_PATH", getCustomPath("count", this.repDataSetName));
        dataSetXml = dataSetXml.replaceAll("CUSTOM_SCHEMA_PATH", getCustomPath("schema", this.repDataSetName));

        Response response = this.consoleHandle.createDataSet(this.repDataSetName, dataSetXml);
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
     * Method that creates a retention dataset
     * @param dataSetFileName - name of the retention dataset
     */
    private void createDoAsRetentionDataSet(String dataSetFileName , String newTargetName1  , String newTargetName2) {
        String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/" + dataSetFileName);
        String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.retDataSetName, dataSetConfigFile);
        String feedName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Parameters" , "value");
        //String sourceName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Sources" , "name");

        dataSetXml = dataSetXml.replaceAll("TARGET1", newTargetName1 );
        dataSetXml = dataSetXml.replaceAll("TARGET2", newTargetName2 );
        dataSetXml = dataSetXml.replaceAll("GROUP_NAME", this.GROUP_NAME);
        dataSetXml = dataSetXml.replaceAll("DATA_OWNER", this.DATA_OWNER);
        dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", this.retDataSetName);
        dataSetXml = dataSetXml.replaceAll("FEED_NAME", feedName );
        dataSetXml = dataSetXml.replaceAll("FEED_STATS", feedName + "_stats" );
        dataSetXml = dataSetXml.replace("HCAT_TYPE", this.HCAT_TYPE);
        dataSetXml = dataSetXml.replaceAll("HCAT_TABLE_NAME", this.acqDataSetName);
        dataSetXml = dataSetXml.replace("NUMBER_OF_INSTANCE_VALUES", "0");

        dataSetXml = dataSetXml.replaceAll("ACQ_CUSTOM_DATA_PATH", getCustomPath("data" , this.acqDataSetName) );
        dataSetXml = dataSetXml.replaceAll("ACQ_CUSTOM_COUNT_PATH", getCustomPath("count" , this.acqDataSetName) );
        dataSetXml = dataSetXml.replaceAll("ACQ_CUSTOM_SCHEMA_PATH", getCustomPath("schema" , this.acqDataSetName));

        dataSetXml = dataSetXml.replaceAll("REP_CUSTOM_DATA_PATH", getCustomPath("data", this.repDataSetName) );  
        dataSetXml = dataSetXml.replaceAll("REP_CUSTOM_COUNT_PATH", getCustomPath("count", this.repDataSetName) );
        dataSetXml = dataSetXml.replaceAll("REP_CUSTOM_SCHEMA_PATH", getCustomPath("schema", this.repDataSetName) ); 

        Response response = this.consoleHandle.createDataSet(this.repDataSetName, dataSetXml);
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

    /**
     * Method to create a custom path for the dataset.
     * @param pathType - its a string type which represents either data/count/schema
     * @param dataSet - dataset name
     * @return
     */
    private String getCustomPath(String pathType , String dataSet) {
        return  "/data/daqdev/"+ pathType +"/"+ dataSet + "/%{date}";
    }
}
