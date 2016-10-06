// Copyright 2016, Yahoo Inc.
package hadooptest.gdm.regression;

import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.CustomNameValuePair;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class BackfillTest extends TestSession {
    private static final String INSTANCE1 = "20151201";
    private static final String INSTANCE2 = "20151202";
    private static final String INSTANCE3 = "20151203";
    private static final int SUCCESS = 200;
    private static final int CREATED = 201;
    private ConsoleHandle consoleHandle = new ConsoleHandle();
    private String dataSetName =  "BackfillTest_" + System.currentTimeMillis();
    private String sourceGrid;
    private String target1;
    private String target2;
    private boolean eligibleForDelete = false;
    
    @BeforeClass
    public static void startTestSession() throws Exception {
        TestSession.start();
    }
    
    @Before
    public void setUp() throws Exception {
        List<String> grids = this.consoleHandle.getUniqueGrids();
        if (grids.size() < 3) {
            Assert.fail("Only " + grids.size() + " of 3 required grids exist");
        }
        this.sourceGrid = grids.get(0);
        this.target1 = grids.get(1);
        this.target2 = grids.get(2);
    }
    
    @Test
    public void runTest() throws Exception {
        // create instance1 on source
        createDataSetInstance(INSTANCE1);
        
        Assert.assertFalse(this.consoleHandle.filesExist(this.target1, "/data/daqdev/data/" + this.dataSetName + "/target1/" + INSTANCE1));
        Assert.assertFalse(this.consoleHandle.filesExist(this.target1, "/data/daqdev/data/" + this.dataSetName + "/target1/" + INSTANCE2));
        Assert.assertFalse(this.consoleHandle.filesExist(this.target1, "/data/daqdev/data/" + this.dataSetName + "/target1/" + INSTANCE3));
        Assert.assertFalse(this.consoleHandle.filesExist(this.target2, "/data/daqdev/data/" + this.dataSetName + "/target2/" + INSTANCE1));
        Assert.assertFalse(this.consoleHandle.filesExist(this.target2, "/data/daqdev/data/" + this.dataSetName + "/target2/" + INSTANCE2));
        Assert.assertFalse(this.consoleHandle.filesExist(this.target2, "/data/daqdev/data/" + this.dataSetName + "/target2/" + INSTANCE3));
        
        // create a dataset copying data from the source to target1
        String instance1ActivationTime = new Long(System.currentTimeMillis()).toString();
        createInitialDataset();
        
        // allow dataset to propagate
        this.consoleHandle.sleep(35000);
        
        // validate instance1 copied to target1
        WorkFlowHelper workFlowHelper = new WorkFlowHelper();
        workFlowHelper.checkWorkFlow(this.dataSetName, "replication", instance1ActivationTime, INSTANCE1);
        
        Assert.assertTrue(this.consoleHandle.filesExist(this.target1, "/data/daqdev/data/" + this.dataSetName + "/target1/" + INSTANCE1));
        Assert.assertFalse(this.consoleHandle.filesExist(this.target1, "/data/daqdev/data/" + this.dataSetName + "/target1/" + INSTANCE2));
        Assert.assertFalse(this.consoleHandle.filesExist(this.target1, "/data/daqdev/data/" + this.dataSetName + "/target1/" + INSTANCE3));
        Assert.assertFalse(this.consoleHandle.filesExist(this.target2, "/data/daqdev/data/" + this.dataSetName + "/target2/" + INSTANCE1));
        Assert.assertFalse(this.consoleHandle.filesExist(this.target2, "/data/daqdev/data/" + this.dataSetName + "/target2/" + INSTANCE2));
        Assert.assertFalse(this.consoleHandle.filesExist(this.target2, "/data/daqdev/data/" + this.dataSetName + "/target2/" + INSTANCE3));
                
        // add target2
        modifyDatasetWithTwoTargets(INSTANCE1);
        
        // allow dataset to propagate
        this.consoleHandle.sleep(35000);
        
        // create instance2 on source
        String instance2ActivationTime = new Long(System.currentTimeMillis()).toString();
        createDataSetInstance(INSTANCE2);
        
        // validate instance2 copied to target1 and target2
        workFlowHelper.checkWorkFlow(this.dataSetName, "replication", instance2ActivationTime, INSTANCE2);
        
        Assert.assertTrue(this.consoleHandle.filesExist(this.target1, "/data/daqdev/data/" + this.dataSetName + "/target1/" + INSTANCE1));
        Assert.assertTrue(this.consoleHandle.filesExist(this.target1, "/data/daqdev/data/" + this.dataSetName + "/target1/" + INSTANCE2));
        Assert.assertFalse(this.consoleHandle.filesExist(this.target1, "/data/daqdev/data/" + this.dataSetName + "/target1/" + INSTANCE3));
        Assert.assertFalse(this.consoleHandle.filesExist(this.target2, "/data/daqdev/data/" + this.dataSetName + "/target2/" + INSTANCE1));
        Assert.assertTrue(this.consoleHandle.filesExist(this.target2, "/data/daqdev/data/" + this.dataSetName + "/target2/" + INSTANCE2));
        Assert.assertFalse(this.consoleHandle.filesExist(this.target2, "/data/daqdev/data/" + this.dataSetName + "/target2/" + INSTANCE3));
        
        // modify dataset to switch date range to start at instance2
        modifyDatasetWithTwoTargets(INSTANCE2);
        
        // create backfill dataset for instance1 on target2
        String instance1BackfillTime = new Long(System.currentTimeMillis()).toString();
        createBackfillDataset();
        
        // allow dataset to propagate
        this.consoleHandle.sleep(35000);
        
        // validate instance1 copied to target2
        workFlowHelper.checkWorkFlow("BACKFILL_" + this.dataSetName, "replication", instance1BackfillTime, INSTANCE1);
        
        Assert.assertTrue(this.consoleHandle.filesExist(this.target1, "/data/daqdev/data/" + this.dataSetName + "/target1/" + INSTANCE1));
        Assert.assertTrue(this.consoleHandle.filesExist(this.target1, "/data/daqdev/data/" + this.dataSetName + "/target1/" + INSTANCE2));
        Assert.assertFalse(this.consoleHandle.filesExist(this.target1, "/data/daqdev/data/" + this.dataSetName + "/target1/" + INSTANCE3));
        Assert.assertTrue(this.consoleHandle.filesExist(this.target2, "/data/daqdev/data/" + this.dataSetName + "/target2/" + INSTANCE1));
        Assert.assertTrue(this.consoleHandle.filesExist(this.target2, "/data/daqdev/data/" + this.dataSetName + "/target2/" + INSTANCE2));
        Assert.assertFalse(this.consoleHandle.filesExist(this.target2, "/data/daqdev/data/" + this.dataSetName + "/target2/" + INSTANCE3));
   
        // create instance3 and validate it goes to both targets
        createDataSetInstance(INSTANCE3);
        workFlowHelper.checkWorkFlow(this.dataSetName, "replication", instance1BackfillTime, INSTANCE3);
        
        Assert.assertTrue(this.consoleHandle.filesExist(this.target1, "/data/daqdev/data/" + this.dataSetName + "/target1/" + INSTANCE1));
        Assert.assertTrue(this.consoleHandle.filesExist(this.target1, "/data/daqdev/data/" + this.dataSetName + "/target1/" + INSTANCE2));
        Assert.assertTrue(this.consoleHandle.filesExist(this.target1, "/data/daqdev/data/" + this.dataSetName + "/target1/" + INSTANCE3));
        Assert.assertTrue(this.consoleHandle.filesExist(this.target2, "/data/daqdev/data/" + this.dataSetName + "/target2/" + INSTANCE1));
        Assert.assertTrue(this.consoleHandle.filesExist(this.target2, "/data/daqdev/data/" + this.dataSetName + "/target2/" + INSTANCE2));
        Assert.assertTrue(this.consoleHandle.filesExist(this.target2, "/data/daqdev/data/" + this.dataSetName + "/target2/" + INSTANCE3));
        
       eligibleForDelete = true;
    }
    
    @After
    public void tearDown() throws Exception {
        Response response = this.consoleHandle.deactivateDataSet(this.dataSetName);
        Assert.assertEquals("ResponseCode - Deactivate DataSet failed", SUCCESS , response.getStatusCode());
        response = this.consoleHandle.deactivateDataSet("BACKFILL_" + this.dataSetName);
        Assert.assertEquals("ResponseCode - Deactivate DataSet failed", SUCCESS , response.getStatusCode());
        
        if (eligibleForDelete == true) {
            this.consoleHandle.removeDataSet(this.dataSetName);
            this.consoleHandle.removeDataSet("BACKFILL_" + this.dataSetName);
        }
    }
    
    private void createDataSetInstance(String instanceId) throws Exception {
        CreateInstanceOnGrid createInstanceOnGridObj = new CreateInstanceOnGrid(this.sourceGrid , "/data/daqdev" , "data/" + this.dataSetName , instanceId);
        createInstanceOnGridObj.execute();
    }
    
    private void createInitialDataset() {
        String basePath = "/data/daqdev/data/" + this.dataSetName + "/%{date}";
        String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/BasicReplDataSet1Target.xml");
        String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.dataSetName, dataSetConfigFile);
        dataSetXml = dataSetXml.replaceAll("TARGET", this.target1);
        dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", this.dataSetName);
        dataSetXml = dataSetXml.replaceAll("SOURCE", this.sourceGrid );
        dataSetXml = dataSetXml.replaceAll("START_TYPE", "fixed" );
        dataSetXml = dataSetXml.replaceAll("START_DATE", INSTANCE1);
        dataSetXml = dataSetXml.replaceAll("END_TYPE", "fixed");
        dataSetXml = dataSetXml.replaceAll("END_DATE", INSTANCE3);
        dataSetXml = dataSetXml.replaceAll("REPLICATION_DATA_PATH", basePath);
        dataSetXml = dataSetXml.replaceAll("CUSTOM_PATH_DATA", "/data/daqdev/data/" + this.dataSetName + "/target1/%{date}");
        Response response = this.consoleHandle.createDataSet(this.dataSetName, dataSetXml);
        if (response.getStatusCode() != SUCCESS) {
            Assert.fail("Response status code is " + response.getStatusCode() + ", expected 200.");
        }
    }
    
    private void modifyDatasetWithTwoTargets(String startDate) {
        String basePath = "/data/daqdev/data/" + this.dataSetName + "/%{date}";
        String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/BasicReplDataSet2Targets.xml");
        String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.dataSetName, dataSetConfigFile);
        dataSetXml = dataSetXml.replaceAll("TARGET_1", this.target1);
        dataSetXml = dataSetXml.replaceAll("TARGET_2", this.target2);
        dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", this.dataSetName);
        dataSetXml = dataSetXml.replaceAll("SOURCE", this.sourceGrid );
        dataSetXml = dataSetXml.replaceAll("START_TYPE", "fixed" );
        dataSetXml = dataSetXml.replaceAll("START_DATE_1", startDate);
        dataSetXml = dataSetXml.replaceAll("START_DATE_2", startDate);
        dataSetXml = dataSetXml.replaceAll("END_TYPE", "fixed");
        dataSetXml = dataSetXml.replaceAll("END_DATE_1", INSTANCE3);
        dataSetXml = dataSetXml.replaceAll("END_DATE_2", INSTANCE3);
        dataSetXml = dataSetXml.replaceAll("REPLICATION_DATA_PATH", basePath);
        dataSetXml = dataSetXml.replaceAll("CUSTOM_PATH_DATA_1", "/data/daqdev/data/" + this.dataSetName + "/target1/%{date}");
        dataSetXml = dataSetXml.replaceAll("CUSTOM_PATH_DATA_2", "/data/daqdev/data/" + this.dataSetName + "/target2/%{date}");
        Response response = this.consoleHandle.modifyDataSet(this.dataSetName, dataSetXml);
        if (response.getStatusCode() != SUCCESS) {
            Assert.fail("Response status code is " + response.getStatusCode() + ", expected 200.");
        }
    }
    
    private void createBackfillDataset() {
        JSONObject feedRequest = new JSONObject();
        JSONObject feedData = new JSONObject();
        feedData.put("ExistingDataSet", this.dataSetName);
        feedData.put("SourceCluster", this.sourceGrid);

        JSONObject target = new JSONObject();
        target.put("TargetCluster", this.target2);
        target.put("StartInstance", INSTANCE1);
        target.put("EndInstance", INSTANCE1);
        
        JSONArray targets = new JSONArray();
        targets.add(target);
        
        feedData.put("Targets", targets);
        feedRequest.put("BackfillRequest", feedData);
        
        ArrayList<CustomNameValuePair> params = new ArrayList<CustomNameValuePair>();
        CustomNameValuePair backfillRequest = new CustomNameValuePair("backfillRequest", feedRequest.toString(4));
        params.add(backfillRequest);
        

        Response response = this.consoleHandle.postToConsole("/rest/config/dataset/backfill/v1", params);
        if (response.getStatusCode() != CREATED) {
            Assert.fail("Response status code is " + response.getStatusCode() + ", expected 201.");
        }
    }
}


