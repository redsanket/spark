// Copyright 2016, Yahoo Inc.
package hadooptest.gdm.regression;

import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;
import java.util.List;
import org.apache.commons.httpclient.HttpStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TopLevelDirTest {
    private static final String INSTANCE1 = "20151201";
    private static final String INSTANCE2 = "20151202";
    private static final String DUMMY_INSTANCE = "20141201";
    private ConsoleHandle consoleHandle = new ConsoleHandle();
    private String dataSetName =  "TopLevel_" + System.currentTimeMillis();
    private String sourceGrid;
    private String target;
    
    @BeforeClass
    public static void startTestSession() throws Exception {
        TestSession.start();
    }
    
    @Before
    public void setUp() throws Exception {
        List<String> grids = this.consoleHandle.getUniqueGrids();
        if (grids.size() < 2) {
            Assert.fail("Only " + grids.size() + " of 2 required grids exist");
        }
        this.sourceGrid = grids.get(0);
        this.target = grids.get(1);
    }
    
    @Test
    public void runTest() throws Exception {
        // verify top level dir does not exist on target
        Assert.assertFalse(this.consoleHandle.filesExist(this.target, "/projects/" + this.dataSetName));
        Assert.assertFalse(this.consoleHandle.filesExist(this.target, "/projects/" + this.dataSetName + "/data/" + INSTANCE1));
        Assert.assertFalse(this.consoleHandle.filesExist(this.target, "/projects/" + this.dataSetName + "/data/" + INSTANCE2));
        
        // create dataset and source instance
        createDataSetInstance(this.sourceGrid, INSTANCE1);
        createDataset();
        
        // allow dataset to propagate
        this.consoleHandle.sleep(35000);
        
        // validate workflow fails
        WorkFlowHelper workFlowHelper = new WorkFlowHelper();
        Assert.assertFalse("Expected workflow to fail", workFlowHelper.workflowPassed(this.dataSetName, "replication", INSTANCE1));
        
        // create top level dir on target
        createDataSetInstance(this.target, DUMMY_INSTANCE);
        
        // now the top level dir should exist on the target, but no instances
        Assert.assertTrue(this.consoleHandle.filesExist(this.target, "/projects/" + this.dataSetName));
        Assert.assertFalse(this.consoleHandle.filesExist(this.target, "/projects/" + this.dataSetName + "/data/" + INSTANCE1));
        Assert.assertFalse(this.consoleHandle.filesExist(this.target, "/projects/" + this.dataSetName + "/data/" + INSTANCE2));
        
        // create new instance on source
        createDataSetInstance(this.sourceGrid, INSTANCE2);
        
        // validate workflow succeeds
        Assert.assertTrue("Expected workflow to succeed", workFlowHelper.workflowPassed(this.dataSetName, "replication", INSTANCE2));
        
        // now instance2 should exist on the target, but not instance1
        Assert.assertTrue(this.consoleHandle.filesExist(this.target, "/projects/" + this.dataSetName));
        Assert.assertFalse(this.consoleHandle.filesExist(this.target, "/projects/" + this.dataSetName + "/data/" + INSTANCE1));
        Assert.assertTrue(this.consoleHandle.filesExist(this.target, "/projects/" + this.dataSetName + "/data/" + INSTANCE2));
    }
    
    @After
    public void tearDown() throws Exception {
        Response response = this.consoleHandle.deactivateDataSet(this.dataSetName);
        Assert.assertEquals("ResponseCode - Deactivate DataSet failed", HttpStatus.SC_OK, response.getStatusCode());
    }
    
    private void createDataSetInstance(String grid, String instanceId) throws Exception {
        CreateInstanceOnGrid createInstanceOnGridObj = new CreateInstanceOnGrid(grid, "/projects/" + this.dataSetName , "data/", instanceId);
        createInstanceOnGridObj.execute();
    }
    
    private void createDataset() {
        String basePath = "/projects/" + this.dataSetName + "/data/%{date}";
        String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/BasicReplDataSet1Target.xml");
        String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.dataSetName, dataSetConfigFile);
        dataSetXml = dataSetXml.replaceAll("TARGET", this.target);
        dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", this.dataSetName);
        dataSetXml = dataSetXml.replaceAll("SOURCE", this.sourceGrid );
        dataSetXml = dataSetXml.replaceAll("START_TYPE", "fixed" );
        dataSetXml = dataSetXml.replaceAll("START_DATE", INSTANCE1);
        dataSetXml = dataSetXml.replaceAll("END_TYPE", "offset");
        dataSetXml = dataSetXml.replaceAll("END_DATE", "0");
        dataSetXml = dataSetXml.replaceAll("REPLICATION_DATA_PATH", basePath);
        dataSetXml = dataSetXml.replaceAll("CUSTOM_PATH_DATA", basePath);
        Response response = this.consoleHandle.createDataSet(this.dataSetName, dataSetXml);
        if (response.getStatusCode() != HttpStatus.SC_OK) {
            Assert.fail("Response status code is " + response.getStatusCode() + ", expected 200.");
        }
    }
    
}

