// Copyright 2016, Yahoo Inc.
package hadooptest.gdm.regression;

import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;
import java.util.List;
import org.apache.commons.httpclient.HttpStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class AcqRepRetTest {
    private static String INSTANCE1 = "20130725";
    private static String INSTANCE2 = "20130726";
    private ConsoleHandle consoleHandle = new ConsoleHandle();
    private String dataSetName = "AcqRepRetTest_" + System.currentTimeMillis();
    private String sourceFDI;
    private String targetGrid1;
    private String targetGrid2;
    
    @BeforeClass
    public static void startTestSession() throws Exception {
        TestSession.start();
    }
    
    @Before
    public void setUp() throws Exception {
        List<String> datastores = this.consoleHandle.getUniqueGrids();
        if (datastores.size() < 2) {
            Assert.fail("Only " + datastores.size() + " of 2 required grids exist");
        }
        this.targetGrid1 = datastores.get(0);
        this.targetGrid2 = datastores.get(1);
        
        datastores = this.consoleHandle.getWarehouseDatastores();
        if (datastores.size() < 1) {
            Assert.fail("No warehouse datastores");
        }
        this.sourceFDI = datastores.get(0);
    }
    
    /**
     * Runs workflows through all facets, doing transformation for Acquisition.
     * @throws Exception
     */
    @Test
    public void runTest() throws Exception {
        createTopLevelDirOnReplTarget();
        createDataset();
        validateAcquisitionWorkflows();
        validateAcquisitionFiles();
        validateReplicationWorkflows();
        enableRetention();
        validateRetentionWorkflow();
        validateReplicationFiles();
    }
    
    // touch a dataset to re-discover for retention
    private void enableRetention() {
        this.consoleHandle.setRetentionPolicyToAllDataSets(this.dataSetName, "3");
        this.consoleHandle.setRetentionPolicyToAllDataSets(this.dataSetName, "1");
    }
    
    private void validateAcquisitionFiles() throws Exception {
        validateInstanceFiles(this.targetGrid1, INSTANCE1, true);
        validateInstanceFiles(this.targetGrid1, INSTANCE2, true);
    }
    
    private void validateReplicationFiles() throws Exception {
        validateInstanceFiles(this.targetGrid2, INSTANCE1, false);  // deleted
        validateInstanceFiles(this.targetGrid2, INSTANCE2, true);
    }
    
    private void validateInstanceFiles(String grid, String instance, boolean exists) throws Exception {
        HadoopFileSystemHelper helper = new HadoopFileSystemHelper(grid);
        Assert.assertEquals(exists, helper.exists("/data/daqdev/data/" + dataSetName + "/" + instance));
        Assert.assertEquals(exists, helper.exists("/data/daqdev/count/" + dataSetName + "/" + instance));
        Assert.assertEquals(exists, helper.exists("/data/daqdev/schema/" + dataSetName + "/" + instance));
    }
    
    private void validateRetentionWorkflow() {
        // make sure retention ran on replication target
        WorkFlowHelper workFlowHelper = new WorkFlowHelper();
        Assert.assertTrue("1 Expected workflow to pass", workFlowHelper.workflowPassed(this.dataSetName, "retention", INSTANCE1));
        
        // make sure retention step to the replication target occurs.  This may be a second workflow.
        boolean retentionOccurred = false;
        long sleepTime = 10 * 60 * 1000;
        while (sleepTime > 0) {
            retentionOccurred = workFlowHelper.doesStepExistInWorkFlowExecution(this.dataSetName, "retention", "completed", INSTANCE1, "retention." + this.targetGrid2);
            if (retentionOccurred) {
                return;
            }
            this.consoleHandle.sleep(5000);
            sleepTime -= 5000;
        }
        Assert.fail("Retention step did not occur");
    }
    
    private void validateReplicationWorkflows() {
        WorkFlowHelper workFlowHelper = new WorkFlowHelper();
        Assert.assertTrue("2 Expected workflow to pass", workFlowHelper.workflowPassed(this.dataSetName, "replication", INSTANCE1));
        Assert.assertTrue("3 Expected workflow to pass", workFlowHelper.workflowPassed(this.dataSetName, "replication", INSTANCE2));
    }
    
    private void validateAcquisitionWorkflows() {
        WorkFlowHelper workFlowHelper = new WorkFlowHelper();
        Assert.assertTrue("4 Expected workflow to pass", workFlowHelper.workflowPassed(this.dataSetName, "acquisition", INSTANCE1));
        Assert.assertTrue("5 Expected workflow to pass", workFlowHelper.workflowPassed(this.dataSetName, "acquisition", INSTANCE2));
    }
    
    private void createDataset() {
        String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/AcqReplDataSet.xml");
        String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.dataSetName, dataSetConfigFile);
        dataSetXml = dataSetXml.replaceAll("DATASET_NAME", dataSetName);
        dataSetXml = dataSetXml.replaceAll("SOURCE_NAME", this.sourceFDI);
        dataSetXml = dataSetXml.replaceAll("TARGET_1", this.targetGrid1);
        dataSetXml = dataSetXml.replaceAll("TARGET_2", this.targetGrid2);
            
        Response response = this.consoleHandle.createDataSet(this.dataSetName, dataSetXml);
        if (response.getStatusCode() != HttpStatus.SC_OK) {
            Assert.fail("6 Response status code is " + response.getStatusCode() + ", expected 200.");
        }
    }
    
    private void createTopLevelDirOnReplTarget() throws Exception {
        HadoopFileSystemHelper helper = new HadoopFileSystemHelper(this.targetGrid2);
        helper.createFile("/data/daqdev/bogus_" + this.dataSetName);
    }
    
    @After
    public void tearDown() throws Exception {
        Response response = this.consoleHandle.deactivateDataSet(this.dataSetName);
        Assert.assertEquals("7 ResponseCode - Deactivate DataSet failed", HttpStatus.SC_OK, response.getStatusCode());
    }
}

