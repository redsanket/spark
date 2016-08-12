// Copyright 2016, Yahoo Inc.
package hadooptest.gdm.regression;

import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.DataSetTarget;
import hadooptest.cluster.gdm.DataSetXmlGenerator;
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
        HadoopFileSystemHelper targetHelper = new HadoopFileSystemHelper(this.target);
        String dataPath = "/projects/" + this.dataSetName + "/data/";
        Assert.assertFalse(targetHelper.exists("/projects/" + this.dataSetName));  
        Assert.assertFalse(targetHelper.exists(dataPath + INSTANCE1));
        Assert.assertFalse(targetHelper.exists(dataPath + INSTANCE2));
        
        // create dataset and source instance
        HadoopFileSystemHelper sourceHelper = new HadoopFileSystemHelper(this.sourceGrid);
        sourceHelper.createFile(dataPath + INSTANCE1 + "/testfile");
        createDataset();
        
        // allow dataset to propagate
        this.consoleHandle.sleep(35000);
        
        // validate workflow passes
        WorkFlowHelper workFlowHelper = new WorkFlowHelper();
        Assert.assertTrue("Expected workflow to fail", workFlowHelper.workflowPassed(this.dataSetName, "replication", INSTANCE1));
        
        // create top level dir on target
        targetHelper.createFile(dataPath + DUMMY_INSTANCE);
        
        // now the top level dir should exist on the target, but no instances
        Assert.assertTrue(targetHelper.exists("/projects/" + this.dataSetName));
        Assert.assertTrue(targetHelper.exists(dataPath + INSTANCE1));
        Assert.assertFalse(targetHelper.exists(dataPath + INSTANCE2));
        
        // create new instance on source
        sourceHelper.createFile(dataPath + INSTANCE2 + "/testfile");
        
        // validate workflow succeeds
        Assert.assertTrue("Expected workflow to succeed", workFlowHelper.workflowPassed(this.dataSetName, "replication", INSTANCE2));
        
        // now instance2 should exist on the target, but not instance1
        Assert.assertTrue(targetHelper.exists("/projects/" + this.dataSetName));
        Assert.assertTrue(targetHelper.exists(dataPath + INSTANCE1));
        Assert.assertTrue(targetHelper.exists(dataPath + INSTANCE2));
    }
    
    @After
    public void tearDown() throws Exception {
        Response response = this.consoleHandle.deactivateDataSet(this.dataSetName);
        Assert.assertEquals("ResponseCode - Deactivate DataSet failed", HttpStatus.SC_OK, response.getStatusCode());
    }
    
    private void createDataset() {
        DataSetXmlGenerator generator = new DataSetXmlGenerator();
        generator.setName(this.dataSetName);
        generator.setDescription(this.getClass().getSimpleName());
        generator.setCatalog(this.getClass().getSimpleName());
        generator.setActive("TRUE");
        generator.setRetentionEnabled("TRUE");
        generator.setPriority("NORMAL");
        generator.setFrequency("daily");
        generator.setDiscoveryFrequency("10");
        generator.setDiscoveryInterface("HDFS");
        generator.addSourcePath("data", "/projects/" + this.dataSetName + "/data/%{date}");
        generator.setSource(this.sourceGrid);
                
        DataSetTarget target = new DataSetTarget();
        target.setName(this.target);
        target.setDateRangeStart(true, "20151201");
        target.setDateRangeEnd(false, "0");
        target.setHCatType("DataOnly");
        target.addPath("data", "/projects/" + this.dataSetName + "/data/%{date}");
        target.setNumInstances("5");
        generator.setTarget(target);
        
        String dataSetXml = generator.getXml();
        
        Response response = this.consoleHandle.createDataSet(this.dataSetName, dataSetXml);
        if (response.getStatusCode() != HttpStatus.SC_OK) {
            TestSession.logger.error("Failed to create dataset, xml: " + dataSetXml);
            Assert.fail("Response status code is " + response.getStatusCode() + ", expected 200.");
        }
    }
    
}

