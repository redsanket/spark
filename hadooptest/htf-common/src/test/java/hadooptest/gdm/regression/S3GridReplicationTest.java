// Copyright 2016, Yahoo Inc.
package hadooptest.gdm.regression;

import hadooptest.TestSession;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.DataSetTarget;
import hadooptest.cluster.gdm.DataSetXmlGenerator;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.httpclient.HttpStatus;
import org.apache.hadoop.fs.FileStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class S3GridReplicationTest {
    private static final String INSTANCE1 = "20160531";
    private static final String INSTANCE2 = "20160601";
    private ConsoleHandle consoleHandle = new ConsoleHandle();
    private String sourceGrid;
    private String targetGrid;
    private String dataSetName = "S3GridRepl_" + System.currentTimeMillis();
    private boolean eligibleForDelete = false;
    
    @BeforeClass
    public static void startTestSession() throws Exception {
        TestSession.start();
    }
    
    @Before
    public void setUp() throws Exception {
        List<String> grids = this.consoleHandle.getS3Grids();
        if (grids.size() < 1) {
            Assert.fail("Only " + grids.size() + " of 1 required S3 grids exist");
        }
        this.sourceGrid = grids.get(0);
        
        grids = this.consoleHandle.getUniqueGrids();
        if (grids.size() < 1) {
            Assert.fail("Only " + grids.size() + " of 1 required grids exist");
        }
        this.targetGrid = grids.get(0);
    }
    
    @Test
    public void runTest() throws Exception {
        createTopLevelDirectoryOnTarget();
        createDataSet();
        validateReplicationWorkflows();
        enableRetention();
        validateRetentionWorkflow();
        
        // if all the above method and their asserts are success then this dataset is eligible for deletion
        eligibleForDelete = true;
    }
    
    @After
    public void tearDown() throws Exception {
        Response response = this.consoleHandle.deactivateDataSet(this.dataSetName);
        Assert.assertEquals("Deactivate DataSet failed", HttpStatus.SC_OK , response.getStatusCode());
        
        if (eligibleForDelete) {
            this.consoleHandle.removeDataSet(this.dataSetName);
        }
    }
    
    private void createTopLevelDirectoryOnTarget() throws Exception {
        HadoopFileSystemHelper targetHelper = new HadoopFileSystemHelper(this.targetGrid);
        targetHelper.createDirectory("/projects/" + dataSetName);
    }
    
    private void createDataSet() {
        String xml = this.getDataSetXml(false);
        Response response = this.consoleHandle.createDataSet(this.dataSetName, xml);
        if (response.getStatusCode() != HttpStatus.SC_OK) {
            TestSession.logger.error("Failed to create dataset, xml: " + xml);
            Assert.fail("Response status code is " + response.getStatusCode() + ", expected 200.");
        }
    }

    private String getDataSetXml(boolean retentionEnabled) {
        DataSetXmlGenerator generator = new DataSetXmlGenerator();
        generator.setName(this.dataSetName);
        generator.setDescription(this.getClass().getSimpleName());
        generator.setCatalog(this.getClass().getSimpleName());
        generator.setActive("TRUE");
        if (retentionEnabled) {
            generator.setRetentionEnabled("TRUE");
        } else {
            generator.setRetentionEnabled("FALSE");
        }
        generator.setPriority("NORMAL");
        generator.setFrequency("daily");
        generator.setDiscoveryFrequency("500");
        generator.setDiscoveryInterface("HDFS");
        generator.addSourcePath("data", "sample-feeds-1/feed1/%{date}");
        generator.addSourcePath("schema", "sample-feeds-1/feed2/%{date}");
        generator.addSourcePath("count", "sample-feeds-1/feed3/%{date}");
        generator.addSourcePath("invalid", "sample-feeds-1/feed4/%{date}");
        generator.addSourcePath("raw", "sample-feeds-1/feed5/%{date}");
        generator.addSourcePath("status", "sample-feeds-1/feed6/%{date}");
        generator.setSource(this.sourceGrid);
                
        DataSetTarget target = new DataSetTarget();
        target.setName(this.targetGrid);
        target.setDateRangeStart(true, "20160531");
        target.setDateRangeEnd(false, "0");
        target.setHCatType("DataOnly");
        target.addPath("data", "/projects/" + this.dataSetName + "/feed1/%{date}");
        target.addPath("schema", "/projects/" + this.dataSetName + "/feed2/%{date}");
        target.addPath("count", "/projects/" + this.dataSetName + "/feed3/%{date}");
        target.addPath("invalid", "/projects/" + this.dataSetName + "/feed4/%{date}");
        target.addPath("raw", "/projects/" + this.dataSetName + "/feed5/%{date}");
        target.addPath("status", "/projects/" + this.dataSetName + "/feed6/%{date}");
        
        target.setNumInstances("1");
        target.setReplicationStrategy("DistCp");
        generator.setTarget(target);
        
        generator.addParameter("uselocalkeys3.keydb.secretkey.keyname", "gdmdev_test1_secret");
        generator.addParameter("uselocalkeys3.keydb.keyid.keyname", "gdmdev_test1_access");
        generator.addParameter("s3.keydb.directory", "gdmtest");
        generator.addParameter("s3service.https-only", "false");
        
        generator.setGroup("jaggrp");
        generator.setOwner("jagpip");
        generator.setPermission("750");
        
        String dataSetXml = generator.getXml();
        return dataSetXml;
    }
    
    private void validateReplicationWorkflows() throws Exception {
        instanceExists(INSTANCE1, false);
        instanceExists(INSTANCE2, false);
        
        WorkFlowHelper workFlowHelper = new WorkFlowHelper();
        Assert.assertTrue("Expected workflow to pass for instance " + INSTANCE1, workFlowHelper.workflowPassed(this.dataSetName, "replication", INSTANCE1));
        Assert.assertTrue("Expected workflow to pass for instance " + INSTANCE2, workFlowHelper.workflowPassed(this.dataSetName, "replication", INSTANCE2));
        
        instanceExists(INSTANCE1, true);
        instanceExists(INSTANCE2, true);
    }
    
    private void instanceExists(String instance, boolean exists) throws Exception {
        instanceExistsForFeed(instance, exists, "feed1");
        instanceExistsForFeed(instance, exists, "feed2");
        instanceExistsForFeed(instance, exists, "feed3");
        instanceExistsForFeed(instance, exists, "feed4");
        instanceExistsForFeed(instance, exists, "feed5");
        instanceExistsForFeed(instance, exists, "feed6");
        
        // feed7 not specified, verify not copied
        HadoopFileSystemHelper targetHelper = new HadoopFileSystemHelper(this.targetGrid);
        boolean found = targetHelper.exists("/projects/" + this.dataSetName + "/feed7/");
        Assert.assertFalse("copied feed7 for instance " + instance, found);
    }
    
    private void instanceExistsForFeed(String instance, boolean exists, String feed) throws Exception {
        HadoopFileSystemHelper targetHelper = new HadoopFileSystemHelper(this.targetGrid);
        String path = "/projects/" + this.dataSetName + "/" + feed + "/" + instance + "/sampleData";
        boolean found = targetHelper.exists(path);
        Assert.assertEquals("incorrect state for sample data for instance " + instance, exists, found);
        if (exists) {
            validatePermissions(targetHelper, path);
        }
    }
    
    private void validatePermissions(HadoopFileSystemHelper helper, String path) throws Exception {
        FileStatus fileStatus = helper.getFileStatus(path);
        if (fileStatus.isDirectory()) {
            Assert.assertEquals("Unexpected permission for path " + path, "drwxr-x---", fileStatus.getPermission().toString());
        } else {
            Assert.assertEquals("Unexpected permission for path " + path, "rw-r-----", fileStatus.getPermission().toString());
        }
        Assert.assertEquals("Unexpected owner for path " + path, fileStatus.getOwner(), "jagpip");
        Assert.assertEquals("Unexpected group for path " + path, fileStatus.getGroup(), "jaggrp");
    }
    
    private void enableRetention() {
        String dataSetXml = this.getDataSetXml(true);
        Response response = this.consoleHandle.modifyDataSet(this.dataSetName, dataSetXml);
        if (response.getStatusCode() != HttpStatus.SC_OK) {
            TestSession.logger.error("Failed to create dataset, xml: " + dataSetXml);
            Assert.fail("Response status code is " + response.getStatusCode() + ", expected 200.");
        }
    }
    
    private void validateRetentionWorkflow() throws Exception {     
        WorkFlowHelper workFlowHelper = new WorkFlowHelper();
        Assert.assertTrue("Expected workflow to pass for instance " + INSTANCE1, workFlowHelper.workflowPassed(this.dataSetName, "retention", INSTANCE1));
        instanceExists(INSTANCE1, false);
        instanceExists(INSTANCE2, true);
    }
     
}

