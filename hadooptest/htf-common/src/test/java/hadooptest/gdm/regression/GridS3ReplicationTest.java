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

public class GridS3ReplicationTest {
    private static final String INSTANCE1 = "20160531";
    private ConsoleHandle consoleHandle = new ConsoleHandle();
    private String sourceGrid;
    private String targetGrid;
    private String dataSetName = "GridS3Repl_" + System.currentTimeMillis();
    private boolean eligibleForDelete = false;
    
    @BeforeClass
    public static void startTestSession() throws Exception {
        TestSession.start();
    }
    
    @Before
    public void setUp() throws Exception {
        List<String> grids = this.consoleHandle.getUniqueGrids();
        if (grids.size() < 1) {
            Assert.fail("Only " + grids.size() + " of 1 required S3 grids exist");
        }
        this.sourceGrid = grids.get(0);
        
        grids = this.consoleHandle.getS3Grids();
        if (grids.size() < 1) {
            Assert.fail("Only " + grids.size() + " of 1 required grids exist");
        }
        this.targetGrid = grids.get(0);
    }
    
    @Test
    public void runTest() throws Exception {
        createDataSetInstance();
        createDataSet();
        validateReplicationWorkflows();
        
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
    
    private void createDataSetInstance() throws Exception {
        HadoopFileSystemHelper sourceHelper = new HadoopFileSystemHelper(this.sourceGrid);
        sourceHelper.createFile("/projects/" + this.dataSetName + "/feed1/" + INSTANCE1 + "/sometext.txt");
        sourceHelper.createFile("/projects/" + this.dataSetName + "/feed2/" + INSTANCE1 + "/sometext.txt");
        sourceHelper.createFile("/projects/" + this.dataSetName + "/feed3/" + INSTANCE1 + "/sometext.txt");
        sourceHelper.createFile("/projects/" + this.dataSetName + "/feed4/" + INSTANCE1 + "/sometext.txt");
        sourceHelper.createFile("/projects/" + this.dataSetName + "/feed5/" + INSTANCE1 + "/sometext.txt");
        sourceHelper.createFile("/projects/" + this.dataSetName + "/feed6/" + INSTANCE1 + "/sometext.txt");
        sourceHelper.createFile("/projects/" + this.dataSetName + "/feed7/" + INSTANCE1 + "/sometext.txt");
    }
    
    private void createDataSet() {
        String xml = this.getDataSetXml();
        Response response = this.consoleHandle.createDataSet(this.dataSetName, xml);
        if (response.getStatusCode() != HttpStatus.SC_OK) {
            TestSession.logger.error("Failed to create dataset, xml: " + xml);
            Assert.fail("Response status code is " + response.getStatusCode() + ", expected 200.");
        }
    }

    private String getDataSetXml() {
        DataSetXmlGenerator generator = new DataSetXmlGenerator();
        generator.setName(this.dataSetName);
        generator.setDescription(this.getClass().getSimpleName());
        generator.setCatalog(this.getClass().getSimpleName());
        generator.setActive("TRUE");
        generator.setRetentionEnabled("FALSE");
        generator.setPriority("NORMAL");
        generator.setFrequency("daily");
        generator.setDiscoveryFrequency("500");
        generator.setDiscoveryInterface("HDFS");
        generator.addSourcePath("data", "/projects/" + this.dataSetName + "/feed1/%{date}");
        generator.addSourcePath("schema", "/projects/" + this.dataSetName + "/feed1/%{date}");
        generator.addSourcePath("count", "/projects/" + this.dataSetName + "/feed1/%{date}");
        generator.addSourcePath("invalid", "/projects/" + this.dataSetName + "/feed1/%{date}");
        generator.addSourcePath("raw", "/projects/" + this.dataSetName + "/feed1/%{date}");
        generator.addSourcePath("status", "/projects/" + this.dataSetName + "/feed1/%{date}");
        generator.setSource(this.sourceGrid);
                
        DataSetTarget target = new DataSetTarget();
        target.setName(this.targetGrid);
        target.setDateRangeStart(true, "20160531");
        target.setDateRangeEnd(false, "0");
        target.setHCatType("DataOnly");
        target.addPath("data", "upload-test-bucket-1/project-foo/" + this.dataSetName + "/feed1/%{date}");
        target.addPath("schema", "upload-test-bucket-1/project-foo/" + this.dataSetName + "/feed2/%{date}");
        target.addPath("count", "upload-test-bucket-1/project-foo/" + this.dataSetName + "/feed3/%{date}");
        target.addPath("invalid", "upload-test-bucket-1/project-foo/" + this.dataSetName + "/feed4/%{date}");
        target.addPath("raw", "upload-test-bucket-1/project-foo/" + this.dataSetName + "/feed5/%{date}");
        target.addPath("status", "upload-test-bucket-1/project-foo/" + this.dataSetName + "/feed6/%{date}");
        
        target.setNumInstances("1");
        target.setReplicationStrategy("DistCp");
        generator.setTarget(target);
        
        generator.addParameter("fs.s3a.conf.file", "/home/gs/sink/gdmtest/s3_gdm_dev_1.aws");
        generator.addParameter("working.dir", "upload-test-bucket-1/user/daqload/daqtest/tmp1/");
        
        generator.setGroup("jaggrp");
        generator.setOwner("jagpip");
        generator.setPermission("750");
        
        String dataSetXml = generator.getXml();
        return dataSetXml;
    }
    
    private void validateReplicationWorkflows() throws Exception {
        instanceExists(INSTANCE1, false);
        
        WorkFlowHelper workFlowHelper = new WorkFlowHelper();
        Assert.assertTrue("Expected workflow to pass for instance " + INSTANCE1, workFlowHelper.workflowPassed(this.dataSetName, "replication", INSTANCE1));
        
        instanceExists(INSTANCE1, true);
    }
    
    // TODO: get validation of file copying to work
    private void instanceExists(String instance, boolean exists) throws Exception {
        /*
        instanceExistsForFeed(instance, exists, "feed1");
        instanceExistsForFeed(instance, exists, "feed2");
        instanceExistsForFeed(instance, exists, "feed3");
        instanceExistsForFeed(instance, exists, "feed4");
        instanceExistsForFeed(instance, exists, "feed5");
        instanceExistsForFeed(instance, exists, "feed6");
        
        // feed7 should never be copied
        instanceExistsForFeed(instance, false, "feed7");
        */
    }
    
    private void instanceExistsForFeed(String instance, boolean exists, String feed) throws Exception {
        String path = "s3a://upload-test-bucket-1/project-foo/" + this.dataSetName + "/" + feed + "/" + INSTANCE1;
        boolean found = this.consoleHandle.s3FilesExist(this.targetGrid, path, this.dataSetName);
        Assert.assertEquals("incorrect state for sample data for instance " + instance, exists, found);

        if (found) {
            path = "s3a://upload-test-bucket-1/project-foo/" + this.dataSetName + "/" + feed + "/" + INSTANCE1 + "/sometext.txt";
            found = this.consoleHandle.s3FilesExist(this.targetGrid, path, this.dataSetName);
            Assert.assertEquals("incorrect state for sample data for instance " + instance, exists, found);
        }
    }
}

