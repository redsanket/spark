// Copyright 2018, Yahoo Inc.
package hadooptest.gdm.regression;

import hadooptest.TestSession;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.DataSetTarget;
import hadooptest.cluster.gdm.DataSetXmlGenerator;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

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

public class S3ConsistentUploadDownloadTest {
    private static final String INSTANCE1 = "20160531";
    private static final String INSTANCE2 = "20160601";
    private static final String INSTANCE3 = "20160101";
    /* This option will set correct manifest name in the dataset, and replication start date will be set as "20160101"
     * However, manifest file in instance "20160101" specifies additional file that does not exist in that instance.
     * Replication should copy instance1, instance2 and skip instance3
     */
    private static final int VALID_MANIFEST_SOME = 0;
    /* This option will set correct manifest name in the dataset, and replication start date will be set as "20160501"
     * However, manifest file in instance "20160101" specifies additional file that does not exist in that instance.
     * Replication should copy instance1, instance2 and skip instance3
     */
    private static final int VALID_MANIFEST_ALL = 1;
    /* This option will set incorrect manifest name in the dataset, and replication start date will be set as "20160501"
     * However, manifest file in instance "20160101" specifies additional file that does not exist in that instance.
     * Replication should copy instance1, instance2 and skip instance3
     */
    private static final int INVALID_MANIFEST = 2;
    private static final String[] OPTIONS = {"VALID_MANIFEST_SOME_","VALID_MANIFEST_ALL_","INVALID_MANIFEST_"};
    private ConsoleHandle consoleHandle = new ConsoleHandle();
    private String grid;
    private String s3Grid;
    private String datasetName;
    private static final String YKEYKEY_PARAM = "fs.s3a.ykeykey.keyname";
    private static final String YKEYKEY_NAME = "gdm.dev.s3.key";

    @BeforeClass
    public static void startTestSession() throws Exception {
        TestSession.start();
    }

    @Before
    public void setUp() throws Exception {

        List<String> S3Grids = this.consoleHandle.getS3Grids();
        if (S3Grids.size() < 1) {
            Assert.fail("Only " + S3Grids.size() + " of 1 required S3 grids exist");
        }

        List<String> grids = this.consoleHandle.getUniqueGrids();
        if (grids.size() < 1) {
            Assert.fail("Only " + grids.size() + " of 1 required grids exist");
        }

        for (String s3Grid : S3Grids) {
            for (String grid : grids) {
                if (s3Grid.contains(grid)){
                    this.s3Grid = s3Grid;
                    this.grid = grid;
                    break;
                }
            }
        }

        if (!this.s3Grid.contains(this.grid)){
            Assert.fail("No matching grids from Source grid: " + grids + " and target grid: " + S3Grids);
        }

        this.datasetName = "GridS3Repl_" + System.currentTimeMillis();
        createUploadDataSetInstance(this.datasetName);
        createDataSet(this.datasetName, this.getUploadDataSetXml(this.datasetName));
        validateUploadReplicationWorkflows(this.datasetName);
        tearDown(this.datasetName);
    }

    @Test
    public void runTestManifest() throws Exception {
        runTest(VALID_MANIFEST_SOME);
        runTest(VALID_MANIFEST_ALL);
        runTest(INVALID_MANIFEST);
    }

    private void createUploadDataSetInstance(String dataSetName) throws Exception {
        CreateFileHelper CFH = new CreateFileHelper(new HadoopFileSystemHelper(this.grid));

        String fileContent = CFH.addJsonObject("s3a://s3-manifest-test/project-foo/" + dataSetName + "/feed1/20160531/sampleData").generateFileContent();
        CFH.createFile("/projects/" + dataSetName + "/feed1/" + INSTANCE1 + "/sampleData").createFile("/projects/" + dataSetName + "/feed1/" + INSTANCE1 + "/s3_manifest.aws", fileContent);

        fileContent = CFH.addJsonObject("s3a://s3-manifest-test/project-foo/" + dataSetName + "/feed1/20160601/sampleData").generateFileContent();
        CFH.createFile("/projects/" + dataSetName + "/feed1/" + INSTANCE2 + "/sampleData").createFile("/projects/" + dataSetName + "/feed1/" + INSTANCE2 + "/s3_manifest.aws", fileContent);

        fileContent = CFH.addJsonObject("s3a://s3-manifest-test/project-foo/" + dataSetName + "/feed1/20160101/sampleData")
            .addJsonObject("s3a://s3-manifest-test/project-foo/" + dataSetName + "/feed1/20160101/sampleData.NotExist")
            .generateFileContent();
        CFH.createFile("/projects/" + dataSetName + "/feed1/" + INSTANCE3 + "/sampleData").createFile("/projects/" + dataSetName + "/feed1/" + INSTANCE3 + "/s3_manifest.aws", fileContent);
    }

    private void createDataSet(String dataSetName, String xml) {
        Response response = this.consoleHandle.createDataSet(dataSetName, xml);
        if (response.getStatusCode() != HttpStatus.SC_OK) {
            TestSession.logger.error("Failed to create dataset, xml: " + xml);
            Assert.fail("Response status code is " + response.getStatusCode() + ", expected 200.");
        }
    }

    private String getUploadDataSetXml(String dataSetName) {
        DataSetXmlGenerator generator = new DataSetXmlGenerator();
        generator.setName(dataSetName);
        generator.setDescription(this.getClass().getSimpleName());
        generator.setCatalog(this.getClass().getSimpleName());
        generator.setActive("TRUE");
        generator.setRetentionEnabled("FALSE");
        generator.setPriority("NORMAL");
        generator.setFrequency("daily");
        generator.setDiscoveryFrequency("500");
        generator.setDiscoveryInterface("HDFS");
        generator.addSourcePath("data", "/projects/" + dataSetName + "/feed1/%{date}");
        generator.addSourcePath("schema", "/projects/" + dataSetName + "/feed1/%{date}");
        generator.addSourcePath("count", "/projects/" + dataSetName + "/feed1/%{date}");
        generator.addSourcePath("invalid", "/projects/" + dataSetName + "/feed1/%{date}");
        generator.addSourcePath("raw", "/projects/" + dataSetName + "/feed1/%{date}");
        generator.addSourcePath("status", "/projects/" + dataSetName + "/feed1/%{date}");
        generator.setSource(this.grid);

        DataSetTarget target = new DataSetTarget();
        target.setName(this.s3Grid);
        target.setDateRangeStart(true, "20160101");
        target.setDateRangeEnd(false, "0");
        target.setHCatType("DataOnly");
        target.addPath("data", "s3-manifest-test/project-foo/" + dataSetName + "/feed1/%{date}");
        target.addPath("schema", "s3-manifest-test/project-foo/" + dataSetName + "/feed2/%{date}");
        target.addPath("count", "s3-manifest-test/project-foo/" + dataSetName + "/feed3/%{date}");
        target.addPath("invalid", "s3-manifest-test/project-foo/" + dataSetName + "/feed4/%{date}");
        target.addPath("raw", "s3-manifest-test/project-foo/" + dataSetName + "/feed5/%{date}");
        target.addPath("status", "s3-manifest-test/project-foo/" + dataSetName + "/feed6/%{date}");

        target.setNumInstances("1");
        target.setReplicationStrategy("DistCp");
        generator.setTarget(target);

        generator.addParameter(YKEYKEY_PARAM, YKEYKEY_NAME);
        generator.addParameter("working.dir", "s3-manifest-test/user/daqload/daqtest/tmp1/");

        generator.setGroup("jaggrp");
        generator.setOwner("jagpip");
        generator.setPermission("750");

        String dataSetXml = generator.getXml();
        return dataSetXml;
    }

    private void validateUploadReplicationWorkflows(String dataSetName) throws Exception {
        WorkFlowHelper workFlowHelper = new WorkFlowHelper();
        Assert.assertTrue("Expected workflow to pass for instance " + INSTANCE1, workFlowHelper.workflowPassed(dataSetName, "replication", INSTANCE1));
        Assert.assertTrue("Expected workflow to pass for instance " + INSTANCE2, workFlowHelper.workflowPassed(dataSetName, "replication", INSTANCE2));
        Assert.assertTrue("Expected workflow to pass for instance " + INSTANCE3, workFlowHelper.workflowPassed(dataSetName, "replication", INSTANCE3));
        instanceExistsForDownloadFeed(INSTANCE1, exists, "feed1", dataSetName);
        instanceExistsForDownloadFeed(INSTANCE2, exists, "feed1", dataSetName);
        instanceExistsForDownloadFeed(INSTANCE3, exists, "feed1", dataSetName);
    }

    private void instanceExistsForUploadFeed(String instance, boolean exists, String feed, String dataSetName) throws Exception {
        HadoopFileSystemHelper targetHelper = new HadoopFileSystemHelper(this.s3Grid);
        String path = "s3-manifest-test/project-foo/" + dataSetName + "/feed1/" + instance + "/sampleData";
        boolean found = targetHelper.exists(path);
        Assert.assertEquals("incorrect state for sample data for instance " + instance, exists, found);
        if (exists) {
            validatePermissions(targetHelper, path);
        }
    }

    private void runTest(int option) throws Exception{
        String dataSetName = "S3GridRepl_" + this.OPTIONS[option] + System.currentTimeMillis();
        createTopLevelDirectoryOnTarget(dataSetName);
        createDataSet(dataSetName, this.getDownloadDataSetXml(option,dataSetName,false));
        validateDownloadReplicationWorkflows(option, dataSetName);
        // if all the above method and their asserts are success then this dataset is eligible for deletion
        tearDown(dataSetName);
    }

    private void createTopLevelDirectoryOnTarget(String dataSetName) throws Exception {
        HadoopFileSystemHelper targetHelper = new HadoopFileSystemHelper(this.grid);
        targetHelper.createDirectory("/projects/" + dataSetName);
    }

    private String getDownloadDataSetXml(int option, String dataSetName, boolean retentionEnabled) {
        DataSetXmlGenerator generator = new DataSetXmlGenerator();
        generator.setName(dataSetName);
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
        generator.addSourcePath("data", "s3-manifest-test/project-foo/" + this.datasetName + "/feed1/%{date}");
        generator.addSourcePath("schema", "s3-manifest-test/project-foo/" + this.datasetName + "/feed2/%{date}");
        generator.addSourcePath("count", "s3-manifest-test/project-foo/" + this.datasetName + "/feed3/%{date}");
        generator.addSourcePath("invalid", "s3-manifest-test/project-foo/" + this.datasetName + "/feed4/%{date}");
        generator.addSourcePath("raw", "s3-manifest-test/project-foo/" + this.datasetName + "/feed5/%{date}");
        generator.addSourcePath("status", "s3-manifest-test/project-foo/" + this.datasetName + "/feed6/%{date}");
        generator.setSource(this.s3Grid);

        DataSetTarget target = new DataSetTarget();
        target.setName(this.grid);
        if (option == VALID_MANIFEST_SOME) {
            target.setDateRangeStart(true, "20160101");
        } else {
            target.setDateRangeStart(true, "20160531");
        }
        target.setDateRangeEnd(false, "0");
        target.setHCatType("DataOnly");
        target.addPath("data", "/projects/" + dataSetName + "/feed1/%{date}");
        target.addPath("schema", "/projects/" + dataSetName + "/feed2/%{date}");
        target.addPath("count", "/projects/" + dataSetName + "/feed3/%{date}");
        target.addPath("invalid", "/projects/" + dataSetName + "/feed4/%{date}");
        target.addPath("raw", "/projects/" + dataSetName + "/feed5/%{date}");
        target.addPath("status", "/projects/" + dataSetName + "/feed6/%{date}");

        target.setNumInstances("1");
        target.setReplicationStrategy("DistCp");
        generator.setTarget(target);

        generator.addParameter(YKEYKEY_PARAM, YKEYKEY_NAME);

        if (option == INVALID_MANIFEST){
            generator.addParameter("fs.s3a.manifest.file", "s3_manifest.invalid");
        } else {
            generator.addParameter("fs.s3a.manifest.file", "s3_manifest.aws");
        }

        generator.setGroup("jaggrp");
        generator.setOwner("jagpip");
        generator.setPermission("750");

        String dataSetXml = generator.getXml();
        return dataSetXml;
    }

    private void validateDownloadReplicationWorkflows(int option, String dataSetName) throws Exception {
        instanceDownloadExists(INSTANCE1, false, dataSetName);
        instanceDownloadExists(INSTANCE2, false, dataSetName);
        if (option == VALID_MANIFEST_SOME){
            instanceDownloadExists(INSTANCE3, false, dataSetName);
        }

        WorkFlowHelper workFlowHelper = new WorkFlowHelper();
        if (option == VALID_MANIFEST_ALL) {
            Assert.assertTrue("Expected workflow to pass for instance " + INSTANCE1, workFlowHelper.workflowPassed(dataSetName, "replication", INSTANCE1));
            Assert.assertTrue("Expected workflow to pass for instance " + INSTANCE2, workFlowHelper.workflowPassed(dataSetName, "replication", INSTANCE2));
            instanceDownloadExists(INSTANCE1, true, dataSetName);
            instanceDownloadExists(INSTANCE2, true, dataSetName);
        } else if (option == VALID_MANIFEST_SOME) {
            Assert.assertTrue("Expected workflow to pass for instance " + INSTANCE1, workFlowHelper.workflowPassed(dataSetName, "replication", INSTANCE1));
            Assert.assertTrue("Expected workflow to pass for instance " + INSTANCE2, workFlowHelper.workflowPassed(dataSetName, "replication", INSTANCE2));
            Assert.assertFalse("Expected workflow to not exist for instance " + INSTANCE3, workFlowHelper.workflowExists(dataSetName, "replication", INSTANCE3));
            instanceDownloadExists(INSTANCE1, true, dataSetName);
            instanceDownloadExists(INSTANCE2, true, dataSetName);
            instanceDownloadExists(INSTANCE3, false, dataSetName);
        } else if (option == INVALID_MANIFEST) {
            Assert.assertFalse("Expected workflow to not exist for instance " + INSTANCE1, workFlowHelper.workflowExists(dataSetName, "replication", INSTANCE1));
            Assert.assertFalse("Expected workflow to not exist for instance " + INSTANCE2, workFlowHelper.workflowExists(dataSetName, "replication", INSTANCE2));
            instanceDownloadExists(INSTANCE1, false, dataSetName);
            instanceDownloadExists(INSTANCE2, false, dataSetName);
        }

    }

    private void instanceDownloadExists(String instance, boolean exists, String dataSetName) throws Exception {
        instanceExistsForDownloadFeed(instance, exists, "feed1", dataSetName);
        instanceExistsForDownloadFeed(instance, exists, "feed2", dataSetName);
        instanceExistsForDownloadFeed(instance, exists, "feed3", dataSetName);
        instanceExistsForDownloadFeed(instance, exists, "feed4", dataSetName);
        instanceExistsForDownloadFeed(instance, exists, "feed5", dataSetName);
        instanceExistsForDownloadFeed(instance, exists, "feed6", dataSetName);

        // feed7 not specified, verify not copied
        HadoopFileSystemHelper targetHelper = new HadoopFileSystemHelper(this.grid);
        boolean found = targetHelper.exists("/projects/" + dataSetName + "/feed7/");
        Assert.assertFalse("copied feed7 for instance " + instance, found);
    }

    private void instanceExistsForDownloadFeed(String instance, boolean exists, String feed, String dataSetName) throws Exception {
        HadoopFileSystemHelper targetHelper = new HadoopFileSystemHelper(this.grid);
        String path = "/projects/" + dataSetName + "/" + feed + "/" + instance + "/sampleData";
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

    private void tearDown(String dataSetName) throws Exception {
        Response response = this.consoleHandle.deactivateDataSet(dataSetName);
        Assert.assertEquals("Deactivate DataSet failed", HttpStatus.SC_OK , response.getStatusCode());

        this.consoleHandle.removeDataSet(dataSetName);
    }

}