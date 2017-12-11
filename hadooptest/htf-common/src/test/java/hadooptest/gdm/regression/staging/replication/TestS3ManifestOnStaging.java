// Copyright 2017, Yahoo Inc.
package hadooptest.gdm.regression.staging.replication;

import hadooptest.TestSession;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.DataSetTarget;
import hadooptest.cluster.gdm.DataSetXmlGenerator;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;
import hadooptest.gdm.regression.HadoopFileSystemHelper;

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

public class TestS3ManifestOnStaging {
	private static final String INSTANCE1 = "20160531";
	private static final String INSTANCE2 = "20160601";
	private static final String INSTANCE3 = "20160101";
	private static final String TEST_BUCKET = "upload-test-bucket-1";
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
	private String target;
	private String source;
	private String uploadDataSetName;

	@BeforeClass
	public static void startTestSession() throws Exception {
		TestSession.start();
	}

	@Before
	public void setUp() throws Exception {
		this.source = "JetBlue-S3";
		this.target = "JetBlue";

		this.uploadDataSetName = "GridToS3OnStaging_" + System.currentTimeMillis();
		createUploadDataSetInstance(this.uploadDataSetName);
		createDataSet(this.uploadDataSetName, this.getUploadDataSetXml(this.uploadDataSetName));
		validateUploadReplicationWorkflows(this.uploadDataSetName);
		tearDown(this.uploadDataSetName);
	}

	@Test
	public void runTestManifest() throws Exception {
		runTest(VALID_MANIFEST_SOME);
		runTest(VALID_MANIFEST_ALL);
		runTest(INVALID_MANIFEST);
	}

	private void createUploadDataSetInstance(String dataSetName) throws Exception {
		HadoopFileSystemHelper sourceHelper = new HadoopFileSystemHelper(this.target);
		JSONObject fileContent=new JSONObject();
		JSONArray urls=new JSONArray();
		JSONObject jsonObject = new JSONObject();

		jsonObject.put("url", "s3a://" + TEST_BUCKET + "/project-foo/" + dataSetName + "/feed1/" + INSTANCE1 + "/sampleData");
		urls.add(jsonObject);
		fileContent.put("entries",urls);
		sourceHelper.createFile("/GDM/" + dataSetName + "/feed1/" + INSTANCE1 + "/sampleData");
		sourceHelper.createFile("/GDM/" + dataSetName + "/feed1/" + INSTANCE1 + "/s3_manifest.aws", fileContent.toString());

		urls.clear();
		jsonObject.put("url", "s3a://" + TEST_BUCKET + "/project-foo/" + dataSetName + "/feed1/" + INSTANCE2 + "/sampleData");
		urls.add(jsonObject);
		fileContent.put("entries",urls);
		sourceHelper.createFile("/GDM/" + dataSetName + "/feed1/" + INSTANCE2 + "/sampleData");
		sourceHelper.createFile("/GDM/" + dataSetName + "/feed1/" + INSTANCE2 + "/s3_manifest.aws", fileContent.toString());

		urls.clear();
		jsonObject.put("url", "s3a://" + TEST_BUCKET + "/project-foo/" + dataSetName + "/feed1/" + INSTANCE3 + "/sampleData");
		urls.add(jsonObject);
		jsonObject.put("url", "s3a://" + TEST_BUCKET + "/project-foo/" + dataSetName + "/feed1/" + INSTANCE3 + "/sampleData.NotExist");
		urls.add(jsonObject);
		fileContent.put("entries",urls);
		sourceHelper.createFile("/GDM/" + dataSetName + "/feed1/" + INSTANCE3 + "/sampleData");
		sourceHelper.createFile("/GDM/" + dataSetName + "/feed1/" + INSTANCE3 + "/s3_manifest.aws", fileContent.toString());
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
		generator.addSourcePath("data", "/GDM/" + dataSetName + "/feed1/%{date}");
		generator.setSource(this.target);

		DataSetTarget target = new DataSetTarget();
		target.setName(this.source);
		target.setDateRangeStart(true, "20160101");
		target.setDateRangeEnd(false, "0");
		target.setHCatType("DataOnly");
		target.addPath("data", TEST_BUCKET + "/project-foo/" + dataSetName + "/feed1/%{date}");

		target.setNumInstances("1");
		target.setReplicationStrategy("DistCp");
		generator.setTarget(target);

		generator.addParameter("fs.s3a.conf.file", "/home/gs/sink/gdmtest/s3_gdm_dev_1.aws");
		generator.addParameter("working.dir", TEST_BUCKET + "/user/daqload/daqtest/tmp1/");

		generator.setGroup("dfsload");
		generator.setOwner("groups");
		generator.setPermission("750");

		String dataSetXml = generator.getXml();
		return dataSetXml;
	}

	private void validateUploadReplicationWorkflows(String dataSetName) throws Exception {
		WorkFlowHelper workFlowHelper = new WorkFlowHelper();
		Assert.assertTrue("Expected workflow to pass for instance " + INSTANCE1, workFlowHelper.workflowPassed(dataSetName, "replication", INSTANCE1));
		Assert.assertTrue("Expected workflow to pass for instance " + INSTANCE2, workFlowHelper.workflowPassed(dataSetName, "replication", INSTANCE2));
		Assert.assertTrue("Expected workflow to pass for instance " + INSTANCE3, workFlowHelper.workflowPassed(dataSetName, "replication", INSTANCE3));
	}

	private void runTest(int option) throws Exception{
		String dataSetName = "S3ToGridOnStaging_" + this.OPTIONS[option] + System.currentTimeMillis();
		createTopLevelDirectoryOnTarget(dataSetName);
		createDataSet(dataSetName, this.getDownloadDataSetXml(option,dataSetName,false));
		validateDownloadReplicationWorkflows(option, dataSetName);
		if (option != INVALID_MANIFEST) {
			enableRetention(option, dataSetName);
			validateRetentionWorkflow(dataSetName);
		}
		// if all the above method and their asserts are success then this dataset is eligible for deletion
		tearDown(dataSetName);
	}

	private void createTopLevelDirectoryOnTarget(String dataSetName) throws Exception {
		HadoopFileSystemHelper targetHelper = new HadoopFileSystemHelper(this.target);
		targetHelper.createDirectory("/GDM/" + dataSetName);
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
		generator.addSourcePath("data", TEST_BUCKET + "/project-foo/" + this.uploadDataSetName + "/feed1/%{date}");
		generator.setSource(this.source);

		DataSetTarget target = new DataSetTarget();
		target.setName(this.target);
		if (option == VALID_MANIFEST_SOME) {
			target.setDateRangeStart(true, "20160101");
		} else {
			target.setDateRangeStart(true, "20160531");
		}
		target.setDateRangeEnd(false, "0");
		target.setHCatType("DataOnly");
		target.addPath("data", "/GDM/" + dataSetName + "/feed1/%{date}");

		target.setNumInstances("1");
		target.setReplicationStrategy("DistCp");
		generator.setTarget(target);

		generator.addParameter("fs.s3a.conf.file", "/home/gs/sink/gdmtest/s3_gdm_dev_1.aws");

		if (option == INVALID_MANIFEST){
			generator.addParameter("fs.s3a.manifest.file", "s3_manifest.invalid");
		} else {
			generator.addParameter("fs.s3a.manifest.file", "s3_manifest.aws");
		}

		generator.setGroup("dfsload");
		generator.setOwner("users");
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

		// feed2 not specified, verify not copied
		HadoopFileSystemHelper targetHelper = new HadoopFileSystemHelper(this.target);
		boolean found = targetHelper.exists("/GDM/" + dataSetName + "/feed2/");
		Assert.assertFalse("copied feed2 for instance " + instance, found);
	}

	private void instanceExistsForDownloadFeed(String instance, boolean exists, String feed, String dataSetName) throws Exception {
		HadoopFileSystemHelper targetHelper = new HadoopFileSystemHelper(this.target);
		String path = "/GDM/" + dataSetName + "/" + feed + "/" + instance + "/sampleData";
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

	private void enableRetention(int option, String dataSetName) {
		String dataSetXml = this.getDownloadDataSetXml(option,dataSetName,true);
		Response response = this.consoleHandle.modifyDataSet(dataSetName, dataSetXml);
		if (response.getStatusCode() != HttpStatus.SC_OK) {
			TestSession.logger.error("Failed to create dataset, xml: " + dataSetXml);
			Assert.fail("Response status code is " + response.getStatusCode() + ", expected 200.");
		}
	}

	private void validateRetentionWorkflow(String dataSetName) throws Exception {
		WorkFlowHelper workFlowHelper = new WorkFlowHelper();
		Assert.assertTrue("Expected workflow to pass for instance " + INSTANCE1, workFlowHelper.workflowPassed(dataSetName, "retention", INSTANCE1));
		instanceDownloadExists(INSTANCE1, false, dataSetName);
		instanceDownloadExists(INSTANCE2, true, dataSetName);
	}

	private void tearDown(String dataSetName) throws Exception {
		Response response = this.consoleHandle.deactivateDataSet(dataSetName);
		Assert.assertEquals("Deactivate DataSet failed", HttpStatus.SC_OK , response.getStatusCode());

		this.consoleHandle.removeDataSet(dataSetName);
	}
}
