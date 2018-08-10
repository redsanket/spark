package hadooptest.gdm.regression;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.httpclient.HttpStatus;
import org.apache.hadoop.fs.FileStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import hadooptest.TestSession;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.DataSetTarget;
import hadooptest.cluster.gdm.DataSetXmlGenerator;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;

public class TestPreserveEmptyFolder {
    private static final String INSTANCE1 = "20180701";
    private static final String INSTANCE2 = "20180702";
    private static final String INSTANCE3 = "20180703";

    private ConsoleHandle consoleHandle = new ConsoleHandle();
    private String sourceGrid;
    private String targetGrid;
    private String datasetActivationTime;
    private String sourcePath;
    private static final String BASE_DATA_FOLDER = "/data/daqdev/";
    private static final String  INSTANCE_FILE_NAME = "instanceFile.txt";
    private List<String> sourceInstanceValueList = new ArrayList<String>(Arrays.asList(INSTANCE1, INSTANCE2, INSTANCE3)) ;
    private List<String> dataSetNameList = new ArrayList<String>();
    private String dataSetNameWithPreverseTrue = "TestRetPresEmptyFldTrue_" + System.currentTimeMillis();
    private String dataSetNameWithOutPreverse = "TestRetPresEmptyFldFalse_" + System.currentTimeMillis();
    private String dataSetNameDefaultPath = "TestRetUsualPath_" + System.currentTimeMillis();
    private HadoopFileSystemHelper hadoopFileSystemHelperSource;
    private HadoopFileSystemHelper hadoopFileSystemHelperTarget ;
    private Map<String, String> dataSetNameAndPath = new HashMap<String,String>();

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
	this.sourceGrid = datastores.get(0);
	this.targetGrid = datastores.get(1);
	
	dataSetNameAndPath.put(dataSetNameWithPreverseTrue, BASE_DATA_FOLDER + dataSetNameWithPreverseTrue + "/generate=20180701");
	dataSetNameAndPath.put(dataSetNameWithOutPreverse, BASE_DATA_FOLDER + dataSetNameWithOutPreverse + "/generate=20180701");
	dataSetNameAndPath.put(dataSetNameDefaultPath, BASE_DATA_FOLDER + dataSetNameDefaultPath + "/20180701");

	hadoopFileSystemHelperSource = new HadoopFileSystemHelper(this.sourceGrid);
	hadoopFileSystemHelperTarget = new HadoopFileSystemHelper(this.targetGrid);
 
	
	for ( String dsName : dataSetNameAndPath.keySet() ) {
	    TestSession.logger.info("creating folder base folder for " + dsName + "  path : " + dataSetNameAndPath.get(dsName));
	    createBaseDataFolder(hadoopFileSystemHelperSource, dataSetNameAndPath.get(dsName));
	}
    }

    @Test
    public void test() throws Exception {
	TestSession.logger.info("*******  TestCase ********  ");
	for ( String dsName : dataSetNameAndPath.keySet() ) {
	    checkInstanceCreated(hadoopFileSystemHelperSource, dataSetNameAndPath.get(dsName));
	    
	    // create dataset
	    if ( dsName.startsWith("TestRetPresEmptyFldTrue_")) {
		int index = dataSetNameAndPath.get(dsName).indexOf("=");
		String path = dataSetNameAndPath.get(dsName).substring(0, index) ;
		TestSession.logger.info("path - " + path);
		this.createDataSetXml(dsName, path + "%{date}", "true");
	    }
	}
    }
    
    private void createBaseDataFolder(HadoopFileSystemHelper hadoopFileSystemHelper , String folderName) throws IOException, InterruptedException {
	String fileContent = "TestingPreserveEmptyFolderDelete \n TestingPreserveEmptyFolderDelete \n TestingPreserveEmptyFolderDelete";
	hadoopFileSystemHelper.createDirectory(folderName);
	Thread.sleep(5000);
	hadoopFileSystemHelper.createFile( folderName + "/" + INSTANCE_FILE_NAME , fileContent);
	Thread.sleep(5000);
    }

    private void checkInstanceCreated(HadoopFileSystemHelper hadoopFileSystemHelper, String folderName) throws IOException, InterruptedException {
	String fullPath = folderName + "/" + INSTANCE_FILE_NAME;
	TestSession.logger.info(fullPath + " on target " + this.targetGrid);
	Thread.sleep(5000);
	Assert.assertTrue(" Failed to create " + fullPath , hadoopFileSystemHelper.exists(fullPath));
    }

    private void checkReplicationWorkFlow() throws Exception {
	for ( String dataSetName :  this.dataSetNameList) {
	    validateWorkflows(dataSetName, "replication");
	    for (String instanceDir : sourceInstanceValueList) {
		String fullPath = this.sourcePath + "generate=" + instanceDir + "/" + INSTANCE_FILE_NAME;
		TestSession.logger.info("Checking for " + fullPath  + "  on  " + this.targetGrid );
		Assert.assertTrue(fullPath + "does not exits on " + this.targetGrid, hadoopFileSystemHelperTarget.exists(fullPath));
	    }
	}
    }

    private void checkRetentionWorkFlow() throws Exception {
	for ( String dataSetName :  this.dataSetNameList) {

	    // set retention policy to zero
	    this.consoleHandle.setRetentionPolicyToAllDataSets(dataSetName , "0");
	    Thread.sleep(2000);
	    validateWorkflows(dataSetName, "retention");
	    for (String instanceDir : sourceInstanceValueList) {
		String fullPath = this.sourcePath + "generate=" + instanceDir + "/" + INSTANCE_FILE_NAME;
		TestSession.logger.info("Checking for " + fullPath  + "  on  " + this.targetGrid ); 
		Assert.assertFalse(fullPath + "does not exits on " + this.targetGrid, hadoopFileSystemHelperTarget.exists(fullPath) );
	    }
	}
    }

  //  @Test
    public void testPreserveEmptyFolder() throws Exception {
	TestSession.logger.info("  ******  testPreserveEmptyFolder  *****  ");

	for ( int i=0; i <  this.dataSetNameList.size() ; i++) {
	    String dataSetName = this.dataSetNameList.get(i);

	    if ( i == 0) {
		this.createDataSetXml(dataSetName, this.sourcePath + "generate=" , "true");
	    } else if ( i == 1) {
		this.createDataSetXml(dataSetName, this.sourcePath + "generate=" , "false");
	    }
	}
	checkReplicationWorkFlow(); 
	checkRetentionWorkFlow();
	/*if ( i == 1) {
	    Assert.assertTrue(" expected " + this.sourcePath + " to exist  on " + this.targetGrid, hadoopFileSystemHelperTarget.exists(this.sourcePath) == true);
	} else if ( i == 2) {
	    Assert.assertFalse(" expected " + this.sourcePath + " to exist  on " + this.targetGrid, hadoopFileSystemHelperTarget.exists(this.sourcePath) == true);
	}*/
    }
    
 

    private void createDataSetXml(String dataSetName , String dataPath , String preserveEmptyFolder) {
	DataSetXmlGenerator generator = new DataSetXmlGenerator();
	generator.setName(dataSetName);
	generator.setDescription(this.getClass().getSimpleName());
	generator.setCatalog(this.getClass().getSimpleName());
	generator.setActive("TRUE");
	generator.setRetentionEnabled("TRUE");
	generator.setPriority("NORMAL");
	generator.setFrequency("daily");
	generator.setDiscoveryFrequency("50");
	generator.setDiscoveryInterface("HDFS");
	generator.addSourcePath("data",  dataPath + "=%{date}");
	generator.setSource(this.sourceGrid);

	DataSetTarget target = new DataSetTarget();
	target.setName(this.targetGrid);
	target.setDateRangeStart(true, "20180601");
	target.setDateRangeEnd(false, "20180901");

	target.setHCatType("DataOnly");
	target.setNumInstances("10");
	generator.setTarget(target);
	generator.addParameter("preserveEmptyFolder", preserveEmptyFolder);

	String dataSetXml = generator.getXml();
	Response response = this.consoleHandle.createDataSet(dataSetName, dataSetXml);
	if (response.getStatusCode() != HttpStatus.SC_OK) {
	    TestSession.logger.error("Failed to create dataset, xml: " + dataSetXml);
	    Assert.fail("Response status code is " + response.getStatusCode() + ", expected 200.");
	}
	TestSession.logger.info("Wait for some time, so that dataset gets created, activated and ready for replication.");
	this.consoleHandle.sleep(5000);
    }

    private void validateUploadReplicationWorkflows(String dataSetName) throws Exception {
	WorkFlowHelper workFlowHelper = new WorkFlowHelper();
	Assert.assertTrue("Expected workflow to pass for instance " + INSTANCE1, workFlowHelper.workflowPassed(dataSetName, "replication", INSTANCE1));
	Assert.assertTrue("Expected workflow to pass for instance " + INSTANCE2, workFlowHelper.workflowPassed(dataSetName, "replication", INSTANCE2));
	Assert.assertTrue("Expected workflow to pass for instance " + INSTANCE3, workFlowHelper.workflowPassed(dataSetName, "replication", INSTANCE3));
    }

    private void validateWorkflows(String dataSetName , String facetName) throws Exception {
	WorkFlowHelper workFlowHelper = new WorkFlowHelper();
	Assert.assertTrue("Expected workflow to pass for instance " + INSTANCE1, workFlowHelper.workflowPassed(dataSetName, facetName, INSTANCE1));
	Assert.assertTrue("Expected workflow to pass for instance " + INSTANCE2, workFlowHelper.workflowPassed(dataSetName, facetName, INSTANCE2));
	Assert.assertTrue("Expected workflow to pass for instance " + INSTANCE3, workFlowHelper.workflowPassed(dataSetName, facetName, INSTANCE3));
    }

    private void tearDown(String dataSetName) throws Exception {
	Response response = this.consoleHandle.deactivateDataSet(dataSetName);
	Assert.assertEquals("Deactivate DataSet failed", HttpStatus.SC_OK , response.getStatusCode());

	this.consoleHandle.removeDataSet(dataSetName);
    }
}
