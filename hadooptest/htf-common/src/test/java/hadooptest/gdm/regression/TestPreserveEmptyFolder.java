package hadooptest.gdm.regression;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.lang3.StringUtils;
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
	WorkFlowHelper workFlowHelper = new WorkFlowHelper();
	TestSession.logger.info("*******  TestCase ********  ");
	for ( String dsName : dataSetNameAndPath.keySet() ) {
	    checkInstanceCreated(hadoopFileSystemHelperSource, dataSetNameAndPath.get(dsName));
	    
	    // create dataset
	    if ( dsName.startsWith("TestRetPresEmptyFldTrue_")) {
		int index = dataSetNameAndPath.get(dsName).indexOf("=");
		String path = dataSetNameAndPath.get(dsName).substring(0, index) ;
		TestSession.logger.info("path - " + path);
		this.createDataSetXml(dsName, path + "=%{date}", "true");
		
		workFlowHelper.workflowPassed(dsName, "replication", "20180701");
		this.consoleHandle.setRetentionPolicyToAllDataSets(dsName , "0");
		workFlowHelper.workflowPassed(dsName, "retention", "20180701");
		hadoopFileSystemHelperTarget.exists(BASE_DATA_FOLDER + dataSetNameWithPreverseTrue);
		
		Assert.assertTrue(BASE_DATA_FOLDER + dataSetNameWithPreverseTrue + "does not exits on " + this.targetGrid, hadoopFileSystemHelperTarget.exists(BASE_DATA_FOLDER + dataSetNameWithPreverseTrue));
		
	    } else if ( dsName.startsWith("TestRetPresEmptyFldFalse_")) {
		int index = dataSetNameAndPath.get(dsName).indexOf("=");
		String path = dataSetNameAndPath.get(dsName).substring(0, index) ;
		TestSession.logger.info("path - " + path);
		this.createDataSetXml(dsName, path + "=%{date}", "false");
		
		workFlowHelper.workflowPassed(dsName, "replication", "20180701");
		this.consoleHandle.setRetentionPolicyToAllDataSets(dsName , "0");
		workFlowHelper.workflowPassed(dsName, "retention", "20180701");
		
		Assert.assertFalse(BASE_DATA_FOLDER + dataSetNameWithOutPreverse + "does not exits on " + this.targetGrid, hadoopFileSystemHelperTarget.exists(BASE_DATA_FOLDER + dataSetNameWithOutPreverse));
		
		
	    } else  if ( dsName.startsWith("TestRetUsualPath_")) {
		int index = dataSetNameAndPath.get(dsName).lastIndexOf("/");
		String path = dataSetNameAndPath.get(dsName).substring(0, index) ;
		TestSession.logger.info("path - " + path);
		this.createDataSetXml(dsName, path + "/%{date}", "false");
		
		workFlowHelper.workflowPassed(dsName, "replication", "20180701");
		this.consoleHandle.setRetentionPolicyToAllDataSets(dsName , "0");
		workFlowHelper.workflowPassed(dsName, "retention", "20180701");
		
		Assert.assertTrue(BASE_DATA_FOLDER + dataSetNameDefaultPath + "does not exits on " + this.targetGrid, hadoopFileSystemHelperTarget.exists(BASE_DATA_FOLDER + dataSetNameDefaultPath));
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
	generator.addSourcePath("data",  dataPath);
	generator.setSource(this.sourceGrid);

	DataSetTarget target = new DataSetTarget();
	target.setName(this.targetGrid);
	target.setDateRangeStart(true, "20180601");
	target.setDateRangeEnd(false, "20180901");

	target.setHCatType("DataOnly");
	target.setNumInstances("10");
	generator.setTarget(target);
	if ( StringUtils.isNotBlank(preserveEmptyFolder)) {
	    generator.addParameter("preserveEmptyFolder", preserveEmptyFolder);
	}

	String dataSetXml = generator.getXml();
	Response response = this.consoleHandle.createDataSet(dataSetName, dataSetXml);
	if (response.getStatusCode() != HttpStatus.SC_OK) {
	    TestSession.logger.error("Failed to create dataset, xml: " + dataSetXml);
	    Assert.fail("Response status code is " + response.getStatusCode() + ", expected 200.");
	}
	TestSession.logger.info("Wait for some time, so that dataset gets created, activated and ready for replication.");
	this.consoleHandle.sleep(5000);
    }
}
