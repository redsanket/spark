package hadooptest.gdm.regression.encryptionZone;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.httpclient.HttpStatus;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import hadooptest.TestSession;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.DataSetTarget;
import hadooptest.cluster.gdm.DataSetXmlGenerator;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;
import hadooptest.gdm.regression.HadoopFileSystemHelper;
import org.junit.Assert;

public class TestEncryptionZone extends TestSession {

    private String datasetActivationTime;
    private WorkFlowHelper workFlowHelper;
    private ConsoleHandle consoleHandle = new ConsoleHandle();
    private HadoopFileSystemHelper hadoopFileSystemHelper;
    private final static String START_INSTANCE_RANGE = "20180202";
    private final static String END_INSTANCE_RANGE = "20180416";
    private final static String EZ_PATH = "/data/daqdev/ez";
    private final static String NON_EZ_PATH = "/data/daqdev/nonEZ";
    private final static String NON_EZ_SOURCE_DATA_PATH = NON_EZ_PATH + "/data/";
    private final static String NON_EZ_TARGET_DATA_PATH = NON_EZ_PATH + "/data/";
    private final static String EZ_SOURCE_DATA_PATH = EZ_PATH + "/data/";
    private final static String EZ_TARGET_DATA_PATH = EZ_PATH + "/data/";
    private final static String EZ_WORKING_DIR =  EZ_PATH + "/working/";
    private final static String EZ_EVICTION_DIR =  EZ_PATH + "/todelete/";
    private final static String NON_EZ_WORKING_DIR =  NON_EZ_PATH + "/working/";
    private final static String NON_EZ_EVICTION_DIR =  NON_EZ_PATH + "/todelete/";
    private final static String CRYPTO_LIST_CMD = "hdfs crypto -listZones";
    private final static String HADOOP_HOME = "export HADOOP_HOME=/home/gs/hadoop/current;";
    private final static String JAVA_HOME = "export JAVA_HOME=/home/gs/java/jdk64/current/;";
    private final static String HADOOP_CONF_DIR = "export HADOOP_CONF_DIR=/home/gs/conf/current;";
    private final static String KINIT_COMMAND = "kinit -k -t /homes/dfsload/dfsload.dev.headless.keytab dfsload;";
    private String [] dataSetNames = {
	    "SourceEZ-TargetEZ_Replication_DataOnly_" + System.currentTimeMillis() ,
	    	"SourceNonEZ-TargetEZ_Replication_DataOnly_" + System.currentTimeMillis(),
	    	"SourceEZ-TargetNonEZ_Replication_DataOnly_" + System.currentTimeMillis()
	    //"TestEZ1_to_EZ2_Replication_DataOnly_" + System.currentTimeMillis()
    };

    private String [] sourcePath = {
	    EZ_SOURCE_DATA_PATH,
	    NON_EZ_SOURCE_DATA_PATH,
	    EZ_SOURCE_DATA_PATH
    };

    private String [] targetPath = {
	    EZ_TARGET_DATA_PATH,
	    EZ_TARGET_DATA_PATH,
	    NON_EZ_SOURCE_DATA_PATH
    };

    private boolean testCasePassedFlag = false;
    private String sourceCluster;
    private String targetCluster;

    @BeforeClass
    public static void startTestSession() throws Exception {
	TestSession.start();
    }

    @Before
    public void setUp() throws NumberFormatException, Exception {
	String suffix = String.valueOf(System.currentTimeMillis());
	consoleHandle = new ConsoleHandle();
	workFlowHelper = new WorkFlowHelper();
	List<String> allGrids = this.consoleHandle.getHCatEnabledGrid();
	if (allGrids.size() < 2) {
	    throw new Exception("Unable to run test: 2 hcat enabled grid datasources are required.");
	}
	sourceCluster=allGrids.get(0);
	targetCluster=allGrids.get(1);

	// Check whether source and target path has EZ 
	if (! isSourcePathEZ(sourceCluster)) {
	    Assert.fail(sourceCluster +  "'s " + EZ_PATH  + "  is not a EZ path.");
	}

	if (! isTargetPathEZ(targetCluster)) {
	    Assert.fail(targetCluster +  "'s " + EZ_PATH  + "  is not a EZ path.");
	}

	// create an instance folder & create a data file
	hadoopFileSystemHelper = new HadoopFileSystemHelper(sourceCluster);
	if (hadoopFileSystemHelper.exists(EZ_SOURCE_DATA_PATH + END_INSTANCE_RANGE + "/" + "dataFile.txt") ) {
	    TestSession.logger.info("Yes " + EZ_SOURCE_DATA_PATH + END_INSTANCE_RANGE + "/" + "dataFile.txt   exists");
	} else  {
	    TestSession.logger.info("Yes " + EZ_SOURCE_DATA_PATH + END_INSTANCE_RANGE + "/" + "dataFile.txt  does not exists");
	    Assert.fail(EZ_SOURCE_DATA_PATH + "/" + "dataFile.txt  does not exists. Testcase wn't  be executed.");
	}
    }

    private String getNameNodeHostName(String clusterName) {
	String hostName = workFlowHelper.executeCommand("yinst range -ir \"(@grid_re.clusters." + clusterName.trim() + ".namenode)\"").trim();
	return hostName;
    }

    private boolean isSourcePathEZ(final String clusterName) throws IOException, InterruptedException {
	return checkForEzPath(clusterName, EZ_PATH);
    }

    private boolean isTargetPathEZ(final String clusterName) throws IOException, InterruptedException {
	return checkForEzPath(clusterName, EZ_PATH);
    }

    private boolean checkForEzPath(final String clusterName,final String path) throws IOException, InterruptedException {
	HadoopFileSystemHelper hadoopFileSystemHelper = new HadoopFileSystemHelper(clusterName);
	// check if source path exists
	if (hadoopFileSystemHelper.exists(EZ_PATH) ) {
	    final String SSH_COMMAND = "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null  " + getNameNodeHostName(clusterName) + "  \""  + HADOOP_HOME + JAVA_HOME + HADOOP_CONF_DIR + KINIT_COMMAND + CRYPTO_LIST_CMD + "\"";
	    TestSession.logger.info("SSH_COMMAND - " + SSH_COMMAND);
	    String output = workFlowHelper.executeCommand(SSH_COMMAND);
	    TestSession.logger.info("output - " + output);
	    List<String>  outputList = Arrays.asList(output.split("\n"));
	    if ( outputList.size() > 0) {
		return outputList.parallelStream().filter( item -> item.startsWith(EZ_PATH)).count() > 0 ? true : false;
	    }
	}
	return false;
    }

    @Test
    public void TestEZReplication() throws InterruptedException {
	TestSession.logger.info("Testing EZ Path");

	for ( int i = 0; i < dataSetNames.length ; i++) {
	    createDataSet(dataSetNames[i] , sourcePath[i] , targetPath[i]);
	    
	    Thread.sleep(5000);
	    
	    boolean result = workFlowHelper.workflowPassed(dataSetNames[i], "replication", END_INSTANCE_RANGE);
	    Assert.assertTrue("Expected workflow to pass, but failed", result );
	    if ( result) {
		testCasePassedFlag = true;
	    }

	    // check for replication workflow
	    workFlowHelper.checkWorkFlow(dataSetNames[i], "replication", this.datasetActivationTime);

	    // deactivate the dataset before applying retention to dataset
	    Response response = this.consoleHandle.deactivateDataSet(dataSetNames[i]);
	    Assert.assertEquals("ResponseCode - Deactivate DataSet failed", HttpStatus.SC_OK, response.getStatusCode());

	    // set retention policy to zero
	    this.consoleHandle.setRetentionPolicyToAllDataSets(dataSetNames[i] , "0");
	    Thread.sleep(2000);

	    // check for retention workflow
	    workFlowHelper.checkWorkFlow(dataSetNames[i] , "retention", this.datasetActivationTime);

	    // set to true if both replication and retention are successful
	    testCasePassedFlag = true;
	}
    }

    private void createDataSet(String dName , String sourcePath, String targetPath) {
	DataSetXmlGenerator generator = new DataSetXmlGenerator();
	generator.setName(dName);
	generator.setDescription(this.getClass().getSimpleName());
	generator.setCatalog(this.getClass().getSimpleName());
	generator.setActive("TRUE");
	generator.setRetentionEnabled("TRUE");
	generator.setPriority("NORMAL");
	generator.setFrequency("daily");
	generator.setDiscoveryFrequency("10");
	generator.setDiscoveryInterface("HDFS");
	generator.addSourcePath("data", sourcePath + "%{date}");
	generator.setSource(sourceCluster);

	/*if (dName.split("-")[1].indexOf("NonEZ") > -1) {
	    generator.addParameter("working.dir", NON_EZ_WORKING_DIR);
	    generator.addParameter("eviction.dir", NON_EZ_EVICTION_DIR);
	} else {
	    generator.addParameter("working.dir", EZ_WORKING_DIR);
	    generator.addParameter("eviction.dir", EZ_EVICTION_DIR);
	}*/

	DataSetTarget target = new DataSetTarget();
	target.setName(targetCluster);
	target.setDateRangeStart(true, START_INSTANCE_RANGE);
	target.setDateRangeEnd(true, END_INSTANCE_RANGE);
	target.setHCatType("DataOnly");
	target.addPath("data", targetPath + dName + "/%{date}");
	target.setNumInstances("5");
	target.setReplicationStrategy("DistCp");
	generator.setTarget(target);
	String dataSetXml = generator.getXml();

	Response response = this.consoleHandle.createDataSet(dName, dataSetXml);
	if (response.getStatusCode() != HttpStatus.SC_OK) {
	    TestSession.logger.error("Failed to create dataset, xml: " + dataSetXml);
	    Assert.fail("Response status code is " + response.getStatusCode() + ", expected 200.");
	}

	TestSession.logger.info("Wait for some time, so that dataset gets created, activated and ready for replication.");
	this.consoleHandle.sleep(5000);
    }

}