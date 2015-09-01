package hadooptest.gdm.regression.api;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.io.comparator.LastModifiedFileComparator;
import org.apache.commons.io.filefilter.FileFileFilter;
import org.junit.Before;
import org.junit.BeforeClass;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.jayway.restassured.path.json.JsonPath;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.HTTPHandle;
import hadooptest.cluster.gdm.JSONUtil;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;

/**
 * TestCase : This testcase on Versioning feature.
 * Description : When user makes any changes to any attribute to the dataset, each changes are recorded in a file with dataset name + user name +  time stamp.
 * The location of the version files can get using ygrid_gdm_console_server.config_version_repository_path 
 * 
 * This covers GDM 5.7.0 and 5.8.0 Versioning feature.
 *
 */
@Category(SerialTests.class)
public class TestVersioning extends TestSession {

	private ConsoleHandle consoleHandle;
	private String dataSetName;
	private String baseDataSetName;
	private String cookie;
	private JSONUtil jsonUtil;
	private HTTPHandle httpHandle ;
	private String url;
	private Configuration conf;
	private String versionPath;
	private String hostName;
	private String environmentType;
	private List<String> fileNames = new ArrayList<String>();
	private static final String DATASET_VERSIONING_PATH = "/grid/0/yroot/var/yroots/console/tmp/gdm_configuration_version_repository/datasetconf/dataset/";
	private static final String PauseRetentionPath ="/console/rest/config/dataset/actions";
	private static final String VERSION_YINST_SETTING = "ygrid_gdm_console_server.config_version_repository_path";
	public static final int SUCCESS = 200;
	private static final int SLEEP_TIME = 40000;

	@BeforeClass
	public static void startTestSession() {
		TestSession.start();
	}

	@Before
	public void setup() throws Exception {
		this.consoleHandle = new ConsoleHandle();
		this.url = this.consoleHandle.getConsoleURL();
		TestSession.logger.info("url = " + this.url);
		httpHandle = new HTTPHandle();
		this.cookie = httpHandle.getBouncerCookie();
		jsonUtil = new JSONUtil();
		this.dataSetName = "DataSetVersioningTest_"  + System.currentTimeMillis();

		this.baseDataSetName =  "VerifyAcqRepRetWorkFlowExecutionSingleDate";

		// Read dataset and replace source and target values
		String dataSetXml = this.consoleHandle.getDataSetXml(this.baseDataSetName);

		// Replace the dataset name
		dataSetXml = dataSetXml.replaceAll(this.baseDataSetName, this.dataSetName);

		// Create a new dataset
		Response response = this.consoleHandle.createDataSet(this.dataSetName, dataSetXml);
		assertTrue("Failed to create a dataset " +this. dataSetName , response.getStatusCode() == 200);

		this.consoleHandle.sleep(SLEEP_TIME);

		WorkFlowHelper workFlowHelperObj  = new WorkFlowHelper();
		String configPath = Util.getResourceFullPath("gdm/conf/config.xml");
		this.conf = new XMLConfiguration(configPath);
		this.environmentType = this.conf.getString("hostconfig.console.test_environment_type");
		if (environmentType.equals("oneNode")) {
			if ( checkWhetherFileExists("/grid/0/yroot/var/yroots/console/tmp/") ) {
				TestSession.logger.info("File exist");
			} else {
				TestSession.logger.info("File dn't exist");
			}
		} else if (environmentType.equals("staging")) {
			String consoleURL = this.conf.getString("hostconfig.console.staging_console_url");
			this.hostName = Arrays.asList(consoleURL.split(":")).get(1).replaceAll("//", "").trim();
			final String YINST_COMMAND = "ssh " + hostName + "  yinst set | grep  "+ VERSION_YINST_SETTING ;
			String output = workFlowHelperObj.executeCommand(YINST_COMMAND);
			this.versionPath = Arrays.asList(output.split(" ")).get(1).trim() + "/dataset/";
		} else  {
			TestSession.logger.info("****** Specified invalid test environment ******** ");
			fail("Unknown test environment specified.");
		}
	}

	@Test
	public void testVersioning() throws ConfigurationException {
		testVersioningWhileCreatingDataSet();
		testActivitingDataSetCreatesNewVersioningFile();
		testDisableRetentionCreatesVersioningFile();
		testDeactivateTargets();
		testRemoveTargetFromDataSetAndCheckForCreationOfVersioningFile();
		testDeactivatingDataSetCreateVersioningFile();
		testRemovingDataSetCreatesVersioningFile();
	}

	/**
	 * Test Scenario : Verify whether creating a dataset creates two new versioning file
	 * @throws ConfigurationException 
	 */
	public void testVersioningWhileCreatingDataSet() throws ConfigurationException {
		WorkFlowHelper workFlowHelperObj  = new WorkFlowHelper();
		String configPath = Util.getResourceFullPath("gdm/conf/config.xml");
		this.conf = new XMLConfiguration(configPath);
		if (this.environmentType.equals("oneNode")) {
			getFilesBelongingToDataSet(this.dataSetName , this.DATASET_VERSIONING_PATH, 3);
		} else if (this.environmentType.equals("staging")) {
			int count = getFileCount(hostName , versionPath , this.dataSetName);
			assertTrue("expected fileCount is 3  but got " + count  , count == 3);
		}
	}

	/**
	 * TestCase :  Verify whether activating the dataset create a new versioning file.
	 */
	public void testActivitingDataSetCreatesNewVersioningFile() {
		try {
			this.consoleHandle.activateDataSet(this.dataSetName);
			TestSession.logger.info("Waiting for some time, so that version files are created.");
			this.consoleHandle.sleep(SLEEP_TIME);
			if (this.environmentType.equals("oneNode")) {
				getFilesBelongingToDataSet(this.dataSetName , this.DATASET_VERSIONING_PATH, 5);
			} else if (this.environmentType.equals("staging")) {
				int count = this.getFileCount(hostName , versionPath , this.dataSetName) ;
				assertTrue("expected fileCount is 3  but got " + count  , count == 5);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Test Scenario : Verify whether turning off the rentention creates a new versioning file. 
	 * 
	 */
	public void testDisableRetentionCreatesVersioningFile() {
		String datasetName = this.dataSetName;
		String resource = jsonUtil.constructResourceNamesParameter(Arrays.asList(datasetName));
		com.jayway.restassured.response.Response response = given().cookie(cookie).param("resourceNames", resource).param("command","disableRetention").post(this.url + PauseRetentionPath);
		JsonPath jsonPath = response.jsonPath();
		TestSession.logger.info("Response = " + jsonPath.prettyPrint());
		assertTrue("Expected disableretention but got " + jsonPath.getString("Response.ActionName") , jsonPath.getString("Response.ActionName").equals("disableretention"));
		assertTrue("Expected 0 but got " + jsonPath.getString("Response.ResponseId") , jsonPath.getString("Response.ResponseId").equals("0"));
		assertTrue("Expected successful but got " + jsonPath.getString("Response.ResponseMessage") , jsonPath.getString("Response.ResponseMessage").contains("successful"));

		TestSession.logger.info("Waiting for some time, so that version files are created.");
		this.consoleHandle.sleep(SLEEP_TIME);
		
		if (this.environmentType.equals("oneNode")) {
			getFilesBelongingToDataSet(this.dataSetName , this.DATASET_VERSIONING_PATH, 7);
		} else if (this.environmentType.equals("staging")) {
			int count = this.getFileCount(hostName , versionPath , this.dataSetName) ;
			assertTrue("expected fileCount is 3  but got " + count  , count == 7);
		}
	}

	/**
	 *  Test Scenario : Verify whether deactivating the targets creates a new versioning file. 
	 */
	public void testDeactivateTargets() {
		this.consoleHandle.deactivateTargetsInDataSet(this.dataSetName);

		TestSession.logger.info("Waiting for some time, so that version files are created.");
		this.consoleHandle.sleep(SLEEP_TIME);
		if (this.environmentType.equals("oneNode")) {
			getFilesBelongingToDataSet(this.dataSetName , this.DATASET_VERSIONING_PATH, 9);
		} else if (this.environmentType.equals("staging")) {
			int count = this.getFileCount(hostName , versionPath , this.dataSetName) ;
			assertTrue("expected fileCount is 3  but got " + count  , count == 9);
		}
	}

	/**
	 * Test Scenario : Verify whether removing targets from the dataset, creates a new versioning file.
	 */
	public void testRemoveTargetFromDataSetAndCheckForCreationOfVersioningFile() {
		this.consoleHandle.removeTargetsFromDataset(this.dataSetName);
		TestSession.logger.info("Waiting for some time, so that version files are created.");
		this.consoleHandle.sleep(SLEEP_TIME);
		if (this.environmentType.equals("oneNode")) {
			getFilesBelongingToDataSet(this.dataSetName , this.DATASET_VERSIONING_PATH, 11);
		} else if (this.environmentType.equals("staging")) {
			int count = this.getFileCount(hostName , versionPath  , this.dataSetName) ;
			assertTrue("expected fileCount is 3  but got " + count  , count == 11);
		}
	}

	/**
	 * Test Scenario : Verify whether deactivating the dataset creates a new versioning file
	 */
	public void testDeactivatingDataSetCreateVersioningFile()  {
		Response response = this.consoleHandle.deactivateDataSet(this.dataSetName);
		assertTrue("Failed to deactivate the dataset " +this.dataSetName , response.getStatusCode() == 200);

		TestSession.logger.info("Waiting for some time, so that version files are created.");
		this.consoleHandle.sleep(SLEEP_TIME);
		if (this.environmentType.equals("oneNode")) {
			getFilesBelongingToDataSet(this.dataSetName , this.DATASET_VERSIONING_PATH, 13);
		} else if (this.environmentType.equals("staging")) {
			int count = this.getFileCount(hostName , versionPath , this.dataSetName) ;
			assertTrue("expected fileCount is 3  but got " + count  , count == 13);
		}
	}

	/**
	 * Remove the deactivated dataset and check the versioning file is created.
	 */
	public void testRemovingDataSetCreatesVersioningFile() {
		this.consoleHandle.removeDataSet(this.dataSetName);
		TestSession.logger.info("Waiting for some time, so that version files are created.");
		this.consoleHandle.sleep(SLEEP_TIME);
		
		if (this.environmentType.equals("oneNode")) {
			getFilesBelongingToDataSet(this.dataSetName , this.DATASET_VERSIONING_PATH, 14);
		} else if (this.environmentType.equals("staging")) {
			int count = this.getFileCount(hostName , versionPath , this.dataSetName) ;
			assertTrue("expected fileCount is 3  but got " + count  , count == 14);
		}
	}

	/**
	 * Check whether the file path exists
	 * @param filePath
	 * @return
	 */
	private boolean checkWhetherFileExists(String filePath) {
		File f = new File(filePath);
		return f.exists();
	}

	/**
	 * Verify whether number of files exits in the given path for a dataset.
	 * @param dataSetName
	 * @param filePath
	 * @param expectedFileCount
	 */
	private void getFilesBelongingToDataSet(String dataSetName , String filePath , int expectedFileCount) {

		// making sure that filesystem.xml files are also created on the file system
		this.consoleHandle.sleep(SLEEP_TIME);

		File f = new File(filePath);
		File []allFilesInDirectory = f.listFiles((FileFilter) FileFileFilter.FILE);
		assertTrue("Failed to get files from "+ filePath + allFilesInDirectory != null);
		Arrays.sort(allFilesInDirectory, LastModifiedFileComparator.LASTMODIFIED_COMPARATOR);
		int countFiles = 0;
		for (File file : allFilesInDirectory) {
			if ( file.getName().startsWith(dataSetName) ) {
				String str = new Date(file.lastModified()).toString();
				TestSession.logger.info(str + " - " + file.getName());
				fileNames.add(file.getName());
				countFiles ++;
			}
		}
		assertTrue("expected fileCount is " + expectedFileCount  +"  but got " +countFiles  , countFiles == expectedFileCount);
	}

	/**
	 * Execute the command on the console host and get the file count,
	 * @param hostName
	 * @param versionDSPath
	 * @param dataSetName
	 * @return
	 */
	private int getFileCount(String hostName , String versionDSPath , String dataSetName) {
		WorkFlowHelper workFlowHelperObj  = new WorkFlowHelper();
		final String YINST_COMMAND = "ssh " + hostName + " ls -ltr " + versionDSPath + "/" + dataSetName + "*.*"+ " | wc -l  ";
		String dsCount = workFlowHelperObj.executeCommand(YINST_COMMAND).trim();
		return Integer.parseInt(dsCount);
	}
}