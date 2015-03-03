package hadooptest.gdm.regression.crossHadoopVersion;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.cluster.gdm.HTTPHandle;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.XMLConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * Test Case : To verify whether replication workflow correct between different clusters ( actually testing replication between two different hadoop versions)
 * 
 * Scenario :
 * 			1) Use HDFS api's to copy the dataset files on HDFS.
 * 			2) Create a Matrix using the existing datasource.
 * 			3) Create a datasource to avoid path collision.
 * 			4) Create datasets using the created matrix in step 2.
 * 			5) Navigate all the datasets and check for replication workflow.
 *
 */
public class NewVerifyAllFacetsWorkFlowAcrossHadoopVersionsTest extends TestSession {

	private ConsoleHandle consoleHandle;
	private String cookie;
	private Configuration conf;
	private String gdmVersion;
	private WorkFlowHelper workFlowHelperObj = null;
	private List<String> testMatrixList ;
	private Response response;
	private String dataPath;
	private List<String> instanceDateList ;
	private List<String> datasets = new ArrayList<String>();
	private List<String> datasetActivationTimeList = new ArrayList<String>();
	private Map<String,String> dataSetActivationTimeMap = new HashMap<String , String>();
	private Map<String,String> dataSourceMap = new HashMap<String , String>();
	private static final int SUCCESS = 200;
	private static final String BASE_PATH = "/data/daqdev";
	private final static String LOG_FILE = "/home/y/libexec/yjava_tomcat/webapps/logs/FACET_NAME-application.log";

	@BeforeClass
	public static void startTestSession() throws Exception {
		TestSession.start();
	}

	@Before
	public void setUp() throws NumberFormatException, Exception {
		String configPath = Util.getResourceFullPath("gdm/conf/config.xml");
		this.conf = new XMLConfiguration(configPath);
		this.gdmVersion = this.conf.getString("hostconfig.console.gdm-version").trim();
		HTTPHandle httpHandle = new HTTPHandle();
		this.consoleHandle = new ConsoleHandle();
		this.cookie = httpHandle.getBouncerCookie();
		this.workFlowHelperObj = new WorkFlowHelper();
		this.dataPath = "Cross-Hadoop-Version-Testing-" +  System.currentTimeMillis();

		List<String> gridNames = this.consoleHandle.getAllGridNames();

		// copy data to the grid
		for (String gridName : gridNames ) {
			this.checkPathExistAndHasPermission(gridName, BASE_PATH , this.dataPath);
			String newDataSourceName = gridName + "_" + System.currentTimeMillis();
			this.createDataSource(gridName , newDataSourceName);
		}

		// create test matrix
		testMatrixList = createTestMatrix( gridNames);
		if (testMatrixList == null) {
			fail("Unable to create the test matrix");
		}
		TestSession.logger.info(testMatrixList);

		// create a datasets
		this.createDataSets();
	}

	/**
	 * Test replication workflow for all the datasets that this test has created.
	 * @throws IOException 
	 * 
	 */
	@Test
	public void testRepWorkFlowExecution() throws IOException {
		
		for (String dataSetName : datasets) {						
			String datasetActivationTime = this.dataSetActivationTimeMap.get(dataSetName);
			TestSession.logger.info("**** Verifying replication workflow for " + dataSetName);
			
			// check for workflow
			this.checkWorkFlow(dataSetName , "replication" , datasetActivationTime ,  this.instanceDateList);
		}
	}

	// check whether path exists and has permission
	private void checkPathExistAndHasPermission(String clusterName , String basePath , String dataPath) throws IOException, InterruptedException {
		CreateInstancesAndInstanceFiles createInstanceObject = new CreateInstancesAndInstanceFiles(clusterName , basePath , dataPath);
		createInstanceObject.execute();
		this.instanceDateList = createInstanceObject.getInstanceList();
	}

	/**
	 * Method that creates the new dataset from the basedataset and activates.
	 * The number of dataset(s) that gets created are dependent upon the number of targets specified on the config.xml
	 */
	private void createDataSets() {

		// Navigate the testMatrix, where each element in the testmatrix is the targets for the  dataset
		for (String t : testMatrixList) {

			String tar[] = t.split(":");
			String targetCluster1 = tar[0].trim();
			String targetCluster2 = tar[1].trim();
			TestSession.logger.info(" cluster1 = " + targetCluster1  + "    cluster2 = " + targetCluster2);

			// create dataset Name
			String dataSetName = "GDMWorkFlowTestBw_" + targetCluster1 + "_" + targetCluster2 + "_" + System.currentTimeMillis();
			TestSession.logger.info("dataSetName  = "+dataSetName);

			this.createDataSet(dataSetName , targetCluster1 , targetCluster2);
			datasets.add(dataSetName);
		}
	}

	/**
	 * Create the dataset with the specified dataset name and targets
	 * @param dataSetName
	 * @param target1
	 * @param target2
	 */
	private void createDataSet(String dataSetName, String target1 , String target2) {

		String SOURCE_DATA_PATH = this.BASE_PATH + "/"  +  this.dataPath + "/%{date}" ;
		String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/" + "GDMValidatingHadoopDataSet.xml");
		String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(dataSetName, dataSetConfigFile);
		dataSetXml = dataSetXml.replaceAll("FEED_NAME", dataSetName );
		dataSetXml = dataSetXml.replaceAll("FEED_STATS", dataSetName + "_stats" );
		String targetName = this.dataSourceMap.get(target2);
		dataSetXml = dataSetXml.replaceAll("SOURCE_NAME", target1 );
		dataSetXml = dataSetXml.replaceAll("TARGET_NAME", targetName );
		dataSetXml = dataSetXml.replaceAll("TARGET_ONE_TYPE_START", "offset" );
		dataSetXml = dataSetXml.replaceAll("TARGET_ONE_START_DATE", "30" );
		dataSetXml = dataSetXml.replaceAll("TARGET_ONE_TYPE_END", "Offset" );
		dataSetXml = dataSetXml.replaceAll("TARGET_ONE_END_DATE", "0" );
		dataSetXml = dataSetXml.replaceAll("REPLICATION_SOURCE_DATA_AVAILABLE_PATH", SOURCE_DATA_PATH);
		dataSetXml = dataSetXml.replaceAll("CUSTOM_DATA_PATH", "/data/daqqe/data/" + dataSetName  + "/%{date}");
		dataSetXml = dataSetXml.replaceAll("TABLE_NAME", dataSetName);
		dataSetXml = dataSetXml.replaceAll("GROUP_NAME", "users");
		dataSetXml = dataSetXml.replaceAll("DATA_OWNER", "");

		TestSession.logger.info("**** DataSet Name = " + dataSetXml   + " ********** ");
		Response response = this.consoleHandle.createDataSet(dataSetName, dataSetXml);
		if (response.getStatusCode() != SUCCESS) {
			try {
				throw new Exception("Response status code is " + response.getStatusCode() + ", expected 200.");
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else if (response.getStatusCode() == SUCCESS)  {
			TestSession.logger.info( dataSetName + "  was successfully created.");
		}
		this.consoleHandle.sleep(30000);

		// activate the dataset
		TestSession.logger.info("Activating " + dataSetName);
		response = this.consoleHandle.activateDataSet(dataSetName);
		assertTrue("Failed to activate dataset " + dataSetName , response.getStatusCode() == SUCCESS);
		this.consoleHandle.sleep(30000);
		String datasetActivationTime = GdmUtils.getCalendarAsString().trim();
		this.dataSetActivationTimeMap.put(dataSetName.trim() , datasetActivationTime);
	}

	/**
	 * Create DataSource for each target, in order to avoid target collision
	 * @param DataSourceName existing target datasource
	 * @param newDataSourceName - new datasource name
	 */
	private void createDataSource(String existingDataSourceName , String newDataSourceName) {
		String xml = this.consoleHandle.getDataSourcetXml(existingDataSourceName);
		xml = xml.replaceFirst(existingDataSourceName,newDataSourceName);

		// change the colo name, so that GDM can consider this as cross colo and use WEBHDFS.
		xml = xml.replaceAll("gq1", "ne1");
		TestSession.logger.info("New DataSource Name = " + xml);;
		boolean isDataSourceCreated = this.consoleHandle.createDataSource(xml);
		assertTrue("Failed to create a DataSource specification " + newDataSourceName , isDataSourceCreated == true);
		this.consoleHandle.sleep(30000);
		this.dataSourceMap.put(existingDataSourceName, newDataSourceName);
	}

	/**
	 * Method to create the test matrix
	 * example : If suppose i have cluster1(grima) and cluster2(densea)
	 * 			then  i have to following test matrix 
	 * 			cluster1(grima) and cluster2(densea)
	 * 			cluster2(densea) and cluster1(grima)
	 * Note : this is not a full fledged matrix, but testing between two hadoop version should be good.
	 * @param target
	 * @return
	 */
	private List<String> createTestMatrix(List<String> grids) {
		List<String> targets = new ArrayList<String>();
		TestSession.logger.info("Installed grids = " + grids);

		for ( int i=0;i<grids.size() - 1 ; i++) {
			for ( int j = i + 1; j< grids.size() ; j++) {
				TestSession.logger.info(grids.get(i) + ":" +grids.get(j));
				if (! (grids.get(i).trim().equals((grids.get(j))))) {
					targets.add(grids.get(i) + ":" +grids.get(j));
				}
			}
		}

		// create matrix
		List<String> testMatrixList = new ArrayList<String>();
		for (String tar : targets){
			testMatrixList.add(tar);
			List<String>tar1 = Arrays.asList(tar.split(":"));

			// swap the targets
			testMatrixList.add(tar1.get(1) + ":" + tar1.get(0));
		}

		TestSession.logger.info("testMatrixList  = " + testMatrixList);
		return testMatrixList;
	}

	/**
	 * Check for different workflow for all the instances of the dataset
	 * @param dataSetName
	 * @param facetName
	 * @param activationTime
	 * @param instanceList
	 */
	private void checkWorkFlow(String dataSetName , String facetName , String activationTime , List<String> instanceList) {
		long waitTimeForWorkflowPolling = 15 * 60 * 1000;
		long waitTime=0;
		String workFlowResult = null;
		com.jayway.restassured.response.Response workFlowResponse = null;
		String completedWorkFlowTestURL = this.consoleHandle.getConsoleURL() + "/console/api/workflows/completed?datasetname="+ dataSetName +"&instancessince=F&joinType=innerJoin&facet=" + facetName;
		String failedWorkFlowTestURL = this.consoleHandle.getConsoleURL() + "/console/api/workflows/failed?datasetname=" + dataSetName +"&instancessince=F&joinType=innerJoin&facet=" + facetName;
		String runningWorkFlowTestURL =  this.consoleHandle.getConsoleURL() + "/console/api/workflows/running?datasetname="+ dataSetName +"&instancessince=F&joinType=innerJoin&facet=" + facetName;

		// check workflow for all the instance of the datasets
		for ( String instanceDate : instanceList) {

			TestSession.logger.info("Checking workflow for " + dataSetName   + " dataset and for " + instanceDate);

			// check for different states of the workflow
			while (waitTime <= waitTimeForWorkflowPolling) {
				long sleepTime = 5000;
				workFlowResponse = given().cookie(this.cookie).get(completedWorkFlowTestURL);
				workFlowResult = this.workFlowHelperObj.checkWorkFlowStatus(workFlowResponse , "completedWorkflows" , instanceDate);
				if (workFlowResult.equals("completed") ) {
					workFlowResult = "completed";
					break;
				}
				workFlowResponse = given().cookie(this.cookie).get(failedWorkFlowTestURL);
				workFlowResult = this.workFlowHelperObj.checkWorkFlowStatus(workFlowResponse , "failedWorkflows" , instanceDate);
				if (workFlowResult.equals("failed") ) {
					workFlowResult = "failed";
					break;
				}
				workFlowResponse = given().cookie(this.cookie).get(runningWorkFlowTestURL);
				workFlowResult = this.workFlowHelperObj.checkWorkFlowStatus(workFlowResponse , "runningWorkflows" , instanceDate);
				if (workFlowResult.equals("running") ) {
					workFlowResult = "running";
				}
				this.consoleHandle.sleep(sleepTime);
				waitTime += sleepTime;
			}
			if (waitTime >= waitTimeForWorkflowPolling) {
				if ( workFlowResult.equals("running")) {
					TestSession.logger.info("Timeout : " + dataSetName  + "   is still running replication workflow");
					fail( dataSetName + " is taking time than usual time.");
				}
			}
			if( workFlowResult.equals("failed")) {
				fail(dataSetName + "  failed : Reason");
			}
			TestSession.logger.info(" ************ " + dataSetName + " completed successfully ************ ");
			assertTrue(" ************ " + dataSetName + " completed successfully ************ " ,  workFlowResult.equals("completed"));
		}

	}

	/**
	 * deactivate the dataset(s)	
	 */
	@After
	public void tearDown() {
		if (datasets != null && datasets.size() > 0) {
			for (String dataSetName : datasets) {
				TestSession.logger.info("Deactivate "+ dataSetName);
				Response response = this.consoleHandle.deactivateDataSet(dataSetName);
				assertTrue("Failed to deactivate dataset " + dataSetName , response.getStatusCode() == SUCCESS);
				assertTrue("Expected terminate action name but got " + response.getElementAtPath("/Response/ActionName").toString() , response.getElementAtPath("/Response/ActionName").toString().equals("terminate") );
				assertTrue("Expected to get 0 response id, but got " + response.getElementAtPath("/Response/ResponseId").toString() , response.getElementAtPath("/Response/ResponseId").toString().equals("0"));
			}
		}
	}

}

