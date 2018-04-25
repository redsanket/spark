package hadooptest.gdm.regression.stackIntegration;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.cluster.gdm.HCatHelper;
import hadooptest.cluster.gdm.HTTPHandle;
import hadooptest.cluster.gdm.JSONUtil;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;
import hadooptest.gdm.regression.HadoopFileSystemHelper;
import hadooptest.gdm.regression.integration.CreateIntegrationDataSet;
import hadooptest.gdm.regression.integration.clusterHealth.CheckClusterHealth;
import hadooptest.gdm.regression.stackIntegration.lib.CheckDistedHadoopVersion;
import hadooptest.gdm.regression.stackIntegration.lib.CommonFunctions;
import hadooptest.gdm.regression.stackIntegration.lib.SystemCommand;

import org.apache.commons.lang3.tuple.ImmutablePair;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.jayway.restassured.path.json.JsonPath;

/**
 * Integration emitter code that does a GDM replication.
 *
 */
public class IntegrationEmitterTest  extends TestSession {

    private ConsoleHandle consoleHandle;
    private List<String> hcatSupportedGrid;
    private List<String> installedGrids;
    private String dataSetName;
    private String cookie;
    private String datasetActivationTime;
    private String enableHCAT;
    private String sourceCluster;
    private String destinationCluster1;
    private String destinationCluster2;
    private int duration;
    private int noOfFeeds;
    private int frequency;
    private String hadoopVesion;
    private boolean isDataSetEligibleForDelete = true;
    private String freq;
    private String targetName;
    private List<String> feedList;
    private WorkFlowHelper workFlowHelper;
    private CreateIntegrationDataSet createIntegrationDataSetObj;
    private HCatHelper hcatHelperObject = null;
    private String HCAT_TYPE =  "DataOnly" ;
    private JSONUtil jsonUtil;
    private static final String TARGET_START_TYPE_MIXED = "Mixed";
    private static final String TARGET_START_TYPE_DATAONLY = "DataOnly";
    private static final int SUCCESS = 200;
    private final static String HADOOP_LS_PATH = "/console/api/admin/hadoopls?dataSource=";
    private final static String ABF_DATA_PATH = "/data/SOURCE_ABF/ABF_Daily/";
    private static final String DATABASE_NAME = "gdm";
    private static final String HADOOPQA_AS_HDFSQA_IDENTITY_FILE = "/homes/hadoopqa/.ssh/flubber_hadoopqa_as_hdfsqa";
    private List<String> targetClusterList = new ArrayList<String>();

    @BeforeClass
    public static void startTestSession() throws Exception {
	TestSession.start();
    }
    
    public String getTargetName() {
        return targetName;
    }

    public void setTargetName(String targetName) {
        this.targetName = targetName;
    }

    @Before
    public void setUp() throws NumberFormatException, Exception {
	this.consoleHandle = new ConsoleHandle();
	HTTPHandle httpHandle = new HTTPHandle();

	// Get all the clusters that GDM knows about
	this.installedGrids = this.consoleHandle.getUniqueGrids();

	// get the source cluster
	this.sourceCluster = GdmUtils.getConfiguration("testconfig.IntegrationTest.sourceCluster");
	TestSession.logger.info("sourceCluster  = " + sourceCluster);
	if ( (this.sourceCluster != null) && ( ! this.installedGrids.contains(this.sourceCluster)) )  {
	    fail("Source cluster is null or Specified a wrong source cluster that is not configured.");
	}

	// get the destination cluster
	String targetClusterNames = GdmUtils.getConfiguration("testconfig.IntegrationTest.destinationCluster");
	TestSession.logger.info("targetClusterNames = " + targetClusterNames);
	this.targetClusterList = Arrays.asList(targetClusterNames.split(" "));
	TestSession.logger.info("targetClusterList  = " + targetClusterList);

	for ( String clusterName : this.targetClusterList) {
	    if (!this.installedGrids.contains(clusterName)) {
		fail("Destination cluster is null or Specified a wrong destination cluster that is not configured.");
		System.exit(1);
	    }
	    setTargetName(clusterName);
	}
	
	String replHostName = this.consoleHandle.getFacetHostName("replication" , "blue" , "gq1");
	System.out.println("------- replHostName --   " + replHostName);
	
	CheckDistedHadoopVersion checkDistedHadoopVersionObject = new CheckDistedHadoopVersion(this.getTargetName() , replHostName);
	if (checkDistedHadoopVersionObject.setYinstSettingAndRestartFacet()){
	    if (! checkDistedHadoopVersionObject.checkClusterHadoopVersionAndReplDistedVersionMatches()){
		    System.out.println("Expected hadoop version - " + checkDistedHadoopVersionObject.getHadoopVersion() + "  but got " + checkDistedHadoopVersionObject.getLatestExistingHadoopVersion());
		    org.junit.Assert.assertFalse("Expected hadoop version - " + checkDistedHadoopVersionObject.getHadoopVersion() + "  but got " + checkDistedHadoopVersionObject.getLatestExistingHadoopVersion(), false);
		}    
	} else {
	    org.junit.Assert.assertFalse("failed to restart replication facet." , false);
	}
	
	createIntegrationDataSetObj = new CreateIntegrationDataSet();
	createIntegrationDataSetObj.setHcatType(TARGET_START_TYPE_DATAONLY);
	createIntegrationDataSetObj.setTargeList(this.targetClusterList);

	// check for cluster health checkup.
	this.checkClusterHealth();

	// get cookie for the headless user.
	this.cookie = httpHandle.getBouncerCookie();
	this.workFlowHelper = new WorkFlowHelper();
    }
    
    public String getEnvVariable(String var) {

TestSession.logger.info("DEBUGPHWooooo: in getEnvVariable, got var: " + "var");

	String retVal = "none";

	Map<String, String> env = System.getenv();
	for (String envName : env.keySet() ) {
TestSession.logger.info("DEBUGPHWooooo: in getEnvVariable for loop, got var: " + var + " and envName " + envName);
		if ( envName.equals(var) ) {
TestSession.logger.info("DEBUGPHWooooo: in getEnvVariable for loop if statement, got var: " + var);
			TestSession.logger.info("DEBUGPHW: in getEnvVariable, got var: " + var + ", and envName: " + 
				env.get(envName));	
			retVal = env.get(envName);
			break;
		}	
	}
	return retVal;
    }

    @Test
    public void integrationTest() throws Exception {
	
	// check whether instance files are available on the specified source
	List<String> dates = getInstanceFileDates();
	assertTrue("Instance files dn't exists at " + ABF_DATA_PATH  +  "  on  " + this.sourceCluster , dates != null);

	this.dataSetName = getDataSetName();

	if (!checkDataSetAlreadyExists()) {
	    // gridci-3045, support pipelines with encryption zones
	    // force it for now, why isn't env var coming across??
	    if ( getEnvVariable("IS_GDM_REPL_SRCDEST_EZ_ENABLED") != "true" ) {
	        createIntegrationDataSetObj.createDataSetEzEnabled();
		TestSession.logger.info("DEBUGPHW: using EZ,  IS_GDM_REPL_SRCDEST_EZ_ENABLED is: " + getEnvVariable("IS_GDM_REPL_SRCDEST_EZ_ENABLED"));
	    }
	    else {
	        createIntegrationDataSetObj.createDataSet();
		TestSession.logger.info("DEBUGPHW: NOT using EZ,  IS_GDM_REPL_SRCDEST_EZ_ENABLED is: " + getEnvVariable("IS_GDM_REPL_SRCDEST_EZ_ENABLED"));
	    }
	    this.modifyDataSet();

	    // activate the dataset
	    this.consoleHandle.checkAndActivateDataSet(this.dataSetName);
	    this.consoleHandle.sleep(40000);
	} else {
	    TestSession.logger.info(this.getDataSetName() + " dataSet already exists...");
	}

	String datasetActivationTime = GdmUtils.getCalendarAsString();  

	// check for replication workflow is success for each instance
	for (String date : dates ) {
	    this.workFlowHelper.checkWorkFlow(this.dataSetName , "replication" , datasetActivationTime , date);
	    this.consoleHandle.sleep(2000);
	}
	
	for ( String clusterName : this.targetClusterList) {

	    // Check whether _SUCCESS file exists.
	    String finalDataPath = "/data/daqdev/abf/data/" + this.dataSetName  + "/20130309/_SUCCESS";
	    TestSession.logger.info("Checking for  "  + finalDataPath + "  in " + clusterName);

	    HadoopFileSystemHelper fs = new HadoopFileSystemHelper(clusterName);
	    if (fs.exists(finalDataPath) == false ) {
		isDataSetEligibleForDelete = false;
		fail("failed to success file - " + finalDataPath );
	    } else {
		/**
		 * If every thing goes well, dataset is eligible is delete.
		 * If either workflow is failed or _SUCCESS  file is missing. the following statement is not executed.
		 */
		isDataSetEligibleForDelete = true;	
	    }
	}
	
    }

    /**
     * Check whether dataSet for the current hour is already created.
     * @return return true if dataset already exists else return false
     */
    public boolean checkDataSetAlreadyExists() {
	List<String> dataSetNameList = this.consoleHandle.getAllDataSetName();
	if (dataSetNameList.size() == 0) {
	    TestSession.logger.info("There is no dataset on the console ");
	    return false;
	} else return dataSetNameList.contains(this.dataSetName.trim());
    }

    /**
     * Return the current hour dataSetName
     * @return
     */
    public String getDataSetName() {
	Calendar dataSetCal = Calendar.getInstance();
	SimpleDateFormat feed_sdf = new SimpleDateFormat("yyyyMMdd");
	feed_sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
	long dataSetHourlyTimeStamp = Long.parseLong(feed_sdf.format(dataSetCal.getTime()));
	String cName = this.targetClusterList.get(0).trim();
	String dSName = cName + "_Integration_Testing_DS_" + dataSetHourlyTimeStamp + "00";
	return dSName;
    }

    private void modifyDataSet() {
	String dataSetConfigFile = createIntegrationDataSetObj.getDataSetPath();
	TestSession.logger.info("modifyDataSet()  =  dataSetConfigFile  = " + dataSetConfigFile);
	String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.dataSetName, dataSetConfigFile);
	TestSession.logger.debug(dataSetXml);
	dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", this.dataSetName);
	dataSetXml = dataSetXml.replaceAll("FEED_NAME", "temp" );
	dataSetXml = dataSetXml.replaceAll("FEED_STATS", "temp" + "_stats" );
	dataSetXml = dataSetXml.replaceAll("SOURCE_NAME", this.sourceCluster );
	dataSetXml = dataSetXml.replace("HCAT_TYPE", this.HCAT_TYPE);
	dataSetXml = dataSetXml.replace("ABF-DATA-PATH", this.ABF_DATA_PATH + "%{date}");
	dataSetXml = dataSetXml.replace("HCAT_TABLE_NAME", this.dataSetName);
	dataSetXml = dataSetXml.replaceAll("DATABASE_NAME", this.DATABASE_NAME);
	Response response = this.consoleHandle.createDataSet(this.dataSetName, dataSetXml);
	if (response.getStatusCode() != SUCCESS) {
	    try {
		throw new Exception("Response status code is " + response.getStatusCode() + ", expected 200.");
	    } catch (Exception e) {
		e.printStackTrace();
	    }
	}
	this.consoleHandle.sleep(5000);
    }

    /**
     * First checks whether ABF data exists on the grid for a given path, if exists returns instance date(s) 
     * @return
     */
    public List<String> getInstanceFileDates() {
	JSONArray jsonArray = null;
	List<String> instanceDates = new ArrayList<String>();
	String testURL = this.consoleHandle.getConsoleURL() + this.HADOOP_LS_PATH + this.sourceCluster + "&path=" + ABF_DATA_PATH + "&format=json";
	TestSession.logger.info("Test url = " + testURL);
	com.jayway.restassured.response.Response res = given().cookie(this.cookie).get(testURL);
	assertTrue("Failed to get the response  " + res , (res != null ) );

	jsonArray = this.consoleHandle.convertResponseToJSONArray(res , "Files");
	if ( jsonArray.size() > 0 ) {
	    Iterator iterator = jsonArray.iterator();
	    while (iterator.hasNext()) {
		JSONObject dSObject = (JSONObject) iterator.next();
		String  directory = dSObject.getString("Directory");
		TestSession.logger.info("#directory = " + directory);
		if (directory.equals("yes")) {
		    String path = dSObject.getString("Path");
		    List<String>instanceFile = Arrays.asList(path.split("/"));
		    if (instanceFile != null ) {
			String dt = instanceFile.get(instanceFile.size() - 1);
			TestSession.logger.info("date = " + dt);
			instanceDates.add(dt);
		    }
		}
	    }
	    return instanceDates;
	}
	return null;
    }

    public void checkClusterHealth() throws IOException {
	CheckClusterHealth checkClusterHealthObject = new CheckClusterHealth();
	for ( String clusterName : this.targetClusterList) {
	    checkClusterHealthObject.setClusterName(clusterName);
	    checkClusterHealthObject.checkClusterMode();
	    boolean safeMode = checkClusterHealthObject.getClusterMode();
	    TestSession.logger.info("cluster mode = " + safeMode);
	    List<String> dataPathList = checkClusterHealthObject.getPathsList();

	    TestSession.logger.info("CleanUpFlag is: " + checkClusterHealthObject.getCleanUpFlag());

	    if (checkClusterHealthObject.getCleanUpFlag() && safeMode == false) {
		for ( String dataPath : dataPathList)
		    checkClusterHealthObject.deletePath(dataPath);
	    }
	}
    }

    @After
    public void tearDown() {

	if ( isDataSetEligibleForDelete ) {
	    // make dataset inactive
	    Response response = this.consoleHandle.deactivateDataSet(this.dataSetName);
	    assertEquals("ResponseCode - Deactivate DataSet", 200, response.getStatusCode());
	    assertEquals("ActionName.", "terminate", response.getElementAtPath("/Response/ActionName").toString());
	    assertEquals("ResponseId", "0", response.getElementAtPath("/Response/ResponseId").toString());
	    assertEquals("ResponseMessage.", "Operation on " + this.dataSetName + " was successful.", response.getElementAtPath("/Response/ResponseMessage/[0]").toString());

	    // remove dataset
	   // this.removeDataSet(); // commenting it right now, we can uncomment in future if dataset needs to be removed			
	}
    }

    /**
     * Remove the current dataset.
     */
    public void removeDataSet() {
	this.jsonUtil = new JSONUtil();
	String resource = this.jsonUtil.constructResourceNamesParameter(Arrays.asList(this.dataSetName));
	String consoleUrl = this.consoleHandle.getConsoleURL();
	com.jayway.restassured.response.Response res = given().cookie(cookie).param("resourceNames", resource).param("command","remove").
		post(consoleUrl + "/console/rest/config/dataset/actions");

	String resString = res.asString();
	TestSession.logger.info("response after trying to remove the active dataset - " + this.jsonUtil.formatString(resString));

	JsonPath jsonPath = new JsonPath(resString);
	String actionName = jsonPath.getString("Response.ActionName");
	String responseId = jsonPath.getString("Response.ResponseId");
	String responseMessage = jsonPath.getString("Response.ResponseMessage");
	TestSession.logger.info("actionName = "+actionName  + "   ResponseId = "  +responseId + "    responseMessage = "+responseMessage);
	assertTrue("Expected remove action name , but found " + actionName , actionName.equals("remove"));
    }
    
}
