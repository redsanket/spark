package hadooptest.gdm.regression;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.cluster.gdm.HCatHelper;
import hadooptest.cluster.gdm.HTTPHandle;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * Test Case : Test whether killing replication running workflow is killed successfully
 * 
 */
public class KillRunningReplicationWorkFlowTest  extends TestSession {

	private ConsoleHandle consoleHandle;
	private List<String> hcatSupportedGrid;
	private String consoleURL;
	private String dataSetName;
	private String cookie;
	private String targetGrid1;
	private String datasetActivationTime;
	private String baseDataSetName = "VerifyAcqRepRetWorkFlowExecutionSingleDate";
	private WorkFlowHelper workFlowHelper;
	private HCatHelper hcatHelperObject = null;
	private static final String HCAT_TYPE = "DataOnly";
	private static final int SUCCESS = 200;
	private static final String SOURCE_NAME= "elrond";
	private final static String HADOOP_LS_PATH = "/console/api/admin/hadoopls?dataSource=";
	private final static String ABF_DATA_PATH = "/data/SOURCE_ABF/ABF_DAILY/";
	private static final String DATABASE_NAME = "gdm";
	private String hostName;
	private String environmentType;
	private List<String> workFlowNames;

	@BeforeClass
	public static void startTestSession() throws Exception {
		TestSession.start();
	}

	@Before
	public void setUp() throws NumberFormatException, Exception {
		this.hcatHelperObject = new HCatHelper();
		this.consoleHandle = new ConsoleHandle();
		HTTPHandle httpHandle = new HTTPHandle();
		this.consoleURL = this.consoleHandle.getConsoleURL();
		this.workFlowHelper = new WorkFlowHelper();
		this.workFlowNames = new ArrayList<String>();
		this.dataSetName = "TestKillRunningReplicationWorkFlow_" + System.currentTimeMillis();
		this.cookie = httpHandle.getBouncerCookie();
		this.environmentType = GdmUtils.getConfiguration("hostconfig.console.test_environment_type");
		if (this.environmentType.equals("staging")) {
			this.hostName = this.getReplicationHostNameForStaging();
		} else if (this.environmentType.equals("oneNode")) {
			this.hostName = this.consoleHandle.getConsoleURL();
		}
		
		List<String> targetList =  this.consoleHandle.getAllGridNames();
		if (targetList.size() > 2  && targetList.contains(SOURCE_NAME)) {
			
			if (!targetList.get(0).equals(SOURCE_NAME)) {
				this.targetGrid1 = targetList.get(0);
			} else {
				this.targetGrid1 = targetList.get(1);
			}
		} else {
			TestSession.logger.info( SOURCE_NAME  +" grid does not exists.");
		}
	}
	
	/**
	 * return the replication host dynamically fetching using yinst set command.
	 * @return
	 */
	private String getReplicationHostNameForStaging() {
		String tempConsoleHostName = Arrays.asList(this.consoleHandle.getConsoleURL().split(":")).get(1);
		String consoleHostName = tempConsoleHostName.replaceAll("//", "");
		String command = "ssh " + consoleHostName + "  " + "yinst set | grep replication_end_point" ;
		String output = this.workFlowHelper.executeCommand(command);
		TestSession.logger.info("output = " + output);
		String path = Arrays.asList(output.split(" ")).get(1).trim();
		TestSession.logger.info("Replication hostname = " + path);
		return path;
	}
	
	@Test
	public void test() throws Exception {
		
		// check whether ABF data is available on the specified source
		List<String>dates = getInstanceFiles();
		assertTrue("ABF data is missing so testing can't be done, make sure whether the ABF data path is correct...!" , dates != null);

		// create the dataset
		createDataSet();

		// activate the dataset
		this.consoleHandle.checkAndActivateDataSet(this.dataSetName);
		this.consoleHandle.sleep(40000);
		String datasetActivationTime = GdmUtils.getCalendarAsString();
		this.getRunningDataSetAndKillWorkFlow("replication");
	}

	/**
	 * Create a dataset specification configuration file.
	 */
	private void createDataSet() {
		String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/ABFHcatDataSet.xml");
		String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.dataSetName, dataSetConfigFile);
		String feedName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Parameters" , "value");
		dataSetXml = dataSetXml.replaceAll("TARGET1_NAME", this.targetGrid1 );
		dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", this.dataSetName);
		dataSetXml = dataSetXml.replaceAll("FEED_NAME", feedName );
		dataSetXml = dataSetXml.replaceAll("FEED_STATS", feedName + "_stats" );
		dataSetXml = dataSetXml.replaceAll("SOURCE_NAME", this.SOURCE_NAME );
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
	public List<String>getInstanceFiles() {
		JSONArray jsonArray = null;
		List<String>instanceDate = new ArrayList<String>();
		String testURL = this.consoleHandle.getConsoleURL() + this.HADOOP_LS_PATH + this.SOURCE_NAME + "&path=" + ABF_DATA_PATH + "&format=json";
		TestSession.logger.info("Test url = " + testURL);
		com.jayway.restassured.response.Response res = given().cookie(this.cookie).get(testURL);
		assertTrue("Failed to get the respons  " + res , (res != null ) );

		jsonArray = this.consoleHandle.convertResponseToJSONArray(res , "Files");
		TestSession.logger.info("********size = " + jsonArray.size());
		if ( jsonArray.size() > 0 ) {
			Iterator iterator = jsonArray.iterator();
			while (iterator.hasNext()) {
				JSONObject dSObject = (JSONObject) iterator.next();
				String  directory = dSObject.getString("Directory");
				TestSession.logger.info("######directory == " + directory);
				if (directory.equals("yes")) {
					String path = dSObject.getString("Path");
					List<String>instanceFile = Arrays.asList(path.split("/"));
					if (instanceFile != null ) {
						String dt = instanceFile.get(instanceFile.size() - 1);
						TestSession.logger.info("^^^^^^ date = " + dt);
						instanceDate.add(dt);
					}	
				}
			}
			return instanceDate;
		}
		return null;
	}
	
	/**
	 * Get a specified running dataset and kills its replication workflow. 
	 */
	public void getRunningDataSetAndKillWorkFlow(String facetName) {
		JSONArray jsonArray = null;
		com.jayway.restassured.response.Response response = null;
		int i = 1;
		String killTestURL = "";
		while (i <= 10)  {
			String runningWorkFlow = this.consoleURL + "/console/api/workflows/running?datasetname="+ this.dataSetName +"&instancessince=F&joinType=innerJoin";
			TestSession.logger.info("testURL = " + runningWorkFlow);
			response = given().cookie( this.cookie).get(runningWorkFlow);
			assertTrue("Failed to get the respons  " + runningWorkFlow , (response != null || response.toString() != "") );
			
		    jsonArray = this.consoleHandle.convertResponseToJSONArray(response , "runningWorkflows");
			TestSession.logger.info("size = " + jsonArray.size());
			if ( jsonArray.size() > 0 ) {
				JSONArray resourceArray = new JSONArray();
				Iterator iterator = jsonArray.iterator();
				while (iterator.hasNext()) {
					JSONObject runningJsonObject = (JSONObject) iterator.next();
					String executionId = runningJsonObject.getString("ExecutionID");
					String fName = runningJsonObject.getString("FacetName");
					String facetColo = runningJsonObject.getString("FacetColo");
					String workFlowName = runningJsonObject.getString("WorkflowName");
					this.workFlowNames.add(workFlowName.trim());

					if (fName.equals(facetName)) {
						
					//  kill the running workflow
						resourceArray.add(new JSONObject().element("ExecutionID",executionId).element("FacetName", fName).element("FacetColo", facetColo));
						
						if (this.environmentType.equals("staging") ){
							killTestURL = this.hostName + "/api/admin/workflows";
						} else if (this.environmentType.equals("oneNode")) {
							killTestURL = this.consoleURL.replace("9999",this.consoleHandle.getFacetPortNo("replication")) + "/replication/api/admin/workflows";
						}
						
						TestSession.logger.info("url  = " + killTestURL);
						TestSession.logger.info("resource = " + resourceArray.toString());
						String jobKilledResponseString = given().cookie(this.cookie).param("command", "kill").param("workflowIds" , resourceArray.toString()).post(killTestURL).getBody().asString();
						TestSession.logger.info("jobKilledResponseString  = " + jobKilledResponseString);
						JSONObject obj =  (JSONObject) JSONSerializer.toJSON(jobKilledResponseString.toString());
						JSONObject jsonResponse = obj.getJSONObject("Response");
						String responseId = jsonResponse.getString("ResponseId");
						String actionName = jsonResponse.getString("ActionName");
						assertTrue("Expected responseId to 0 but got " + responseId , responseId.equals("0"));
						assertTrue("Expected action is terminated but got " + actionName , actionName.equals("WorkflowKillAction"));
					}
				}
			}
			i++;
			this.consoleHandle.sleep(6000);
		}
	}

	// make dataset inactive
	@After
	public  void tearDown() {
		Response response = this.consoleHandle.deactivateDataSet(this.dataSetName);
		assertEquals("ResponseCode - Deactivate DataSet", 200, response.getStatusCode());
		assertEquals("ActionName.", "terminate", response.getElementAtPath("/Response/ActionName").toString());
		assertEquals("ResponseId", "0", response.getElementAtPath("/Response/ResponseId").toString());
		assertEquals("ResponseMessage.", "Operation on " + this.dataSetName + " was successful.", response.getElementAtPath("/Response/ResponseMessage/[0]").toString());
	}
}
