package hadooptest.gdm.regression;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.cluster.gdm.GeneratePerformanceFeeds;
import hadooptest.cluster.gdm.HTTPHandle;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;


import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.XMLConfiguration;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test Case : Verify whether counter, progress and 'Effective Data Rate' fields exists in data.load and data.transform step in acquisition and replication workflow.
 * Note : This testcase will generate the data dynamically, since we need a bigger feed to test the steps.
 * 
 * Test added for GDM Version : 6.1
 *
 */
public class TestWorkFlowStepProgress extends TestSession {

	private ConsoleHandle consoleHandle;
	private Configuration conf;
	private String dataSetName;
	private String datasetActivationTime;
	private String deploymentSuffixName;
	private WorkFlowHelper workFlowHelperObj = null;
	private GeneratePerformanceFeeds generatePerformanceFeeds  = null;
	private String targetGrid1;
	private String targetGrid2;
	private String cookie;
	private static final String baseDataSetName = "VerifyAcqRepRetWorkFlowExecutionSingleDate";
	private static final int SUCCESS = 200;
	final static Charset ENCODING = StandardCharsets.UTF_8;
	private static final String HCAT_TYPE = "DataOnly";
	private static final String START_DATE = "2013010518";   // needed for start date
	private static final String SECOND_INSTANCE = "2013010519";	// actual data instance file
	private static final String END_DATE = "2013010520";  // needed for end date

	@BeforeClass
	public static void startTestSession() throws Exception {
		TestSession.start();
	}

	public TestWorkFlowStepProgress() throws NumberFormatException, Exception {
		String configPath = Util.getResourceFullPath("gdm/conf/config.xml");
		this.conf = new XMLConfiguration(configPath);
		this.deploymentSuffixName = this.conf.getString("hostconfig.console.deployment-suffix-name");
		this.consoleHandle = new ConsoleHandle();
		HTTPHandle httpHandle = new HTTPHandle(); 
		this.cookie = httpHandle.getBouncerCookie();
		this.dataSetName = "TestWorkFlowStepProgress_" + System.currentTimeMillis();
		this.generatePerformanceFeeds  = new GeneratePerformanceFeeds(this.dataSetName , this.deploymentSuffixName.trim());

		workFlowHelperObj = new WorkFlowHelper();

		List<String> targetGrids = this.consoleHandle.getUniqueGrids();
		if (targetGrids != null  && targetGrids.size() >= 2) {
			targetGrid1 = targetGrids.get(0);
			targetGrid2 = targetGrids.get(1);
		} else {
			assertTrue("There is no enough grid installed to test this testcase." , true);
		}
	}

	@Test
	public void testWorkFlowStepProgress() throws Exception {

		// generate data
		generatePerformanceFeeds.generateFeed(SECOND_INSTANCE);

		//create dataset
		createDataSetForAcqRep( );

		// test steps in acquisition facet
		testAcquisitionWorkFlowSteps(this.dataSetName );

		// test steps in replication facet
		testReplicationWorkFlowStep(this.dataSetName);
	}

	/**
	 *  test counters, progress and  Effect Data Rate values in running state for replication workflow. 
	 * @param dataSetName - 
	 */
	public void testReplicationWorkFlowStep(String dataSetName) {
		long waitTimeForWorkflowPolling = 15 * 60 * 1000; // 15 minutes
		long sleepTime = 5000; // 5 sec  sleep time.
		long waitTime=0;
		JSONArray jsonArray = null;
		int returnValue = 0;
		boolean workflowCompleted = false;
		String runningWorkFlowTestURL =  this.consoleHandle.getConsoleURL() + "/console/api/workflows/running?datasetname="+ dataSetName +"&instancessince=F&joinType=innerJoin&facet=" +  "replication";

		while (waitTime <= waitTimeForWorkflowPolling) {

			// invoke workflow running REST API
			com.jayway.restassured.response.Response response = given().cookie(this.cookie).get(runningWorkFlowTestURL);

			// select running workflows JSONArray
			jsonArray = this.consoleHandle.convertResponseToJSONArray(response, "runningWorkflows");

			String replicationStepName = "copy." +  this.targetGrid1 + "." + this.targetGrid2;
			TestSession.logger.info("replicationStepName = " + replicationStepName);

			if (jsonArray.size() == 0) { // workflow is still not in running state 
				this.consoleHandle.sleep(sleepTime);
				waitTime += sleepTime;
			} else if(jsonArray.size() > 0) {  // workflow is in running state
				Iterator iterator = jsonArray.iterator();
				while (iterator.hasNext()) {
					JSONObject jsonObject = (JSONObject) iterator.next();
					TestSession.logger.info("running workflow response  = " + jsonObject.toString());
					String fName = jsonObject.getString("FacetName");
					String facetColo = jsonObject.getString("FacetColo");
					String executionId = jsonObject.getString("ExecutionID");
					String currentStep = jsonObject.getString("CurrentStep");
					String exitStatus = jsonObject.getString("ExitStatus");
					if (currentStep.equals(replicationStepName)) {

						// construct url to fetch the workflow detailed steps
						String testURL =this.consoleHandle.getConsoleURL() +  "/console/api/workflows/" + executionId + "/view?facet=" + fName + "&colo=" + facetColo;
						TestSession.logger.info("url = " + testURL);
						response = given().cookie(this.cookie).get(testURL);
						String res1 = response.getBody().asString();
						TestSession.logger.info(" response = " + res1);

						// convert string representing response to JSONObject
						JSONObject detailsWorkFlowJSONObject =  (JSONObject) JSONSerializer.toJSON(res1.toString());
						TestSession.logger.info("detailsWorkFlowJSONObject = " + detailsWorkFlowJSONObject.toString());

						int progressValue = 0 ;
						boolean isProgressFieldExists = false ,isEffectiveDataRateExist = false;

						// fetch WorkflowExecution array
						JSONObject workflowExecutionJSONArray = detailsWorkFlowJSONObject.getJSONObject("WorkflowExecution");
						if (workflowExecutionJSONArray != null) {
							JSONArray stepExecutionsJSONArray = workflowExecutionJSONArray.getJSONArray("Step Executions");
							Iterator stepExecutionIterator = stepExecutionsJSONArray.iterator();
							while (stepExecutionIterator.hasNext()) {
								JSONObject stepExecutionJsonObject = (JSONObject) stepExecutionIterator.next();
								String stepName = stepExecutionJsonObject.getString("Step Name").trim();
								TestSession.logger.info("Step Name = " + stepName);

								// check for counter, progress value in data.load step
								if (stepName.equals(replicationStepName)) {

									TestSession.logger.info("Found step " + replicationStepName);

									boolean isProgressFieldExistsInDataLoadStep = stepExecutionJsonObject.containsKey("Progress");
									TestSession.logger.info("isProgressFieldExists = " + isProgressFieldExistsInDataLoadStep);

									if (isProgressFieldExistsInDataLoadStep == true) {
										progressValue = Integer.parseInt(stepExecutionJsonObject.getString("Progress"));
										TestSession.logger.info("progress = " + progressValue);
									}

									isEffectiveDataRateExist = stepExecutionJsonObject.containsKey("Effective Data Rate");
									TestSession.logger.info("isEffectiveDataRateExist = " + isEffectiveDataRateExist);

									String existStatus = stepExecutionJsonObject.getString("ExitStatus");
									TestSession.logger.info(this.dataSetName + " dataset is in " + stepName  + "  and its in " + existStatus  + " state.");
									if (existStatus.equals("COMPLETED")) {
										assertTrue("Expected that data.load progress value to be 100 , but got " + progressValue ,  progressValue == 100);
										assertTrue("Expected that 'Effective Data Rate' key exists in data.load step" , isEffectiveDataRateExist == true);

										// check for counter keys in data.load step
										assertTrue("Expected that DownloadedFilesSizeInThisLoad key exists in data.load step " , (stepExecutionJsonObject.containsKey("DownloadedFilesSizeInThisLoad") == true));
										assertTrue("Expected that FileDownloadTime1Min key exists in data.load step " , (stepExecutionJsonObject.containsKey("FileDownloadTime1Min") == true));
										assertTrue("Expected that MapDownloadErrors key exists in data.load step " , (stepExecutionJsonObject.containsKey("MapDownloadErrors") == true));
										assertTrue("Expected that NumFiles1MB key exists in data.load step " , (stepExecutionJsonObject.containsKey("NumFiles1MB") == true));
										assertTrue("Expected that FileDownloadTime1Min key exists in data.load step " , (stepExecutionJsonObject.containsKey("FileDownloadTime1Min") == true));
										assertTrue("Expected that TotalDownloadedFilesSizeInLoad key exists in data.load step " , (stepExecutionJsonObject.containsKey("TotalDownloadedFilesSizeInLoad") == true));
										workflowCompleted = true;
										break;
									}
								} else {
									TestSession.logger.info(replicationStepName  + " dn't found , the current step is " + stepName);
								}
								if (workflowCompleted == true) {
									break;
								}	
							}
						}
					} // exist outter loop
					if (exitStatus.equals("COMPLETED") ) {
						workflowCompleted = true;
						break;
					}
				}
				if (workflowCompleted == true) {
					break;
				}	
			}
			if (workflowCompleted) {
				break;
			}
		}
		if (workflowCompleted == false || waitTime >= waitTimeForWorkflowPolling) {
			fail( this.dataSetName + " did not come to completed state & its TIME OUT in replication workflow.");
		}
	}


	/**
	 * test data.load and data.transfor step for counters, progress and  Effect Data Rate values in running state for acquisition  workflow. 
	 */
	public void testAcquisitionWorkFlowSteps(String dataSetName ) {
		long waitTimeForWorkflowPolling = 15 * 60 * 1000; // 15 minutes
		long sleepTime = 5000; // 5 sec  sleep time.
		long waitTime=0;
		JSONArray jsonArray = null;
		int returnValue = 0;
		boolean workflowCompleted = false;
		String runningWorkFlowTestURL =  this.consoleHandle.getConsoleURL() + "/console/api/workflows/running?datasetname="+ dataSetName +"&instancessince=F&joinType=innerJoin&facet=" +  "acquisition";

		while (waitTime <= waitTimeForWorkflowPolling) {

			// invoke workflow running REST API
			com.jayway.restassured.response.Response response = given().cookie(this.cookie).get(runningWorkFlowTestURL);

			// select running workflows JSONArray
			jsonArray = this.consoleHandle.convertResponseToJSONArray(response, "runningWorkflows");

			if (jsonArray.size() == 0) { // workflow is still not in running state 
				this.consoleHandle.sleep(sleepTime);
				waitTime += sleepTime;
			} else if(jsonArray.size() > 0) {  // workflow is in running state
				Iterator iterator = jsonArray.iterator();
				while (iterator.hasNext()) {
					JSONObject jsonObject = (JSONObject) iterator.next();
					TestSession.logger.info("running workflow response  = " + jsonObject.toString());
					String fName = jsonObject.getString("FacetName");
					String facetColo = jsonObject.getString("FacetColo");
					String executionId = jsonObject.getString("ExecutionID");
					String currentStep = jsonObject.getString("CurrentStep");
					String exitStatus = jsonObject.getString("ExitStatus");
					if (currentStep.equals("data.load")  || currentStep.equals("data.transform")) {

						// construct url to fetch the workflow detailed steps
						String testURL =this.consoleHandle.getConsoleURL() +  "/console/api/workflows/" + executionId + "/view?facet=" + fName + "&colo=" + facetColo;
						TestSession.logger.info("url = " + testURL);
						response = given().cookie(this.cookie).get(testURL);
						String res1 = response.getBody().asString();
						TestSession.logger.info(" response = " + res1);

						// convert string representing response to JSONObject
						JSONObject detailsWorkFlowJSONObject =  (JSONObject) JSONSerializer.toJSON(res1.toString());
						TestSession.logger.info("detailsWorkFlowJSONObject = " + detailsWorkFlowJSONObject.toString());

						int dataLoadProgressInDataLoadStatus = 0 , dataLoadProgressInTranformStatus = 0;
						boolean isEffectiveDataRateExistInDataLoadStep = false ,isEffectiveDataRateExistInDataTransforStep = false;

						// fetch WorkflowExecution array
						JSONObject workflowExecutionJSONArray = detailsWorkFlowJSONObject.getJSONObject("WorkflowExecution");
						if (workflowExecutionJSONArray != null) {
							JSONArray stepExecutionsJSONArray = workflowExecutionJSONArray.getJSONArray("Step Executions");
							Iterator stepExecutionIterator = stepExecutionsJSONArray.iterator();
							while (stepExecutionIterator.hasNext()) {
								JSONObject stepExecutionJsonObject = (JSONObject) stepExecutionIterator.next();
								String stepName = stepExecutionJsonObject.getString("Step Name");
								TestSession.logger.info("Step Name = " + stepName);

								// check for counter, progress value in data.load step
								if (stepName.equals("data.load")) {

									boolean isProgressFieldExistsInDataLoadStep = stepExecutionJsonObject.containsKey("Progress");
									TestSession.logger.info("isProgressFieldExists = " + isProgressFieldExistsInDataLoadStep);

									if (isProgressFieldExistsInDataLoadStep == true) {
										dataLoadProgressInDataLoadStatus = Integer.parseInt(stepExecutionJsonObject.getString("Progress"));
										TestSession.logger.info("progress = " + dataLoadProgressInDataLoadStatus);
									}

									isEffectiveDataRateExistInDataLoadStep = stepExecutionJsonObject.containsKey("Effective Data Rate");
									TestSession.logger.info("isEffectiveDataRateExist = " + isEffectiveDataRateExistInDataLoadStep);

									String existStatus = stepExecutionJsonObject.getString("ExitStatus");
									TestSession.logger.info(this.dataSetName + " dataset is in " + stepName  + "  and its in " + existStatus  + " state.");
									if (existStatus.equals("COMPLETED")) {
										assertTrue("Expected that data.load progress value to be 100 , but got " + dataLoadProgressInDataLoadStatus ,  dataLoadProgressInDataLoadStatus >= 100);
										assertTrue("Expected that 'Effective Data Rate' key exists in data.load step" , isEffectiveDataRateExistInDataLoadStep == true);

										// check for counter keys in data.load step
										assertTrue("Expected that BandwidthBytesPerSecInLoad key exists in data.load step " , (stepExecutionJsonObject.containsKey("BandwidthBytesPerSecInLoad") == true));
										assertTrue("Expected that ChunkedFileCountInLoader key exists in data.load step " , (stepExecutionJsonObject.containsKey("ChunkedFileCountInLoader") == true));
										assertTrue("Expected that DownloadedFileCountInLoader key exists in data.load step " , (stepExecutionJsonObject.containsKey("DownloadedFileCountInLoader") == true));
										assertTrue("Expected that DownloadedFilesSizeInThisLoad key exists in data.load step " , (stepExecutionJsonObject.containsKey("DownloadedFilesSizeInThisLoad") == true));
										assertTrue("Expected that FileDownloadTime1Min key exists in data.load step " , (stepExecutionJsonObject.containsKey("FileDownloadTime1Min") == true));
										assertTrue("Expected that NumFiles1MB key exists in data.load step " , (stepExecutionJsonObject.containsKey("NumFiles1MB") == true));
										assertTrue("Expected that TotalChunkedFileCountInLoader key exists in data.load step " , (stepExecutionJsonObject.containsKey("TotalChunkedFileCountInLoader") == true));
										assertTrue("Expected that TotalDownloadedFilesSizeInLoad key exists in data.load step " , (stepExecutionJsonObject.containsKey("TotalDownloadedFilesSizeInLoad") == true));
										assertTrue("Expected that TotalFileCountInLoader key exists in data.load step " , (stepExecutionJsonObject.containsKey("TotalFileCountInLoader") == true));
									}
								}
								// check for counter, progress value in data.transform step
								if (stepName.equals("data.transform")) {

									boolean isProgressFieldExistsInDataTransformStep = stepExecutionJsonObject.containsKey("Progress");
									TestSession.logger.info("isProgressFieldExistsInDataTransformStep = " + isProgressFieldExistsInDataTransformStep);

									if (isProgressFieldExistsInDataTransformStep == true)  {
										dataLoadProgressInTranformStatus = Integer.parseInt(stepExecutionJsonObject.getString("Progress"));
										TestSession.logger.info("Progress value in data.transform step  = " + dataLoadProgressInTranformStatus);
										assertTrue("Expected that progress value to with in the range 0 - 100 , but got " + dataLoadProgressInTranformStatus , ( dataLoadProgressInTranformStatus >= 0 || dataLoadProgressInTranformStatus <= 100 ));
									}

									String existStatus = stepExecutionJsonObject.getString("ExitStatus");
									TestSession.logger.info(this.dataSetName + " dataset is in " + stepName  + "  and its in " + existStatus  + " state.");
									if (existStatus.equals("COMPLETED")) {
										assertTrue("Expected that data.transform progress value to be 100 , but got " + dataLoadProgressInTranformStatus ,  dataLoadProgressInTranformStatus >= 100);
										assertTrue("Expected that 'Effective Data Rate' key exists in data.load step" , stepExecutionJsonObject.containsKey("Effective Data Rate") == true);

										// check for counter keys data.transform step
										assertTrue("Expected that AddFieldsFilter-#1[datestamp]-Modified key exists in data.load step " , (stepExecutionJsonObject.containsKey("AddFieldsFilter-#1[datestamp]-Modified") == true));
										assertTrue("Expected that InputFileCountInTransformation key exists in data.load step " , (stepExecutionJsonObject.containsKey("InputFileCountInTransformation") == true));
										assertTrue("Expected that InputParsedRecordCountInTransformation key exists in data.load step " , (stepExecutionJsonObject.containsKey("InputParsedRecordCountInTransformation") == true));
										assertTrue("Expected that InputRawRecordsReadCountInTransformation key exists in data.load step " , (stepExecutionJsonObject.containsKey("InputRawRecordsReadCountInTransformation") == true));
										assertTrue("Expected that OutputFileCountInTransformation key exists in data.load step " , (stepExecutionJsonObject.containsKey("OutputFileCountInTransformation") == true));
										assertTrue("Expected that OutputRecordCountInTransformation key exists in data.load step " , (stepExecutionJsonObject.containsKey("OutputRecordCountInTransformation") == true));
										assertTrue("Expected that SortFilter-#2[canon_query,...]-Modified key exists in data.load step " , (stepExecutionJsonObject.containsKey("SortFilter-#2[canon_query,...]-Modified") == true));
									}
								}
							}
						}
					} // if the current step is in data.commit exist out of the loop, since counter , progress values are only in data.load and data.transfor step
					if (currentStep.equals("data.commit") ) {
						workflowCompleted = true;
						break;
					}
				}
			}
			if (workflowCompleted) {
				break;
			}
		}
		if (workflowCompleted == false || waitTime >= waitTimeForWorkflowPolling) {
			fail( this.dataSetName + " did not come to completed state & its TIME OUT in acquisition workflow.");
		}
	}

	/**
	 * Create a dataset & activate it.
	 * @throws Exception
	 */
	public void createDataSetForAcqRep( ) throws Exception {
		String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/" + "GDMPerformanceDataSetWithoutPartition.xml");
		String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.dataSetName, dataSetConfigFile);
		String sourceName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Sources" , "name");
		TestSession.logger.info("**** DataSet Name = " + this.dataSetName   + " ********** ");
		dataSetXml = dataSetXml.replaceAll("FEED_NAME", this.dataSetName );
		dataSetXml = dataSetXml.replaceAll("FEED_STATS", this.dataSetName + "_stats" );
		dataSetXml = dataSetXml.replaceAll("FDI_SERVER_NAME", sourceName );
		dataSetXml = dataSetXml.replaceAll("TARGET1", this.targetGrid1 );
		dataSetXml = dataSetXml.replaceAll("TARGET2", this.targetGrid2 );
		dataSetXml = dataSetXml.replaceAll("START_DATE", this.START_DATE );
		dataSetXml = dataSetXml.replaceAll("END_DATE", this.END_DATE );
		dataSetXml = dataSetXml.replaceAll("DATA_PATH", this.getDataSetDataPath(this.dataSetName ));
		dataSetXml = dataSetXml.replaceAll("COUNT_PATH", this.getDataSetCountPath(this.dataSetName ));
		dataSetXml = dataSetXml.replaceAll("SCHEMA_PATH", this.getDataSetSchemaPath(this.dataSetName ));
		dataSetXml = dataSetXml.replaceAll("HCAT_TYPE", this.HCAT_TYPE);

		Response response = this.consoleHandle.createDataSet(this.dataSetName, dataSetXml);
		if (response.getStatusCode() != SUCCESS) {
			try {
				throw new Exception("Response status code is " + response.getStatusCode() + ", expected 200.");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		this.consoleHandle.sleep(10000);

		// activate the dataset
		response = this.consoleHandle.activateDataSet(dataSetName);
		assertTrue("Failed to activate dataset " + dataSetName , response.getStatusCode() == SUCCESS);
		this.consoleHandle.sleep(30000);
		this.datasetActivationTime = GdmUtils.getCalendarAsString();
	}

	/**
	 * returns dataset path of the dataset
	 * @param dataSetName
	 * @return
	 */
	private String getDataSetDataPath(String dataSetName) {
		String path = "/data/daqqe/data/" + dataSetName  + "/%{date}"; 
		TestSession.logger.info("paht == " + path);
		return  path;
	}

	/**
	 * returns  count path of the dataset
	 * @param dataSetName
	 * @return
	 */
	private String getDataSetCountPath(String dataSetName) {
		return "/data/daqqe/count/" + dataSetName  + "/%{date}" ;
	}

	/**
	 * returns  schema path of the datase
	 * @param dataSetName
	 * @return
	 */
	private String getDataSetSchemaPath(String dataSetName) {
		return "/data/daqqe/schema/" + dataSetName  + "/%{date}" ;
	}	


	/**
	 * Method to deactivate the dataset(s)
	 */
	@After
	public void tearDown() throws Exception {
		Response response = consoleHandle.deactivateDataSet(this.dataSetName);
		assertEquals("ResponseCode - Deactivate DataSet", 200, response.getStatusCode());
		assertEquals("ActionName.", "terminate", response.getElementAtPath("/Response/ActionName").toString());
		assertEquals("ResponseId", "0", response.getElementAtPath("/Response/ResponseId").toString());
		assertEquals("ResponseMessage.", "Operation on " + this.dataSetName + " was successful.", response.getElementAtPath("/Response/ResponseMessage/[0]").toString());
	}
}
