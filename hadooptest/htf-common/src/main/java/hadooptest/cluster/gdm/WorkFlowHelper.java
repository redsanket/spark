package hadooptest.cluster.gdm;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.*;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

import com.jayway.restassured.path.json.JsonPath;

import hadooptest.TestSession;

/**
 * This is the workflow helper class, having methods to check workflow and methods to create dataset files.
 * 
 */
public class WorkFlowHelper {

	private ConsoleHandle consoleHandle;
	private Response response;

	private static final int SUCCESS = 200;
	private static final long waitTimeBeforeWorkflowPollingInMs =  180000L;
	private static final long waitTimeBetweenWorkflowPollingInMs = 60000L;
	private static final long timeoutInMs =  300000L;
	public static final String DISCOVERY_MONITOR = "/api/discovery/monitor";
	private static final int PASS = 1;
	private static final int FAIL = 0;
	private final static String LOG_FILE = "/grid/0/yroot/var/yroots/FACET_NAME/home/y/libexec/yjava_tomcat/webapps/logs/FACET_NAME-application.log";
	private HTTPHandle httpHandle;
	private String cookie;

	public WorkFlowHelper() {
		this.consoleHandle = new ConsoleHandle();
		httpHandle = new HTTPHandle();
		this.cookie = httpHandle.getBouncerCookie();
	}

	/**
	 * Method that checks whether workflow is completed and returns PASS else checks whether workflow failed and returns FAIL 
	 * for any given facet.
	 * @param response
	 * @param facetName
	 * @return
	 */
	public int isFacetWorkflowPassOrFail(Response  response , String facetName) {
		boolean workFlowSuccessFlag = false , workFlowFailed  = false;
		workFlowSuccessFlag = this.consoleHandle.isWorkflowCompleted(response, facetName);
		if (!workFlowSuccessFlag) {
			workFlowFailed = this.consoleHandle.isWorkflowFailed(response, facetName);
			if (workFlowFailed) {
				return FAIL;
			}
		}
		return PASS;
	}

	/**
	 * Method check that checks for workflow pass or fail status for a given 
	 * @param dataSetName
	 * @param facetName
	 * @param datasetActivationTime
	 * @return
	 */
	public boolean checkFacetWorkFlow(String dataSetName , String facetName , String datasetActivationTime ) {
		boolean result = false;
		TestSession.logger.info("Verifying " + facetName +" workflow for " + dataSetName);
		this.consoleHandle.sleep(30000);
		this.response = this.consoleHandle.checkDataSet(dataSetName);
		assertTrue("Failed to checkDataSet dataset " + dataSetName , response.getStatusCode() == SUCCESS);

		long currentTotalWaitingTime = ( waitTimeBeforeWorkflowPollingInMs - waitTimeBetweenWorkflowPollingInMs );
		TestSession.logger.info("Sleeping for " + currentTotalWaitingTime + " ms before checking workflow status");
		this.consoleHandle.sleep(currentTotalWaitingTime);

		int workFlowResult = -1;
		while (currentTotalWaitingTime < timeoutInMs) {
			TestSession.logger.info("Sleeping for " + waitTimeBetweenWorkflowPollingInMs + " ms before checking workflow status");

			this.consoleHandle.sleep(waitTimeBetweenWorkflowPollingInMs);
			currentTotalWaitingTime = currentTotalWaitingTime + waitTimeBetweenWorkflowPollingInMs;
			this.response = this.consoleHandle.getCompletedJobsForDataSet(datasetActivationTime, GdmUtils.getCalendarAsString(), dataSetName);
			workFlowResult = isFacetWorkflowPassOrFail(this.response , facetName);
			if (workFlowResult == FAIL) {
				result = false;
				printMessage(facetName, dataSetName, datasetActivationTime, this.cookie );
				fail(dataSetName + " failed.");
				break;
			} else if (workFlowResult == PASS) {
				result = true;
				TestSession.logger.info(facetName +" workflow for " + dataSetName + "  passed ..!");
				break;
			}
		}
		if ((currentTotalWaitingTime >= timeoutInMs) && workFlowResult == -1 ) {
			fail(dataSetName + " workflow has timed out.");
		}
		this.consoleHandle.sleep(30000);
		return result;
	}
	
	
	/**
	 * checks whether the dataset has come to the running state. If the 
	 * @param dataSetName
	 * @param facetName
	 * @return returns true if dataset is in running state else returns false
	 */
	public boolean checkWhetherDataSetReachedRunningState( String dataSetName  , String facetName) {
		boolean isWorkFlowRunning = false;
		String runningWorkFlowTestURL =  this.consoleHandle.getConsoleURL() + "/console/api/workflows/running?datasetname="+ dataSetName +"&instancessince=F&joinType=innerJoin&facet=" + facetName;
		TestSession.logger.info("runningWorkFlowTestURL =  "  +  runningWorkFlowTestURL);
		long waitTimeForWorkflowPolling = 5 * 60 * 1000;
		long sleepTime = 15000; // 15 sec  sleep time.
		long waitTime=0;
		while (waitTime <= waitTimeForWorkflowPolling) {
			com.jayway.restassured.response.Response workFlowResponse = given().cookie(this.cookie).get(runningWorkFlowTestURL);
			String workFlowResult = checkWorkFlowStatus(workFlowResponse , "runningWorkflows");
			if (workFlowResult.equals("running") ) {
				isWorkFlowRunning = true;
				break;
			} else {
				this.consoleHandle.sleep(sleepTime);
				waitTime += sleepTime;
			}
		}
		return isWorkFlowRunning;
	}

	/**
	 * Checks for the workflow of the specified facet. This method checks for all the workflow ( Failed , Running & Completed )
	 * A timeout is set , if workflow does not come to running state. This method can be used to test for a specific instance workflow complete too.
	 * 
	 * example : checkWorkFlow(dataSetName , "replication" , this.datasetActivationTime , "20130725"); // check workflow by datasetname , facetName, dataset activation time and instance name 
	 * 			 checkWorkFlow(dataSetName , "acquisition" , datasetActivationTime );  // check workflow by datasetname , facetName and dataset activation time
	 *           checkWorkFlow(fiveMinuteDataSetName, FacetName, datasetActivationTime  ,  "Yes");  // // check workflow by datasetname , facetName, dataset activation time and create report Yes or No
	 *           checkWorkFlow(fiveMinuteDataSetName, FacetName, datasetActivationTime  , "20130725", "Yes");   // check workflow by datasetname , facetName, dataset activation time , instance name & create report Yes or No
	 * 
	 * @param dataSetName - name of the dataset
	 * @param facetName  - name of the facet example : acquisistion , replication, retention
	 * @param dataSetActivationTime  - string representing the time of dataset activated
	 * @throws ParseException 
	 */
	public void checkWorkFlow(String... args) {

		String dataSetName = args[0];
		String facetName = args[1];
		String dataSetActivationTime = args[2];
		String instanceDate = null;
		boolean checkInstanceDate = false;
		boolean reportPerformanceValue = false;
		Map<String, String> healthCheckUpMap; 
		double minSystemLoad=0;
		double tempSystemLoad= 0.0;
		double maxSystemLoad=0;
		int argSize = args.length - 1;
		if (argSize == 3) {
			instanceDate = args[args.length - 1];
			TestSession.logger.info("argsSize  = " +argSize + "  instanceDate  = " + instanceDate );
			if (instanceDate.toUpperCase().equals("YES")) {
				reportPerformanceValue  = true;
			} else {
				checkInstanceDate = true;
				TestSession.logger.info("Search for instance workflow = " + instanceDate);
			}
		} else if (argSize == 4) {
			instanceDate = args[3];
			checkInstanceDate = true;
			String performance = args[args.length - 1];
			TestSession.logger.info("InstanceDate = " + instanceDate);
			TestSession.logger.info(" argsSize  = " + argSize +  "  performance  = " + performance ); 
			if (performance.toUpperCase().equals("YES")) {
				reportPerformanceValue  = true;
			}
		}
		TestSession.logger.info("Verifying  workflow for " + dataSetName);
		Response response = this.consoleHandle.checkDataSet(dataSetName);
		assertTrue("Failed to checkDataSet dataset " + dataSetName , response.getStatusCode() == SUCCESS);

		// Get the healthcheck up before starting the workflow
		if(reportPerformanceValue == true) {
			Map<String , String> applicationSummary  = getFacetHealthDetails(facetName);
			String systemLoad = applicationSummary.get("System Load").replace("%", "").trim();
			tempSystemLoad = Double.valueOf(systemLoad);
			TestSession.logger.info("taking the init minSystemLoad = "  +  minSystemLoad);
			minSystemLoad = tempSystemLoad;
			maxSystemLoad  = tempSystemLoad;
		}
		TestSession.logger.info("Waiting for "+ facetName + "  workflow to start before checking workflow status");
		this.consoleHandle.sleep(120000);

		boolean isWorkFlowRunning = false , isWorkFlowCompleted = false, isWorkFlowFailed = false;
		com.jayway.restassured.response.Response workFlowResponse = null;
		String runningWorkFlowTestURL =  this.consoleHandle.getConsoleURL() + "/console/api/workflows/running?datasetname="+ dataSetName +"&instancessince=F&joinType=innerJoin&facet=" + facetName;
		String completedWorkFlowTestURL = this.consoleHandle.getConsoleURL() + "/console/api/workflows/completed?datasetname="+ dataSetName +"&instancessince=F&joinType=innerJoin&facet=" + facetName;
		String failedWorkFlowTestURL = this.consoleHandle.getConsoleURL() + "/console/api/workflows/failed?datasetname=" + dataSetName +"&instancessince=F&joinType=innerJoin&facet=" + facetName;
		long waitTimeForWorkflowPolling = 0;
		if (checkInstanceDate == true) {
			waitTimeForWorkflowPolling = 40 * 60 * 1000; // 40 minutes  this is due to large dataset files 
		} else if (checkInstanceDate == false) {
			waitTimeForWorkflowPolling = 15 * 60 * 1000; // 15 minutes
		}
		long sleepTime = 5000; // 5 sec  sleep time.
		long waitTime=0;

		while (waitTime <= waitTimeForWorkflowPolling) {
			if ( isWorkFlowRunning == false) {
				System.out.println("runningWorkFlowTestURL = " + runningWorkFlowTestURL);
				workFlowResponse = given().cookie(this.cookie).get(runningWorkFlowTestURL);
				if (checkInstanceDate) {
					String workFlowResult = checkWorkFlowStatus(workFlowResponse , "runningWorkflows" , instanceDate);
					if (workFlowResult.equals("running") ) {
						isWorkFlowRunning = true;
						//break;
					}
				} else if (checkWorkFlowStatus(workFlowResponse , "runningWorkflows").equals("running") ) {
					isWorkFlowRunning = true;
				}
			}  if (isWorkFlowCompleted == false) {
				System.out.println("completedWorkFlowTestURL = "+ completedWorkFlowTestURL);
				workFlowResponse = given().cookie(this.cookie).get(completedWorkFlowTestURL);
				if (checkInstanceDate) {
					String workFlowResult = checkWorkFlowStatus(workFlowResponse , "completedWorkflows" , instanceDate);
					if (workFlowResult.equals("completed") ) {
						isWorkFlowCompleted = true;
						break;
					}
				} else if (checkWorkFlowStatus(workFlowResponse , "completedWorkflows").equals("completed") ) {
					isWorkFlowCompleted = true;
					break;
				}
			}  if (isWorkFlowFailed == false) {
				System.out.println("failedWorkFlowTestURL = " + failedWorkFlowTestURL);
				workFlowResponse = given().cookie(this.cookie).get(failedWorkFlowTestURL);
				if (checkInstanceDate) {
					String workFlowResult = checkWorkFlowStatus(workFlowResponse , "failedWorkflows" , instanceDate);
					if (workFlowResult.equals("failed") ) {
						isWorkFlowFailed = true;
						break;
					}
				} else if (checkWorkFlowStatus(workFlowResponse , "failedWorkflows").equals("failed") ) {
					isWorkFlowFailed = true;
					break;
				}
			}
			if(reportPerformanceValue == true) {
				Map<String , String> applicationSummary  = getFacetHealthDetails(facetName);
				String systemLoad = applicationSummary.get("System Load").replaceAll("%", "").trim();
				tempSystemLoad = Double.valueOf(systemLoad);
				if(tempSystemLoad > maxSystemLoad) {
					maxSystemLoad = tempSystemLoad;
				} else if (tempSystemLoad < minSystemLoad) {
					 minSystemLoad = tempSystemLoad;
				}	
			}
			this.consoleHandle.sleep(sleepTime);
			waitTime += sleepTime;
		}

		// check workflow status 
		if (waitTime >= waitTimeForWorkflowPolling) {
			if (isWorkFlowRunning) { // workflow is still in running state, but took more than max time out
				TestSession.logger.info("=======================================running ================================================================");
				if (reportPerformanceValue == true) {
					collectDataForReporting(runningWorkFlowTestURL, "runningWorkflows" , dataSetName , Double.toString(minSystemLoad) , Double.toString(maxSystemLoad));
				}
				fail("Time out during  " + facetName  + "  workfing is still running. " );
			} else if (isWorkFlowRunning == false) { // workflow did not come to running state
				if (reportPerformanceValue == true) {
					collectDataForReporting(runningWorkFlowTestURL, "runningWorkflows" , dataSetName , Double.toString(minSystemLoad) , Double.toString(maxSystemLoad));
				}
				fail("Time out : looks like the " + dataSetName +"  dn't come to running state.");
			}
		} else if (isWorkFlowFailed) { // workflow is in failed state
			TestSession.logger.info(facetName  + " failed : " + dataSetName + "  dataset \n Reason :  ");
			TestSession.logger.info("=======================================  failed ================================================================");
			if (reportPerformanceValue == true) {
				collectDataForReporting(failedWorkFlowTestURL, "failedWorkflows" , dataSetName , Double.toString(minSystemLoad) , Double.toString(maxSystemLoad));
			}
			this.printMessage(facetName, dataSetName, dataSetActivationTime, this.cookie);
			fail(facetName  + " failed : " + dataSetName + "  dataset, Reason :  " );
		} else if (isWorkFlowCompleted) { // workflow successfully completed
			TestSession.logger.info("=======================================  completed ================================================================"); 
			if (reportPerformanceValue == true) {
				collectDataForReporting(completedWorkFlowTestURL, "completedWorkflows" , dataSetName , Double.toString(minSystemLoad) , Double.toString(maxSystemLoad));
			}
			assertTrue(facetName + " completed : " + dataSetName + "   dataset  ", isWorkFlowCompleted == true);
		}
	}
		
	/**
	 * Navigate throught the workflow response and create the performance report.
	 * @param workFlowResponse
	 * @param workFlowType
	 * @param datasetName
	 * @param minSystemLoad
	 * @param maxSystemLoad
	 * @throws ParseException 
	 */
	public void collectDataForReporting(String workFlowURL , String workFlowType , String datasetName , String minSystemLoad , String maxSystemLoad)  {
		com.jayway.restassured.response.Response workFlowResponse = given().cookie(this.cookie).get(workFlowURL);
		JSONArray jsonArray = this.consoleHandle.convertResponseToJSONArray(workFlowResponse, workFlowType);
		SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
		if (jsonArray.size() > 0) {
			Iterator iterator = jsonArray.iterator();
			while (iterator.hasNext()) {
				JSONObject jsonObject = (JSONObject) iterator.next();
				Date startDate=null , endDate=null;
				try {
					startDate = format.parse(jsonObject.getString("StartTime"));
					endDate =  format.parse(jsonObject.getString("EndTime"));
				} catch (ParseException e) {
					e.printStackTrace();
				}
				long diffTiime = endDate.getTime() - startDate.getTime();
				String facetName = jsonObject.getString("FacetName") ;
				String data = facetName + "\t" + jsonObject.getString("WorkflowName") + "\t"  +   minSystemLoad + "\t" +maxSystemLoad  + "\t"  + startDate + "\t" + 
						endDate + "\t" +   diffTiime + " ms"   + "\t"  +  jsonObject.getString("Elapsed")  + "\t"  + jsonObject.getString("Attempt") + " Attempt" +  "\t" + jsonObject.getString("ExitStatus")  + "\n";
				writeExecutionResultToFile(datasetName , facetName , data );
			}
		}
	}

	private String checkWorkFlowStatus(com.jayway.restassured.response.Response workFlowResponse , String... args) {
		JSONArray jsonArray = null;
		String currentWorkFlowStatus = null;
		String workFlowStatus = args[0];
		int argsSize = args.length - 1;
		jsonArray = this.consoleHandle.convertResponseToJSONArray(workFlowResponse, workFlowStatus);

		// if user want to check for specified instance of the workflow.
		if (argsSize > 0) {
			System.out.println("****************** checking for instance **********");
			currentWorkFlowStatus = getStatus(jsonArray , args[args.length - 1]);
		} else  if (argsSize == 0) {

			// if user just want to check for workflow, then set it to "-1"
			currentWorkFlowStatus = getStatus(jsonArray, "-1");
		}
		return currentWorkFlowStatus;
	}
	
	


	private String getStatus(JSONArray jsonArray , String...args) {
		String instanceDate = args[0];
		String currentWorkFlowStatus = "";
		if (jsonArray.size() > 0) {
			Iterator iterator = jsonArray.iterator();
			while (iterator.hasNext()) {
				JSONObject runningJsonObject = (JSONObject) iterator.next();
				String exitStatus = runningJsonObject.getString("ExitStatus");
				if (exitStatus.equals("RUNNING")) {
					currentWorkFlowStatus = "running";
					break;
				} else if (exitStatus.equals("COMPLETED")) {
					if (!instanceDate.equals("-1")) {
						String workFlowName = runningJsonObject.getString("WorkflowName");
						String actualInstance = workFlowName.substring(workFlowName.lastIndexOf("/") + 1);
						System.out.println("************************** actualInstance = " + actualInstance);
						if (actualInstance.equals(instanceDate)) {
							System.out.println("Instance file is found - " + actualInstance);
							currentWorkFlowStatus = "completed";
							break;
						} 
					} else if (instanceDate.equals("-1")) {
						currentWorkFlowStatus = "completed";
						break;
					}
				} else if (exitStatus.equals("SHUTDOWN") || exitStatus.equals("FAILED")) {
					currentWorkFlowStatus = "failed";
					break;
				}
			}
		}
		return currentWorkFlowStatus;
	}

	/**
	 * Print the error message, so that it can be used in Hudson log easily
	 * @param facetName
	 * @param dataSetName
	 * @param datasetActivationTime
	 * @param cookie
	 */
	private void printMessage(String facetName, String dataSetName , String datasetActivationTime , String cookie)  {
		TestSession.logger.info("=======================================================================================================");
		TestSession.logger.info("========================================     TestCase failed  =========================================");
		TestSession.logger.info("Facet : " + facetName );
		TestSession.logger.info("DataSet Name : " + dataSetName );
		this.consoleHandle.getFailureInformation(dataSetName, datasetActivationTime, cookie);
		TestSession.logger.info("========================================================================================================");
	}

	/**
	 * Get all the grid or cluster that dn't support HCAT 
	 * @return
	 */
	public List<String> getNonHCatSupportedGrids() {
		List<String> grid = null;
		String testURL = this.consoleHandle.getConsoleURL() + "/console/query/hadoop/versions";

		JsonPath jsonPath = given().cookie(this.cookie).get(testURL).jsonPath();
		TestSession.logger.info("Get all the Hcat enabled grid response = " + jsonPath.prettyPrint());
		grid = jsonPath.getList("HadoopClusterVersions.findAll { ! it.HCatVersion.startsWith('hcat_common') }.ClusterName ");
		if (grid == null) {
			try {
				throw new Exception("Failed to get hcatEnabled");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return grid;
	}

	/**
	 * Search the specified string in the facet application.log file.
	 * @param datasetName - dataset name string for which the string has to be search.
	 * @param facetName - facet name to select the <facet>.application file
	 * @param stringToSearch  - actual string to be searched.
	 * @throws IOException
	 */
	public void checkStringInLog(String datasetName, String facetName , String stringToSearch) throws IOException {
		Pattern pattern = Pattern.compile(stringToSearch);

		// read the log file.
		String facetApplicationLogFile = LOG_FILE.replace("FACET_NAME", facetName);

		// Read the facet application log file.
		FileInputStream input = new FileInputStream(facetApplicationLogFile);
		FileChannel channel = input.getChannel();

		ByteBuffer bbuf = channel.map(FileChannel.MapMode.READ_ONLY, 0, (int) channel.size());
		CharBuffer cbuf = Charset.forName("8859_1").newDecoder().decode(bbuf);
		Matcher matcher = pattern.matcher(cbuf);
		long count = 0;
		while (matcher.find()) {
			String match = matcher.group().trim();
			System.out.println(match);
			assertTrue("Failed : " + stringToSearch + " dn't match with  " +  match , stringToSearch.equals(match));
			count++;
		}
		TestSession.logger.info("count = " + count);
	}


	/**
	 * Return JSONArray of discovery monitor for the given dataset.
	 * @return
	 */
	public JSONArray isDiscoveryMonitoringStarted( String facetName , String dataSetName) {
		long waitTimeForWorkflowPolling = 5 * 60 * 1000; // 15 minutes
		long sleepTime = 5000; // 5 sec  sleep time.
		long waitTime=0;
		JSONArray jsonArray = null;
		while (waitTime <= waitTimeForWorkflowPolling) {
			String discoveryMonitoringStarted = this.consoleHandle.getConsoleURL().replace("9999", this.consoleHandle.getFacetPortNo(facetName)) + "/" + facetName + this.DISCOVERY_MONITOR + "?dataset=" + dataSetName;
			TestSession.logger.info("discoveryMonitoringStarted = " + discoveryMonitoringStarted);
			com.jayway.restassured.response.Response response = given().cookie(this.cookie).get(discoveryMonitoringStarted);
			jsonArray = this.consoleHandle.convertResponseToJSONArray(response, "Results");
			if (jsonArray.size() == 0) {
				this.consoleHandle.sleep(sleepTime);
				waitTime += sleepTime;
			} else if(jsonArray.size() > 0) {
				break;
			}
		}
		return jsonArray;
	}

	/**
	 * Get facet health check details
	 * @param facetName
	 * @return
	 */
	public Map<String , String> getFacetHealthDetails(String facetName) {
		String healthCheckUpUrl = this.consoleHandle.getConsoleURL() + "/console/api/proxy/health?facet=" + facetName + "&colo=gq1";
		TestSession.logger.info("healthCheckUpUrl  =  " + healthCheckUpUrl);
		com.jayway.restassured.response.Response response = given().cookie(this.cookie).get(healthCheckUpUrl);
		assertTrue("Failed to get the response for " + healthCheckUpUrl , (response != null) );
		String resString = response.asString();
		TestSession.logger.info("response = " + resString);
		JsonPath jsonPath = new JsonPath(resString);
		Map<String , String>applicationSummary = new HashMap<String, String>();
		List<String> keys = jsonPath.get("ApplicationSummary.Parameter");
		List<String> values = jsonPath.get("ApplicationSummary.Value");
		for(int i = 0;i<keys.size() ; i++){
			applicationSummary.put(keys.get(i), values.get(i));
		}
		return applicationSummary;
	}
	
	/**
	 * Create and appened the content to the file.
	 * @param reportFileName
	 * @param content
	 */
	public void writeExecutionResultToFile(String reportFileName , String facetName , String content)  {
		List<String>temp = Arrays.asList(reportFileName.split("-"));
		File aFile = new File(temp.get(0) +  "_"  + facetName +  "_ExecutionReport.txt");
		TestSession.logger.info("Report FileName = " + aFile.getAbsolutePath());
		FileOutputStream outputFile = null;  
		try {
			outputFile = new FileOutputStream(aFile, true);
			System.out.println("File stream created successfully.");
		} catch (FileNotFoundException e) {
			e.printStackTrace(System.err);
		}
		
		FileChannel outChannel = outputFile.getChannel();
		ByteBuffer buf = ByteBuffer.allocate(1024);
		
		// Load the data into the buffer
		for (char ch : content.toCharArray()) {
			buf.putChar(ch);
		}
		buf.flip();
		
		try {
			outChannel.write(buf);
			outputFile.close();
			TestSession.logger.info("Buffer contents written to file.");
		} catch (IOException e) {
			e.printStackTrace(System.err);
		}
	}
	
	/**
	 * Get the steps involved in workflow    
	 */
	public void checkStepExistsInWorkFlowExecutionSteps(String dataSetName , String datasetActivationTime , String workflowType , String stepName , String stepValue) {

		String endTime = GdmUtils.getCalendarAsString();
		JSONUtil jsonUtil = new JSONUtil();
		JSONArray jsonArray = null;
		TestSession.logger.info("endTime = " + endTime);
		String url = this.consoleHandle.getConsoleURL() + "/console/api/workflows/" + workflowType   + "?exclude=false&starttime=" + datasetActivationTime + "&endtime=" + endTime +
				"&joinType=innerJoin&datasetname=" +  dataSetName ;
		TestSession.logger.info("url = " + url);
		com.jayway.restassured.response.Response response = given().cookie(cookie).get(url);
		String res = response.getBody().asString();
		TestSession.logger.info("response = " + res);

		// convert string to jsonObject
		JSONObject obj =  (JSONObject) JSONSerializer.toJSON(res.toString());
		TestSession.logger.info("obj = " + obj.toString());
		if (workflowType.equals("running")) {
			jsonArray = obj.getJSONArray("runningWorkflows");
		} else if (workflowType.equals("completed")) {
			jsonArray = obj.getJSONArray("completedWorkflows");
		} else if (workflowType.equals("failed")) {
			jsonArray = obj.getJSONArray("failedWorkflows");
		}

		if ( jsonArray.size() > 0 ) {
			Iterator iterator = jsonArray.iterator();
			while (iterator.hasNext()) {
				JSONObject jsonObject = (JSONObject) iterator.next();
				TestSession.logger.info("failedJsonObject  = " + jsonObject.toString());
				String fName = jsonObject.getString("FacetName");
				String facetColo = jsonObject.getString("FacetColo");
				String executionId = jsonObject.getString("ExecutionID");

				// get steps for workflow 
				String testURL =this.consoleHandle.getConsoleURL() +  "/console/api/workflows/" + executionId + "/view?facet=" + fName + "&colo=" + facetColo;
				TestSession.logger.info("url = " + testURL);
				response = given().cookie(this.cookie).get(testURL);
				String res1 = response.getBody().asString();
				TestSession.logger.info(" response = " + res1);
				JSONObject obj1 =  (JSONObject) JSONSerializer.toJSON(res1.toString());
				TestSession.logger.info("obj1 = " + obj1.toString());
				JSONObject workflowExecutionJsonObject = (JSONObject) obj1.get("WorkflowExecution");
				JSONArray jsonArray1 = workflowExecutionJsonObject.getJSONArray("Step Executions");
				boolean isStepExist = checkStepExists(jsonArray1 , stepName.trim() , stepValue.trim());
				TestSession.logger.info(" result = " + isStepExist );
				assertTrue("Failed, " +  stepValue + " is not present in " + stepName ,  isStepExist == true);
			}
		}
	}
	
	/**
	 * Gets the Progress value  from the running workflow details 
	 * @param dataSetName
	 * @param datasetActivationTime
	 * @param workflowType
	 * @return
	 */
	public int getProgressValue(String dataSetName , String datasetActivationTime , String workflowType) {
		return getWorkFlowProgressOREffectiveDataRate(dataSetName , datasetActivationTime , workflowType , "Progress");
	}
	
	/**
	 * Get the 'Effective Data Rate' value  from  running workflow details.
	 * @param dataSetName
	 * @param datasetActivationTime
	 * @param workflowType
	 * @return
	 */
	public int getEffectiveDataRateValue(String dataSetName , String datasetActivationTime , String workflowType) {
		return getWorkFlowProgressOREffectiveDataRate(dataSetName , datasetActivationTime , workflowType , "Effective Data Rate");
	}
	
	/**
	 * Get the progress value or Effective Data Rate value from the running workflow json response
	 * @param dataSetName
	 * @param datasetActivationTime
	 * @param workflowType
	 * @param checkValueFor
	 * @return
	 */
	public int getWorkFlowProgressOREffectiveDataRate(String dataSetName , String datasetActivationTime , String workflowType , String checkValueFor) {
		String endTime = GdmUtils.getCalendarAsString();
		JSONUtil jsonUtil = new JSONUtil();
		JSONArray jsonArray = null;
		boolean isDataSetRunning = false;
		boolean isDataCompletedInCompletedState = false;
		TestSession.logger.info("endTime = " + endTime);
		String url =  this.consoleHandle.getConsoleURL() + "/console/api/workflows/running?datasetname="+ dataSetName +"&instancessince=F&joinType=innerJoin&facet=" + workflowType;
	/*	String url = this.consoleHandle.getConsoleURL() + "/console/api/workflows/" + workflowType   + "?exclude=false&starttime=" + datasetActivationTime + "&endtime=" + endTime +
				"&joinType=innerJoin&datasetname=" +  dataSetName ;*/
		TestSession.logger.info("url = " + url);
		com.jayway.restassured.response.Response response = given().cookie(cookie).get(url);
		String res = response.getBody().asString();
		TestSession.logger.info("response = " + res);

		// convert string to jsonObject
		JSONObject obj =  (JSONObject) JSONSerializer.toJSON(res.toString());
		TestSession.logger.info("obj = " + obj.toString());
		
		if (workflowType.equals("running")) {
			jsonArray = obj.getJSONArray("runningWorkflows");
			isDataSetRunning = true;
		} else if (workflowType.equals("completed")) {
			jsonArray = obj.getJSONArray("completedWorkflows");
			isDataCompletedInCompletedState = true;
		}/* else if (workflowType.equals("failed")) {
			jsonArray = obj.getJSONArray("failedWorkflows");
		}*/

		int returnValue = 0;
		String currentStep = null;
		
		if (jsonArray.size() > 0 && jsonArray != null) {
			Iterator iterator = jsonArray.iterator();
			while (iterator.hasNext()) {
				JSONObject jsonObject = (JSONObject) iterator.next();
				TestSession.logger.info("failedJsonObject  = " + jsonObject.toString());
				String fName = jsonObject.getString("FacetName");
				String facetColo = jsonObject.getString("FacetColo");
				String executionId = jsonObject.getString("ExecutionID");

				// get steps for workflow 
				String testURL =this.consoleHandle.getConsoleURL() +  "/console/api/workflows/" + executionId + "/view?facet=" + fName + "&colo=" + facetColo;
				TestSession.logger.info("url = " + testURL);
				response = given().cookie(this.cookie).get(testURL);
				String res1 = response.getBody().asString();
				TestSession.logger.info(" response = " + res1);
				JSONObject detailsWorkFlowJSONObject =  (JSONObject) JSONSerializer.toJSON(res1.toString());
				TestSession.logger.info("obj1 = " + detailsWorkFlowJSONObject.toString());

				currentStep = detailsWorkFlowJSONObject.getString("CurrentStep").trim();
				
				if (currentStep.equals("data.load")  || currentStep.equals("data.transform")) {
					
					if (checkValueFor.equals("")) {
						returnValue =  Integer.parseInt( detailsWorkFlowJSONObject.getString("Progress") );
						TestSession.logger.info("Progress value = " + returnValue);
					} else if (checkValueFor.equals("")) {
						List<String> temp = Arrays.asList(detailsWorkFlowJSONObject.getString("Effective Data Rate").split(" "));
						int dataRate =  Integer.parseInt( temp.get(0) );
						returnValue = dataRate;
					}
				}
			}
		} else if (isDataSetRunning ) {
			TestSession.logger.info(dataSetName + " dataset is running, current step is  " + currentStep);
		} else if (!isDataSetRunning) {
			TestSession.logger.info(dataSetName + " dataset is not in running state, wait till the dataset comes to running state.");
		}
		return returnValue;

	}
	
	
	
	
	/**
	 * Check whether the given stepName and stepValue exists in jsonArray.
	 * @param jsonArray  - array representing "Step Executions" in workflow
	 * @param stepName  - stepName represents workflow steps example step name , Start Time etc
	 * @param stepValue - 
	 * @return
	 */
	public boolean checkStepExists(JSONArray jsonArray , String stepName , String stepValue) {
		boolean exists = false;
		if (jsonArray.size() > 0) {
			Iterator iterator = jsonArray.iterator();
			while (iterator.hasNext()) {
				JSONObject runningJsonObject = (JSONObject) iterator.next();
				String step = runningJsonObject.getString(stepName);
				TestSession.logger.info("step = " + step);
				if (step.equals(stepValue)) {
					exists = true;
					break;
				}
			}
		}
		return exists;
	}
	
	
	/**
	 * restart the completed  acquisition workflow. 
	 */
	public void restartCompletedWorkFlow(String dataSetName) {
		JSONArray jsonArray = null;
		com.jayway.restassured.response.Response response = null;
		boolean didDataSetIsInCompletedState = false;
		int i = 1;
		while (i <= 10)  {
			String testURL =  this.consoleHandle.getConsoleURL()  + "/console/api/workflows/completed?datasetname="+ dataSetName.trim() +"&instancessince=F&joinType=innerJoin";
			TestSession.logger.info("testURL = " + testURL);
			response = given().cookie(this.cookie).get(testURL);

			assertTrue("Failed to get the response for " + testURL , (response != null || response.toString() != "") );

			jsonArray = this.consoleHandle.convertResponseToJSONArray(response , "completedWorkflows");
			TestSession.logger.info("size = " + jsonArray.size());
			if ( jsonArray.size() > 0 ) {
				JSONArray resourceArray = new JSONArray();
				Iterator iterator = jsonArray.iterator();
				while (iterator.hasNext()) {
					JSONObject runningJsonObject = (JSONObject) iterator.next();
					String exitStatus = runningJsonObject.getString("ExitStatus");
					if (exitStatus.equals("COMPLETED")) {
						didDataSetIsInCompletedState = true;
						String executionId = runningJsonObject.getString("ExecutionID");
						String facetName = runningJsonObject.getString("FacetName");
						String facetColo = runningJsonObject.getString("FacetColo");
						
						resourceArray.add(new JSONObject().element("ExecutionID",executionId).element("FacetName", facetName).element("FacetColo", facetColo));
						String url =  this.consoleHandle.getConsoleURL()  + "/console/api/admin/proxy/workflows";
						com.jayway.restassured.response.Response jobKilledResponse = given().cookie(this.cookie).param("command", "restart")
								.param("workflowIds" , resourceArray.toString()).post(url);

						assertTrue("Failed to get the response for " + url , (jobKilledResponse != null || jobKilledResponse.toString() != "") );

						TestSession.logger.info("Restarting the completed workflow Response code = " + jobKilledResponse.getStatusCode());
						JsonPath jsonPath = jobKilledResponse.getBody().jsonPath();
						TestSession.logger.info("** response = " + jsonPath.prettyPrint());
						String responseId = jsonPath.getString("Response.ResponseId");
						String responseMessage = jsonPath.getString("Response.ResponseMessage");
						boolean flag = responseMessage.contains("Successful");
						assertTrue("Expected ResponseId is 0 , but got " +responseId  , responseId.equals("0"));
						assertTrue("Expected response message , but got " + responseMessage  , flag == true);
						break;
					}
				}
			}
			i++;
			this.consoleHandle.sleep(15000);
		}
		// check whether did dataset was to completed state or not.
		assertTrue("Failed : " + dataSetName + "  dataSet dn't came to completed state, may be the dataset is still in running state. ", didDataSetIsInCompletedState == true);
	}

}
