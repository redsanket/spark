package hadooptest.cluster.gdm;

import hadooptest.TestSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.httpclient.HttpMethod;

public final class ConsoleHandle
{

	private static final String WORKFLOW_COMPLETED_EXECUTION_STATUS = "COMPLETED";
	private static final String WORKFLOW_COMPLETED_EXIT_STATUS = "COMPLETED";
	private static final String WORKFLOW_FAILED_EXIT_STATUS = "FAILED";

	private HTTPHandle httpHandle = new HTTPHandle();
	private Response response;
	private String consoleURL;
	private Configuration conf;
	private String username;
	private String passwd;
	private final int workflowPingIterations;
	private final long timeInSecToReachRunningState;
	private String source1;
	private String source2;
	private String target1;
	private String target2;
	private String target3;

	public ConsoleHandle()
	{
		try
		{
			this.conf = new XMLConfiguration("/home/y/conf/gdm_qe_test/config.xml");
			this.consoleURL = this.conf.getString("hostconfig.console.base_url");
			TestSession.logger.debug("Found conf/config.xml configuration file.");
			TestSession.logger.debug("Console Base URL: " + this.consoleURL);
		} catch (ConfigurationException ex) {
			TestSession.logger.error(ex.toString());
		}
		this.workflowPingIterations = 20;
		this.timeInSecToReachRunningState = 300L;  // should be detected within 5 minutes
		//this.timeInSecToReachRunningState = 1200L;

		this.source1 = this.conf.getString("sources.source1");
		this.source2 = this.conf.getString("sources.source2");

		this.target1 = this.conf.getString("targets.target1");
		this.target2 = this.conf.getString("targets.target2");

		try
		{
			this.username = this.conf.getString("auth.usr");
			this.passwd = this.conf.getString("auth.pp");
			this.httpHandle.logonToBouncer(this.username, this.passwd);
			TestSession.logger.info("logon OK");
		} catch (Exception ex) {
			TestSession.logger.error("Exception thrown", ex);
		}
	}

	public Configuration getConf (){
		return conf;
	}

	public Response activateDataSet(String dataSetName)
	{
		String resource = this.conf.getString("hostconfig.console.actions");
		ArrayList data = new ArrayList();
		HttpMethod postMethod = null;

		data.add(new CustomNameValuePair("command", "unterminate"));
		data.add(new CustomNameValuePair("resourceNames", "[{\"ResourceName\":\"" + dataSetName + "\"}]"));

		postMethod = this.httpHandle.makePOST(resource, null, data);
		TestSession.logger.info("** Activating DataSet " + dataSetName);
		this.response = new Response(postMethod);
		TestSession.logger.info(this.response.toString());

		return this.response;
	}

	public Response deactivateDataSet(String dataSetName)
	{
		String resource = this.conf.getString("hostconfig.console.actions");
		ArrayList data = new ArrayList();
		HttpMethod postMethod = null;

		data.add(new CustomNameValuePair("command", "terminate"));
		data.add(new CustomNameValuePair("resourceNames", "[{\"ResourceName\":\"" + dataSetName + "\"}]"));

		postMethod = this.httpHandle.makePOST(resource, null, data);
		TestSession.logger.info("** Dectivating DataSet " + dataSetName);
		this.response = new Response(postMethod);
		TestSession.logger.info(this.response.toString());

		return this.response;
	}

	public Response getCompletedJobsForDataSet(String startTime, String endTime, String dataSetName)
	{
		String resource = this.conf.getString("hostconfig.console.workflows.completed.resource");
		ArrayList params = new ArrayList();
		String instancesSince = "F";

		TestSession.logger.info("**getCompletedJobsForADataSet(startTime=" + startTime + ", endTime=" + endTime + ", dataSetName=" + dataSetName + ")");

		params.add(new CustomNameValuePair("starttime", startTime));
		params.add(new CustomNameValuePair("endtime", endTime));
		params.add(new CustomNameValuePair("datasetname", dataSetName));
		params.add(new CustomNameValuePair("instancessince", "F"));

		HttpMethod getMethod = this.httpHandle.makeGET(this.consoleURL, resource, params);
		this.response = new Response(getMethod);

		TestSession.logger.info(this.response.toString());
		return this.response;
	}

	public Response getCompletedWorkflowForDataSet(String startTime, String endTime, String dataSetName)
	{
		String resource = this.conf.getString("hostconfig.console.workflows.completed.resource");
		ArrayList params = new ArrayList();
		String instancesSince = "F";

		TestSession.logger.info("**getCompletedJobsForADataSet(startTime=" + startTime + ", endTime=" + endTime + ", dataSetName=" + dataSetName + ")");

		params.add(new CustomNameValuePair("starttime", startTime));
		params.add(new CustomNameValuePair("endtime", endTime));
		params.add(new CustomNameValuePair("datasetname", dataSetName));
		params.add(new CustomNameValuePair("instancessince", "F"));

		HttpMethod getMethod = this.httpHandle.makeGET(this.consoleURL, resource, params);
		this.response = new Response(getMethod);

		TestSession.logger.info(this.response.toString());
		return this.response;
	}

	private Response getJobsForDataSet(String status, String startTime, String endTime, String dataSetName, String instancesSince, String facet) {
		String resource = this.conf.getString("hostconfig.console.workflows.completed.resource");
		ArrayList params = new ArrayList();

		TestSession.logger.info("**getCompletedJobsForADataSet(startTime=" + startTime + ", endTime=" + endTime + ", dataSetName=" + dataSetName + ", instanceSince=" + instancesSince + ")");

		params.add(new CustomNameValuePair("starttime", startTime));
		params.add(new CustomNameValuePair("endtime", endTime));
		params.add(new CustomNameValuePair("datasetname", dataSetName));
		params.add(new CustomNameValuePair("instancessince", instancesSince));

		HttpMethod getMethod = this.httpHandle.makeGET(this.consoleURL, resource, params);
		this.response = new Response(getMethod);

		TestSession.logger.info(this.response.toString());
		return this.response;
	}

	public Response getCompletedJobsForDataSet(String startTime, String endTime, String dataSetName, String instancesSince)
	{
		String resource = this.conf.getString("hostconfig.console.workflows.completed.resource");
		ArrayList params = new ArrayList();

		TestSession.logger.info("**getCompletedJobsForADataSet(startTime=" + startTime + ", endTime=" + endTime + ", dataSetName=" + dataSetName + ", instanceSince=" + instancesSince + ")");

		params.add(new CustomNameValuePair("starttime", startTime));
		params.add(new CustomNameValuePair("endtime", endTime));
		params.add(new CustomNameValuePair("datasetname", dataSetName));
		params.add(new CustomNameValuePair("instancessince", instancesSince));

		HttpMethod getMethod = this.httpHandle.makeGET(this.consoleURL, resource, params);
		this.response = new Response(getMethod);

		TestSession.logger.info(this.response.toString());
		return this.response;
	}

	public Response getFailedJobsForDataSet(String startTime, String endTime, String dataSetName, String instancesSince)
	{
		String resource = this.conf.getString("hostconfig.console.workflows.failed.resource");
		ArrayList params = new ArrayList();

		TestSession.logger.info("**getFailedJobsForADataSet(startTime=" + startTime + ", endTime=" + endTime + ", dataSetName=" + dataSetName + ", instancesSince=" + instancesSince + ")");

		params.add(new CustomNameValuePair("starttime", startTime));
		params.add(new CustomNameValuePair("endtime", endTime));
		params.add(new CustomNameValuePair("datasetname", dataSetName));
		instancesSince = instancesSince == null ? "F" : instancesSince;
		params.add(new CustomNameValuePair("instancessince", instancesSince));

		HttpMethod getMethod = this.httpHandle.makeGET(this.consoleURL, resource, params);
		this.response = new Response(getMethod);

		TestSession.logger.info(String.valueOf(this.response.getStatusCode()));
		return this.response;
	}

	public Response getWorkflowStepExecution(String executionId, String facetName, String facetColo)
	{
		String resource = this.conf.getString("hostconfig.console.workflows.stepexecutions.resource");
		resource = resource.replace("WORKFLOW_EXECUTION_ID", executionId);

		ArrayList params = new ArrayList();

		TestSession.logger.info("**getWorkflowStepExecution(executionId=" + executionId + ", facetName=" + facetName + ", facetColo=" + facetColo + ")");

		params.add(new CustomNameValuePair("facet", facetName));
		params.add(new CustomNameValuePair("colo", facetColo));

		HttpMethod getMethod = this.httpHandle.makeGET(this.consoleURL, resource, params);
		this.response = new Response(getMethod);

		return this.response;
	}

	public Response cloneDataSet(String dataSetName, String configDataFile, String originalDataSetName)
	{
		String resource = this.conf.getString("hostconfig.console.datasets.clone.resource");

		String xmlFileContent = GdmUtils.readFile(configDataFile);
		ArrayList params = new ArrayList();
		StringBuilder postBody = new StringBuilder();

		xmlFileContent = xmlFileContent.replaceAll("name=\"" + originalDataSetName, "name=\"" +dataSetName);

		TestSession.logger.debug("XML length: " + xmlFileContent.length());

		postBody.append("xmlFileContent=");
		postBody.append(xmlFileContent);
		postBody.append("\naction=Create\n");
		postBody.append("operation=1\n");

		TestSession.logger.info("cloneDataSet(dataSetName=" + dataSetName + ", xmlFileContent=" + xmlFileContent);
		TestSession.logger.info("** cloneDataSet(dataSetName=" + dataSetName + ", xmlFileContent=" + xmlFileContent);
		HttpMethod postMethod = this.httpHandle.makePOST(resource, null, postBody.toString());
		this.response = new Response(postMethod, false);

		return this.response;
	}

	public Response cloneDataSet(String dataSetName, String configDataFile)
	{
		String resource = this.conf.getString("hostconfig.console.datasets.clone.resource");

		String xmlFileContent = GdmUtils.readFile(configDataFile);
		ArrayList params = new ArrayList();
		StringBuilder postBody = new StringBuilder();

		xmlFileContent = xmlFileContent.replaceAll("NEW_DATA_SET_NAME", dataSetName);
		xmlFileContent = xmlFileContent.replaceAll("SOURCE_1", this.source1);
		xmlFileContent = xmlFileContent.replaceAll("TARGET_1", this.target1);

		TestSession.logger.debug("XML length: " + xmlFileContent.length());

		postBody.append("xmlFileContent=");
		postBody.append(xmlFileContent);
		postBody.append("\naction=Create\n");
		postBody.append("operation=1\n");

		TestSession.logger.info("cloneDataSet(dataSetName=" + dataSetName + ", xmlFileContent=" + xmlFileContent);
		TestSession.logger.info("** cloneDataSet(dataSetName=" + dataSetName + ", xmlFileContent=" + xmlFileContent);
		HttpMethod postMethod = this.httpHandle.makePOST(resource, null, postBody.toString());
		this.response = new Response(postMethod, false);

		return this.response;
	}

	public Response checkDataSet(String dataSetName)
	{
		String resource = this.conf.getString("hostconfig.console.datasets.view.resource");

		ArrayList params = new ArrayList();

		TestSession.logger.info("**checkDataSet(dataSetName=" + dataSetName + ")");

		params.add(new CustomNameValuePair("checkverify", "true"));
		params.add(new CustomNameValuePair("prefix", dataSetName));

		HttpMethod getMethod = this.httpHandle.makeGET(this.consoleURL, resource, params);
		this.response = new Response(getMethod);

		return this.response;
	}

	/**
	 * Replaces the contents of an existing dataSet
	 *
	 * @param dataSetName  the dataSet to modify
	 * @param xmlFileContent  the new dataSet contents
	 * @return the console response
	 */
	public Response modifyDataSet(String dataSetName, String xmlFileContent) {
		String resource = this.conf.getString("hostconfig.console.datasets.clone.resource");
		StringBuilder postBody = new StringBuilder();
		postBody.append("xmlFileContent=");
		postBody.append(xmlFileContent);
		postBody.append("\naction=Edit\n");
		postBody.append("operation=1\n");

		TestSession.logger.info("modifyDataSet(dataSetName=" + dataSetName + ", xmlFileContent=" + xmlFileContent);
		TestSession.logger.info("** modifyDataSet(dataSetName=" + dataSetName + ", xmlFileContent=" + xmlFileContent);
		HttpMethod postMethod = this.httpHandle.makePOST(resource, null, postBody.toString());
		this.response = new Response(postMethod, false);
		return this.response;
	}

	public Response getRunningWorkflowsForADataSet(String dataSetName)
	{
		String resource = this.conf.getString("hostconfig.console.workflows.running.resource");
		ArrayList params = new ArrayList();
		String instancesSince = "F";

		TestSession.logger.info("**getRunningWorkflowsForADataSet(dataSetName=" + dataSetName + ")");

		params.add(new CustomNameValuePair("datasetname", dataSetName));
		params.add(new CustomNameValuePair("instancessince", instancesSince));

		HttpMethod getMethod = this.httpHandle.makeGET(this.consoleURL, resource, params);
		this.response = new Response(getMethod);

		TestSession.logger.info(this.response.toString());
		return this.response;
	}

	public Response getWorkflowDetails(String executionId, String facetName, String colo)
	{
		return null;
	}

	public String pingWorkflow(String dataSetName, String feedSubmisionTime, long waitTimeInSec)
	{
		long waitTimeForAnIteration = waitTimeInSec / 20L;
		TestSession.logger.debug("Total poll time: " + waitTimeForAnIteration / 60L + "(s)");
		String searchingFor = "";
		for (int i = 0; i < 20; i++) {
			String now = GdmUtils.getCalendarAsString();

			TestSession.logger.debug("Polling for the workflow completion(completed/failed). Iteration#" + i + ", checking for " + searchingFor);
			if (i % 2 == 0) {
				this.response = getCompletedJobsForDataSet(feedSubmisionTime, now, dataSetName);
				searchingFor = "completion";
				if (Integer.valueOf(this.response.getElementAtPath("/completedWorkflows/[:size]/").toString()).intValue() > 0)
					return "COMPLETED";
			}
			else
			{
				this.response = getFailedJobsForADataSet(feedSubmisionTime, now, dataSetName);
				searchingFor = "failure";
				if (Integer.valueOf(this.response.getElementAtPath("/failedWorkflows/[:size]/").toString()).intValue() > 0) {
					return "FAILED";
				}
			}

			try
			{
				Thread.sleep(waitTimeForAnIteration);
			} catch (InterruptedException ex) {
				TestSession.logger.error(ex.toString());
			}
		}
		return "UNKNOWN";
	}

	public String pingWorkflowExecution(String dataSetName, String feedSubmisionTime, long waitTimeInSec) {
		long waitTimePerIteration = waitTimeInSec / this.workflowPingIterations;

		boolean isRunning = false;
		String intermediateState = "RUNNING";

		for (int i = 1; i < 4; i++) {
			TestSession.logger.debug("PHW: the input wait time is: " +  this.timeInSecToReachRunningState);
			TestSession.logger.debug("Waiting for " + this.timeInSecToReachRunningState / i + " secondssec. before digging into running workflows.");
			try {
				TestSession.logger.debug("PHW: wait time calc is: " +  (this.timeInSecToReachRunningState / i * 1000L));
				Thread.sleep(this.timeInSecToReachRunningState / i * 1000L);
			} catch (InterruptedException ex) {
				TestSession.logger.error(ex.toString());
			}
			TestSession.logger.debug("Checking workflow in running state[iteration " + i + " ].");
			this.response = getRunningWorkflowsForADataSet(dataSetName);
			if (Integer.valueOf(this.response.getElementAtPath("/runningWorkflows/[:size]/").toString()).intValue() > 0) {
				TestSession.logger.debug("Workflow has come to running state .");
				break;
			}
		}

		TestSession.logger.debug("Checking workflow in running state[iteration4].");
		this.response = getRunningWorkflowsForADataSet(dataSetName);
		if (Integer.valueOf(this.response.getElementAtPath("/runningWorkflows/[:size]/").toString()).intValue() < 1) {
			TestSession.logger.debug("Workflow has not come to running state. Checking in completed list.");
			String now = GdmUtils.getCalendarAsString();
			this.response = getCompletedJobsForDataSet(feedSubmisionTime, now, dataSetName);
			if (Integer.valueOf(this.response.getElementAtPath("/completedWorkflows/[:size]/").toString()).intValue() > 0) {
				TestSession.logger.debug("Workflow has come to 'COMPLETED' state. Returning 'COMPLETED'");
				return "COMPLETED";
			}

			TestSession.logger.debug("Workflow has not come to completed state either. Checking in failed list.");
			now = GdmUtils.getCalendarAsString();
			this.response = getFailedJobsForADataSet(feedSubmisionTime, now, dataSetName);
			if (Integer.valueOf(this.response.getElementAtPath("/failedWorkflows/[:size]/").toString()).intValue() > 0) {
				TestSession.logger.debug("Workflow has come to 'FAILED' state. Returning 'FAILED'");
				return "FAILED";
			}

			TestSession.logger.debug("Workflow has not come to any state so far. Returning 'UNKNOWN STATE'");
			return "UNKNOWN";
		}

		for (int i = 0; i <= this.workflowPingIterations; i++) {
			TestSession.logger.debug("Workflow has come to running state. Checking till it is out. Iteration #" + (i + 1));
			String now = GdmUtils.getCalendarAsString();
			this.response = getRunningWorkflowsForADataSet(dataSetName);
			if (Integer.valueOf(this.response.getElementAtPath("/runningWorkflows/[:size]/").toString()).intValue() < 1) {
				TestSession.logger.debug("Workflow has come out of running state.");
				break;
			}
			try {
				Thread.sleep(waitTimePerIteration);
			} catch (InterruptedException ex) {
				TestSession.logger.error(ex.toString());
			}
		}

		TestSession.logger.debug("Workflow has come out of running state. Checking in completed list.");
		String now = GdmUtils.getCalendarAsString();
		this.response = getCompletedJobsForDataSet(feedSubmisionTime, now, dataSetName);
		if (Integer.valueOf(this.response.getElementAtPath("/completedWorkflows/[:size]/").toString()).intValue() > 0) {
			TestSession.logger.debug("Workflow has come to 'COMPLETED' state. Returning 'COMPLETED'");
			return "COMPLETED";
		}

		now = GdmUtils.getCalendarAsString();
		this.response = getFailedJobsForADataSet(feedSubmisionTime, now, dataSetName);
		if (Integer.valueOf(this.response.getElementAtPath("/failedWorkflows/[:size]/").toString()).intValue() > 0) {
			TestSession.logger.debug("Workflow has come to 'FAILED' state. Returning 'FAILED'");
			return "FAILED";
		}

		TestSession.logger.debug("After waiting for so long (" + waitTimeInSec + " sec.), the workflow is still in RUNNING state.");
		return "RUNNING";
	}

	public Response getFailedJobsForADataSet(String startTime, String endTime, String dataSetName)
	{
		return getFailedJobsForDataSet(startTime, endTime, dataSetName, null);
	}

	public Response getWorkflowList(String dataConfigName)
	{
		return null;
	}

	public boolean isWorkflowCompleted(Response response, String facet){
		boolean result = false;
		Iterator iterator = null;
		JSONArray completedWorkflows = (JSONArray)response.getJsonObject().get("completedWorkflows");
		if(completedWorkflows != null) {
			iterator = completedWorkflows.iterator();
			while (iterator.hasNext()) {
				JSONObject workflow = (JSONObject) iterator.next();
				if(!facet.equalsIgnoreCase(workflow.getString("FacetName"))){
					continue;
				}
				if(WORKFLOW_COMPLETED_EXECUTION_STATUS.equalsIgnoreCase(workflow.getString("ExecutionStatus")) && 
						WORKFLOW_COMPLETED_EXIT_STATUS.equalsIgnoreCase(workflow.getString("ExitStatus"))) {
					result = true;
				}
			}
		}
		return result;
	}

	private List<String> getTargets(JSONObject workflow) {
		// "Targets":"omegap1:http://gsbl90338.blue.ygrid.yahoo.com:8088/proxy/application_137353/,grima:http://gsbl90638.blue.ygrid.yahoo.com:8088/proxy/application_13655996_3219/"
		String targetString = workflow.getString("Targets");
		List<String> targets = new ArrayList<String>(Arrays.asList(targetString.split(",")));
		for (int i=0; i<targets.size(); i++) {
			String target = targets.get(i);
			target = target.split("\\:")[0];
			targets.set(i, target);
		}
		return targets;
	}

	/**
	 * Verifies a Response indicates a workflow is completed, checking the facet, targets, and dataSetInstance match.
	 *
	 * @param response    Response to verify
	 * @param facet      the facet type to verify
	 * @param desiredTargets  the target grids for the workflow to verify
	 * @param dataSetInstance  the dataSet instance date to verify
	 * @return true if the Response indicates a successful workflow completion for the provided arguments
	 */
	public boolean isWorkflowCompleted(Response response, String facet, List<String> desiredTargets, String dataSetInstance) {
		boolean result = false;
		Iterator iterator = null;
		JSONArray completedWorkflows = (JSONArray)response.getJsonObject().get("completedWorkflows");
		if (completedWorkflows != null) {
			iterator = completedWorkflows.iterator();
			while (iterator.hasNext()) {
				JSONObject workflow = (JSONObject) iterator.next();
				String workflowFacet = workflow.getString("FacetName");
				if(!facet.equalsIgnoreCase(workflowFacet)){
					TestSession.logger.debug("workflow facet was " + workflowFacet + ". Looking for " + facet);
					continue;
				}
				// "WorkflowName":"Switchover01_1376662405822/20120126"
				String workflowName = workflow.getString("WorkflowName");
				String actualInstance = workflowName.substring(workflowName.lastIndexOf("/") + 1);
				if (actualInstance.equalsIgnoreCase(dataSetInstance) == false) {
					TestSession.logger.info("Found dataSet instance " + actualInstance + ".  Was looking for " + dataSetInstance);
					continue;
				}
				if (WORKFLOW_COMPLETED_EXECUTION_STATUS.equalsIgnoreCase(workflow.getString("ExecutionStatus")) && 
						WORKFLOW_COMPLETED_EXIT_STATUS.equalsIgnoreCase(workflow.getString("ExitStatus"))) {
					List<String> actualTargets = this.getTargets(workflow);
					if (actualTargets.size() == desiredTargets.size()) {
						result = true;
						for (String target : desiredTargets) {
							boolean foundTarget = false;
							for (String actualTarget : actualTargets) {
								if (actualTarget.equalsIgnoreCase(target)) {
									foundTarget = true;
									break;
								}
							}
							if (foundTarget == false) {
								TestSession.logger.debug("Did not find target " + target);
								return false;
							}
						}
					} 
				}
			}
		}
		return result;
	}

	public int isWorkflowCompleted(Response response, String facet, boolean multiple){
		int count = 0;
		Iterator iterator = null;
		JSONArray completedWorkflows = (JSONArray)response.getJsonObject().get("completedWorkflows");
		if(completedWorkflows != null) {
			iterator = completedWorkflows.iterator();
			while (iterator.hasNext()) {
				JSONObject workflow = (JSONObject) iterator.next();
				if(!facet.equalsIgnoreCase(workflow.getString("FacetName"))){
					continue;
				}
				if(WORKFLOW_COMPLETED_EXECUTION_STATUS.equalsIgnoreCase(workflow.getString("ExecutionStatus")) && 
						WORKFLOW_COMPLETED_EXIT_STATUS.equalsIgnoreCase(workflow.getString("ExitStatus"))) {
					count++;
				}
			}
		}
		return count;   
	}

	public boolean isWorkflowFailed(Response response, String facet){
		boolean result = false;
		Iterator iterator = null;
		JSONArray failedWorkflows = (JSONArray)response.getJsonObject().get("failedWorkflows");

		if(failedWorkflows != null) {
			iterator = failedWorkflows.iterator();

			while (iterator.hasNext()) {
				JSONObject workflow = (JSONObject) iterator.next();
				if(!facet.equalsIgnoreCase(workflow.getString("FacetName"))){
					continue;
				}
				if(!WORKFLOW_COMPLETED_EXECUTION_STATUS.equalsIgnoreCase(workflow.getString("ExecutionStatus")) ||
						!WORKFLOW_FAILED_EXIT_STATUS.equalsIgnoreCase(workflow.getString("ExitStatus"))) {
					result = true;
				}
			}       
		}

		return result;  
	}

	/**
	 * Returns a dataSet config xml created from a config file.
	 *
	 * @param dataSetName    Desired name of dataSet
	 * @param configDataFile   Base config file to create dataSet from
	 * @return xml for the dataSet
	 */
	public String createDataSetXmlFromConfig(String dataSetName, String configDataFile) {
		String xmlFileContent = GdmUtils.readFile(configDataFile);
		xmlFileContent = xmlFileContent.replaceAll("NEW_DATA_SET_NAME", dataSetName);
		TestSession.logger.info("createDataSetFromConfig(dataSetName=" + dataSetName + ", xmlFileContent=" + xmlFileContent);
		TestSession.logger.info("** createDataSetFromConfig(dataSetName=" + dataSetName + ", xmlFileContent=" + xmlFileContent);
		return xmlFileContent;
	}

	/**
	 * Creates a dataSet config from an Xml source and submits it to the console
	 *
	 * @param dataSetName    Desired name of dataSet
	 * @param xmlFileContent   Xml for dataSet
	 * @return console response from submitting dataSet
	 */
	public Response createDataSet(String dataSetName, String xmlFileContent) {
		String resource = this.conf.getString("hostconfig.console.datasets.clone.resource");
		StringBuilder postBody = new StringBuilder();
		postBody.append("xmlFileContent=");
		postBody.append(xmlFileContent);
		postBody.append("\naction=Create\n");
		postBody.append("operation=1\n");
		TestSession.logger.info("createDataSet(dataSetName=" + dataSetName + ", xmlFileContent=" + xmlFileContent);
		TestSession.logger.info("** createDataSet(dataSetName=" + dataSetName + ", xmlFileContent=" + xmlFileContent);
		HttpMethod postMethod = this.httpHandle.makePOST(resource, null, postBody.toString());
		this.response = new Response(postMethod, false);
		return this.response;
	}

	/**
	 * Verifies console dataSet exists and activates it
	 *
	 * @param dataSetName    Desired name of dataSet
	 */
	public void checkAndActivateDataSet(String dataSetName) throws Exception {
		this.sleep(5000);
		TestSession.logger.info("Checking dataSet " + dataSetName);

		String statusResponse = "200";
		
		Response response = null;
		for (int i=0; i<30; i++) {
			this.sleep(3000);
			response = this.checkDataSet(dataSetName);
			
			if(!(Integer.toString(response.getStatusCode()).equals(statusResponse))) {
				throw new Exception("ResponseCode - checkDataSet - response code does not equal status reponse code 200");
			}
			else {
				TestSession.logger.info("ResponseCode - checkDataSet");
			}
			
			String responseString = response.getResponseBodyAsString();
			if (responseString.contains(dataSetName)) {
				break;
			}
		}
		
		if ( !(response.getElementAtPath("/DatasetsResult/[0]/DatasetName").toString()).equals(dataSetName) ) {
			throw new Exception("DataSetName does not match.");
		}
		else {
			TestSession.logger.info("DataSetName matches.");
		}
		
		TestSession.logger.info("Activating dataSet " + dataSetName);
		response = this.activateDataSet(dataSetName);

		if (! (response.getStatusCode() == 200) ) {
			throw new Exception("ResponseCode did not equal 200");
		}
		else {
			TestSession.logger.info("ResponseCode - Activate DataSet");
		}
		
		if (! (response.getElementAtPath("/Response/ActionName").toString().equals("unterminate")) ) {
			throw new Exception("ActionName did not equal *unterminate*");
		}
		else {
			TestSession.logger.info("ActionName.");
		}
		
		if (! (response.getElementAtPath("/Response/ResponseId").toString().equals("0")) ) {
			throw new Exception("ResponseId did not equal 0");
		}
		else {
			TestSession.logger.info("ResponseId");
		}
		
		if (! (response.getElementAtPath("/Response/ResponseMessage/[0]").toString().equals("Operation on " + dataSetName + " was successful.")) ) {
			throw new Exception("ResponseMessage did not equal success message.");
		}
		else {
			TestSession.logger.info("ResponseMessage.");
		}
		
		this.sleep(20000);
		for (int i=0; i<20; i++) {
			response = this.checkDataSet(dataSetName);
			if (response.getElementAtPath("DatasetsResult/[0]/Status").toString().equals("ACTIVE")) {
				break;
			}
			this.sleep(2000);
		}

		if (! (response.getStatusCode() == 200) ) {
			throw new Exception("ResponseCode did not equal 200");
		}
		else {
			TestSession.logger.info("ResponseCode - checkDataSet");
		}
		
		if (! (response.getElementAtPath("DatasetsResult/[0]/DatasetName").toString().equals(dataSetName)) ) {
			throw new Exception("DataSetName did not match.");
		}
		else {
			TestSession.logger.info("DataSetName matches.");
		}
		
		if (! (response.getElementAtPath("DatasetsResult/[0]/Status").toString().equals("ACTIVE")) ) {
			throw new Exception("Feed is not active.");
		}
		else {
			TestSession.logger.info("Feed is ACTIVE.");
		}
	}

	public void sleep(long ms) {
		try {
			Thread.sleep(ms);
		} catch (InterruptedException ex) {
			TestSession.logger.error(ex.toString());
		}
	}


	/**
	 * Waits for a dataSet to finish a workflow execution
	 *
	 * @param dataSetName  the dataSet to wait for
	 * @param startTime   the first time an instance could have finished
	 * @param timeoutMilliSec  The timeout value in milliseconds
	 * @return the state of the workflow, either UNKNOWN, COMPLETED, FAILED, or RUNNING
	 */
	public String waitForWorkflowExecution(String dataSetName, String startTime, long timeoutMilliSec) {
		String state = "UNKNOWN";
		long sleepTime = 8000;
		while (timeoutMilliSec > 0) {
			state = "UNKNOWN";

			this.sleep(sleepTime);
			timeoutMilliSec -= sleepTime;

			String now = GdmUtils.getCalendarAsString();
			this.response = getCompletedJobsForDataSet(startTime, now, dataSetName);
			if (Integer.valueOf(this.response.getElementAtPath("/completedWorkflows/[:size]/").toString()).intValue() > 0) {
				TestSession.logger.debug("Workflow has come to 'COMPLETED' state. Returning 'COMPLETED'");
				return "COMPLETED";
			}

			this.sleep(sleepTime);
			timeoutMilliSec -= sleepTime;

			now = GdmUtils.getCalendarAsString();
			this.response = getFailedJobsForADataSet(startTime, now, dataSetName);
			if (Integer.valueOf(this.response.getElementAtPath("/failedWorkflows/[:size]/").toString()).intValue() > 0) {
				TestSession.logger.debug("Workflow has come to 'FAILED' state. Returning 'FAILED'");
				return "FAILED";
			}

			this.sleep(sleepTime);
			timeoutMilliSec -= sleepTime;

			this.response = getRunningWorkflowsForADataSet(dataSetName);
			if (Integer.valueOf(this.response.getElementAtPath("/runningWorkflows/[:size]/").toString()).intValue() > 0) {
				TestSession.logger.debug("Workflow in running state.");
				state = "RUNNING";

				this.sleep(sleepTime);
				timeoutMilliSec -= sleepTime;
			}
		}
		return state;
	}





}
