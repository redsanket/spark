package hadooptest.cluster.gdm;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import hadooptest.TestSession;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import junit.framework.Assert;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.httpclient.HttpMethod;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.jayway.restassured.RestAssured;
import com.jayway.restassured.path.json.JsonPath;
import com.jayway.restassured.path.xml.XmlPath;

import hadooptest.Util;

public final class ConsoleHandle
{
	private static final String WORKFLOW_COMPLETED_EXECUTION_STATUS = "COMPLETED";
	private static final String WORKFLOW_COMPLETED_EXIT_STATUS = "COMPLETED";
	private static final String WORKFLOW_FAILED_EXIT_STATUS = "FAILED";
	private static final int SUCCESS = 200;

	private HTTPHandle httpHandle = new HTTPHandle();
	private Response response;
	private String consoleURL;
	private String crossColoConsoleURL;
	private String preserveConsoleURL;
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
			String configPath = Util.getResourceFullPath(
					"gdm/conf/config.xml");

			this.conf = new XMLConfiguration(configPath);
			this.consoleURL = this.conf.getString("hostconfig.console.base_url");
			this.crossColoConsoleURL = this.conf.getString("hostconfig.console.crossColo_url");
			TestSession.logger.info("crossColoConsoleURL  = " + this.crossColoConsoleURL);

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

	public String getConsoleURL() {
		return this.consoleURL;
	}

	/**
	 * This method is very helpful when you want to work with two console
	 * example : Cross colo testing, we have to create the dataset on the cross colo console and then back to the original console.
	 * @param currentConsoleURL
	 */
	public void setCurrentConsoleURL(String currentConsoleURL) {
		this.consoleURL = currentConsoleURL;
		this.httpHandle.setBaseURL(currentConsoleURL);
		TestSession.logger.info("INFO : Current console URL = " + this.consoleURL);
	}

	/**
	 * Get the current console url
	 * @return
	 */
	public String getCurrentConsoleURL() {
		return this.consoleURL;
	}

	/*
	 * Get the cross colo console url
	 */
	public String getCrossColoConsoleURL() {
		return this.crossColoConsoleURL;
	}

	/*
	 * This method will restore the original console url.
	 */
	public void restoreConsoleURL() {
		this.consoleURL = this.preserveConsoleURL;
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
		//TestSession.logger.info("** modifyDataSet(dataSetName=" + dataSetName + ", xmlFileContent=" + xmlFileContent);
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
	 * This function iterates a JSON array looking for an object which has a 
	 * String which matches the desired key/value pair
	 * @param array    JSONArray to search within
	 * @param key   
	 * @param value
	 * @return the JSONObject within the array matching the key/value pair.  Returns null if no such object exists.
	 */
	private JSONObject findObjectInJsonArray(JSONArray array, String key, String value) {
		Iterator iterator = array.iterator();
		while (iterator.hasNext()) {
			JSONObject o = (JSONObject) iterator.next();
			if (value.equalsIgnoreCase(o.getString(key))){
				return o;
			}
		}
		return null;
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

		if (findObjectInJsonArray((JSONArray)response.getJsonObject().get("DatasetsResult"), "DatasetName", dataSetName) == null) {
			throw new Exception("failed to find dataset " + dataSetName + " in response " + response);
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
			JSONObject json = findObjectInJsonArray((JSONArray)response.getJsonObject().get("DatasetsResult"), "DatasetName", dataSetName);
			if (json != null) {
				if (json.getString("Status").equalsIgnoreCase("ACTIVE")) {
					break;
				}
			}           
			this.sleep(2000);
		}

		if (! (response.getStatusCode() == 200) ) {
			throw new Exception("ResponseCode did not equal 200");
		}
		else {
			TestSession.logger.info("ResponseCode - checkDataSet");
		}

		JSONObject json = findObjectInJsonArray((JSONArray)response.getJsonObject().get("DatasetsResult"), "DatasetName", dataSetName);
		if (json == null) {
			throw new Exception("failed to find dataset " + dataSetName + " in response " + response);
		}

		if (json.getString("Status").equalsIgnoreCase("ACTIVE") == false) {
			throw new Exception(dataSetName + " is not active: " + response);
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

	/**
	 * Creates a dataSource config from Xml and submits it to the console
	 *
	 * @param xmlFileContent   Xml for dataSource
	 * @return console response from submitting dataSet
	 */
	public boolean createDataSource(String xmlFileContent) {
		String resource = this.conf.getString("hostconfig.console.datasource.clone.resource");
		StringBuilder postBody = new StringBuilder();
		postBody.append("xmlFileContent=");
		postBody.append(xmlFileContent);
		postBody.append("\naction=Create\n");
		postBody.append("operation=1\n");
		HttpMethod postMethod = this.httpHandle.makePOST(resource, null, postBody.toString());
		this.response = new Response(postMethod, false);
		if (response.getStatusCode() == 200) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Returns dataset specification file ( xml file ) as String
	 * @param dataSetName
	 * @return xml file as String
	 */
	public String getDataSetXml(String dataSetName) {
		String url = this.getConsoleURL() + "/console/query/config/dataset/"+dataSetName;
		TestSession.logger.info("test url = " + url);
		return this.getXml(url);
	}

	/**
	 * Returns datasource specification file ( xml file ) as String
	 * @param dataSetName
	 * @return xml file as String
	 */
	public String getDataSourcetXml(String dataSourceName) {               
		String url = this.getConsoleURL() + "/console/query/config/datasource/"+dataSourceName;
		TestSession.logger.info("url = " + url);
		return this.getXml(url);
	}

	private String getXml(String query) {
		String xml = null;      
		com.jayway.restassured.response.Response response = RestAssured.given().cookie(httpHandle.cookie).get(query);
		if (response.getStatusCode() == 200) {
			xml = response.andReturn().asString();
			TestSession.logger.info("dataset = " + xml);
			return xml;
		}
		return xml;
	}

	/**
	 * @return list of grid dataSource names containing unique version strings
	 */
	public List<String> getUniqueGrids() throws Exception {
		List<String> grids = new ArrayList<String>();
		String url = this.consoleURL + "/console/api/datasources/view";
		com.jayway.restassured.response.Response response = RestAssured.given().cookie(httpHandle.cookie).get(url);
		if (response.getStatusCode() == 200) {
			String dataSourceListing = response.andReturn().asString();
			TestSession.logger.info("Received datasources listing: " + dataSourceListing);
			// create a map of versions to grid names to get unique grids.  One node deploy contains duplicate grid dataSources.
			Map<String, String> gridMap = new HashMap<String, String>();
			JSONObject json = (JSONObject)JSONSerializer.toJSON(dataSourceListing);
			JSONArray dataSourceResult = (JSONArray)json.get("DataSourceResult");
			if (dataSourceResult != null) {
				Iterator iterator = dataSourceResult.iterator();
				while (iterator.hasNext()) {
					JSONObject dataSource = (JSONObject)iterator.next();
					if (dataSource.getString("Type").equalsIgnoreCase("grid")) {
						String name = dataSource.getString("DataSourceName");
						String version = dataSource.getString("Version");
						gridMap.put(version, name);
					}

				}
			} else {
				throw new Exception("unable to find DataSourceResult");
			}
			// iterate the map and add to the grid set 
			for (String grid : gridMap.values()) {
				grids.add(grid);
			}
		} else {
			throw new Exception(url + " returned status code " + response.getStatusCode());
		}
		return grids;
	}

	/**
	 * Creates a dataSet config from an Xml source and submits it to the console
	 *
	 * @param dataSetName    Desired name of dataSet
	 * @param xmlFileContent   Xml for dataSet
	 * @return console response from submitting dataSet
	 */
	public Response createDataSource(String oldDataSetName, String newDataSetName, String xmlFileContent) {
		String resource = this.conf.getString("hostconfig.console.datasource.clone.resource");

		xmlFileContent = xmlFileContent.replaceAll(oldDataSetName, newDataSetName);
		TestSession.logger.info("xmlFileContent  = "+xmlFileContent);

		StringBuilder postBody = new StringBuilder();
		postBody.append("xmlFileContent=");
		postBody.append(xmlFileContent);
		postBody.append("\naction=Create\n");
		postBody.append("operation=1\n");
		TestSession.logger.info("createDataSet(dataSetName=" + newDataSetName + ", xmlFileContent=" + xmlFileContent);
		HttpMethod postMethod = this.httpHandle.makePOST(resource, null, postBody.toString());
		this.response = new Response(postMethod, false);
		return this.response;
	}

	/**
	 * Modify the specified tag value of the specified datasource
	 * @param dataSourceName  - datasource name 
	 * @param tagName - tag that needs to modify its value
	 * @param oldValue  - old value of the tag
	 * @param newValue = new value of the tag
	 */
	public void modifyDataSource(String cookie , String dataSourceName ,  String tagName , String oldValue , String newValue){

		// read the specified datasource file & change the HCatSupported tag value
		String hostName = this.getConsoleURL();
		String xml = given().cookie(cookie).get(hostName + "/console/query/config/datasource/"+dataSourceName).andReturn().asString();
		String oldTag = "<"+tagName+">"+oldValue +"</"+tagName+">";
		String newTag = "<"+tagName+">"+newValue +"</"+tagName+">";
		xml = xml.replaceAll(oldTag , newTag );
		TestSession.logger.info("xml = "+xml);

		StringBuilder postBody = new StringBuilder();
		postBody.append("xmlFileContent=");
		postBody.append(xml);
		postBody.append("\naction=Edit\n");
		postBody.append("operation=1\n");

		// post the modified datasource file. 
		HttpMethod postMethod = this.httpHandle.makePOST("/console/rest/config/datasource", null, postBody.toString());
		this.response = new Response(postMethod, false);
		assertTrue("Cloned failed and got  http response " + response.getStatusCode() , response.getStatusCode() == SUCCESS);
	}

	/**
	 * Get a list of source or target names of the specified dataset
	 * @param dataSetName - dataset for which either source or target name(s) need be returned.
	 * @param sourceType - specify either source or target name.
	 * @param attribute - attribute name of the source or target tage usually its name attribute.
	 * @return - List<String> of source or target name(s) of the dataset.
	 */
	public List<String> getDataSource(String dataSetName , String sourceType , String attribute ) {
		List<String>source = new ArrayList<String>();
		String xml = this.getDataSetXml(dataSetName);
		XmlPath xmlPath = new XmlPath(xml);
		xmlPath.setRoot("DataSet");
		int size = 0;
		if(sourceType.equals("source")){
			size = xmlPath.get("Sources.Source.size()");
		}else if(sourceType.equals("target")){
			size = xmlPath.get("Targets.Target.size()");
		}
		TestSession.logger.info("size  = "+size);
		String value = null;
		for(int i=0;i<= size - 1 ; i++){
			if(sourceType.equals("source")){
				value = xmlPath.getString("Sources.Source["+ i +"].@" +attribute);
				source.add(value);
			}if(sourceType.equals("target")){
				if(attribute.equals("status")){

					// helpful in identify which target is active or inactive
					value = xmlPath.getString("Targets.Target["+ i +"].@name");
					value = value +" : " + xmlPath.getString("Targets.Target["+ i +"].@" +attribute );
				}else if(attribute.equals("name")){
					value = xmlPath.getString("Targets.Target["+ i +"].@name");
				}
				source.add(value);
			}
		}
		return source;
	}


	/**
	 * Print the reason for the failure of the dataset. Failure information is very much useful for debugging when testcase fails 
	 * on CI.  
	 * @param dataSetName  - dataset for which failure information need to fetch.
	 * @param datasetActivationTime -  dataset activate time.
	 */
	public void getFailureInformation(String url , String cookie , String dataSetName , String datasetActivationTime) {		
		String endTime = GdmUtils.getCalendarAsString();
		JSONUtil jsonUtil = new JSONUtil();
		TestSession.logger.info("endTime = " + endTime);
		com.jayway.restassured.response.Response res = given().cookie(cookie).get(url + "/console/api/workflows/failed?exclude=false&starttime=" + datasetActivationTime + "&endtime=" + endTime +
				"&joinType=innerJoin&datasetname=" +  dataSetName);
		JsonPath jsonPath = res.getBody().jsonPath();
		String executionID = jsonPath.getString("failedWorkflows[0].ExecutionID");
		TestSession.logger.info("executionID = " + executionID);
		String facetName = jsonPath.getString("failedWorkflows[0].FacetName");
		TestSession.logger.info("FacetName = " + facetName);
		String coloName = jsonPath.getString("failedWorkflows[0].FacetColo");
		TestSession.logger.info("coloName = " + coloName);

		// get the failure information.
		Response response = this.getWorkflowStepExecution(executionID, facetName, coloName);
		TestSession.logger.info("Failed Response = " + jsonUtil.formatString(response.toString()));
	}

	/**
	 * Deactivate all the targets from the given dataset
	 * @param dataSetName -  for which the targets needs to deactivate.
	 */
	public void deactivateTargetsInDataSet(String dataSetName ) {
		List<String>newDataTargetList = this.getDataSource(dataSetName, "target" ,"name");
		TestSession.logger.info("this.newDataTargetList  = " + newDataTargetList);
		int size = newDataTargetList.size();

		JSONUtil jsonUtil = new JSONUtil();

		// create args parameter
		String args = jsonUtil.constructArgumentParameter(newDataTargetList,"deactivateTarget");
		TestSession.logger.info("args *****"+jsonUtil.formatString(args));

		// wait for some time, so that changes are reflected in the dataset i,e TARGETS gets INACTIVE
		this.sleep(40000);

		String resource = jsonUtil.constructResourceNamesParameter(Arrays.asList(dataSetName));
		TestSession.logger.info("resource = "+jsonUtil.formatString(resource));

		com.jayway.restassured.response.Response res = given().cookie(this.httpHandle.cookie).param("resourceNames", resource).param("command","update").param("args", args)
				.post(this.getConsoleURL() + "/console/rest/config/dataset/actions");
		String resString = res.asString();
		TestSession.logger.info("response after deactivating the targets *****"+jsonUtil.formatString(resString));

		// check for response values
		JsonPath jsonPath = new JsonPath(resString);
		String actionName = jsonPath.getString("Response.ActionName");
		String responseId = jsonPath.getString("Response.ResponseId");
		assertTrue("Expected update action name , but found " + actionName , actionName.equals("update"));
		assertTrue("Expected 0, but found " + responseId , responseId.equals("0"));
		String responseMessage = jsonPath.getString("Response.ResponseMessage");
		boolean flag = responseMessage.contains(dataSetName) && responseMessage.contains("successful");
		assertTrue("failed to get the correct message, but found " + responseMessage , flag == true);

		// wait for some time, so that changes are reflected in the dataset i,e TARGETS gets INACTIVE
		this.sleep(30000);

		// Check whether targets in dataset are set to INACTIVE state
		List<String>targetsStatus = this.getDataSource(dataSetName , "target" , "status");
		TestSession.logger.info("************ testDeativateAllTargetsInDataSet ******** = " +targetsStatus.toString() );
		for(String tarStatus : targetsStatus){
			String status[] = tarStatus.split(":");
			assertTrue("Expected that targets are inactive , but got " + tarStatus , status[1].trim().equals("inactive"));
		}

		this.sleep(30000);
	}


	/**
	 * remove all the targets from the dataset.
	 * @param dataSetName
	 */
	public void removeTargetsFromDataset(String dataSetName) {
		JSONUtil jsonUtil = new JSONUtil();
		List<String>newDataTargetList = this.getDataSource(dataSetName, "target" ,"name");
		String resource = jsonUtil.constructResourceNamesParameter(Arrays.asList(dataSetName));
		TestSession.logger.info("resource = "+jsonUtil.formatString(resource));
		int size = newDataTargetList.size();

		// Since last target in the dataset can't be removed, so removing all the targets expect the last one.
		List<String>targets = newDataTargetList.subList(0, (size - 1));
		String args = jsonUtil.constructArgumentParameter(targets,"removeTarget");
		TestSession.logger.info("args *****"+jsonUtil.formatString(args));
		this.sleep(40000);

		com.jayway.restassured.response.Response res = given().cookie(this.httpHandle.cookie).param("resourceNames", resource).param("command","update").param("args", args)
				.post( this.getConsoleURL() + "/console/rest/config/dataset/actions");

		String resString = res.asString();
		TestSession.logger.info("response after removing the targets *****"+jsonUtil.formatString(resString));

		// Check for Response values
		JsonPath jsonPath = new JsonPath(resString);
		String actionName = jsonPath.getString("Response.ActionName");
		String responseId = jsonPath.getString("Response.ResponseId");
		assertTrue("Expected update action name , but found " + actionName , actionName.equals("update"));
		assertTrue("Expected 0, but found " + responseId , responseId.equals("0"));
		String responseMessage = jsonPath.getString("Response.ResponseMessage");
		boolean flag = responseMessage.contains(dataSetName) && responseMessage.contains("successful");
		assertTrue("failed to get the correct message, but found " + responseMessage , flag == true);
	}

	/**
	 * Removes or deletes the given dataset
	 */
	public void removeDataSet( String dataSetName ) {
		JSONUtil jsonUtil = new JSONUtil();
		String resource = jsonUtil.constructResourceNamesParameter(Arrays.asList(dataSetName));
		TestSession.logger.info("resource = "+jsonUtil.formatString(resource));

		// remove the dataset
		com.jayway.restassured.response.Response res = given().cookie(this.httpHandle.cookie).param("resourceNames", resource).param("command","remove")
				.post(this.getConsoleURL() + "/console/rest/config/dataset/actions");

		String resString = res.asString();
		TestSession.logger.info("response *****"+jsonUtil.formatString(resString));

		// Check for Response
		JsonPath jsonPath = new JsonPath(resString);
		String actionName = jsonPath.getString("Response.ActionName");
		String responseId = jsonPath.getString("Response.ResponseId");
		assertTrue("Expected remove action, but got " + actionName , actionName.equals("remove"));
		assertTrue("Expected 0, but found " + responseId , responseId.equals("0"));
		String responseMessage = jsonPath.getString("Response.ResponseMessage");
		boolean flag = responseMessage.contains(dataSetName) && responseMessage.contains("successful");
		assertTrue("failed to get the correct message, but found " + responseMessage , flag == true);
	}

	/*
	 * Print the reason for the failure of the dataset. Failure information is very much useful for debugging when testcase fails
	 * on CI.
	 * @param dataSetName  - dataset for which failure information need to fetch.
	 * @param datasetActivationTime -  dataset activate time.
	 */
	public void getFailureInformation(String dataSetName , String datasetActivationTime , String cookie) {
		String endTime = GdmUtils.getCalendarAsString();
		JSONUtil jsonUtil = new JSONUtil();
		TestSession.logger.info("endTime = " + endTime);
		String failureURL = this.consoleURL + "/console/api/workflows/failed?exclude=false&starttime=" + datasetActivationTime + "&endtime=" + endTime +
				"&joinType=innerJoin&datasetname=" +  dataSetName;
		com.jayway.restassured.response.Response res = given().cookie(cookie).get(failureURL);
		assertTrue("Failed to get the response for " + failureURL , (response != null || response.toString() != "") );

		JsonPath jsonPath = res.getBody().jsonPath();
		String executionID = jsonPath.getString("failedWorkflows[0].ExecutionID");
		TestSession.logger.info("ExecutionID = " + executionID);
		String facetName = jsonPath.getString("failedWorkflows[0].FacetName");
		TestSession.logger.info("FacetName = " + facetName);
		String coloName = jsonPath.getString("failedWorkflows[0].FacetColo");
		TestSession.logger.info("ColoName = " + coloName);

		// get the failure information.
		Response response = this.getWorkflowStepExecution(executionID, facetName, coloName);
		TestSession.logger.info("Failed Response = " + jsonUtil.formatString(response.toString()));
	}

	/**
	 * Get all the hcat enabled clusters
	 * @return - returns List<String> having all the hcat enabled cluster or return null if no cluster is hcat is enabled
	 */
	public List<String> getHCatEnabledGrid( ) {
		List<String> hcatEnabledGrid = new ArrayList<String>();
		String testURL = this.getConsoleURL() + "/console/query/hadoop/versions";
		TestSession.logger.info("testURL = " + testURL);
		JsonPath jsonPath = given().cookie(httpHandle.cookie).get(testURL).jsonPath();
		TestSession.logger.info("Get all the Hcat enabled grid response = " + jsonPath.prettyPrint());
		hcatEnabledGrid = jsonPath.getList("HadoopClusterVersions.findAll { it.HCatVersion.startsWith('hcat_common') }.ClusterName ");
		if (hcatEnabledGrid == null) {
			try {
				throw new Exception("Failed to get hcatEnabled");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return hcatEnabledGrid;
	}

	/**
	 * Get HCatSupported tag value for a given datasource
	 * @param dataSourceName - Name of the datasource
	 * @return
	 */
	public boolean isHCatEnabledForDataSource(String dataSourceName) {
		// get xml representation string of datasource
		String url = this.consoleURL + "/console/query/config/datasource/"+dataSourceName.trim();
		TestSession.logger.info("url = " + url);
		String xml = given().cookie(httpHandle.cookie).get(url).andReturn().asString();
		assertTrue("failed to get the source value for " + url , (xml != null || xml != "") );
		XmlPath xmlPath = new XmlPath(xml);
		xmlPath.setRoot("DataSource");
		boolean flag = xmlPath.getBoolean("HCatSupported");
		return flag;
	}

	/**
	 * Modify the specified tag value of the specified datasource
	 * @param hostName -
	 * @param dataSourceName - datasource name
	 * @param tagName - tag that needs to modify its value
	 * @param oldValue - old value of the tag
	 * @param newValue = new value of the tag
	 */
	public void modifyDataSource(String dataSourceName , String tagName , String oldValue , String newValue) {

		// read the specified datasource file & change the HCatSupported tag value
		String hostName =  this.conf.getString("hostconfig.console.base_url");
		String cookie = this.httpHandle.cookie;
		String testURL = hostName + "/console/query/config/datasource/"+dataSourceName;
		String xml = given().cookie(cookie).get(testURL).andReturn().asString();
		assertTrue("failed to get the source value for " + testURL , (xml != null || xml != "") );
		String oldTag = "<"+tagName+">"+oldValue +"</"+tagName+">";
		String newTag = "<"+tagName+">"+newValue +"</"+tagName+">";
		// check if the <tagName> exists i,e HCatSupported tag exists
		boolean isTagExist = xml.contains("HCatSupported");
		TestSession.logger.info("************  isTagExist = "+isTagExist);
		if (!isTagExist) {
			StringBuilder strBuilder = new StringBuilder(xml);
			int len = strBuilder.indexOf("<Resources>");
			TestSession.logger.info("length = " + len);
			strBuilder.insert(len , newTag);
			TestSession.logger.info("strBuilder = " + strBuilder);
			xml = strBuilder.toString();
			TestSession.logger.info("&&&&&&&& xml = "+xml);

		} else  {
			xml = xml.replaceAll(oldTag , newTag );
		}

		TestSession.logger.info("xml = "+xml);

		StringBuilder postBody = new StringBuilder();
		postBody.append("xmlFileContent=");
		postBody.append(xml);
		postBody.append("\naction=Edit\n");
		postBody.append("operation=1\n");

		// post the modified datasource file.
		HttpMethod postMethod = this.httpHandle.makePOST("/console/rest/config/datasource", null, postBody.toString());
		this.response = new Response(postMethod, false);
		assertTrue("Cloned failed and got http response " + response.getStatusCode() , response.getStatusCode() == 200);
		this.sleep(30000);
	}

	/**
	 * Method that returns specified attribute value of the tag
	 * @param dataSetName
	 * @param tagName
	 * @param attributeName
	 * @return
	 */
	public String getDataSetTagsAttributeValue(String dataSetName , String tagName, String attributeName) {
		String attributeValue = null;
		String xml = this.getDataSetXml(dataSetName);
		TestSession.logger.info("*****************xml = " + xml);
		XmlPath xmlPath = new XmlPath(xml);
		if(xmlPath == null)  {
			try {
				throw new Exception("Could not able to create an instance of xmlPath");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		TestSession.logger.info("*****" + xmlPath.prettyPrint());
		xmlPath.setRoot("DataSet");
		int size = 0;
		if (tagName.equals("Parameters")) {
			attributeValue = xmlPath.getString("Parameters.attribute[0].@" + attributeName);
		} else if (tagName.equals("Sources")) {
			attributeValue = xmlPath.getString("Sources.Source[0].@" + attributeName);
		} else if (tagName.equals("DateRange")) {
			attributeValue = xmlPath.getString("DateRange.@" + attributeName);
		}
		return attributeValue;
	}

	/**
	 * Get the HCat information about the given dataset.
	 * @param dataSourceName
	 * @param dataSetName
	 * @return
	 */
	public com.jayway.restassured.response.Response getHCatTableDetailsForDataSet(String dataSourceName , String dataSetName) {
		String HCatList = "/acquisition/api/admin/hcat/table/list";
		String dbName = "gdm";
		this.sleep(40000);
		String hcatListURL =  this.getConsoleURL().replace("9999", "4080") + HCatList +"?dataSource=" + dataSourceName + "&dbName="+ dbName + "&dataSet=" + dataSetName;
		TestSession.logger.info("hcatListURL  = "  + hcatListURL);
		com.jayway.restassured.response.Response response = given().cookie(this.httpHandle.cookie).get(hcatListURL);
		assertTrue("Failed to get the response for " + hcatListURL , (response != null || response.toString() != "") );
		return response;
	}

	/*
	 * Convert com.jayway.restassured.response.Response to jsonArray
	 */
	public JSONArray convertResponseToJSONArray(com.jayway.restassured.response.Response response , String jsonName) {
		JSONArray jsonArray =  null ;
		String res = response.getBody().asString();
		TestSession.logger.info("response = " + res);
		JSONObject obj =  (JSONObject) JSONSerializer.toJSON(res.toString());
		TestSession.logger.info("obj = " + obj.toString());
		jsonArray = obj.getJSONArray(jsonName);
		return jsonArray;
	}

	/**
	 * Get the port number for the specified facet
	 * @param facetName
	 * @return
	 */
	public String getFacetPortNo(String facetName) {
		String portNo = "9999";
		if (facetName.equals("acquisition")) {
			portNo = "4080";
		} else if (facetName.equals("replication")) {
			portNo = "4081";
		} else if (facetName.equals("retention")) {
			portNo = "4082";
		}
		return portNo;
	}

	/**
	 * Restart completed workflow of the specified workflow
	 * @param dataSetName - completed workflow dataset name
	 * @param facetName - facetname
	 */
	public void restartCompletedWorkFlow( String dataSetName , String facetName ) {
		String completedWorkFlowURL = this.getConsoleURL() +  "/" +   "console/api/workflows/completed?datasetname=" + dataSetName + "&instancessince=F&joinType=innerJoin";
		TestSession.logger.info("----------------------- restartWorkFlowURL = " + completedWorkFlowURL);
		String cookie = this.httpHandle.getBouncerCookie();
		com.jayway.restassured.response.Response restartCompletedWorkFlowResponse = given().cookie(cookie).get(completedWorkFlowURL);
		JSONArray jsonArray =   convertResponseToJSONArray(restartCompletedWorkFlowResponse , "completedWorkflows");

		TestSession.logger.info("--------------  size  = " + jsonArray.size());

		//String restartCompletedWorkFlowURL = this.getConsoleURL().replace("9999", this.getFacetPortNo(facetName)) +  "/" + facetName + "/api/admin/workflows";

		if ( jsonArray.size() > 0 ) {
			Iterator iterator = jsonArray.iterator();
			while (iterator.hasNext()) {
				JSONObject jsonObject = (JSONObject) iterator.next();
				TestSession.logger.info("failedJsonObject  = " + jsonObject.toString());
				String fName = jsonObject.getString("FacetName");
				String facetColo = jsonObject.getString("FacetColo");
				String executionId = jsonObject.getString("ExecutionID");

				JSONArray resourceArray = new JSONArray();
				resourceArray.add(new JSONObject().element("ExecutionID",executionId).element("FacetName", facetName).element("FacetColo", facetColo));
				
				String restartCompletedWorkFlowURL = this.getConsoleURL().replace("9999", this.getFacetPortNo(facetName)) +  "/" + facetName + "/api/admin/workflows";
				TestSession.logger.info("restartCompletedWorkFlowURL   = "  + restartCompletedWorkFlowURL);
				com.jayway.restassured.response.Response restartResponse = given().cookie(cookie).param("command" , "restart").parameters("workflowIds", resourceArray.toString()).post(restartCompletedWorkFlowURL);
				JsonPath jsonPath = restartResponse.getBody().jsonPath();
				TestSession.logger.info("restartResponse = " + jsonPath.prettyPrint());

				String responseId = jsonPath.getString("Response.ResponseId");
				assertTrue("Failed to restart the completed workflow for executionId  = " + executionId + "   dataset name = "  + dataSetName +  "   facet = " + facetName +"    responseId = "+ responseId  , responseId.equals("0"));
			}
		} 
	}

	/**
	 * returns HCat Partition values for a given dataSet
	 * @param dataSetName  - 
	 * @param dataSource
	 * @return
	 */
	public List<List<String>> checkHCatParitionValues(String dataSetName , String dataSource , String facetName) {
		String cookie = this.httpHandle.getBouncerCookie();
		String HCatParition = "/api/admin/hcat/partition/list";
		String hcatPartition = this.getConsoleURL().replace("9999", this.getFacetPortNo(facetName)) + "/"  + facetName + HCatParition +"?dataSource=" + dataSource + "&dataSet=" + dataSetName;
		TestSession.logger.info("hcatPartition  url =  " + hcatPartition);
		com.jayway.restassured.response.Response response = given().cookie(cookie).get(hcatPartition);
		JsonPath jsonPath = response.getBody().jsonPath();
		TestSession.logger.info("haoopLs = " + jsonPath.prettyPrint());

		List<List<String>>partitions = jsonPath.getList("Partitions.Values.Value");
		TestSession.logger.info("partitions  = " + partitions.toString() );
		return partitions;
	}

	/**
	 *  Get all the dataset name
	 * @return - all the dataset names as List<String>
	 */
	public List<String> getAllDataSetName()  {
		String cookie = this.httpHandle.getBouncerCookie();
		String url = this.getConsoleURL() + "/console/api/datasets/view";
		com.jayway.restassured.response.Response response = given().cookie(cookie).get(url);
		JsonPath jsonPath = response.getBody().jsonPath();
		TestSession.logger.info("haoopLs = " + jsonPath.prettyPrint());

		List<String> datasetNames = jsonPath.getList("DatasetsResult.DatasetName");
		assertTrue("Failed to get the dataset name = "  , datasetNames != null && datasetNames.size() > 0);
		return datasetNames;
	}

	/**
	 * Get all the installed grid names
	 * @return grid names as List
	 */
	public List<String> getAllInstalledGridName() {
		List<String> grids = null;
		String testURL = this.getConsoleURL() + "/console/api/datasources/view";
		TestSession.logger.info("testURL = " + testURL);
		String cookie = this.httpHandle.getBouncerCookie();
		JsonPath jsonPath = given().cookie(cookie).get(testURL).jsonPath();
		TestSession.logger.info("Get all the Hcat enabled grid response = " + jsonPath.prettyPrint());
		grids = jsonPath.getList("DataSourceResult.findAll { it.Type.equals('grid') }.DataSourceName ");
		return grids;
	}
	
	/**
	 * Returns a JSONArray representing the instance files for a given path in the given datasource
	 * @param dataSourceName - target grid name, where the instance files exists
	 * @param dataPath - string representing the data example : /data/daqdev/data/ or /data/daqdev/data/datasetName
	 * @return
	 */
	public JSONArray getDataSetInstanceFilesDetailsByPath(String dataSourceName , String dataPath) {
		String hadoopLSCommand = "/console/api/admin/hadoopls";
		String testURL = this.getConsoleURL()  + hadoopLSCommand + "?dataSource=" + dataSourceName + "&path=" +  dataPath  + "&format=json";
		TestSession.logger.info("testurl = " + testURL ) ;
		String cookie = this.httpHandle.getBouncerCookie();
		com.jayway.restassured.response.Response response = given().cookie(cookie).get(testURL);
		String responseString = response.getBody().asString();
		TestSession.logger.info("Response  : " + responseString);
		
		JSONObject jsonObject = (JSONObject)JSONSerializer.toJSON(responseString);
		JSONArray filesJSONArray = jsonObject.getJSONArray("Files");
		return filesJSONArray;
	}
	
	/**
	 * Returns a JSONArray representing the instance files for a given dataset & given datasource
	 * @param dataSourceName
	 * @param dataSetName
	 * @return
	 */
	public JSONArray getDataSetInstanceFilesDetailsByDataSetName(String dataSourceName , String dataSetName) {
		JSONArray files = null;
		String hadoopLSCommand = "/console/api/admin/hadoopls"; 
		String testURL = this.getConsoleURL()  + hadoopLSCommand + "?dataSource=" + dataSourceName + "&dataSet=" +  dataSetName  + "&format=json";
		TestSession.logger.info("testurl = " + testURL ) ;
		String cookie = this.httpHandle.getBouncerCookie();
		com.jayway.restassured.response.Response response = given().cookie(cookie).get(testURL);
		String responseString = response.getBody().asString();
		TestSession.logger.info("Response  : " + responseString);
		
		JSONObject jsonObject = (JSONObject)JSONSerializer.toJSON(responseString);
		JSONArray filesJSONArray = jsonObject.getJSONArray("Files");
		TestSession.logger.info("filesJSONArray  = " + filesJSONArray.size());
		return filesJSONArray;
	}
	
	
	public int getInstanceFilesExistsOnHDFSSearchByDataSetName(String dataSourceName , String dataSetName) {
		JSONArray instanceFilesJSONArray = this.getDataSetInstanceFilesDetailsByDataSetName(dataSourceName , dataSetName);
		TestSession.logger.info("instanceFilesJSONArray  = " + instanceFilesJSONArray.size());
		List<String> filesNames = new ArrayList<String>();
		int fileCount = 0;
		// select only the files
		if (instanceFilesJSONArray.size() > 0) {
			Iterator iterator = instanceFilesJSONArray.iterator();
			while (iterator.hasNext()) {
				JSONObject jsonObject = (JSONObject) iterator.next();
				TestSession.logger.info("Instance files details  = " + jsonObject.toString());
				
				String directory = jsonObject.getString("Directory");
				if (directory.equals("no")) {
					String path = jsonObject.getString("Path").trim();
					filesNames.add(path);
				}
			}
		}
		
		// check for data files only , we can discard schema and  count files
		for (String fileName : filesNames) {
			if (! (fileName.contains("schema")) || (fileName.contains("count")) ) {
				if (fileName.contains("part-")) {
					fileCount ++;
				}
			}
		}
		TestSession.logger.info("There are " + fileCount + " data files for " + dataSetName);
		return fileCount;
	}
	
	
	public JsonPath getAcquiredFilesFromHDFS(String dataSetName, String targetName, String path) {
        JsonPath acqJsonPath = getHDSFFileInfo(dataSetName,targetName,path);
        return acqJsonPath;
    }

    public void getReplicationFilesFromHDFS(String dataSetName, String targetName, String path) {
        JsonPath repJsonPath = getHDSFFileInfo(dataSetName,targetName,path);
    }
   
    public JsonPath getHDSFFileInfo(String dataSetName , String targetName , String path) {
        String hadoopLSCommand = "/console/api/admin/hadoopls";
        String hadoopLsURL = "http://" + TestSession.conf.getProperty("GDM_CONSOLE_NAME") + ":" + Integer.parseInt(TestSession.conf.getProperty("GDM_CONSOLE_PORT")) +
                        hadoopLSCommand + "?format=json&dataSource="+targetName + "&path="+path + dataSetName;
        TestSession.logger.info("hadoopLsURL = " + hadoopLsURL);
                com.jayway.restassured.response.Response response = given().cookie(httpHandle.cookie).get(hadoopLsURL);
                JsonPath jsonPath = response.getBody().jsonPath();
                //List<String> hdfsPath = jsonPath.getList("Files.Path");
                TestSession.logger.info("haoopLs = " + jsonPath.prettyPrint());
                return jsonPath;
    }
    
    /**
     * Returns  all the grids name as List
     * @return List<String>
     */
    public List<String> getAllGridNames() {
    	List<String> gridNames = new ArrayList<String>();
    	String testURL = this.getConsoleURL() + "/console/query/hadoop/versions";
    	TestSession.logger.info("Test URL = " + testURL);
    	com.jayway.restassured.response.Response response = given().cookie(httpHandle.cookie).get(testURL);
    	JsonPath jsonPath = response.getBody().jsonPath();
    	gridNames = jsonPath.getList("HadoopClusterVersions.ClusterName");
    	assertTrue("There is no any grids installed " + gridNames.size() , gridNames.size() > 0);
    	return gridNames;
    }
    
    /**
     * Returns the NameNode name of the specified cluster.
     * @param clusterName
     */
    public String getClusterNameNodeName(String clusterName) {
		String nameNodeName = null;
		
		String xml = this.getDataSourcetXml(clusterName);
		TestSession.logger.info("*****************xml = " + xml);
		XmlPath xmlPath = new XmlPath(xml);
		if(xmlPath == null)  {
			try {
				throw new Exception("Could not able to create an instance of xmlPath");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		TestSession.logger.info("*****" + xmlPath.prettyPrint());
		xmlPath.setRoot("DataSource");
		String value = null;
		List<String>clusterNames = xmlPath.getList("Interface.Command.BaseUrl");
		assertTrue("Failed to get the name node name, please check whether "+ clusterName +" datasource specification file exists." , clusterNames.size() > 0);
		for (String n : clusterNames) {
			TestSession.logger.info(n);
		}
		
		// remove protocol name like webhdfs, hdfs etc, just return only namenode name.
		String nn = clusterNames.get(0);
		int indexOf = nn.indexOf("//") + 2;
		nameNodeName = nn.substring(indexOf);
		TestSession.logger.info(nameNodeName  + "  is the NameNode of  " + clusterName);
		return nameNodeName;
	}
    
    /**
	 * Verify whether killed dataset is in failed state.
	 */
	public JSONArray validateDatasetHasFailedWorkflow(String facetName , String datasetName , String activationTime)  {
		int failedJob = 0;
		JSONArray jsonArray = null;
		String failedWorkFlowURL = this.getConsoleURL() + "/console/api/workflows/failed?starttime=" + activationTime +"&endtime=" + GdmUtils.getCalendarAsString() + "&datasetname=" + datasetName 
				+ "&instancessince=F&joinType=innerJoin";
		TestSession.logger.info("Failed workflow testURL" + failedWorkFlowURL);
		String cookie =  this.httpHandle.getBouncerCookie();
		com.jayway.restassured.response.Response failedResponse = given().cookie(cookie).get(failedWorkFlowURL);
		assertTrue("Failed to get the respons  " + failedResponse , (failedResponse != null ) );
		
		jsonArray = this.convertResponseToJSONArray(failedResponse , "failedWorkflows");
		if ( jsonArray.size() > 0 ) {
			failedJob = jsonArray.size() ;
			Iterator iterator = jsonArray.iterator();
			while (iterator.hasNext()) {
				JSONObject failedJsonObject = (JSONObject) iterator.next();
				TestSession.logger.info("failedJsonObject  = " + failedJsonObject.toString());
				String fs = failedJsonObject.getString("FacetName");
				String workFlowName = failedJsonObject.getString("WorkflowName");
				String executionStatus = failedJsonObject.getString("ExecutionStatus");
				String exitStatus = failedJsonObject.getString("ExitStatus");

				assertTrue("Expected facetName is acquisition , but got " + fs , facetName.equalsIgnoreCase(facetName.trim()) ); 
				assertTrue("Expected executionStatus is STOPPED , but got " + exitStatus , executionStatus.equalsIgnoreCase("STOPPED") );
				assertTrue("Expected exitStatus is INTERRUPTED , but got " + exitStatus , exitStatus.equalsIgnoreCase("INTERRUPTED") );
			}
		} else if ( jsonArray.size()  == 0) {
			fail("Failed : " + datasetName  +"   dn't exists in failed workflow.");
		}
		return jsonArray;
	}

	/**
	 * set retention policy for a given dataset
	 * @param dataSetName - name of the dataset
	 */
	public void setRetentionPolicyToAllDataSets(String dataSetName , String retentionValue) {
		String testURL = this.getConsoleURL() + "/console/rest/config/dataset/getRetentionPolicies";
		JSONUtil jsonUtil = new JSONUtil();
		// navigate all the datasets and set the retention policy to all the targets

		TestSession.logger.info("dsName = " + dataSetName);
		List<String> dataTargetList = this.getDataSource(dataSetName , "target" ,"name");
		TestSession.logger.info("dataTargetList  = "  + dataTargetList);
		String resourceName = jsonUtil.constructResourceNamesParameter(Arrays.asList(dataSetName));
		List<String> arguments = new ArrayList<String>();
		for (String target : dataTargetList) {
			arguments.add("numberOfInstances:"+ retentionValue.trim() +":" + target.trim());
			TestSession.logger.info("grid = " + target);
			String args = constructPoliciesArguments(arguments , "updateRetention");
			TestSession.logger.info("args = "+args);
			TestSession.logger.info("test url = " + testURL);
			com.jayway.restassured.response.Response res = given().cookie(httpHandle.cookie).param("resourceNames", resourceName).param("command","update").param("args", args)
					.post(this.getConsoleURL() + "/console/rest/config/dataset/actions");
			TestSession.logger.info("Response code = " + res.getStatusCode());
			assertTrue("Failed to modify or set the retention policy for " + dataSetName , res.getStatusCode() == SUCCESS);

			String resString = res.getBody().asString();
			TestSession.logger.info("resString = " + resString);
			
			this.sleep(5000);
		}

	}
	
	/**
	 * Construct a policyType for the given grids or targets
	 * @param policiesArguments
	 * @param action
	 * @return
	 */
	private String constructPoliciesArguments(List<String>policiesArguments , String action) {
		JSONObject actionObject = new JSONObject().element("action", action);
		String args = null;
		JSONArray resourceArray = new JSONArray();
		for ( String policies : policiesArguments) {
			List<String> values = Arrays.asList(policies.split(":"));
			JSONObject policy = new JSONObject().element("policyType", values.get(0)).element("days", values.get(1)).element("target", values.get(2));
			resourceArray.add(policy);
		}
		actionObject.put("policies", resourceArray);
		args = actionObject.toString();
		return args;
	}
	
	/**
	 * Remove the specified datasource
	 * @param dataSourceName
	 */
	public void removeDataSource(String dataSourceName) {
		JSONUtil jsonUtil = new JSONUtil();
		String resource = jsonUtil.constructResourceNamesParameter(Arrays.asList(dataSourceName));

		// Deactivate datasource
		com.jayway.restassured.response.Response res = given().cookie(httpHandle.cookie).param("resourceNames", resource).param("command","terminate")
				.post(this.getConsoleURL() + "/console/rest/config/datasource/actions");

		String resString = res.asString();
		TestSession.logger.info("response *****"+ jsonUtil.formatString(resString));

		// Check for Response
		JsonPath jsonPath = new JsonPath(resString);
		String actionName = jsonPath.getString("Response.ActionName");
		String responseId = jsonPath.getString("Response.ResponseId");
		assertTrue("Expected terminate keyword, but got " + actionName , actionName.equals("terminate"));
		assertTrue("Expected 0, but found " + responseId , responseId.equals("0"));
		String responseMessage = jsonPath.getString("Response.ResponseMessage");
		boolean flag = responseMessage.contains(dataSourceName.trim()) && responseMessage.contains("successful");
		assertTrue("failed to get the correct message, but found " + responseMessage , flag == true);

		// wait for some time, so that changes are reflected to the datasource specification file i,e active to inactive
		this.sleep(5000);

		// remove datasource
		res = given().cookie(httpHandle.cookie).param("resourceNames", resource).param("command","remove")
				.post(this.getConsoleURL() + "/console/rest/config/datasource/actions");

		resString = res.asString();
		TestSession.logger.info("response *****"+ jsonUtil.formatString(resString));

		// Check for Response
		jsonPath = new JsonPath(resString);
		actionName = jsonPath.getString("Response.ActionName");
		responseId = jsonPath.getString("Response.ResponseId");
		assertTrue("Expected terminate keyword, but got " + actionName , actionName.equals("remove"));
		assertTrue("Expected 0, but found " + responseId , responseId.equals("0"));
		responseMessage = jsonPath.getString("Response.ResponseMessage");
		flag = responseMessage.contains(dataSourceName.trim()) && responseMessage.contains("successful");
		assertTrue("failed to get the correct message, but found " + responseMessage , flag == true);
	}
}
