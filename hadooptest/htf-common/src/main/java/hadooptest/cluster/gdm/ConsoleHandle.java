// Copyright 2016, Yahoo Inc.
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
import java.util.stream.Collectors;

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
import org.apache.commons.lang.StringUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.jayway.restassured.RestAssured;
import com.jayway.restassured.path.json.JsonPath;
import com.jayway.restassured.path.xml.XmlPath;

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import hadooptest.Util;

public final class ConsoleHandle {
    private static final String WORKFLOW_COMPLETED_EXECUTION_STATUS = "COMPLETED";
    private static final String WORKFLOW_COMPLETED_EXIT_STATUS = "COMPLETED";
    private static final String WORKFLOW_FAILED_EXIT_STATUS = "FAILED";
    private static final int SUCCESS = 200;
    public static final String HCAT_LIST_REST_API = "/api/admin/hcat/table/list?dataSource=";
    public static final String RUNNING_WORKFLOW = "/api/workflows/running?exclude=false&joinType=innerJoin";
    public static final String KILL_WORKFLOW = "/api/admin/proxy/workflows";
    public static final String HCAT_TABLE_PARTITION = "replication/api/admin/hcat/partition/list?";
    private static final String HEALTH_CHECKUP_API = "/console/api/proxy/health";
    
    public HTTPHandle httpHandle = null;
    private Response response;
    private String consoleURL;
    private String crossColoConsoleURL;
    private String preserveConsoleURL;
    private Configuration conf;
    private String username;
    private String passwd;
    private final int workflowPingIterations = 20;
    private final long timeInSecToReachRunningState =300L;
    private String source1;
    private String source2;
    private String target1;
    private String target2;
    private String target3;
    private String environmentType;
    private String acquisitionHostName;
    private String replicationHostName;
    private String retentionHostName;
    
    public ConsoleHandle() {
        init();
        this.httpHandle = new HTTPHandle();
        try
        {
            this.username = this.conf.getString("auth.usr");
            this.passwd = Util.getTestUserPasswordFromYkeykey(this.username);
            if ( this.passwd != null) {
        	this.httpHandle.logonToBouncer(this.username, this.passwd);
            }
            TestSession.logger.debug("logon OK");
        } catch (Exception ex) {
            TestSession.logger.error("Exception thrown", ex);
        }
    }
    
    /**
     * This constructor is used by non-admin 
     * @param userName
     * @param passWord
     */
    public ConsoleHandle(String userName , String passWord) {
        init();
        this.httpHandle = new HTTPHandle(userName,passWord);
        this.httpHandle.logonToBouncer(userName,passWord);
        TestSession.logger.debug("logon OK");
    }
    
    public HTTPHandle getHttpHandle() {
        return this.httpHandle;
    }
    
    private void init() {
        try
        {
            String configPath = Util.getResourceFullPath("gdm/conf/config.xml");
            this.conf = new XMLConfiguration(configPath);
            this.environmentType = this.conf.getString("hostconfig.console.test_environment_type");
            if (this.environmentType.equals("staging")) {
                TestSession.logger.debug("****** Staging test Environment ******** ");
                this.consoleURL = this.conf.getString("hostconfig.console.staging_console_url");
                this.acquisitionHostName = this.conf.getString("hostconfig.console.acquisitionHostName");
                this.replicationHostName = this.conf.getString("hostconfig.console.replicationHostName");
                this.retentionHostName = this.conf.getString("hostconfig.console.retentionHostName");
            } else  {
                TestSession.logger.info("****** Specified invalid test environment ******** ");
                System.exit(1);
            }
            this.crossColoConsoleURL = this.conf.getString("hostconfig.console.crossColo_url");
            TestSession.logger.debug("crossColoConsoleURL  = " + this.crossColoConsoleURL);
            TestSession.logger.debug("Console Base URL: " + this.consoleURL);
            
            this.source1 = this.conf.getString("sources.source1");
            this.source2 = this.conf.getString("sources.source2");

            this.target1 = this.conf.getString("targets.target1");
            this.target2 = this.conf.getString("targets.target2");
        } catch (ConfigurationException ex) {
            TestSession.logger.error(ex.toString());
        }
    }
    
    /**
     * return the acquisition hostname
     * @return
     */
    private String getAcquisitionHostName(){
        return this.acquisitionHostName;
    }

    /**
     * return the replication name
     * @return
     */
    private String getReplicationHostName() {
        if (this.replicationHostName == null) {
            Assert.fail("replicationHostName is not defined");
        }
        return this.replicationHostName;
    }

    /**
     * return the retention hostname
     * @return
     */
    private String getRetentionHostName() {
        return this.retentionHostName;
    }

    /**
     * return the specified facet hostname
     * @param facetName
     * @return
     */
    public String getFacetHostName(String facetName) {
        String hostName = null;
        if (facetName.equals("acquisition")) {
            hostName = this.getAcquisitionHostName();
        } else if (facetName.equals("replication")) {
            hostName = this.getReplicationHostName();
        } else if (facetName.equals("retention")) {
            hostName = this.getRetentionHostName();
        } else if (facetName.equals("console")) {
            hostName = Arrays.asList(this.consoleURL.split(":")).get(1).replaceAll("//", "");
        } else {
            TestSession.logger.error("Unknown facetName specified " + facetName);
            fail("Unknown facetName specified " + facetName);
        }
        return hostName;
    }
    
    /**
     * get the rest api based on the environment and facet type
     * @param restApiType
     * @param facetName
     * @return
     */
    public String getRestAPI(String restApiType , String facetName ) {
        String api = this.getAPI(restApiType);
        StringBuffer buffer = new StringBuffer("http://").append(this.getFacetHostName(facetName)).append(":9999/").append(facetName).append(api);
        String url = buffer.toString();
        return url;
    }
    
    private String getAPI(String restApiType) {
        String api = null;
        if (restApiType.equals(HCAT_LIST_REST_API)) {
            api = HCAT_LIST_REST_API;
        } else if (restApiType.equals(RUNNING_WORKFLOW)) {
            api = RUNNING_WORKFLOW;
        } else if (restApiType.equals(KILL_WORKFLOW)) {
            api = KILL_WORKFLOW;
        } else if (restApiType.equals(HCAT_LIST_REST_API)) {
            api = HCAT_LIST_REST_API;
        } else if (restApiType.equals(HCAT_TABLE_PARTITION)) {
            api = HCAT_TABLE_PARTITION;
        }
        return api;
    }
    
    
    /**
     * Return the instance of Configuration
     * @return
     */
    public Configuration getConf(){
        if (this.conf == null) {
            String configPath = Util.getResourceFullPath("gdm/conf/config.xml");
            try {
                this.conf = new XMLConfiguration(configPath);
            } catch (ConfigurationException e) {
                e.printStackTrace();
            }
        }
        return this.conf;
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

        postMethod = this.httpHandle.makePOST(resource, data);
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

        postMethod = this.httpHandle.makePOST(resource, data);
        TestSession.logger.info("Deactivating DataSet " + dataSetName);
        this.response = new Response(postMethod);
        TestSession.logger.debug(this.response.toString());

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
        String resource = this.conf.getString("hostconfig.console.datasets.clone.resource")  + "?action=Create&operation=1";
        TestSession.logger.info("cloneDataSet resource - " + resource);
        String xmlFileContent = GdmUtils.readFile(configDataFile);
        ArrayList params = new ArrayList();
        StringBuilder postBody = new StringBuilder();

        xmlFileContent = xmlFileContent.replaceAll("name=\"" + originalDataSetName, "name=\"" +dataSetName);

        TestSession.logger.debug("XML length: " + xmlFileContent.length());

        postBody.append("xmlFileContent=");
        postBody.append(xmlFileContent);

        TestSession.logger.info("cloneDataSet(dataSetName=" + dataSetName + ", xmlFileContent=" + xmlFileContent);
        TestSession.logger.info("** cloneDataSet(dataSetName=" + dataSetName + ", xmlFileContent=" + xmlFileContent);

        HttpMethod postMethod = this.httpHandle.makePOST(resource, postBody.toString());
        this.response = new Response(postMethod, false);

        return this.response;
    }

    public Response cloneDataSet(String dataSetName, String configDataFile)
    {
        String resource = this.conf.getString("hostconfig.console.datasets.clone.resource") + "?action=Create&operation=1";

        String xmlFileContent = GdmUtils.readFile(configDataFile);
        ArrayList params = new ArrayList();
        StringBuilder postBody = new StringBuilder();

        xmlFileContent = xmlFileContent.replaceAll("NEW_DATA_SET_NAME", dataSetName);
        xmlFileContent = xmlFileContent.replaceAll("SOURCE_1", this.source1);
        xmlFileContent = xmlFileContent.replaceAll("TARGET_1", this.target1);

        TestSession.logger.debug("XML length: " + xmlFileContent.length());
        postBody.append("xmlFileContent=");
        postBody.append(xmlFileContent);
        
        TestSession.logger.info("cloneDataSet(dataSetName=" + dataSetName + ", xmlFileContent=" + xmlFileContent);
        TestSession.logger.info("** cloneDataSet(dataSetName=" + dataSetName + ", xmlFileContent=" + xmlFileContent);
        HttpMethod postMethod = this.httpHandle.makePOST(resource, postBody.toString());
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
     * Returns true if files exist on the specified grid path, false otherwise.
     *
     * @param grid  a grid datastore name
     * @param path   the path to check
     * @return true if files exist for the path, false otherwise
     * 
     * @deprecated  HadoopFileSystemHelper.exists() should be used
     */
    @Deprecated
    public boolean filesExist(String grid, String path) {
        ArrayList params = new ArrayList();
        params.add(new CustomNameValuePair("dataSource", grid));
        params.add(new CustomNameValuePair("path", path));
        HttpMethod getMethod = this.httpHandle.makeGET(this.consoleURL, "/console/api/admin/hadoopls", params);
        Response response = new Response(getMethod);
        if (response.getStatusCode() != SUCCESS) {
            Assert.fail("filesExist check for file " + path + " on datastore " + grid + " failed with status " + response.getStatusCode());
        }
        String responseString = response.getResponseBodyAsString();
        if (StringUtils.isBlank(responseString)) {
            TestSession.logger.info(path + " on " + grid + " does not exist");
            return false;
        } else {
            TestSession.logger.info(path + " on " + grid + " exists");
            return true;
        }
    }
    
    /**
     * Returns true if files exist on the specified grid path, false otherwise.
     *
     * @param grid  a grid datastore name
     * @param path   the path to check
     * @param dataSetName   dataset to be used for S3 parameters
     * @return true if files exist for the path, false otherwise
     * 
     */
    // TODO: currently not working as headless user is not an admin on replication
    public boolean s3FilesExist(String grid, String path, String dataSetName) {
        ArrayList params = new ArrayList();
        params.add(new CustomNameValuePair("dataSource", grid));
        params.add(new CustomNameValuePair("path", path));
        params.add(new CustomNameValuePair("paramsDataSet", dataSetName));
        String endpoint = "http://" + this.getReplicationHostName() + ":4080";
        HttpMethod getMethod = this.httpHandle.makeGET(endpoint, "/replication/api/admin/hadoopls", params);
        Response response = new Response(getMethod);
        if (response.getStatusCode() != SUCCESS) {
            Assert.fail("filesExist check for file " + path + " on datastore " + grid + " failed with status " + response.getStatusCode());
        }
        String responseString = response.getResponseBodyAsString();
        if (StringUtils.isBlank(responseString)) {
            TestSession.logger.info(path + " on " + grid + " does not exist");
            return false;
        } else {
            TestSession.logger.info(path + " on " + grid + " exists");
            return true;
        }
    }

    /**
     * Replaces the contents of an existing dataSet
     *
     * @param dataSetName  the dataSet to modify
     * @param xmlFileContent  the new dataSet contents
     * @return the console response
     */
    public Response modifyDataSet(String dataSetName, String xmlFileContent) {
        String resource = this.conf.getString("hostconfig.console.datasets.clone.resource") + "?action=Edit&operation=1";;
        StringBuilder postBody = new StringBuilder();
        postBody.append("xmlFileContent=");
        postBody.append(xmlFileContent);

        TestSession.logger.info("modifyDataSet(dataSetName=" + dataSetName + ", xmlFileContent=" + xmlFileContent);
        HttpMethod postMethod = this.httpHandle.makePOST(resource, postBody.toString());
        this.response = new Response(postMethod, false);
        return this.response;
    }
    
    /**
     * Does a POST to a console url and returns the response
     *
     * @param urlPath  sub-url after the console endpoint
     * @param paramArrayList   parameters to post
     * @return the console response
     */
    public Response postToConsole(String urlPath, ArrayList<CustomNameValuePair> paramArrayList) {      
        String url = "/console" + urlPath;
        HttpMethod postMethod = this.httpHandle.makePOST(url, paramArrayList);        
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
    
    public Response getFailedJobsForADataSet(String startTime, String endTime, String dataSetName) {
        return getFailedJobsForDataSet(startTime, endTime, dataSetName, null);
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
        TestSession.logger.debug("createDataSetFromConfig(dataSetName=" + dataSetName + ", xmlFileContent=" + xmlFileContent);
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
        String resource = this.conf.getString("hostconfig.console.datasets.clone.resource") + "?action=Create&operation=1";
        StringBuilder postBody = new StringBuilder();
        postBody.append("xmlFileContent=");
        postBody.append(xmlFileContent);
        TestSession.logger.debug("createDataSet(dataSetName=" + dataSetName + ", xmlFileContent=" + xmlFileContent);
        HttpMethod postMethod = this.httpHandle.makePOST(resource, postBody.toString());
        this.response = new Response(postMethod, false);
        TestSession.logger.info("Created dataset " + dataSetName);
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
        String resource = this.conf.getString("hostconfig.console.datasource.clone.resource")  + "?action=Create&operation=1";
        TestSession.logger.info("CreateDataSource resource = " + resource);
        StringBuilder postBody = new StringBuilder();
        postBody.append("xmlFileContent=");
        postBody.append(xmlFileContent);
        HttpMethod postMethod = this.httpHandle.makePOST(resource, postBody.toString());
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
    public String getDataSourceXml(String dataSourceName) {               
        String url = this.getConsoleURL() + "/console/query/config/datasource/"+dataSourceName;
        TestSession.logger.debug("url = " + url);
        return this.getXml(url);
    }

    private String getXml(String query) {
        String xml = null;      
        com.jayway.restassured.response.Response response = RestAssured.given().cookie(httpHandle.cookie).get(query);
        if (response.getStatusCode() == 200) {
            xml = response.andReturn().asString();
            TestSession.logger.debug("xml = " + xml);
            return xml;
        }
        return xml;
    }

    /**
     * @return list of grid dataSource names on the console
     */
    public List<String> getUniqueGrids() {
        try {
            List<String> grids = new ArrayList<String>();
            JSONArray dataSourceResult = this.getDataSources();
            if (dataSourceResult != null) {
                Iterator iterator = dataSourceResult.iterator();
                while (iterator.hasNext()) {
                    JSONObject dataSource = (JSONObject)iterator.next();
                    if (dataSource.getString("Type").equalsIgnoreCase("grid")) {
                        
                        String name = dataSource.getString("DataSourceName");
                        
                        // if the grid skips uploading jars, assume it is an S3 grid
                        String dataSourceXml = this.getDataSourceXml(name);
                        if (dataSourceXml.contains("skip.job.jars.upload")) {
                            continue;
                        }
                        
                        // ignore inactive grids
                        if (dataSource.getString("IsActive").equalsIgnoreCase("true")) { 
                            grids.add(name);
                        }
                    }
                }
            } else {
                throw new Exception("unable to find DataSourceResult");
            }
            
            String listString = "Unique grids are: ";
            for (String s : grids) {
                listString += s + "  ";
            }
            return getHealthGrids(grids);
        } catch (Exception e) {
            TestSession.logger.error("Unexpected exception", e);
            Assert.fail("Unexpected exception - " + e.getMessage());
            return null;
        }
    }
    
    public List<String> getHealthGrids(List<String> gridList) throws Exception {
	Collection<Callable<String>> gridHostList = new ArrayList<Callable<String>>();
	Collection<Callable<String>> tGridHostList = new ArrayList<Callable<String>>();
	List<String> healthyGrids = new ArrayList<String>();
	List<String> unHealthyGrids = new ArrayList<String>();
	ExecutorService executor = Executors.newFixedThreadPool(5);
	ExecutorService executor1 = Executors.newFixedThreadPool(5);
	
	for ( String grid : gridList) {
	    gridHostList.add(new GetGridNameNodeName(grid));
	}
	
	// get namenode hostname of all the grid
	List<Future<String>> executorResultList = executor.invokeAll(gridHostList);
	for (Future<String> result : executorResultList) {
	    List<String> value = Arrays.asList(result.get().split("~"));
	    tGridHostList.add(new ClusterHealthCheckup(value.get(0) , value.get(1)));
	}
	
	executor.shutdown();

	// check whether the given grid is healthy
	executorResultList = executor1.invokeAll(tGridHostList);
	for (Future<String> result : executorResultList) {
	    List<String> value = Arrays.asList(result.get().split(":"));
	    String gName = value.get(0).trim();
	    String hStatus = value.get(1).trim();
	    if (hStatus.equals("UP")) {
		healthyGrids.add(gName);
	    } else if (hStatus.equals("DOWN")) {
		unHealthyGrids.add(gName);
	    }
	}
	
	executor1.shutdown();
	
	TestSession.logger.info("Healthy Grids = " + healthyGrids.toString());
	if (unHealthyGrids.size() > 0) {
	    TestSession.logger.info("UnHealthy Grids = " + unHealthyGrids.toString());
	}
	
	return healthyGrids;
    }
    
    class GetGridNameNodeName implements Callable<String> {
	private String clusterName;
	
	public GetGridNameNodeName(String clusterName ) {
		this.clusterName = clusterName;
	}
	
	@Override
	public String call() throws Exception {
		String command = "yinst range -ir \"(@grid_re.clusters." + this.clusterName + "." + "namenode" +")\"";
		TestSession.logger.info("Command = " + command);
		WorkFlowHelper workFlowHelper = new WorkFlowHelper();
		String hostName = workFlowHelper.executeCommand(command);
		Thread.sleep(100);
		return this.clusterName  + "~" + hostName;
	}
}
    
    class ClusterHealthCheckup implements java.util.concurrent.Callable<String>{
	private String clusterName;
	private String nameNodeHostName;
	
	public ClusterHealthCheckup(String clusterName, String nameNodeHostName) {
	    this.clusterName = clusterName;
	    this.nameNodeHostName = nameNodeHostName;
	}
	
	@Override
	public String call() {
	    WorkFlowHelper workFlowHelper = new WorkFlowHelper();
	    String command = "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null  " + this.nameNodeHostName +  "  \"" + "hadoop version\"";
	    TestSession.logger.info("command - " + command);
	    String result = workFlowHelper.executeCommand(command);
	    return this.getHadoopVersion(result) ? this.clusterName + ":UP" : this.clusterName +":DOWN";
	}
	
	private boolean getHadoopVersion(String result) {
	    String hadoopVersion = null;
	    boolean flag = false;
	    if (result != null) {
		TestSession.logger.info("hadoop version result = " + result);
		java.util.List<String>outputList = Arrays.asList(result.split("\n"));
		for ( String str : outputList) {
		    TestSession.logger.info(str);
		    if ( str.startsWith("Hadoop") == true ) {
			hadoopVersion = Arrays.asList(str.split(" ")).get(1);
			flag = true;
			break;
		    }
		}
		TestSession.logger.info("Hadoop Version - " + hadoopVersion);	
	    }
	    return flag;
	}
    }
    
    /**
     * @return list of S3 grid dataSource names on the console
     */
    public List<String> getS3Grids() {
        try {
            List<String> grids = new ArrayList<String>();
            
            JSONArray dataSourceResult = this.getDataSources();
            if (dataSourceResult != null) {
                Iterator iterator = dataSourceResult.iterator();
                while (iterator.hasNext()) {
                    JSONObject dataSource = (JSONObject)iterator.next();
                    if (dataSource.getString("Type").equalsIgnoreCase("grid")) {
                        
                        String name = dataSource.getString("DataSourceName");
                        
                        // if the grid skips uploading jars, assume it is an S3 grid
                        String dataSourceXml = this.getDataSourceXml(name);
                        if (!dataSourceXml.contains("skip.job.jars.upload")) {
                            continue;
                        }
                        
                        // ignore inactive grids
                        if (dataSource.getString("IsActive").equalsIgnoreCase("true")) { 
                            grids.add(name);
                        }
                    }
                }
            } else {
                throw new Exception("unable to find DataSourceResult");
            }
            
            String listString = "Unique S3 grids are: ";
            for (String s : grids) {
                listString += s + "  ";
            }
            TestSession.logger.info(listString);
            
            return grids;
        } catch (Exception e) {
            TestSession.logger.error("Unexpected exception", e);
            Assert.fail("Unexpected exception - " + e.getMessage());
            return null;
        }
    }
    
    private JSONArray getDataSources() throws Exception {
        String url = this.consoleURL + "/console/api/datasources/view";
        String cookie = this.httpHandle.getBouncerCookie();
        com.jayway.restassured.response.Response response = RestAssured.given().cookie(cookie).get(url);
        if (response.getStatusCode() == 200) {
            String dataSourceListing = response.andReturn().asString();
            TestSession.logger.debug("Received datasources listing: " + dataSourceListing);
            JSONObject json = (JSONObject)JSONSerializer.toJSON(dataSourceListing);
            JSONArray dataSourceResult = (JSONArray)json.get("DataSourceResult");
            return dataSourceResult;
        } else {
            throw new Exception(url + " returned status code " + response.getStatusCode());
        }
    }
    
    /**
     * @return list of active warehouse datastores
     * @throws Exception
     */
    public List<String> getWarehouseDatastores() throws Exception {
        List<String> datastores = new ArrayList<String>();
        
        JSONArray dataSourceResult = this.getDataSources();
        if (dataSourceResult != null) {
            Iterator iterator = dataSourceResult.iterator();
            while (iterator.hasNext()) {
                JSONObject dataSource = (JSONObject)iterator.next();
                if (dataSource.getString("Type").equalsIgnoreCase("warehouse")) {
                    // ignore inactive grids
                    if (dataSource.getString("IsActive").equalsIgnoreCase("true")) { 
                        String name = dataSource.getString("DataSourceName");
                        datastores.add(name);
                    }
                }
            }
        } else {
            throw new Exception("unable to find DataSourceResult");
        }
        
        return datastores;
    }
    
    /**
     * @return list of active archival datastores
     * @throws Exception 
     */
    public List<String> getArchivalDataStores() throws Exception {
        List<String> datastores = new ArrayList<String>();
        
        JSONArray dataSourceResult = this.getDataSources();
        if (dataSourceResult != null) {
            Iterator iterator = dataSourceResult.iterator();
            while (iterator.hasNext()) {
                JSONObject dataSource = (JSONObject)iterator.next();
                if (dataSource.getString("Type").equalsIgnoreCase("hiosink")) {
                    // ignore inactive grids
                    if (dataSource.getString("IsActive").equalsIgnoreCase("true")) { 
                        String name = dataSource.getString("DataSourceName");
                        datastores.add(name);
                    }
                }
            }
        } else {
            throw new Exception("unable to find DataSourceResult");
        }
        return datastores;    	
    }

    /**
     * Creates a dataSet config from an Xml source and submits it to the console
     *
     * @param dataSetName    Desired name of dataSet
     * @param xmlFileContent   Xml for dataSet
     * @return console response from submitting dataSet
     */
    public Response createDataSource(String oldDataSetName, String newDataSetName, String xmlFileContent) {
    	String resource = this.conf.getString("hostconfig.console.datasource.clone.resource") + "?action=Create&operation=1";

    	// just change the name attribute, remaining be the same
    	xmlFileContent = xmlFileContent.replaceFirst(oldDataSetName, newDataSetName);
    	TestSession.logger.info("xmlFileContent  = "+xmlFileContent);
    	StringBuilder postBody = new StringBuilder();
    	postBody.append("xmlFileContent=");
    	postBody.append(xmlFileContent);
    	TestSession.logger.info("createDataSet(dataSetName=" + newDataSetName + ", xmlFileContent=" + xmlFileContent);
    	HttpMethod postMethod = this.httpHandle.makePOST(resource, postBody.toString());
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

        // post the modified datasource file. 
        HttpMethod postMethod = this.httpHandle.makePOST("/console/rest/config/datasource" + "?action=Edit&operation=1", postBody.toString());
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
        try {
            String executionID = jsonPath.getString("failedWorkflows[0].ExecutionID");
            TestSession.logger.info("ExecutionID = " + executionID);
            String facetName = jsonPath.getString("failedWorkflows[0].FacetName");
            TestSession.logger.info("FacetName = " + facetName);
            String coloName = jsonPath.getString("failedWorkflows[0].FacetColo");
            TestSession.logger.info("ColoName = " + coloName);
    
            // get the failure information.
            Response response = this.getWorkflowStepExecution(executionID, facetName, coloName);
            TestSession.logger.info("Failed Response = " + jsonUtil.formatString(response.toString()));
        } catch (Exception e) {
            TestSession.logger.error("Unable to get failure information", e);
        }
    }

    /**
     * Get all the hcat enabled clusters
     * @return - returns List<String> having all the hcat enabled cluster or return null if no cluster is hcat is enabled
     */
    public List<String> getHCatEnabledGrid( ) {
        List<String> hcatEnabledGridList = new ArrayList<String>();
        String testURL = this.getConsoleURL() + "/console/query/hadoop/versions";    
        com.jayway.restassured.response.Response response = given().cookie(httpHandle.cookie).get(testURL);
        String responseString = response.getBody().asString();
        String gridName="";
        JSONObject versionObj =  (JSONObject) JSONSerializer.toJSON(responseString.toString());
        Object obj = versionObj.get("HadoopClusterVersions");
        if (obj instanceof JSONArray) {
            JSONArray sizeLimitAlertArray = versionObj.getJSONArray("HadoopClusterVersions");
            Iterator iterator = sizeLimitAlertArray.iterator();
            while (iterator.hasNext()) {
                JSONObject jsonObject = (JSONObject) iterator.next();
                Iterator<String> keys  = jsonObject.keys();
                while( keys.hasNext() ) {
                    String key = (String)keys.next();
                    if (key.equals("DataStoreName") ) {
                        gridName = jsonObject.getString(key);
                    }
                    String value = jsonObject.getString(key);
                    if (value.startsWith("hcat_common")) {
                        if (! gridName.startsWith("gdm")) {
                            hcatEnabledGridList.add(gridName);  
                        }
                    }
                }
            }
            TestSession.logger.info(hcatEnabledGridList);
        }
        return hcatEnabledGridList;
    }
    
    /**
     * Get the hadoop vesion installed on the specified cluster.
     * @param clusterName
     * @return
     */
    public String getClusterInstalledVersion(String clusterName) {
        boolean flag = false;
        String version = null;
        String testURL = this.getConsoleURL() + "/console/query/hadoop/versions";
        TestSession.logger.info("testURL = " + testURL);        
        com.jayway.restassured.response.Response response = given().cookie(httpHandle.cookie).get(testURL);
        String responseString = response.getBody().asString();
        String gridName="";
        JSONObject versionObj =  (JSONObject) JSONSerializer.toJSON(responseString.toString());
        Object obj = versionObj.get("HadoopClusterVersions");
        if (obj instanceof JSONArray) {
            JSONArray sizeLimitAlertArray = versionObj.getJSONArray("HadoopClusterVersions");
            Iterator iterator = sizeLimitAlertArray.iterator();
            while (iterator.hasNext()) {
                JSONObject jsonObject = (JSONObject) iterator.next();
                Iterator<String> keys  = jsonObject.keys();
                while( keys.hasNext() ) {
                    String key = (String)keys.next();
                    if (key.equals("ClusterTag") ) {
                        gridName = jsonObject.getString(key);
                        if (gridName.equals(clusterName)) {
                            version = jsonObject.getString("Package1");
                            flag = true;
                            break;
                        }
                    }
                    if (flag == true) {
                        break;
                    }
                }
            }
        }
        return version;
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
        String testURL = this.getConsoleURL().trim() + "/console/query/config/datasource/"+dataSourceName;
        TestSession.logger.info("Test URL = " + testURL);
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

        // post the modified datasource file.
        HttpMethod postMethod = this.httpHandle.makePOST("/console/rest/config/datasource?action=Edit&operation=1", postBody.toString());
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

    /*
     * Convert com.jayway.restassured.response.Response to jsonArray
     */
    public JSONArray convertResponseToJSONArray(com.jayway.restassured.response.Response response , String jsonName) {
        JSONArray jsonArray =  null ;
        String res = response.getBody().asString();
        TestSession.logger.debug("response = " + res);
        JSONObject obj =  (JSONObject) JSONSerializer.toJSON(res.toString());
        TestSession.logger.debug("obj = " + obj.toString());
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
     *  Get all the dataset name
     * @return - all the dataset names as List<String>
     */
    public List<String> getAllDataSetName()  {
        String cookie = this.httpHandle.getBouncerCookie();
        String url = this.getConsoleURL() + "/console/api/datasets/view";
        com.jayway.restassured.response.Response response = given().cookie(cookie).get(url);
        JsonPath jsonPath = response.getBody().jsonPath();
        List<String> datasetNames = jsonPath.getList("DatasetsResult.DatasetName");
        if ( datasetNames != null && datasetNames.size() > 0) {
        	TestSession.logger.info("There is no dataset available on the " + this.consoleURL);
        }
        return datasetNames;
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
    
    /**
     * Returns  all the grids name as List
     * @return List<String>
     */
    public List<String> getAllGridNames() {
        List<String> gridNames = new ArrayList<String>();
        String testURL = this.getConsoleURL() + "/console/query/hadoop/versions";
        TestSession.logger.info("testURL = " + testURL);        
        com.jayway.restassured.response.Response response = given().cookie(httpHandle.cookie).get(testURL);
        List<String> tempGridNameList = response.jsonPath().getList("HadoopClusterVersions.DataStoreName");
        
        // filter any datastore staring with gdm and S3 or s3
        gridNames = tempGridNameList.stream().filter(list-> ! (list.startsWith("gdm") || list.startsWith("S3") || list.startsWith("s3"))).collect(Collectors.toList());
        return gridNames;
    }
    
    /**
     * Returns the NameNode name of the specified cluster.
     * @param clusterName
     */
    public String getClusterNameNodeName(String clusterName) {
        String nameNodeName = null;
        TestSession.logger.info("clusterName  - " + clusterName);
        
        String xml = this.getDataSourceXml(clusterName);
        TestSession.logger.debug("*****************xml = " + xml);
        XmlPath xmlPath = new XmlPath(xml);
        if(xmlPath == null)  {
            try {
                throw new Exception("Could not able to create an instance of xmlPath");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        TestSession.logger.debug("*****" + xmlPath.prettyPrint());
        xmlPath.setRoot("DataSource");
        String value = null;
        List<String>clusterNames = xmlPath.getList("Interface.Command.BaseUrl");
        assertTrue("Failed to get the name node name, please check whether "+ clusterName +" datasource specification file exists." , clusterNames.size() > 0);
        for (String n : clusterNames) {
            TestSession.logger.debug(n);
        }
        
        // remove protocol name like webhdfs, hdfs etc, just return only namenode name.
        String nn = clusterNames.get(0);
        int indexOf = nn.indexOf("//") + 2;
        nameNodeName = nn.substring(indexOf);
        TestSession.logger.debug(nameNodeName  + "  is the NameNode of  " + clusterName);
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
     * @param retentionValue
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
    
    /**
     * Get the current gdm version
     * @return gdm version
     */
    public String getGDMVersion() {
        String version = null;
        boolean flag = false;
        String url = this.getConsoleURL() + "/console/api/proxy/health?facet=console&colo=gq1";
        com.jayway.restassured.response.Response response = given().cookie(httpHandle.cookie).get(url);
        String responseString = response.getBody().asString(); 
        TestSession.logger.info("responseString = " + responseString);
        JSONObject responseJsonObject =  (JSONObject) JSONSerializer.toJSON(responseString);
        Object obj  = responseJsonObject.getJSONArray("ApplicationSummary");
        if (obj instanceof JSONArray) {
            JSONArray applicationSummaryJsonArray  = responseJsonObject.getJSONArray("ApplicationSummary");
            Iterator iterator = applicationSummaryJsonArray.iterator();
            while (iterator.hasNext()) {
                JSONObject jsonObject = (JSONObject) iterator.next();
                Iterator<String> keys  = jsonObject.keys();
                while( keys.hasNext() ) {
                    String key = (String)keys.next();
                    if (key.equals("Parameter") ) {
                        String value = jsonObject.getString(key);
                        if (value.equals("build.version")) {
                            version = jsonObject.getString("Value");
                            flag = true;
                            break;
                        }
                    }
                }
                if (flag == true) {
                    break;
                }
            }
        }
        return version;
    }
    
    /**
     * Query health checkup on console and get red replication hostname
     * @return
     */
    public String getFacetHostName(String facetName , String coloColor , String coloName) {
	String healthCheckUpURL = this.getCurrentConsoleURL() + HEALTH_CHECKUP_API + "?facet=console&colo="+ coloName + "&type=health";
	String replicationHostName= "";
	TestSession.logger.info("health checkup api - " + healthCheckUpURL);
	com.jayway.restassured.response.Response response = given().cookie(this.httpHandle.cookie).get(healthCheckUpURL);
	if (response != null) {
	    String resString = response.asString();
	    TestSession.logger.info("response = " + resString);
	    JsonPath jsonPath = new JsonPath(resString);
	    Map<String , String>applicationSummary = new HashMap<String, String>();
	    List<String> keys = jsonPath.get("ApplicationSummary.Parameter");
	    List<String> values = jsonPath.get("ApplicationSummary.Value");
	    for(int i = 0;i<keys.size() ; i++){
		applicationSummary.put(keys.get(i), values.get(i));
	    }
	    List<String> hostNames = Arrays.asList(applicationSummary.get("Facet Endpoints").split(" "));
	    List<String> hostName = hostNames.stream().filter(hostname -> hostname.indexOf(coloColor) > -1 && hostname.indexOf(facetName) > -1).collect(Collectors.toList());
	    if (hostName.size() == 0) {
		TestSession.logger.error(facetName + " is not configured for " + this.getConsoleURL());
	    }
	    if (hostName.size() > 0) {
		replicationHostName = hostName.get(0).replaceAll("https://" , "").replaceAll(":4443/" + facetName, "");
	    }
	}
	return replicationHostName;
    }
    
    /**
     * Get facet status, through health checkup  
     * @param facetName 
     * @param coloColor 
     * @param coloName 
     * @return status of the facet ( running, down etc)
     */
    public boolean isFacetRunning(String facetName , String coloColor , String coloName) {
	boolean flag = false;
	String healthCheckUpURL = this.getCurrentConsoleURL() + HEALTH_CHECKUP_API + "?facet=" + facetName + "&colo="+ coloName + "&type=health";
	TestSession.logger.info("health checkup api - " + healthCheckUpURL);
	com.jayway.restassured.response.Response response = given().cookie(this.httpHandle.cookie).get(healthCheckUpURL);
	if (response != null) {
	    String resString = response.asString();
	    TestSession.logger.info("response = " + resString);
	    JsonPath jsonPath = new JsonPath(resString);
	    Map<String , String>applicationSummary = new HashMap<String, String>();
	    List<String> keys = jsonPath.get("ApplicationSummary.Parameter");
	    List<String> values = jsonPath.get("ApplicationSummary.Value");
	    for(int i = 0;i<keys.size() ; i++){
		applicationSummary.put(keys.get(i), values.get(i));
	    }
	    if (applicationSummary.containsKey("ApplicationStatus"))  {
		if ( applicationSummary.get("ApplicationStatus").trim().equalsIgnoreCase("Running") == true){
		    flag = true;    
		}
	    }
	}
	return flag;
    }

    /**
     * Check whether any dataset(s) exists for the given path, on the given cluster
     * This is to avoid path collision when creating a new dataset.
     */
    public List<String> checkDataSetExistForGivenPath(String dataPath , String clusterName) {
	List<String> dataSetNameList = new ArrayList<String>();
	String url = this.getConsoleURL() + "/console/api/datasets/view?prefix=" + dataPath +  "&dataSource=" + clusterName;
	TestSession.logger.info("url - " + url);
	com.jayway.restassured.response.Response response = given().cookie(this.httpHandle.cookie).get(url);
	if (response != null ) {
	    dataSetNameList = response.getBody().jsonPath().getList("DatasetsResult.DatasetName");
	}
	return dataSetNameList;
    }
    
    /**
     * Deactivate and remove dataset
     * @param dataSetName
     */
    public void deActivateAndRemoveDataSet(String dataSetName) {
	
	// deactivate dataset
	Response response = this.deactivateDataSet(dataSetName);
	assertTrue("Failed to deactivate the dataset " +dataSetName  , response.getStatusCode() == SUCCESS);
	TestSession.logger.info("deactivate   " +  dataSetName  + "  dataset");

	// wait for some time, so that changes are reflected in the dataset specification file i,e active to inactive
	this.sleep(3000);

	this.removeDataSet(dataSetName);
	TestSession.logger.info("Deleted " + dataSetName  + " dataset");
    }
}
