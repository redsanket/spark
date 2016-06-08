// Copyright 2016, Yahoo Inc.
package hadooptest.cluster.gdm;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertEquals;
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
import java.security.PrivilegedExceptionAction;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.*;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

import com.jayway.restassured.path.json.JsonPath;

import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.cluster.gdm.SystemCommand;
import hadooptest.TestSession;
import hadooptest.Util;

/**
 * This is the workflow helper class, having methods to check workflow and methods to create dataset files.
 * 
 */
public class WorkFlowHelper {

    private ConsoleHandle consoleHandle;

    private static final int SUCCESS = 200;
    public static final String DISCOVERY_MONITOR = "/api/discovery/monitor";
    private static String PROTOCOL = "hdfs://";
    private static String OWNER = "dfsload";
    private static String GROUP = "users";
    private static String KEYTAB_DIR = "keytabDir";
    private static String KEYTAB_USER = "keytabUser";
    private static String OWNED_FILE_WITH_COMPLETE_PATH = "ownedFile";
    private static String USER_WHO_DOESNT_HAVE_PERMISSIONS = "userWhoDoesntHavePermissions";
    private static HashMap<String, HashMap<String, String>> supportingData = new HashMap<String, HashMap<String, String>>();
    private static final String COMPLETED  = "completed";
    private static final String FAILED = "failed";
    private static final String RUNNING = "running";
    private static final String COMPLETE_STEPS = "COMPLETE_STEPS";
    private static final String LAST_STEP = "LAST_STEP";
    private final static String HADOOP_LS_PATH = "/console/api/admin/hadoopls?dataSource=";
    
    private String cookie;

    public WorkFlowHelper() {
        this.consoleHandle = new ConsoleHandle();
        HTTPHandle httpHandle = new HTTPHandle();
        this.cookie = httpHandle.getBouncerCookie();
    }
    
    public WorkFlowHelper(String userName , String passWord) {
        this.consoleHandle = new ConsoleHandle(userName, passWord);
        HTTPHandle httpHandle = new HTTPHandle(userName ,passWord);
        this.cookie = httpHandle.getBouncerCookie();    
    }
    
    /**
     * Returns true if a workflow passed, false otherwise
     * @param dataSetName
     * @param facetName
     * @param instanceDate
     * @return Returns true if a workflow passed, false otherwise
     */
    public boolean workflowPassed(String dataSetName, String facetName, String instanceDate) {
        long totSleepTimeMs = 0L;
        long sleepTime = 30 * 1000;
        String completedWorkFlowURL = this.consoleHandle.getConsoleURL() + "/console/api/workflows/completed?datasetname="+ dataSetName +"&instancessince=F&joinType=innerJoin&facet=" + facetName;
        String failedWorkFlowURL = this.consoleHandle.getConsoleURL() + "/console/api/workflows/failed?datasetname=" + dataSetName +"&instancessince=F&joinType=innerJoin&facet=" + facetName;
        while (totSleepTimeMs < 10 * 60 * 1000) {
            com.jayway.restassured.response.Response workFlowResponse = given().cookie(this.cookie).get(completedWorkFlowURL);
            String workFlowResult = checkWorkFlowStatus(workFlowResponse , "completedWorkflows" , instanceDate);
            if (workFlowResult.equals("completed") ) {
                TestSession.logger.info("Workflow " + dataSetName + "/" + instanceDate + " for " + facetName + " completed.");
                return true;
            }
            
            workFlowResponse = given().cookie(this.cookie).get(failedWorkFlowURL);
            workFlowResult = checkWorkFlowStatus(workFlowResponse , "failedWorkflows" , instanceDate);
            if (workFlowResult.equals("failed") ) {
                TestSession.logger.info("Workflow " + dataSetName + "/" + instanceDate + " for " + facetName + " failed.");
                return false;
            }
            
            this.consoleHandle.sleep(sleepTime);
            totSleepTimeMs += sleepTime;
        }
        fail("Workflow " + instanceDate + " timed out (not completed or failed)");
        return false;
    }
    
    /**
     * Returns true if a workflow exists for a given instance of a given dataset on a given facet in any state, false otherwise
     * @param dataSetName
     * @param facetName
     * @param instanceDate
     * @return Returns true if a workflow exists, false otherwise
     */
    public boolean workflowExists(String dataSetName, String facetName, String instanceDate) {
        String completedWorkFlowURL = this.consoleHandle.getConsoleURL() + "/console/api/workflows/completed?datasetname="+ dataSetName +"&instancessince=F&joinType=innerJoin&facet=" + facetName;
        String failedWorkFlowURL = this.consoleHandle.getConsoleURL() + "/console/api/workflows/failed?datasetname=" + dataSetName +"&instancessince=F&joinType=innerJoin&facet=" + facetName;
        String runningWorkFlowURL = this.consoleHandle.getConsoleURL() + "/console/api/workflows/running?datasetname=" + dataSetName +"&instancessince=F&joinType=innerJoin&facet=" + facetName;
        com.jayway.restassured.response.Response workFlowResponse = given().cookie(this.cookie).get(completedWorkFlowURL);
        String workFlowResult = checkWorkFlowStatus(workFlowResponse , "completedWorkflows" , instanceDate);
        if (workFlowResult.equals("completed") ) {
            return true;
        }
        workFlowResponse = given().cookie(this.cookie).get(failedWorkFlowURL);
        workFlowResult = checkWorkFlowStatus(workFlowResponse , "failedWorkflows" , instanceDate);
        if (workFlowResult.equals("failed") ) {
            return true;
        }
        workFlowResponse = given().cookie(this.cookie).get(runningWorkFlowURL);
        workFlowResult = checkWorkFlowStatus(workFlowResponse , "runningWorkflows" , instanceDate);
        if (workFlowResult.equals("running") ) {
            return true;
        }
        return false;
    }
    
    
    /**
     * checks whether the dataset has come to the running state on a given facet.
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
     *           checkWorkFlow(dataSetName , "acquisition" , datasetActivationTime );  // check workflow by datasetname , facetName and dataset activation time
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
        TestSession.logger.info("Waiting for "+ facetName + " workflow to start before checking workflow status");
        this.consoleHandle.sleep(60000);

        boolean isWorkFlowRunning = false , isWorkFlowCompleted = false, isWorkFlowFailed = false;
        com.jayway.restassured.response.Response workFlowResponse = null;
        String runningWorkFlowTestURL =  this.consoleHandle.getConsoleURL() + "/console/api/workflows/running?datasetname="+ dataSetName +"&instancessince=F&joinType=innerJoin&facet=" + facetName;
        String completedWorkFlowTestURL = this.consoleHandle.getConsoleURL() + "/console/api/workflows/completed?datasetname="+ dataSetName +"&instancessince=F&joinType=innerJoin&facet=" + facetName;
        String failedWorkFlowTestURL = this.consoleHandle.getConsoleURL() + "/console/api/workflows/failed?datasetname=" + dataSetName +"&instancessince=F&joinType=innerJoin&facet=" + facetName;
        long waitTimeForWorkflowPolling = 0;
        if (checkInstanceDate == true) {
            waitTimeForWorkflowPolling = 40 * 60 * 1000; // 40 minutes  this is due to large dataset files 
        } else if (checkInstanceDate == false) {
            waitTimeForWorkflowPolling = 20 * 60 * 1000; // 20 minutes
        }
        long sleepTime = 5000; // 5 sec  sleep time.
        long waitTime=0;

        while (waitTime <= waitTimeForWorkflowPolling) {
            if ( isWorkFlowRunning == false) {
                TestSession.logger.info("runningWorkFlowTestURL = " + runningWorkFlowTestURL);
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
                TestSession.logger.info("completedWorkFlowTestURL = "+ completedWorkFlowTestURL);
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
                TestSession.logger.info("failedWorkFlowTestURL = " + failedWorkFlowTestURL);
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
                fail("Time out : looks like " + dataSetName +"  did not come to running state in "  + facetName  + "  facet.");
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

    public String checkWorkFlowStatus(com.jayway.restassured.response.Response workFlowResponse , String... args) {
        JSONArray jsonArray = null;
        String currentWorkFlowStatus = null;
        String workFlowStatus = args[0];
        int argsSize = args.length - 1;
        jsonArray = this.consoleHandle.convertResponseToJSONArray(workFlowResponse, workFlowStatus);

        // if user want to check for specified instance of the workflow.
        if (argsSize > 0) {
            currentWorkFlowStatus = getStatus(jsonArray , args[args.length - 1]);
        } else  if (argsSize == 0) {

            // if user just want to check for workflow, then set it to "-1"
            currentWorkFlowStatus = getStatus(jsonArray, "-1");
        }
        return currentWorkFlowStatus;
    }
    
    


    private String getStatus(JSONArray jsonArray , String...args) {
        String instanceDate = args[0];
        TestSession.logger.debug("****************** checking for instance " + instanceDate + " **********");
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
                        TestSession.logger.debug("************************** actualInstance = " + actualInstance);
                        if (actualInstance.equals(instanceDate)) {
                            TestSession.logger.debug("Instance file is found - " + actualInstance);
                            currentWorkFlowStatus = "completed";
                            break;
                        } 
                    } else if (instanceDate.equals("-1")) {
                        currentWorkFlowStatus = "completed";
                        break;
                    }
                } else if (exitStatus.equals("SHUTDOWN") || exitStatus.equals("FAILED")) {
                    if (!instanceDate.equals("-1")) {
                        String workFlowName = runningJsonObject.getString("WorkflowName");
                        String actualInstance = workFlowName.substring(workFlowName.lastIndexOf("/") + 1);
                        TestSession.logger.debug("************************** actualInstance = " + actualInstance);
                        if (actualInstance.equals(instanceDate)) {
                            TestSession.logger.debug("Instance file is found - " + actualInstance);
                            currentWorkFlowStatus = "failed";
                            break;
                        } 
                    } else if (instanceDate.equals("-1")) {
                        currentWorkFlowStatus = "failed";
                        break;
                    }
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
     * Return JSONArray of discovery monitor for the given dataset.
     * @return
     */
    public JSONArray isDiscoveryMonitoringStarted( String facetName , String dataSetName) {
        long waitTimeForWorkflowPolling = 5 * 60 * 1000; // 15 minutes
        long sleepTime = 5000; // 5 sec  sleep time.
        long waitTime=0;
        JSONArray jsonArray = null;
        String testURL = null;
        
        org.apache.commons.configuration.Configuration configuration = this.consoleHandle.getConf();
        String environmentType = configuration.getString("hostconfig.console.test_environment_type");
        if (environmentType.equals("oneNode")) {
            TestSession.logger.info("****** QE or Dev test Environment ******** ");
            String hostName = configuration.getString("hostconfig.console.base_url");
            testURL = hostName.replace("9999", this.consoleHandle.getFacetPortNo(facetName)) + "/" + facetName + this.DISCOVERY_MONITOR + "?dataset=" + dataSetName;
        } else if (environmentType.equals("staging")) {
            TestSession.logger.info("****** Staging test Environment ******** ");
            String stagingHostName = configuration.getString("hostconfig.console.staging_console_url");
            WorkFlowHelper workFlowHelperObj = new WorkFlowHelper();
            String hostName = workFlowHelperObj.getFacetHostName(stagingHostName , facetName);
            testURL = hostName + this.DISCOVERY_MONITOR + "?dataset=" + dataSetName;
        } else  {
            TestSession.logger.info("****** Specified invalid test environment ******** ");
            System.exit(1);
        }
        
        while (waitTime <= waitTimeForWorkflowPolling) {
            String discoveryMonitoringStarted = testURL;
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
            TestSession.logger.info("File stream created successfully.");
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
     * Returns if the step exists in a workflow for the desired dataset and workflow type  
     * 
     * @param dataSetName
     * @param workflowType - running, completed or failed
     * @param instance
     * @param stepValue
     * @return  true if the step exists in the workflow, false otherwise
     */
    public boolean doesStepExistInWorkFlowExecution(String dataSetName, String facet, String workflowType, String instance, String stepValue) {

        String endTime = GdmUtils.getCalendarAsString();
        JSONUtil jsonUtil = new JSONUtil();
        JSONArray jsonArray = null;
        TestSession.logger.info("endTime = " + endTime);
        String url = this.consoleHandle.getConsoleURL() + "/console/api/workflows/" + workflowType   + "?exclude=false&instancessince=F&joinType=innerJoin&datasetname=" +  dataSetName ;
        TestSession.logger.info("url = " + url);
        com.jayway.restassured.response.Response response = given().cookie(cookie).get(url);
        String res = response.getBody().asString();
        TestSession.logger.info("response = " + res);

        // convert string to jsonObject
        JSONObject obj =  (JSONObject) JSONSerializer.toJSON(res.toString());
        TestSession.logger.info("obj = " + obj.toString());
        jsonArray = obj.getJSONArray(workflowType + "Workflows");

        if ( jsonArray.size() > 0 ) {
            Iterator iterator = jsonArray.iterator();
            while (iterator.hasNext()) {
                JSONObject jsonObject = (JSONObject) iterator.next();
                TestSession.logger.info("jsonObject  = " + jsonObject.toString());
                
                String workflowName = jsonObject.getString("WorkflowName");
                String fName = jsonObject.getString("FacetName");
                if (workflowName.contains(instance) && fName.equalsIgnoreCase(facet)) {
                    // get steps for workflow 
                    String executionId = jsonObject.getString("ExecutionID");
                    
                    String facetColo = jsonObject.getString("FacetColo");
                    String testURL =this.consoleHandle.getConsoleURL() +  "/console/api/workflows/" + executionId + "/view?facet=" + fName + "&colo=" + facetColo;
                    TestSession.logger.info("url = " + testURL);
                    response = given().cookie(this.cookie).get(testURL);
                    String res1 = response.getBody().asString();
                    TestSession.logger.info(" response = " + res1);
                    JSONObject obj1 =  (JSONObject) JSONSerializer.toJSON(res1.toString());
                    TestSession.logger.info("obj1 = " + obj1.toString());
                    JSONObject workflowExecutionJsonObject = (JSONObject) obj1.get("WorkflowExecution");
                    JSONArray jsonArray1 = workflowExecutionJsonObject.getJSONArray("Step Executions");
                    boolean doesStepExist = checkStepExists(jsonArray1 , "Step Name", stepValue.trim());
                    TestSession.logger.info(" result = " + doesStepExist );
                    return doesStepExist;
                }
            }
        }
        return false;
    }
    
    /**
     * Check whether the given stepName and stepValue exists in jsonArray.
     * @param jsonArray  - array representing "Step Executions" in workflow
     * @param stepName  - stepName represents workflow steps example step name , Start Time etc
     * @param stepValue - 
     * @return
     */
    public boolean checkStepExists(JSONArray jsonArray , String stepName, String stepValue) {
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
        assertTrue("Failed : " + dataSetName + "  dataSet did n't came to completed state, may be the dataset is still in running state. ", didDataSetIsInCompletedState == true);
    }

    /**
     * set the hadoop user details , this is a helper method in creating the configuration object.
     */
    public void setHadoopUserDetails() {
        // Populate the details for DFSLOAD
        HashMap<String, String> fileOwnerUserDetails = new HashMap<String, String>();
        fileOwnerUserDetails = new HashMap<String, String>();
        fileOwnerUserDetails.put(KEYTAB_DIR, HadooptestConstants.Location.Keytab.DFSLOAD);
        fileOwnerUserDetails.put(KEYTAB_USER, HadooptestConstants.UserNames.DFSLOAD + "@DEV.YGRID.YAHOO.COM");
        fileOwnerUserDetails.put(OWNED_FILE_WITH_COMPLETE_PATH, "/tmp/"+ HadooptestConstants.UserNames.DFSLOAD + "Dir/" + HadooptestConstants.UserNames.DFSLOAD + "File");
        fileOwnerUserDetails.put(USER_WHO_DOESNT_HAVE_PERMISSIONS, HadooptestConstants.UserNames.HADOOPQA);

        this.supportingData.put(HadooptestConstants.UserNames.DFSLOAD,fileOwnerUserDetails);
        TestSession.logger.info("CHECK:" + this.supportingData);
    }

    /**
     * Returns the remote cluster configuration object.
     * @param aUser  - user
     * @param nameNode - name of the cluster namenode.
     * @return
     */
    Configuration getConfForRemoteFS(String nameNode) {
        Configuration conf = new Configuration(true);
        String namenodeWithChangedSchema = nameNode.replace(HadooptestConstants.Schema.HTTP,HadooptestConstants.Schema.HDFS);
        TestSession.logger.info("********************* namenodeWithChangedSchema  = "  + namenodeWithChangedSchema);

        String namenodeWithChangedSchemaAndPort =  this.PROTOCOL + namenodeWithChangedSchema.replace("50070", HadooptestConstants.Ports.HDFS);
        TestSession.logger.info("For HDFS set the namenode to:[" + namenodeWithChangedSchemaAndPort + "]");
        conf.set("fs.defaultFS", namenodeWithChangedSchemaAndPort);
        conf.set("dfs.namenode.kerberos.principal", "hdfs/_HOST@DEV.YGRID.YAHOO.COM");
        conf.set("hadoop.security.authentication", "true");
        TestSession.logger.info(conf);
        return conf;
    }

    /**
     * Returns the UGI of the specified user.
     * @param aUser
     * @return
     */
    UserGroupInformation getUgiForUser(String aUser) {
        String keytabUser = this.supportingData.get(aUser).get(KEYTAB_USER);
        TestSession.logger.info("Set keytab user=" + keytabUser);
        String keytabDir = this.supportingData.get(aUser).get(KEYTAB_DIR);
        TestSession.logger.info("Set keytab dir=" + keytabDir);
        UserGroupInformation ugi = null;
        try {
            ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(keytabUser, keytabDir);
            TestSession.logger.info("UGI=" + ugi);
            TestSession.logger.info("credentials:" + ugi.getCredentials());
            TestSession.logger.info("group names" + ugi.getGroupNames());
            TestSession.logger.info("real user:" + ugi.getRealUser());
            TestSession.logger.info("short user name:" + ugi.getShortUserName());
            TestSession.logger.info("token identifiers:" + ugi.getTokenIdentifiers());
            TestSession.logger.info("tokens:" + ugi.getTokens());
            TestSession.logger.info("username:" + ugi.getUserName());
            TestSession.logger.info("current user:" + UserGroupInformation.getCurrentUser());
            TestSession.logger.info("login user:" + UserGroupInformation.getLoginUser());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return ugi;
    }

    /**
     * Invokes PrivilegedExceptionActionImpl class and helps in checking and changing user, group and permission of the file.
     * @param nameNodeName
     * @param path
     * @throws IOException
     * @throws InterruptedException
     */
    public void checkAndSetPermision(String nameNodeName , String path) throws IOException, InterruptedException {
        TestSession.logger.info("checkAndSetPermision  ********  nameNodeName =   " + nameNodeName   + "     path = " + path );
        
        this.setHadoopUserDetails();
        for (String aUser : this.supportingData.keySet()) {
            TestSession.logger.info("aUser = " + aUser);
            Configuration configuration = this.getConfForRemoteFS(nameNodeName );
            UserGroupInformation ugi = this.getUgiForUser(aUser);

            PrivilegedExceptionActionImpl privilegedExceptionActioniImplOject = new PrivilegedExceptionActionImpl(path , configuration);
            String result = ugi.doAs(privilegedExceptionActioniImplOject);
            TestSession.logger.info("Result = " + result);
        }
    }
    
    /**
     * Class that checks paths exists and change group , user and permission of the given path. 
     * 
     */
    class PrivilegedExceptionActionImpl implements PrivilegedExceptionAction<String> {
        Configuration configuration;
        String basePath ;
        String destinationFolder;
        String srcFilePath;
        String instanceFileCount;
        List<String> instanceFolderNames;

        public PrivilegedExceptionActionImpl(String basePath , Configuration configuration ) {
            this.configuration = configuration;
            this.basePath = basePath;
        }

        public String run() throws Exception {
            String result = "fail";
            TestSession.logger.info("configuration   =  " + this.configuration.toString());

            FileSystem remoteFS = FileSystem.get(this.configuration);
            Path path = new Path(this.basePath.trim());

            // check whether remote path exists on the grid
            if ( remoteFS.exists(path) ) {
                if (remoteFS.isDirectory(path)) {
                    FileStatus fileStatus = remoteFS.getFileStatus(path);
                    String owner = fileStatus.getOwner();
                    TestSession.logger.info("Group = " + fileStatus.getGroup());
                    TestSession.logger.info("Owner = " + owner);
                    if (! owner.equals(WorkFlowHelper.OWNER)) {
                        remoteFS.setOwner(path, WorkFlowHelper.OWNER, WorkFlowHelper.GROUP);
                        TestSession.logger.info("Successfully changed owner and group for  " + path.toString());
                    }
                    FsPermission fsPermission = fileStatus.getPermission();
                    String pathPermission = fsPermission.getDirDefault().toString();
                    TestSession.logger.info("pathPermission = " + pathPermission );
                    if ( ! pathPermission.equals("drwxrwxrwx") ) {
                        remoteFS.setPermission(path, new  FsPermission(FsAction.ALL,FsAction.ALL,FsAction.ALL ));
                        TestSession.logger.info("Successfully changed permission for  " + path.toString());
                    }
                    result = "success";
                }
            }
            return result;
        }

    }
    
    /**
     * Get last successful executed step as JSONObject
     * @param workFlowName - name of the workflow ( completed, failed , running)
     * @param dataSetName - name of the dataset.
     * @return
     */
    public JSONObject getWorkFlowDetailedSteps(String facetName, String workFlowName , String dataSetName ,String stepType) {
        JSONObject detailedStepJsonObject = null;
        JSONArray jsonArray = null;
        com.jayway.restassured.response.Response response = null;

        // check specified workflow name is correct.
        boolean isSpecifiedWorkFlowCorrect = (workFlowName.equals(COMPLETED) || workFlowName.equals(RUNNING) || workFlowName.equals(FAILED) );
        assertTrue(workFlowName + " workflow name is not correct = " , isSpecifiedWorkFlowCorrect == true);

        String testURL = this.consoleHandle.getConsoleURL().replaceAll("9999", this.consoleHandle.getFacetPortNo(facetName)) + "/"+ facetName +"/api/workflows/" + workFlowName + "?exclude=false&joinType=innerJoin&datasetname=" + dataSetName;
        TestSession.logger.info("testURL    - " + testURL);
        response = given().cookie(this.cookie).get(testURL);
        assertTrue("Failed to get the respons  " + response , (response != null ) );

        jsonArray = this.consoleHandle.convertResponseToJSONArray(response , workFlowName +"Workflows");
        if ( jsonArray.size() > 0 ) {
            Iterator iterator = jsonArray.iterator();
            while (iterator.hasNext()) {
                JSONObject jsonObject = (JSONObject) iterator.next();
                TestSession.logger.info("failedJsonObject  = " + jsonObject.toString());
                String fName = jsonObject.getString("FacetName").trim();
                String executionId = jsonObject.getString("ExecutionID").trim();
                TestSession.logger.info("ExecutionID =  " + executionId);

                String detailedWorkFlowURL = this.consoleHandle.getConsoleURL() + "/console/api/workflows/"+ executionId + "/view?facet="+ fName +"&colo=gq1";
                TestSession.logger.info("detailedWorkFlowURL  = " + detailedWorkFlowURL);
                com.jayway.restassured.response.Response workFlowDetailedResponse = given().cookie(this.cookie).get(detailedWorkFlowURL);
                String workFlowDetailsStr =  workFlowDetailedResponse.getBody().asString();
                TestSession.logger.info("WorkFlowDetailedResponse   =  " + workFlowDetailsStr);
                JSONObject obj =  (JSONObject) JSONSerializer.toJSON(workFlowDetailsStr.toString());
                JSONObject obj1 = obj.getJSONObject("WorkflowExecution");
                TestSession.logger.info("Exit Status  = " + obj1.getString("Exit Status"));

                JSONArray detailedWorkFlowDetails = obj1.getJSONArray("Step Executions");
                int length = detailedWorkFlowDetails.size() - 1;
                TestSession.logger.info("size  = " + length);

                if (length > 0 && stepType.equals(this.LAST_STEP)) {
                    detailedStepJsonObject = new JSONObject(); 
                    JSONObject tempObj = detailedWorkFlowDetails.getJSONObject(length - 1);
                    String stepName = tempObj.getString("Step Name").trim();
                    String startTime = tempObj.getString("Start Time").trim();
                    String endTime = tempObj.getString("EndTime").trim();
                    detailedStepJsonObject.put("Step Name",stepName );
                    detailedStepJsonObject.put("Start Time", startTime);
                    detailedStepJsonObject.put("EndTime",endTime );
                    TestSession.logger.info("Step Name = " + stepName);
                    TestSession.logger.info("start time = " + startTime);
                    TestSession.logger.info("end Time  = " + endTime);
                } else if  ( stepType.equals(this.COMPLETE_STEPS) ) {
                    detailedStepJsonObject = new JSONObject();
                    detailedStepJsonObject.put("Step Name", "detailedStep");
                    detailedStepJsonObject.put("Step Executions", detailedWorkFlowDetails);
                    TestSession.logger.info("detailedWorkFlowDetails  = " + detailedStepJsonObject.toString(5));
                }
            }
        }
        return detailedStepJsonObject;
    }
    
    /**
     * Execute a given command and return the output of the command.
     * @param command
     * @return
     */
    public String executeCommand(String command) {
        String output = null;
        TestSession.logger.info("command - " + command);
        ImmutablePair<Integer, String> result = SystemCommand.runCommand(command);
        if ((result == null) || (result.getLeft() != 0)) {
            if (result != null) { 
                // save script output to log
                TestSession.logger.info("Command exit value: " + result.getLeft());
                TestSession.logger.info(result.getRight());
            }
            throw new RuntimeException("Exception" );
        } else {
            output = result.getRight();
            TestSession.logger.info("log = " + output);
        }
        return output;
    }
    
    /**
     * return  the specified hostname, by executing the yinst command on console host.
     * Note : make sure that hadoopqa is able to do ssh to the console host.
     * @param consoleHostName
     * @param facetName
     * @return
     */
    public String getFacetHostName(String consoleHostName , String facetName) {
        String facetHostName = null;
        String hostName = Arrays.asList(consoleHostName.split(":")).get(1).replaceAll("//" , "").trim();
        final String command = "ssh " + hostName.trim() + " " + "\"yinst set | grep " + facetName + "_end_point\"" ;
        TestSession.logger.info("command " + command);
        String output = this.executeCommand(command);
        
        TestSession.logger.info("output: " + output);
        
        // can get error output on command, we need to grab the output starting with ygrid_gdm_console_server
        //
        // Failed to add the host to the list of known hosts (/home/hadoopqa/.ssh/known_hosts).
        // ygrid_gdm_console_server.gq1_replication_end_point: https://opengdm2blue-n4.blue.ygrid.yahoo.com:4443/replication
        int index = output.indexOf("ygrid_gdm_console_server");
        if (index > 0) {
            output = output.substring(index);
            TestSession.logger.info("new output: " + output);
        }

        facetHostName = Arrays.asList(output.split(" ")).get(1).trim();
        TestSession.logger.info("facet hostname = " + facetHostName);
        return facetHostName;
    }
    
    /**
     * Return the datapath of the specified dataset on the datasource
     * @param dataSourceName
     * @param dataSetName
     * @return
     */
    public List<String> getInstanceFileAfterWorkflow(String dataSourceName , String dataSetName) {
        JSONArray jsonArray = null;
        List<String> dataPath = new ArrayList<String>();
        String testURL = this.consoleHandle.getConsoleURL() + this.HADOOP_LS_PATH + dataSourceName + "&dataSet=" + dataSetName + "&format=json";
        TestSession.logger.info("Test url = " + testURL);
        com.jayway.restassured.response.Response res = given().cookie(this.cookie).get(testURL);
        assertTrue("Failed to get the respons  " + res , (res != null ) );
        jsonArray = this.consoleHandle.convertResponseToJSONArray(res , "Files");
        if ( jsonArray.size() > 0 ) {
            Iterator iterator = jsonArray.iterator();
            while (iterator.hasNext()) {
                JSONObject dSObject = (JSONObject) iterator.next();
                String  directory = dSObject.getString("Directory");
                if (directory.equals("yes")) {
                    String path = dSObject.getString("Path");
                    dataPath.add(path);
                }
            }
        }
        return dataPath;
    }
    
}
