// Copyright 2016, Yahoo Inc.
package hadooptest.gdm.regression;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import hadooptest.TestSession;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.cluster.gdm.HTTPHandle;
import hadooptest.cluster.gdm.JSONUtil;
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

import com.jayway.restassured.path.json.JsonPath;


/**
 * Test Scenario : Verify whether GDM end to end case is covered.
 * Description :
 *  1) Get all the installed grids( to use as targets)
 *  2) Create datasources ( targets) to avoid path collision , if already existing datasource is used.
 *  3) Create a dataset.
 *  4) Activate the dataset
 *  5) Start the workflow.
 *  6) Kill the workflow
 *  7) Check whether the dataset is in failed workflow state.
 *  8) Resume the failed, check whether previous successful step is not executed.
 *  9) Reset the completed workflow 
 *  10) Kill the running workflow
 *  11) Restart the workflow. Verify whether all the step are executed for beginning.
 *  12) Deactivate the dataset.
 *  13) Deactiavate the targets
 *  14) Remove targets from dataset
 *  15) Remove dataset
 *  16) Remove datasource
 *
 */
public class TestGDMEndToEnd extends TestSession {

    private ConsoleHandle consoleHandle;
    private String cookie;
    private String url;
    private String testURL;
    private JSONUtil jsonUtil;
    private String dataSetName;
    private String target1;
    private String target2;
    private String dsActivationTime; 
    private WorkFlowHelper workFlowHelperObj = null;
    private List<String>dataSetList;
    private List<String> workFlowNames = new ArrayList<String>();
    private List<String> dataSourceList= new ArrayList<String>();
    private static String baseDataSetName = "VerifyAcqRepRetWorkFlowExecutionSingleDate";
    public static final String dataSetPath = "/console/query/config/dataset/getDatasets";
    public static final String dataSourcePath = "/console/query/config/datasource";
    private static final int SUCCESS = 200;
    private static final String COMPLETED  = "completed";
    private static final String FAILED = "failed";
    private static final String RUNNING = "running";
    private static final String COMPLETE_STEPS = "COMPLETE_STEPS";
    private static final String LAST_STEP = "LAST_STEP";

    @BeforeClass
    public static void startTestSession() {
        TestSession.start();
    }

    @Before
    public void setup() throws Exception {
        this.consoleHandle = new ConsoleHandle();
        HTTPHandle httpHandle = new HTTPHandle();
        this.cookie = httpHandle.getBouncerCookie();
        this.jsonUtil = new JSONUtil();
        this.dataSetName = "Testing_GDMEnd2End_Cases_" + System.currentTimeMillis();
        this.url = this.consoleHandle.getConsoleURL(); 
        this.testURL = this.consoleHandle.getConsoleURL() + "/console/rest/config/dataset/getRetentionPolicies";
        this.workFlowHelperObj = new WorkFlowHelper();
    }

    @Test
    public void testEnd2End() throws Exception {
        this.dataSourceList = this.consoleHandle.getAllGridNames();
        assertTrue("Need atleast two cluster and" , this.dataSourceList.size() >= 2);
        this.target1 = this.dataSourceList.get(0) + "_GDMEnd2End_dataStore_" + System.currentTimeMillis();
        this.target2 = this.dataSourceList.get(1) + "_GDMEnd2End_dataStore_" + System.currentTimeMillis();

        // create datasource
        this.createTestDataSource( this.dataSourceList.get(0), this.target1);
        this.createTestDataSource( this.dataSourceList.get(1), this.target2);

        // create dataset
        this.createTestDataSet();

        // activate the dataset
        this.activateDataSet();

        // check whether dataset is in queue after discover
        this.getItemInQueue(isItemInQueue("acquisition") , this.dataSetName);

        // kill running workflow
        this.getRunningDataSetAndKillWorkFlow("acquisition" , this.dataSetName);

        // check whether killed dataset is in failed state
        JSONArray failedWorkFlowJson = this.consoleHandle.validateDatasetHasFailedWorkflow("acquisition" , this.dataSetName , this.dsActivationTime);

        // get last successful executed step name, start date and end date.
        JSONObject lastStepSuccessFulStepDetails =  this.workFlowHelperObj.getWorkFlowDetailedSteps("acquisition" , "failed" , this.dataSetName , this.LAST_STEP);

        // resume running workflow
        resumeORRestartWorkflow("acquisition" , this.dataSetName , "resume", failedWorkFlowJson);

        // check for workflow
        this.workFlowHelperObj.checkWorkFlow(this.dataSetName , "acquisition", this.dsActivationTime);

        // restart the completed workflow
        this.consoleHandle.restartCompletedWorkFlow(this.dataSetName, "acquisition");

        // kill the running workflow
        this.getRunningDataSetAndKillWorkFlow("acquisition" , this.dataSetName);

        // check whether killed dataset is in failed state
        failedWorkFlowJson = null;
        failedWorkFlowJson = this.consoleHandle.validateDatasetHasFailedWorkflow("acquisition" , this.dataSetName , this.dsActivationTime);

        // restart the workflow
        resumeORRestartWorkflow("acquisition" , this.dataSetName , "restart", failedWorkFlowJson);

        // check for workflow
        this.workFlowHelperObj.checkWorkFlow(this.dataSetName , "acquisition", this.dsActivationTime);

        // check for replication workflow 
        this.workFlowHelperObj.checkWorkFlow(this.dataSetName , "replication", this.dsActivationTime);

        // set retention policy
        this.consoleHandle.setRetentionPolicyToAllDataSets(this.dataSetName , "0");

        // check for retention workflow 
        this.workFlowHelperObj.checkWorkFlow(this.dataSetName , "retention", this.dsActivationTime);

        // deactivate dataset
        Response response = consoleHandle.deactivateDataSet(this.dataSetName);
        assertEquals("ResponseCode - Deactivate DataSet", 200, response.getStatusCode());
        assertEquals("ActionName.", "terminate", response.getElementAtPath("/Response/ActionName").toString());
        assertEquals("ResponseMessage.", "Operation on " + this.dataSetName + " was successful.", response.getElementAtPath("/Response/ResponseMessage/[0]").toString());

        // deactivate targets
        this.consoleHandle.deactivateTargetsInDataSet(this.dataSetName);
        
        // remove targets from the dataset
        this.consoleHandle.removeTargetsFromDataset(this.dataSetName);

        // remove dataset
        this.consoleHandle.removeDataSet(this.dataSetName);

        // remove datasource
        this.consoleHandle.removeDataSource(this.target1);
        this.consoleHandle.removeDataSource(this.target2);
    }

    

    /**
     * Compare resumeCompleted workflow steps with restartedCompleted workflow steps.
     * @param resumedCompletedWorkFlow
     * @param restartedCompletedWorkFlow
     */
    public void checkCompletedWorkFlows(JSONObject resumedCompletedWorkFlow , JSONObject restartedCompletedWorkFlow) {
        assertTrue("Either  resumedCompletedWorkFlow or restartedCompletedWorkFlow is null." , resumedCompletedWorkFlow != null || restartedCompletedWorkFlow != null);
        JSONArray resumedJsonArray = resumedCompletedWorkFlow.getJSONArray("Step Executions");
        JSONArray restartedJsonArray = restartedCompletedWorkFlow.getJSONArray("Step Executions");
        int i = 0;
        int size1 = resumedJsonArray.size();
        int size2 = restartedJsonArray.size();
        while( i < size1 && i < size2) {
            JSONObject resumedJsonObject = resumedJsonArray.getJSONObject(i);
            JSONObject restartedJsonObject = restartedJsonArray.getJSONObject(i);
            assertTrue("Expected that step names are equal but got Resume Step name = " + resumedJsonObject.getString("Step Name")   + " and Restarted Step name = " + restartedJsonObject.getString("Step Name") , resumedJsonObject.getString("Step Name").equals(restartedJsonObject.getString("Step Name")));
            assertTrue("Expected that start date are not equal but got Resume start time  = " + resumedJsonObject.getString("Start Time")   + " and Restarted start time = " + restartedJsonObject.getString("Start Time") ,
                    !(resumedJsonObject.getString("Start Time").equals(restartedJsonObject.getString("Start Time"))));
            assertTrue("Expected that start date are not equal but got Resume end time  = " + resumedJsonObject.getString("EndTime")   + " and Restarted end time = " + restartedJsonObject.getString("EndTime") , 
                    !(resumedJsonObject.getString("EndTime").equals(restartedJsonObject.getString("EndTime"))));

            i++;
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


    public void resumeORRestartWorkflow(String facetName , String dataSetName , String action , JSONArray failedWorkfFlowJsonArray) {

        // check for action
        if ( ! (action.trim().equals("resume") || action.trim().equals("restart"))) {
            fail("Action should be either resume or restart, but you have specified as " + action);
        }

        if ( failedWorkfFlowJsonArray.size() > 0 ) {
            JSONArray resourceArray = new JSONArray();
            Iterator iterator = failedWorkfFlowJsonArray.iterator();
            while (iterator.hasNext()) {
                JSONObject jsonObject = (JSONObject) iterator.next();
                TestSession.logger.info("jsonObject  = " + jsonObject.toString());
                String fsName = jsonObject.getString("FacetName");
                String executionId = jsonObject.getString("ExecutionID");
                String facetColo = jsonObject.getString("FacetColo");

                assertTrue( "Expected facetName is " + facetName +  " , but  got " + fsName  , fsName.equals(facetName));

                //  resume the failed workflow
                resourceArray.add(new JSONObject().element("ExecutionID",executionId).element("FacetName", fsName).element("FacetColo", facetColo));
                String killTestURL = this.consoleHandle.getConsoleURL().replace("9999", this.consoleHandle.getFacetPortNo(facetName.trim())) + "/" + facetName.trim() + "/api/admin/workflows";
                TestSession.logger.info("url  = " + killTestURL);
                com.jayway.restassured.response.Response jobKilledResponse = given().cookie(this.cookie).param("command", action.trim()).param("workflowIds" , resourceArray.toString()).post(killTestURL);
                JsonPath jsonPath = jobKilledResponse.getBody().jsonPath();
                TestSession.logger.info("response = " + jsonPath.prettyPrint());
                String responseId = jsonPath.getString("Response.ResponseId");
                assertTrue("Expected responseId is zer (0) but got " + responseId  , responseId.equals("0"));
            }
        }
    }

    public void createTestDataSource(String dataSourceName , String newDataSourceName) {
        String xml = this.consoleHandle.getDataSourceXml(dataSourceName);
        xml = xml.replaceFirst(dataSourceName,newDataSourceName);
        boolean isDataSourceCreated = this.consoleHandle.createDataSource(xml);
        assertTrue("Failed to create a DataSource specification " + newDataSourceName , isDataSourceCreated == true);
        this.consoleHandle.sleep(5000);
    }

    public void createTestDataSet() {
        this.dataSetList = given().cookie(this.cookie).get(url + this.dataSetPath ).getBody().jsonPath().getList("DatasetsResult.DatasetName");
        assertTrue("" , this.dataSetList.contains(this.baseDataSetName) == true);

        List<String> dataTargetList = this.consoleHandle.getDataSource(this.baseDataSetName , "target" ,"name");
        String dataSetXml = this.consoleHandle.getDataSetXml(this.baseDataSetName);
        TestSession.logger.info("dataSetXml  = " + dataSetXml);

        // replace basedatasetName with the new datasetname
        dataSetXml = dataSetXml.replaceAll(this.baseDataSetName, this.dataSetName);
        dataSetXml = dataSetXml.replaceAll(dataTargetList.get(0), this.target1);
        dataSetXml = dataSetXml.replaceAll(dataTargetList.get(1), this.target2);

        TestSession.logger.info("after changing the dataset name    = " + dataSetXml);

        // Create a new dataset
        Response response = this.consoleHandle.createDataSet(this.dataSetName, dataSetXml);
        assertTrue("Failed to create the dataset " + this.dataSetName ,  response.getStatusCode() == SUCCESS);

        this.consoleHandle.sleep(5000);
    }

    public void activateDataSet() throws Exception {
        
        this.consoleHandle.sleep(5000);
        
        // activate the dataset
        this.consoleHandle.checkAndActivateDataSet(this.dataSetName);
        this.dsActivationTime = GdmUtils.getCalendarAsString();
    }

    /**
     * Check whether item(s) are in queue.
     * @return - return the item in the queue.
     */
    private JSONArray isItemInQueue(String facetName) {
        com.jayway.restassured.response.Response response = null;
        JSONArray jsonArray = null;
        long waitTimeForWorkflowPolling = 5 * 60 * 1000; // 5 minutes
        long sleepTime = 100; // 0.5 sec  sleep time.
        long waitTime = 0;
        boolean flag = false;

        while (waitTime <= waitTimeForWorkflowPolling) {
            String testURL = this.consoleHandle.getConsoleURL().replace("9999", this.consoleHandle.getFacetPortNo(facetName.trim())) + "/" + facetName.trim() + "/api/workflow/queue";
            TestSession.logger.info(" Queue testURL   - " + testURL);
            response = given().cookie(this.cookie).get(testURL);

            String queueResponseString = response.asString();
            TestSession.logger.info("queueResponseString   = " + queueResponseString);

            // if dataset is not in queue, go for next iteration.
            if (! queueResponseString.equals("{\"QueueResponse\":[]}")) {

                // convert String to jsonObject
                JSONObject obj =  (JSONObject) JSONSerializer.toJSON(queueResponseString);
                TestSession.logger.info("obj = " + obj.toString());

                // convert string to jsonArray
                jsonArray = obj.getJSONArray("QueueResponse");
                flag = true;
                break;
            }
            waitTime += sleepTime;
            this.consoleHandle.sleep(sleepTime);
        }
        if (waitTime >= waitTimeForWorkflowPolling) {
            fail("Time out. " + this.dataSetName  + "  was inserted into the queue.");
        }
        if (flag == false) {
            TestSession.logger.info(this.dataSetName  + "  was inserted into the queue.");
            fail(this.dataSetName  + "  was inserted into the queue.");
        }
        return jsonArray;
    }

    /**
     * Get the workflow name from the queue
     * @param jsonArray
     * @param dataSetName
     * @return
     */
    private String getItemInQueue(JSONArray jsonArray , String dataSetName)  {
        assertTrue("There is no item inserted queue." , jsonArray != null && jsonArray.size() > 0);
        String workflowName = null;
        Iterator iterator  = jsonArray.iterator();
        while ( iterator.hasNext() ) {
            JSONObject runningJsonObject = (JSONObject) iterator.next();
            workflowName = runningJsonObject.getString("WorkflowName");
            if ( workflowName.equals(dataSetName)) {
                break;
            }
        }
        return workflowName;
    }


    /**
     * Get a specified running dataset and kills its workflow. 
     */
    public void getRunningDataSetAndKillWorkFlow(String facetName , String dataSetName) {
        JSONArray jsonArray = null;
        com.jayway.restassured.response.Response response = null;
        int i = 1 , runningCount = 0, killedCount = 0;
        while (i <= 10)  {
            String runningWorkFlow =  this.consoleHandle.getConsoleURL().replace("9999", this.consoleHandle.getFacetPortNo(facetName.trim())) + "/" + facetName.trim() + "/api/workflows/running?datasetname="+ dataSetName +"&instancessince=F&joinType=innerJoin";
            TestSession.logger.info("testURL = " + runningWorkFlow);
            response = given().cookie( this.cookie).get(runningWorkFlow);
            assertTrue("Failed to get the respons  " + runningWorkFlow , (response != null) );

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

                    // workflow name value is used in checkDataSetInFailedWorkFlow() to check workflow name in failed workflow.
                    this.workFlowNames.add(workFlowName.trim());

                    if (fName.equals(facetName)) {
                        runningCount++;

                        //  kill the running workflow
                        resourceArray.add(new JSONObject().element("ExecutionID",executionId).element("FacetName", fName).element("FacetColo", facetColo));
                        String killTestURL = this.consoleHandle.getConsoleURL().replace("9999", this.consoleHandle.getFacetPortNo(facetName.trim())) + "/" + facetName.trim() + "/api/admin/workflows";
                        TestSession.logger.info("url  = " + killTestURL  + "   resource - " + resourceArray.toString() );
                        com.jayway.restassured.response.Response jobKilledResponse = given().cookie(this.cookie).param("command", "kill").param("workflowIds" , resourceArray.toString()).post(killTestURL);
                        JsonPath jsonPath = jobKilledResponse.getBody().jsonPath();
                        TestSession.logger.info("response = " + jsonPath.prettyPrint());

                        String responseId = jsonPath.getString("Response.ResponseId");
                        if (responseId.equals("0")) {
                            killedCount++;
                        }
                    }
                }
            }
            i++;
            this.consoleHandle.sleep(5000);
        }
        assertTrue(this.dataSetName + " dn't arrived to running state." , runningCount > 0 );
        assertTrue("Failed to kill one of the instance of " + this.dataSetName  + " running job =  " + runningCount   + " killed job =  " + killedCount  ,  runningCount == killedCount );
    }
    
}
