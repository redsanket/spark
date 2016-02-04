// Copyright 2016, Yahoo Inc.
package hadooptest.gdm.regression;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Assert;

import java.util.ArrayList;
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

import hadooptest.TestSession;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.cluster.gdm.HTTPHandle;
import hadooptest.cluster.gdm.Response;

/**
 * Test Case : Verify whether running workflow can be killed and it is marked as failed state.
 */
public class KillRunningAcquisitionWorkFlowTest extends TestSession {
    private static final String baseDataSetName = "VerifyAcqRepRetWorkFlowExecutionDateRange";
    private ConsoleHandle console;
    private HTTPHandle httpHandle;  
    private String dataSetName;
    private String hostName;
    private String cookie;
    private String datasetActivationTime = null;
    private Response response;
    private List<String> workFlowNames;
    private static final int SUCCESS = 200;
    private static final int SLEEP_TIME = 40000;

    @BeforeClass
    public static void startTestSession() throws Exception {
        TestSession.start();
    }

    @Before
    public void setUp() throws NumberFormatException, Exception {
        this.console = new ConsoleHandle();
        this.httpHandle = new HTTPHandle();
        this.dataSetName = "Test_KillRunningWorkFlow_" + System.currentTimeMillis();
        this.hostName =  this.console.getConsoleURL();
        this.cookie = this.httpHandle.getBouncerCookie();
        this.workFlowNames = new ArrayList<String>();
    }

    @Test
    public void testKillRunningWorkFlow() {
        createDataSet();

        this.response = this.console.activateDataSet(this.dataSetName);
        assertTrue("Failed to activate dataset " + dataSetName , response.getStatusCode() == SUCCESS);

        this.datasetActivationTime = GdmUtils.getCalendarAsString();
        TestSession.logger.info("DataSet Activation Time: " + datasetActivationTime);

        // wait for some time so that discovery starts and workflow comes to running state.
        this.console.sleep(SLEEP_TIME);

        // check whether workflow is in acquisition running state , if yes, kill
        getRunningDataSetAndKillWorkFlow("acquisition");

        // check whether killed workflow jobs are in failed state.
        checkDataSetInFailedWorkFlow();
    }

    /**
     * Get a specified running dataset and kills its workflow. 
     */
    public void getRunningDataSetAndKillWorkFlow(String facetName) {
        JSONArray jsonArray = null;
        com.jayway.restassured.response.Response response = null;
        int i = 1 , runningCount = 0, killedCount = 0;
        while (i <= 10)  {
            String runningWorkFlow =  this.hostName.replaceAll("9999", "4080") + "/acquisition/api/workflows/running?datasetname="+ this.dataSetName +"&instancessince=F&joinType=innerJoin";
            TestSession.logger.info("testURL = " + runningWorkFlow);
            response = given().cookie( this.cookie).get(runningWorkFlow);
            
            assertTrue("Failed to get the respons  " + runningWorkFlow , (response != null || response.toString() != "") );
            
            jsonArray = this.console.convertResponseToJSONArray(response , "runningWorkflows");
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
                        runningCount++;
                        
                    //  kill the running workflow
                        resourceArray.add(new JSONObject().element("ExecutionID",executionId).element("FacetName", fName).element("FacetColo", facetColo));
                        String killTestURL = this.hostName.replace("9999","4080") + "/acquisition/api/admin/workflows";
                        TestSession.logger.info("url  = " + killTestURL);
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
            this.console.sleep(SLEEP_TIME);
        }
        assertTrue(this.dataSetName + " dn't arrived to running state." , runningCount > 0 );
        assertTrue("Failed to kill one of the instance of " + this.dataSetName  + " running job =  " + runningCount   + " killed job =  " + killedCount  ,  runningCount == killedCount );
    }

    /**
     * Verify whether killed dataset is in failed state.
     */
    private void checkDataSetInFailedWorkFlow()  {
        int failedJob = 0;
        JSONArray jsonArray = null;
        String failedWorkFlowURL = this.hostName + "/console/api/workflows/failed?starttime=" + this.datasetActivationTime +"&endtime=" + GdmUtils.getCalendarAsString() + "&datasetname=" + this.dataSetName 
                + "&instancessince=F&joinType=innerJoin";
        TestSession.logger.info("Failed workflow testURL" + failedWorkFlowURL);
        com.jayway.restassured.response.Response failedResponse = given().cookie(this.cookie).get(failedWorkFlowURL);
        assertTrue("Failed to get the respons  " + failedResponse , (response != null || response.toString() != "") );
        
        jsonArray = this.console.convertResponseToJSONArray(failedResponse , "failedWorkflows");
        if ( jsonArray.size() > 0 ) {
            failedJob = jsonArray.size() ;
            Iterator iterator = jsonArray.iterator();
            while (iterator.hasNext()) {
                JSONObject failedJsonObject = (JSONObject) iterator.next();
                TestSession.logger.info("failedJsonObject  = " + failedJsonObject.toString());
                String facetName = failedJsonObject.getString("FacetName");
                String workFlowName = failedJsonObject.getString("WorkflowName");
                String executionStatus = failedJsonObject.getString("ExecutionStatus");
                String exitStatus = failedJsonObject.getString("ExitStatus");

                assertTrue("workflow name dn't match  " + this.workFlowNames.toString() , this.workFlowNames.contains(workFlowName.trim()) );
                assertTrue("Expected facetName is acquisition , but got " + facetName , facetName.equalsIgnoreCase("acquisition") ); 
                assertTrue("Expected executionStatus is STOPPED , but got " + exitStatus , executionStatus.equalsIgnoreCase("STOPPED") );
                assertTrue("Expected exitStatus is INTERRUPTED , but got " + exitStatus , exitStatus.equalsIgnoreCase("INTERRUPTED") );
            }
        } else if ( jsonArray.size()  == 0) {
            Assert.fail("Failed : " + this.dataSetName  +"   dn't exists in failed workflow.");
        }
    }

    /**
     * create a dataset
     */
    private void createDataSet() {
        String dataSetXml = this.console.getDataSetXml(this.baseDataSetName);
        if (dataSetXml == null) {
            Assert.fail("Failed to get dataSetXml for " + this.baseDataSetName);
        }
        dataSetXml = dataSetXml.replaceAll(this.baseDataSetName, this.dataSetName);
        Response response = this.console.createDataSet(this.dataSetName, dataSetXml);
        assertTrue("Failed to create a dataset " + dataSetName , response.getStatusCode() == SUCCESS);

        // wait so that datset specfication file is created.
        this.console.sleep(SLEEP_TIME);
    }

    /**
     *  Deactivate the dataset
     * @throws Exception
     */
    @After
    public void tearDown() throws Exception {
        Response response = this.console.deactivateDataSet(this.dataSetName);
        assertEquals("ResponseCode - Deactivate DataSet", SUCCESS , response.getStatusCode());
        assertEquals("ActionName.", "terminate", response.getElementAtPath("/Response/ActionName").toString());
        assertEquals("ResponseId", "0", response.getElementAtPath("/Response/ResponseId").toString());
        assertEquals("ResponseMessage.", "Operation on " + this.dataSetName + " was successful.", response.getElementAtPath("/Response/ResponseMessage/[0]").toString());
    }
    
}
