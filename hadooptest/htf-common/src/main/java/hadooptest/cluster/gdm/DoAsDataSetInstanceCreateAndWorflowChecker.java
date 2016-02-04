// Copyright 2016, Yahoo Inc.
package hadooptest.cluster.gdm;

import hadooptest.TestSession;
import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertTrue;
import org.junit.Assert;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.jayway.restassured.path.json.JsonPath;

public class DoAsDataSetInstanceCreateAndWorflowChecker {

    private static final int SUCCESS = 200;
    private static final long waitTimeBeforeWorkflowPollingInMs = 180000L;
    private static final long waitTimeBetweenWorkflowPollingInMs = 60000L;
    private static final long timeoutInMs = 300000L;
    private static final String HadoopLS = "/console/api/admin/hadoopls";

    /**
     *  Method to read Acquisition and Replication dataset file and modify its value
     * @param dataSourceXmlFileName
     * @return
     */
    public String createAcqRepDataSetXml( ConsoleHandle console, String dataSetName, String dataSetConfigBase , String dataSourceXmlFileName , HashMap<String, String> dataSetInfo ) {
        String dataSetXml = console.createDataSetXmlFromConfig(dataSetName, dataSetConfigBase + dataSourceXmlFileName);
        if (dataSetXml == null) {
            Assert.fail("Failed to get dataset xml for dataSetName: " + dataSetName + ", dataSetConfigBase: " + dataSetConfigBase + ", dataSourceXmlFileName: " + dataSourceXmlFileName);
        }
        if ( !dataSetInfo.containsKey("GROUP_OWNER_VALUE")) {
            dataSetXml = dataSetXml.replace("GROUP_OWNER_VALUE", "");
        }
        for (Map.Entry<String,String> entry : dataSetInfo.entrySet()) {
            String key = entry.getKey();
            dataSetXml = dataSetXml.replaceAll(key, dataSetInfo.get(key));
        }
        return dataSetXml;
    }

    /**
     * Method to read Retention dataset file and modify its values.
     * @param console
     * @param retDataSetName
     * @param acqRepDataSetName
     * @param dataSourceXmlFileName
     * @param dataSetInfo
     * @return
     */
    public String createRetDataSetXml( ConsoleHandle console, String retDataSetName,String dataSetConfigBase, String acqRepDataSetName , String dataSourceXmlFileName , HashMap<String, String> dataSetInfo  ) {
        String dataSetXml = console.createDataSetXmlFromConfig(retDataSetName, dataSetConfigBase + dataSourceXmlFileName );
        if ( !dataSetInfo.containsKey("GROUP_OWNER_VALUE")) {
            dataSetXml = dataSetXml.replace("GROUP_OWNER_VALUE", "");
        }
        for (Map.Entry<String,String> entry : dataSetInfo.entrySet()) {
            String key = entry.getKey();
            dataSetXml = dataSetXml.replaceAll(key, dataSetInfo.get(key));
        }
        dataSetXml = dataSetXml.replaceAll("DATA_PATH", this.getDataPath( acqRepDataSetName ));
        dataSetXml = dataSetXml.replaceAll("SCHEMA_PATH", this.getSchemaPath( acqRepDataSetName ));
        dataSetXml = dataSetXml.replaceAll("COUNT_PATH", this.getCountPath( acqRepDataSetName) );
        return dataSetXml;
    }


    /**
     * Method that checks for the acquisition and replication facet workflow status
     */
    public void testAcqRepWorkFlow( ConsoleHandle console , String dataSetName ,  String datasetActivationTime ) {
        Response response = null;
        long currentTotalWaitingTime = waitTimeBeforeWorkflowPollingInMs - waitTimeBetweenWorkflowPollingInMs;
        TestSession.logger.info("Sleeping for " + currentTotalWaitingTime + " ms before checking workflow status");
        console.sleep(currentTotalWaitingTime);

        boolean acqSuccess = false, repSuccess = false;
        while (currentTotalWaitingTime < timeoutInMs) {
            TestSession.logger.info("Sleeping for " + waitTimeBetweenWorkflowPollingInMs + " ms before checking workflow status");
            console.sleep(currentTotalWaitingTime);

            currentTotalWaitingTime = currentTotalWaitingTime + waitTimeBetweenWorkflowPollingInMs;
            response = console.getCompletedJobsForDataSet(datasetActivationTime, GdmUtils.getCalendarAsString(), dataSetName);

            if (!acqSuccess) {
                acqSuccess = console.isWorkflowCompleted(response, "acquisition");
                if (acqSuccess) { 
                    assertTrue("Acquisition workflow Passed." , acqSuccess == true);
                }                
                else if(!acqSuccess && console.isWorkflowFailed(response, "acquisition")) {
                    Assert.fail("Acquisition workflow Passed.");
                }
            }
            if (acqSuccess && !repSuccess) {
                repSuccess = console.isWorkflowCompleted(response, "replication");
                if (repSuccess) {
                    assertTrue("Replication workflow Passed." , repSuccess == true);
                }                
                else if (!repSuccess && console.isWorkflowFailed(response, "replication")) {
                    Assert.fail("Replication workflow Failed.");
                }
            }

            if (acqSuccess && repSuccess ) {
                break;   
            }
        }

        boolean notAllSuccessful = !acqSuccess || !repSuccess ;
        if ((currentTotalWaitingTime >= timeoutInMs) && notAllSuccessful) {
            assertTrue("The test has timed out for "+ dataSetName  +" task completion status - " + "Acquistion: " + acqSuccess + ", Replication: " + repSuccess , notAllSuccessful == false);
        }
    }

    /**
     * Method to check the Retention workflow for the given dataset.
     * @param console - console instance for this dataset
     * @param dataSetName   - dataset for which workflow should be tested.
     * @param datasetActivationTime - dataset activation time.
     */ 
    public String testRetWorkFlow( ConsoleHandle console , String dataSetName ,  String datasetActivationTime ) {
        Response response = null;
        boolean retWorkflowStatus = false;
        long currentTotalWaitingTime = waitTimeBeforeWorkflowPollingInMs - waitTimeBetweenWorkflowPollingInMs;
        TestSession.logger.info("Sleeping for " + currentTotalWaitingTime + " ms before checking workflow status");
        console.sleep(currentTotalWaitingTime);

        boolean retSuccess = false;
        while (currentTotalWaitingTime < timeoutInMs) {
            TestSession.logger.info("Sleeping for " + waitTimeBetweenWorkflowPollingInMs + " ms before checking workflow status");
            console.sleep(currentTotalWaitingTime);

            currentTotalWaitingTime = currentTotalWaitingTime + waitTimeBetweenWorkflowPollingInMs;
            response = console.getCompletedJobsForDataSet(datasetActivationTime, GdmUtils.getCalendarAsString(), dataSetName);

            if (!retSuccess) {
                retSuccess = console.isWorkflowCompleted(response, "retention");
                if (retSuccess) {
                    assertTrue("Retention workflow Passed for " + dataSetName + "  dataset  " , retSuccess == true);
                } else if (!retSuccess && console.isWorkflowFailed(response, "retention")) {
                    Assert.fail("Retention Workflow Failed");
                }
            }

            if (retSuccess) {
                break ;
            }
        }

        String result = console.pingWorkflowExecution(dataSetName , datasetActivationTime , currentTotalWaitingTime);
        TestSession.logger.info("result = " + result);

        return result;
    }

    /**
     * Deactivate the dataset
     * @param console - Instance of ConsoleHandle
     * @param dataSetName - dataset that needs to deactivate
     */
    public void deactivateDoAsDataSet(ConsoleHandle console , String dataSetName) {
        // make dataset inactive
        Response response = console.deactivateDataSet(dataSetName);
        assertTrue("Failed to deactivate the dataset " + dataSetName +"  response code = " + response.getStatusCode() , response.getStatusCode() == SUCCESS );
    }

    /**
     * Get owner, group and permission  information for a given dataset for both acquisition and replication workflow
     * @param dataSetName - dataset name for which HDFS information has to be retrieved
     * @param url - console url
     * @param cookie - cookie for REST API
     */
    public void compareAcqReplHDFSFileInfo(String dataSetName , String url , String cookie) {
        String acqusitionTargetClusterName = GdmUtils.getConfiguration("testconfig.DoAsAcqRepByUserRetBySuperUserTest.TARGET1");
        String replicationTargetClusteName = GdmUtils.getConfiguration("testconfig.DoAsAcqRepByUserRetBySuperUserTest.TARGET2");

        String path = "/data/daqdev/data/" + dataSetName ;

        // Get acquisition datapath
        String acqURL = url + this.HadoopLS + "?format=json&dataSource=" + acqusitionTargetClusterName + "&path=" + path;
        TestSession.logger.info("acqURL   =  "  +acqURL + " cookie -  " + cookie);
        com.jayway.restassured.response.Response response = given().cookie(cookie).get(acqURL);
        JsonPath jsonPath = response.getBody().jsonPath();
        String acqOwner = (String) jsonPath.getList("Files.Owner").get(0);
        String acqGroup = (String) jsonPath.getList("Files.Group").get(0);
        String acqPermission = (String) jsonPath.getList("Files.Permission").get(0);
        TestSession.logger.info("owner = " + acqOwner  + "   group = " + acqGroup +   " permission - " + acqPermission) ;

        // Get replication datapath     
        String repURL = url + this.HadoopLS + "?format=json&dataSource=" + replicationTargetClusteName + "&path=" + path;
        response = given().cookie(cookie).get(repURL);
        jsonPath = response.getBody().jsonPath();
        String repOwner = (String) jsonPath.getList("Files.Owner").get(0);
        String repGroup = (String) jsonPath.getList("Files.Group").get(0);
        String repPermission = (String) jsonPath.getList("Files.Permission").get(0);
        TestSession.logger.info("owner = " + repOwner  + "   group = " + repGroup +   " permission - " + repPermission) ;

        assertTrue("acq owner = " + acqOwner +"   is different from replication owner = " + repOwner , acqOwner.equalsIgnoreCase(repOwner));
        assertTrue("acq group = " + acqGroup +"   is different from replication group = " + repOwner , acqGroup.equalsIgnoreCase(repGroup));
        assertTrue("acq file permission = " + acqPermission +"   is different from replication owner = " + repPermission , acqPermission.equalsIgnoreCase(repPermission));
    }

    /**
     * Method do check 
     * @param clusters
     * @param url
     * @param cookie
     * @param path
     * @param dataSetName
     */
    public void checkForFilesOnClusterAfterRetentionWorkflow( String cluster, String url , String cookie , String path , String dataSetName) {
        String testURL = url + this.HadoopLS + "?format=json&dataSource=" + cluster + "&path=" + path +  dataSetName;
        com.jayway.restassured.response.Response response = given().cookie(cookie).get(testURL);
        JsonPath jsonPath = response.getBody().jsonPath();
        int size =  jsonPath.getList("Files").size();
        assertTrue("Retention workflow cn't able to delete files for " + dataSetName , size == 0);
    }

    private String getDataPath(String dataSetName) {
        return "/data/daqdev/data/" + dataSetName + "/%{date}";
    }

    private String getSchemaPath(String dataSetName) {
        return "/data/daqdev/schema/" + dataSetName + "/%{date}";
    }

    private String getCountPath(String dataSetName) {
        return "/data/daqdev/count/" + dataSetName + "/%{date}";
    }

}
