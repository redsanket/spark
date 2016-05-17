// Copyright 2016, Yahoo Inc.
package hadooptest.gdm.regression.stress;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertTrue;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.commons.configuration.XMLConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.cluster.gdm.HTTPHandle;
import hadooptest.cluster.gdm.JSONUtil;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;


/**
 *  This is stress testing main class, it reads the parameter values from config.xml files.
 *  parameters for stress testing are as follows 
 *  No of datasets, no of instances per dataset and no of instance files instance.
 *  
 *  User can modify this parameter values through jenkins, so its dynamic.
 *   
 *  This test reads the above parameter and creates the files on the source cluster,  creates datasets and checks for the workflow,
 *  right now the files size is 350 MB. going forward, even the file sizes will be made dyanamic ( passing though jenkin jobs).  
 *
 */
public class TestGDMStress extends TestSession {

    private XMLConfiguration conf;
    private String noOfInstance;
    private String noOfFilesInInstance;
    private String noOfDataSets;
    private String deploymentSuffixName;
    private StressTestingInit stressTestingInitObj;
    private String sourceCluster;
    private String destinationCluster;
    private String target1;
    private String target2;
    private JSONUtil jsonUtil;
    private String cookie;
    private ConsoleHandle consoleHandle;
    private HTTPHandle httpHandle = null; 
    private WorkFlowHelper workFlowHelperObj = null;
    private List<String> grids ;
    private List<String> instances ;
    private List<String> datasetNames;
    private HashMap<String, String> datasetActivation ;
    private String nameNodeName;
    private String startDate;
    private String endDate;
    private static final String baseDataSetName = "VerifyAcqRepRetWorkFlowExecutionSingleDate";
    private static final String HCAT_TYPE = "DataOnly";
    private static final int SUCCESS = 200;

    @BeforeClass
    public static void testSessionStart() throws Exception {
        TestSession.start();
    }

    @Before
    public void setup() throws Exception {

        // get configuration values from config.xml, this values are supplied from jenkins jobs
        String configPath = Util.getResourceFullPath("gdm/conf/config.xml");
        this.conf = new org.apache.commons.configuration.XMLConfiguration(configPath);
        this.deploymentSuffixName = this.conf.getString("hostconfig.console.deployment-suffix-name");
        this.noOfInstance = this.conf.getString("hostconfig.console.stress-noOfinstances");
        this.noOfFilesInInstance = this.conf.getString("hostconfig.console.stress-filesInInstance");
        this.noOfDataSets = this.conf.getString("hostconfig.console.stress-noOfDataSets");
        this.datasetNames = new ArrayList<String>();
        this.datasetActivation = new HashMap<String, String>();
        this.consoleHandle = new ConsoleHandle();
        this.httpHandle = new HTTPHandle();
        this.cookie = this.httpHandle.getBouncerCookie();
        this.workFlowHelperObj = new WorkFlowHelper();
        this.jsonUtil = new JSONUtil();
        this.grids = this.consoleHandle.getAllGridNames();
        assertTrue("Insufficient Grids to run the Stress testing " + this.grids + "  Need atleast 2 grids." , this.grids.size() >= 2);

        for ( String cluster : grids)  {
            TestSession.logger.info(cluster);
        }

        this.target1 =  this.grids.get(0);
        this.target2 =  this.grids.get(1);
        TestSession.logger.info("sourceCluster  = " + this.sourceCluster   + "  destinationCluster  =   " + this.destinationCluster);

        // get namename node
        this.nameNodeName = this.consoleHandle.getClusterNameNodeName(this.target1);
        TestSession.logger.info(nameNodeName + " is the nameNodeName  of  = " + this.target1 );     

        // init the stressting and create the instance and instance files on the the source cluster.
        this.stressTestingInitObj = new StressTestingInit(this.deploymentSuffixName , this.noOfInstance , this.noOfFilesInInstance , this.nameNodeName);
        this.stressTestingInitObj.execute();
        
        // get sorted instances.
        this.instances = this.stressTestingInitObj.getInstances();
        TestSession.logger.info("Instances = " + this.instances);
        
        // first value in the sorted instance is the start date.
        this.startDate = this.instances.get(0);
        
        // last value in the sorted instance is the end date.
        this.endDate = this.instances.get(this.instances.size() - 1);
        this.workFlowHelperObj = new WorkFlowHelper();
    }

    @Test
    public void test() throws NumberFormatException, Exception {
        TestSession.logger.info("**********************************************************************************************************************************");

        // create dataset and activate, so that workflow may start.
        for ( int i = 0; i < Integer.parseInt(this.noOfDataSets)  ; i++) {
            String dsName = "GDM_StressTesting_Dataset_" + System.currentTimeMillis();
            this.sourceCluster = this.target1 + "_Source_" +  System.currentTimeMillis();
            this.destinationCluster = this.target2 + "_Target_" +  System.currentTimeMillis();
            
            // create datasource to avoid path collision
            this.createTestDataSource( this.target1 , this.sourceCluster );
            this.createTestDataSource( this.target2 , this.destinationCluster);

            // create a dataset
            createDoAsReplicationDataSet(dsName);
            this.datasetNames.add(dsName);

            // activate the dataset
            this.consoleHandle.checkAndActivateDataSet(dsName);
            String dsActivationTime = GdmUtils.getCalendarAsString();
            this.datasetActivation.put(dsName, dsActivationTime);
        }

        // check for the workflow for each dataset.
        for ( String dsName : this.datasetNames) {

            // check workflow for each instance of the dataset.
            for ( int index = 0 ; index < this.instances.size() - 1 ; index++) {
                String instance = this.instances.get(index);
                String activationTime = this.datasetActivation.get(dsName);
                this.workFlowHelperObj.checkWorkFlow(dsName , "replication", activationTime , instance);
            }
        }
        
        this.sourceInstanceCleanUp(this.stressTestingInitObj , "/data/daqdev/" + this.stressTestingInitObj.getDataSourcePath());
        this.setRetentionPolicyToAllDataSets(this.datasetNames);
    
    }
    
    /**
     * Helps in cleaning up the source instance and instance files.
     * @param streeTestingInitObject
     * @param path  - string representing the path to delete
     * @throws IOException
     * @throws InterruptedException
     */
    public void sourceInstanceCleanUp(StressTestingInit streeTestingInitObject , String path) throws IOException, InterruptedException {
        HashMap<String, HashMap<String, String>>  supportingDataMap = stressTestingInitObj.getSupportingDataMap();
        for (String aUser : supportingDataMap.keySet()) {
            Configuration conf = stressTestingInitObj.getConfForRemoteFS();
            UserGroupInformation ugi = stressTestingInitObj.getUgiForUser(aUser);
            CleanSourceClusterDataAction cleanSourceDataActionObject = new CleanSourceClusterDataAction(conf , path);
            ugi.doAs(cleanSourceDataActionObject);
        }
    }
    
    // set retention policy the dataset(s) 
    public void setRetentionPolicyToAllDataSets(List<String> dataSets) {
        String testURL = this.consoleHandle.getConsoleURL() + "/console/rest/config/dataset/getRetentionPolicies";
        
        // navigate all the datasets and set the retention policy to all the targets
        for ( String dsName : dataSets) {
            TestSession.logger.info("dsName = " + dsName);
            List<String> dataTargetList = this.consoleHandle.getDataSource(dsName , "target" ,"name");
            TestSession.logger.info("dataTargetList  = "  + dataTargetList);
            String resourceName = this.jsonUtil.constructResourceNamesParameter(Arrays.asList(dsName));
            List<String> arguments = new ArrayList<String>();
            for (String target : dataTargetList) {
                arguments.add("numberOfInstances:0:" + target.trim());
                TestSession.logger.info("grid = " + target);
                String args = constructPoliciesArguments(arguments , "updateRetention");
                TestSession.logger.info("args = "+args);
                TestSession.logger.info("test url = " + testURL);
                com.jayway.restassured.response.Response res = given().cookie(this.cookie).param("resourceNames", resourceName).param("command","update").param("args", args)
                        .post(this.consoleHandle.getConsoleURL() + "/console/rest/config/dataset/actions");
                TestSession.logger.info("Response code = " + res.getStatusCode());
                assertTrue("Failed to modify or set the retention policy for " + dsName , res.getStatusCode() == SUCCESS);
                
                String resString = res.getBody().asString();
                TestSession.logger.info("resString = " + resString);
            }
        }       
    }

    /**
     * Method that creates a replication dataset
     * @param dataSetFileName - name of the replication dataset
     */
    private void createDoAsReplicationDataSet(String dsName) {
        String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/DoAsReplicationDataSet.xml");
        String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(dsName, dataSetConfigFile);
        String feedName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Parameters" , "value");
        String sourceName =  this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Sources" , "name");

        dataSetXml = dataSetXml.replaceAll("TARGET1", this.destinationCluster );
        dataSetXml = dataSetXml.replaceAll("<RunAsOwner>replication</RunAsOwner>", "");
        dataSetXml = dataSetXml.replaceAll("GROUP_NAME", "users");
        dataSetXml = dataSetXml.replaceAll("owner=\"DATA_OWNER\"", "");
        dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", dsName);
        dataSetXml = dataSetXml.replaceAll("FEED_NAME", feedName );
        dataSetXml = dataSetXml.replaceAll("FEED_STATS", feedName + "_stats" );
        dataSetXml = dataSetXml.replaceAll("ACQUISITION_SOURCE_NAME", this.sourceCluster );
        dataSetXml = dataSetXml.replace("HCAT_TYPE", this.HCAT_TYPE);
        dataSetXml = dataSetXml.replaceAll("HCAT_TABLE_NAME", dsName);
        dataSetXml = dataSetXml.replaceAll("REPLICATION_SOURCE_DATA_PATH", "/data/daqdev/" + this.stressTestingInitObj.getDataSourcePath() + "/%{date}");
        dataSetXml = dataSetXml.replaceAll("REPLICATION_SOURCE_COUNT_PATH", "/data/daqdev/" + this.stressTestingInitObj.getDataSourcePath() + "/%{date}" );
        dataSetXml = dataSetXml.replaceAll("REPLICATION_SOURCE_SCHEMA_PATH", "/data/daqdev/" + this.stressTestingInitObj.getDataSourcePath() + "/%{date}" );
        dataSetXml = dataSetXml.replaceAll("HCAT_TABLE_PROPAGATION", "false");
        dataSetXml = dataSetXml.replaceAll("CUSTOM_DATA_PATH", this.getCustomPath("data" , dsName));
        dataSetXml = dataSetXml.replaceAll("CUSTOM_SCHEMA_PATH", this.getCustomPath("data" , dsName));
        dataSetXml = dataSetXml.replaceAll("CUSTOM_COUNT_PATH", this.getCustomPath("data" , dsName));
        dataSetXml = dataSetXml.replaceAll("20130725", this.startDate);
        dataSetXml = dataSetXml.replaceAll("20220131", this.endDate);
        dataSetXml = dataSetXml.replaceAll("DATABASE_NAME", "gdm");

        Response response = this.consoleHandle.createDataSet(dsName, dataSetXml);
        if (response.getStatusCode() != SUCCESS) {
            try {
                throw new Exception("Response status code is " + response.getStatusCode() + ", expected 200.");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        // sleep so that specification file is created and available to the facets
        this.consoleHandle.sleep(5000);
    }

    /**
     * Method to create a custom path for the dataset.
     * @param pathType - its a string type which represents either data/count/schema
     * @param dataSet - dataset name
     * @return
     */
    private String getCustomPath(String pathType , String dataSet) {
        return  "/data/daqdev/stress-testing/"+ pathType +"/"+ dataSet + "/%{date}";
    }

    /**
     * Create DataSource for each target, in order to avoid target collision
     * @param DataSourceName existing target datasource
     * @param newDataSourceName - new datasource name
     */
    public void createTestDataSource(String dataSourceName , String newDataSourceName) {
        String xml = this.consoleHandle.getDataSourceXml(dataSourceName);
        xml = xml.replaceFirst(dataSourceName,newDataSourceName);
        boolean isDataSourceCreated = this.consoleHandle.createDataSource(xml);
        assertTrue("Failed to create a DataSource specification " + newDataSourceName , isDataSourceCreated == true);
        this.consoleHandle.sleep(5000);
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
     * deactivate the dataset(s)    
     */
    @After
    public void tearDown() {
        // deactivate all the datasets
        for(String dsName : this.datasetNames) {
            TestSession.logger.info("Deactivate "+ dsName  +"  dataset ");
            Response response = this.consoleHandle.deactivateDataSet(dsName);
            assertTrue("Failed to deactivate dataset " + dsName , response.getStatusCode() == SUCCESS);
            assertTrue("Expected terminate action name but got " + response.getElementAtPath("/Response/ActionName").toString() , response.getElementAtPath("/Response/ActionName").toString().equals("terminate") );
            assertTrue("Expected to get 0 response id, but got " + response.getElementAtPath("/Response/ResponseId").toString() , response.getElementAtPath("/Response/ResponseId").toString().equals("0"));    
        }
    }
}
