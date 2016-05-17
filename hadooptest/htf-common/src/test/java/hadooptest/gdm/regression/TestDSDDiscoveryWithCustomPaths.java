// Copyright 2016, Yahoo Inc.
package hadooptest.gdm.regression;

import static org.junit.Assert.assertTrue;
import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;

import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test Scenario : Verify whether discovery happens and acquisition and replication workflow is success for different custom path as below
 * 
 * 1) "/data/daqdev/"+ pathType +"/"+ dataSet + "/%{srcid}/%{date}" + EXTRA_PATH_AFTER_DATE; 
 * 2) "/data/daqdev/"+ pathType +"/"+ dataSet + "/%{srcid}/"+ EXTRA_PATH_BEFORE_DATE  + "%{date}";
 * 3) "/data/daqdev/"+ pathType +"/"+ dataSet + "/%{srcid}"+ EXTARA_PATH_AFTER_DSD_SRC + "/%{date}";
 * 4) "/data/daqdev/"+ pathType +"/"+ dataSet + "/" + EXTARA_PATH_BEFORE_DSD_SRC + "%{srcid}/%{date}";
 * 
 */
public class TestDSDDiscoveryWithCustomPaths  extends TestSession {

    private ConsoleHandle consoleHandle;
    private String url;
    private String baseDataSetName = "VerifyAcqRepRetWorkFlowExecutionDateRange";
    private String datasetActivationTime;
    private String dataSetName;
    private String sourceName;
    private String targetGrid1;
    private String targetGrid2;
    private WorkFlowHelper workFlowHelper;
    private List<String>grids = null;
    private static final String HCAT_ENABLED = "FALSE";
    private static final String PARTITION_NAME = "srcid";
    private static final String PARTITION_ID_1 = "1780";
    private static final String PARTITION_ID_2 = "23440";
    private static final String EXTRA_PATH_AFTER_DATE = "ExtraPathAfterDateDSD";
    private static final String EXTRA_PATH_BEFORE_DATE = "ExtraPathBeforeDateDSD_";
    private static final String EXTARA_PATH_AFTER_DSD_SRC = "DSDExtraPathAfterSRCID";
    private static final String EXTARA_PATH_BEFORE_DSD_SRC = "DSDExtraPathBeforeSRCID";
    private static int SUCCESS = 200;
    private static final String customPathString = "<Paths><Path location=\"CUSTOM_COUNT_PATH\" type=\"count\"/><Path location=\"CUSTOM_DATA_PATH\" type=\"data\"/><Path location=\"CUSTOM_SCHEMA_PATH\" type=\"schema\"/></Paths>";
    private static final String FIRST_DSD_VALUE = "1780";
    private static final String SECOND_DSD_VALUE = "23440";

    @BeforeClass
    public static void startTestSession() throws Exception {
        TestSession.start();
    }

    @Before
    public void setUp() throws NumberFormatException, Exception {
        consoleHandle = new ConsoleHandle();
        
        // Get all the grid with the current deployment and select first two grid as targets for datasest.  
        grids = this.consoleHandle.getUniqueGrids();
        if (grids.size() >= 2) {
            this.targetGrid1 = grids.get(0);
            this.targetGrid2 = grids.get(1);

            TestSession.logger.info("target1 = " + this.targetGrid1); 
            TestSession.logger.info("target2 = " + this.targetGrid2);
        }
        this.workFlowHelper = new WorkFlowHelper();
    }

    @Test
    public void testDiscoveryMonitorWithDSDDataSet() throws Exception {

        for ( int  testScenarioNo  = 1 ; testScenarioNo <=4; testScenarioNo++ ) {
            
            if (testScenarioNo ==  1) {
                this.dataSetName = "TestDSDDiscoveryWith_" + EXTRA_PATH_AFTER_DATE + "_" +  System.currentTimeMillis();
            } else if (testScenarioNo ==  2) {
                this.dataSetName = "TestDSDDiscoveryWith_" + EXTRA_PATH_BEFORE_DATE + "_" +  System.currentTimeMillis();
            } else if (testScenarioNo ==  3) {
                this.dataSetName = "TestDSDDiscoveryWith_" +  EXTARA_PATH_AFTER_DSD_SRC + "_" +  System.currentTimeMillis();
            } else if (testScenarioNo ==  4) {
                this.dataSetName = "TestDSDDiscoveryWith_"+ EXTARA_PATH_BEFORE_DSD_SRC + "_" +  System.currentTimeMillis();
            }
                
            // create a dataset
            createDataSetAndActivate(testScenarioNo);

            // check for acquisition workflow
            this.workFlowHelper.checkWorkFlow(this.dataSetName, "acquisition", this.datasetActivationTime);
            this.checkCustomPathIsCreated(this.targetGrid1 , testScenarioNo);

            // check for replication workflow
            this.workFlowHelper.checkWorkFlow(this.dataSetName, "replication", this.datasetActivationTime);
            this.checkCustomPathIsCreated(this.targetGrid1 , testScenarioNo);
            
            
            // set retention policy and check for retention workflow
            this.consoleHandle.setRetentionPolicyToAllDataSets(this.dataSetName, "0");
            this.workFlowHelper.checkWorkFlow(this.dataSetName, "retention", this.datasetActivationTime);
        }
    }

    public void checkCustomPathIsCreated(String targetName , int testScenarioNo) {

        // get all the custom path of the dataset
        List<String> pathList = this.workFlowHelper.getInstanceFileAfterWorkflow(targetName, this.dataSetName);

        // check whether path exists
        for ( String pathValue : pathList) {
            
            if (pathValue.indexOf(FIRST_DSD_VALUE) > 0) {
                if (testScenarioNo == 1) {
                    assertTrue("Expected that path's subdirectory to "+ EXTRA_PATH_AFTER_DATE + " , but got " + pathValue ,  pathValue.indexOf(EXTRA_PATH_AFTER_DATE) > 0);
                } else if (testScenarioNo == 2) {
                    assertTrue("Expected that path's subdirectory to "+ EXTRA_PATH_BEFORE_DATE + " , but got " + pathValue ,  pathValue.indexOf(EXTRA_PATH_BEFORE_DATE) > 0);
                } else if (testScenarioNo == 3) {
                    assertTrue("Expected that path's subdirectory to "+ EXTARA_PATH_AFTER_DSD_SRC + " , but got " + pathValue ,  pathValue.indexOf(EXTARA_PATH_AFTER_DSD_SRC) > 0);
                } else if (testScenarioNo == 4) {
                    assertTrue("Expected that path's subdirectory to "+ EXTARA_PATH_BEFORE_DSD_SRC + " , but got " + pathValue ,  pathValue.indexOf(EXTARA_PATH_BEFORE_DSD_SRC) > 0);
                }
            } else if (pathValue.indexOf(SECOND_DSD_VALUE) > 0) {
                if (testScenarioNo == 1) {
                    assertTrue("Expected that path's subdirectory to "+ EXTRA_PATH_AFTER_DATE + " , but got " + pathValue ,  pathValue.indexOf(EXTRA_PATH_AFTER_DATE) > 0);
                } else if (testScenarioNo == 2) {
                    assertTrue("Expected that path's subdirectory to "+ EXTRA_PATH_BEFORE_DATE + " , but got " + pathValue ,  pathValue.indexOf(EXTRA_PATH_BEFORE_DATE) > 0);
                } else if (testScenarioNo == 3) {
                    assertTrue("Expected that path's subdirectory to "+ EXTARA_PATH_AFTER_DSD_SRC + " , but got " + pathValue ,  pathValue.indexOf(EXTARA_PATH_AFTER_DSD_SRC) > 0);
                } else if (testScenarioNo == 4) {
                    assertTrue("Expected that path's subdirectory to "+ EXTARA_PATH_BEFORE_DSD_SRC + " , but got " + pathValue ,  pathValue.indexOf(EXTARA_PATH_BEFORE_DSD_SRC) > 0);
                }
            }
        }
    }

    /**
     * Method to create a dataset for acquisition and replication 
     * @param baseDataSet
     * @throws Exception 
     */
    public void createDataSetAndActivate( int testScenarioNo) throws Exception {
        String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/" + "DSDPartitionDataSet.xml");
        String tempdataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.dataSetName, dataSetConfigFile);
        String feedName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Parameters" , "value");
        this.sourceName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Sources" , "name");

        StringBuffer newDataSetXml = new StringBuffer(tempdataSetXml);

        // insert custom path for first target ( acquisition ) 
        int firstIndexValue =  newDataSetXml.indexOf("</DateRange>") + "</DateRange>".length();
        newDataSetXml.insert(firstIndexValue, customPathString);

        // insert custom path for second target ( replication ) 
        int secondIndexValue = newDataSetXml.lastIndexOf("</DateRange>") + "</DateRange>".length();
        newDataSetXml.insert(secondIndexValue, customPathString);

        String dataSetXml = newDataSetXml.toString();
        dataSetXml = dataSetXml.replaceAll("FEED_NAME", feedName );
        dataSetXml = dataSetXml.replaceAll("FEED_STATS", feedName + "_stats" );
        dataSetXml = dataSetXml.replaceAll("FDI_SERVER_NAME", this.sourceName );
        dataSetXml = dataSetXml.replaceAll("TARGET1", this.targetGrid1 );
        dataSetXml = dataSetXml.replaceAll("TARGET2", this.targetGrid2 );
        dataSetXml = dataSetXml.replaceAll("HCAT_ENABLED", this.HCAT_ENABLED );
        dataSetXml = dataSetXml.replaceAll("CUSTOM_DATA_PATH", this.getCustomPath("data" , this.dataSetName ,testScenarioNo ) );
        dataSetXml = dataSetXml.replaceAll("CUSTOM_COUNT_PATH",  this.getCustomPath("count" , this.dataSetName ,testScenarioNo) );
        dataSetXml = dataSetXml.replaceAll("CUSTOM_SCHEMA_PATH",  this.getCustomPath("schema" , this.dataSetName ,testScenarioNo) );
        hadooptest.cluster.gdm.Response response = this.consoleHandle.createDataSet(this.dataSetName, dataSetXml);
        if (response.getStatusCode() != SUCCESS) {
            try {
                throw new Exception("Response status code is " + response.getStatusCode() + ", expected 200.");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // wait for some time so that specification files are created
        this.consoleHandle.sleep(40000);

        // activate the dataset
        try {
            this.consoleHandle.checkAndActivateDataSet(this.dataSetName);
            this.datasetActivationTime = GdmUtils.getCalendarAsString();
        } catch (Exception e) { 
            e.printStackTrace();
        }

        this.consoleHandle.sleep(4000);
    }


    /**
     * Method to create a custom path without date in path.
     * @param pathType - its a string type which represents either data/count/schema
     * @param dataSet - dataset name
     * @return
     */
    private String getCustomPath(String pathType , String dataSet  , int testScenarioNo) {
        if (testScenarioNo == 1) {
            return  "/data/daqdev/"+ pathType +"/"+ dataSet + "/%{srcid}/%{date}" + EXTRA_PATH_AFTER_DATE; 
        } else if (testScenarioNo == 2)  {
            return  "/data/daqdev/"+ pathType +"/"+ dataSet + "/%{srcid}/"+ EXTRA_PATH_BEFORE_DATE  + "%{date}";
        }  else if(testScenarioNo == 3)  {
            return  "/data/daqdev/"+ pathType +"/"+ dataSet + "/%{srcid}"+ EXTARA_PATH_AFTER_DSD_SRC + "/%{date}";
        } else {
            return  "/data/daqdev/"+ pathType +"/"+ dataSet + "/" + EXTARA_PATH_BEFORE_DSD_SRC + "%{srcid}/%{date}";
        }
    }

    /**
     * deactivate the dataset(s)    
     */
    @After
    public void tearDown() {
        TestSession.logger.info("Deactivate "+ this.dataSetName  +"  dataset ");
        Response response = this.consoleHandle.deactivateDataSet(this.dataSetName);
        assertTrue("Failed to deactivate dataset " + dataSetName , response.getStatusCode() == SUCCESS);
        assertTrue("Expected terminate action name but got " + response.getElementAtPath("/Response/ActionName").toString() , response.getElementAtPath("/Response/ActionName").toString().equals("terminate") );
        assertTrue("Expected to get 0 response id, but got " + response.getElementAtPath("/Response/ResponseId").toString() , response.getElementAtPath("/Response/ResponseId").toString().equals("0"));
    }

}
