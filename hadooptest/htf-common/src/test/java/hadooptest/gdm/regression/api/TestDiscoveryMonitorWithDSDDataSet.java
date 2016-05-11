// Copyright 2016, Yahoo Inc.
package hadooptest.gdm.regression.api;

import static org.junit.Assert.assertTrue;
import hadooptest.Util;
import hadooptest.TestSession;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;

import java.util.Iterator;
import java.util.List;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test Case : To Verify whether discovery monitor return the correct discovered value for DSD partition.
 *
 */
public class TestDiscoveryMonitorWithDSDDataSet extends TestSession {

    private ConsoleHandle consoleHandle;
    private String url;
    private String baseDataSetName = "VerifyAcqRepRetWorkFlowExecutionSingleDate";
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
    private static int SUCCESS = 200;

    @BeforeClass
    public static void startTestSession() throws Exception {
        TestSession.start();
    }

    @Before
    public void setUp() throws NumberFormatException, Exception {
        consoleHandle = new ConsoleHandle();
        this.dataSetName = "TestDiscoveryMonitoring_" +  System.currentTimeMillis();
        
        // Get all the grid with the current deployment and select first two grid as targets for datasest.  
        grids = this.consoleHandle.getUniqueGrids();
        if (grids.size() > 2) {
            this.targetGrid1 = grids.get(0);
            this.targetGrid2 = grids.get(1);

            TestSession.logger.info("target1 = " + this.targetGrid1); 
            TestSession.logger.info("target2 = " + this.targetGrid2);
        }
        this.workFlowHelper = new WorkFlowHelper();
    }

    @Test
    public void testDiscoveryMonitorWithDSDDataSet() throws Exception {

        // create a dataset
        createDataSetAndActivate( );
        
        // check whether discover is successfull.
        JSONArray jsonArray = this.workFlowHelper.isDiscoveryMonitoringStarted("acquisition", this.dataSetName);
        if (jsonArray.size() > 0) {
            Iterator iterator = jsonArray.iterator();
            while (iterator.hasNext()) {
                JSONObject jsonObject = (JSONObject) iterator.next();
                String dataSet = jsonObject.getString("Dataset");
                String sourceName = jsonObject.getString("Source");
                String partition = jsonObject.getString("Partition");
                String status = jsonObject.getString("Status");
                assertTrue("Failed to get the  " + dataSet  , jsonObject.getString("Dataset").equals(this.dataSetName));
                assertTrue("Failed to get the " + sourceName , sourceName.equals(this.sourceName));
                assertTrue("Failed to get the srcid & partition id " + partition  , partition.equals( this.PARTITION_NAME + "_" + this.PARTITION_ID_1)  || partition.equals( this.PARTITION_NAME + "_" + this.PARTITION_ID_2));
                assertTrue("Looks like there is some issue while discovery " + status  , status.equals("OK") );
            }
        }
        assertTrue("Failed to get discovery monitoring on acquisition for " + this.dataSetName , jsonArray.size() > 0);
    }

    /**
     * Method to create a dataset for acquisition and replication 
     * @param baseDataSet
     * @throws Exception 
     */
    public void createDataSetAndActivate( ) throws Exception {
        String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/" + "DSDPartitionDataSet.xml");
        String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.dataSetName, dataSetConfigFile);
        String feedName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Parameters" , "value");
        this.sourceName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Sources" , "name");
        dataSetXml = dataSetXml.replaceAll("FEED_NAME", feedName );
        dataSetXml = dataSetXml.replaceAll("FEED_STATS", feedName + "_stats" );
        dataSetXml = dataSetXml.replaceAll("FDI_SERVER_NAME", this.sourceName );
        dataSetXml = dataSetXml.replaceAll("TARGET1", this.targetGrid1 );
        dataSetXml = dataSetXml.replaceAll("TARGET2", this.targetGrid2 );
        dataSetXml = dataSetXml.replaceAll("HCAT_ENABLED", this.HCAT_ENABLED );

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
