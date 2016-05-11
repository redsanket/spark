// Copyright 2016, Yahoo Inc.
package hadooptest.gdm.regression.nonAdmin;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.cluster.gdm.HTTPHandle;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;

import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.XMLConfiguration;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
 
/**
 * Test Scenario  : Verify whether non admin user is able to create replication dataset using REST API
 * 
 */
public class TestNonAdminAbleToCreateReplicationDataSet extends TestSession {

    private ConsoleHandle consoleHandle;
    private Configuration conf;
    private String targetGrid1;
    private String cookie;
    private String repDataSetName;
    private String baseDataSetName = "VerifyAcqRepRetWorkFlowExecutionSingleDate";
    private WorkFlowHelper workFlowHelper;
    private List<String> gridList;
    private static final String HCAT_TYPE = "Mixed";
    private static final int SUCCESS = 200;
    private static final int FAILURE = 500;
    private static final String SOURCE_NAME = "elrond";
    private final static String ABF_DATA_PATH = "/data/SOURCE_ABF/ABF_DAILY/";
    private static final String OPS_DB_GROUP = "ygrid_group_gdmtest";
    private static final String DATA_OWNER = "hitusr_2";
    private static final String GROUP_NAME = "users";

    @BeforeClass
    public static void startTestSession() throws Exception {
        TestSession.start();
    }

    @Before
    public void setUp() throws NumberFormatException, Exception {
        String configPath = Util.getResourceFullPath("gdm/conf/config.xml");
        this.conf = new XMLConfiguration(configPath);
        String nonAdminUserName = this.conf.getString("auth.nonAdminUser");
        String nonAdminPassWord = this.conf.getString("auth.nonAdminPassWord");
        
        this.consoleHandle =  new ConsoleHandle(nonAdminUserName , nonAdminPassWord);
        this.workFlowHelper = new WorkFlowHelper();
        HTTPHandle httpHandle = new HTTPHandle(nonAdminUserName , nonAdminPassWord);
        this.cookie = httpHandle.getBouncerCookie();
        
        TestSession.logger.info("cookie - " + this.cookie);
        
        // check source exist 
        this.gridList = this.consoleHandle.getUniqueGrids();
        if (! this.gridList.contains(SOURCE_NAME)) {
            TestSession.logger.info("Data Source " + SOURCE_NAME + " does not exists");
            fail("Data Source " + SOURCE_NAME + " does not exists");
        }
        
        if ( !(this.gridList.size() > 0 ) ) {
            fail("Atleast one target should exists.");
        }

        
        // check target is not equal to source
        for (String grid : this.gridList)  {
            
            if (! grid.contains(SOURCE_NAME)) {
                this.targetGrid1 = grid;
                break;
            }
        }
        
    }
    
    @Test
    public void test() {        
        this.repDataSetName = "Test_NonAdmin_Replication_DataSet_Creation_" + System.currentTimeMillis();
        this.createDoAsReplicationDataSet("SelfServeReplicationOnlyDataSet.xml");
        
        this.repDataSetName = "Test_NonAdmin_Replication_DataSet_Creation_FDI_" + System.currentTimeMillis();
        this.createReplicationDataSetWithDiscoveryTypeFDI("SelfServeReplicationOnlyDataSet.xml");
    }
    
    /**
     * Test Scenario  : Verify whether non admin user is able to create replication dataset using REST API
     *
     */
    private void createDoAsReplicationDataSet(String dataSetFileName) {
        String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/" + dataSetFileName);
        String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.repDataSetName, dataSetConfigFile);
        String feedName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Parameters" , "value");
        
        dataSetXml = dataSetXml.replaceAll("TARGET1", this.targetGrid1 );
        dataSetXml = dataSetXml.replaceAll("DISCOVERY_TYPE", "HDFS" );
        dataSetXml = dataSetXml.replaceAll("GROUP_NAME", this.OPS_DB_GROUP);
        dataSetXml = dataSetXml.replaceAll("DATA_OWNER", this.DATA_OWNER);
        dataSetXml = dataSetXml.replaceAll("OPS_DB_GROUP", this.OPS_DB_GROUP);
        dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", this.repDataSetName);
        dataSetXml = dataSetXml.replaceAll("FEED_NAME", feedName );
        dataSetXml = dataSetXml.replaceAll("FEED_STATS", feedName + "_stats" );
        dataSetXml = dataSetXml.replaceAll("ACQUISITION_SOURCE_NAME", this.SOURCE_NAME );
        dataSetXml = dataSetXml.replace("HCAT_TYPE", this.HCAT_TYPE);
        dataSetXml = dataSetXml.replaceAll("HCAT_TABLE_NAME", this.repDataSetName);
        dataSetXml = dataSetXml.replaceAll("REPLICATION_SOURCE_DATA_PATH", ABF_DATA_PATH );
        dataSetXml = dataSetXml.replaceAll("REPLICATION_SOURCE_COUNT_PATH", ABF_DATA_PATH );
        dataSetXml = dataSetXml.replaceAll("REPLICATION_SOURCE_SCHEMA_PATH", ABF_DATA_PATH);
        dataSetXml = dataSetXml.replaceAll("CUSTOM_DATA_PATH", getCustomPath("data" , this.repDataSetName));
        dataSetXml = dataSetXml.replaceAll("CUSTOM_COUNT_PATH", getCustomPath("count", this.repDataSetName));
        dataSetXml = dataSetXml.replaceAll("CUSTOM_SCHEMA_PATH", getCustomPath("schema", this.repDataSetName));
        dataSetXml = dataSetXml.replaceAll("HCAT_TABLE_NAME", this.repDataSetName);
        Response response = this.consoleHandle.createDataSet(this.repDataSetName, dataSetXml);
        if (response.getStatusCode() != SUCCESS) {
            try {
                throw new Exception("Response status code is " + response.getStatusCode() + ", expected 200.");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        this.consoleHandle.sleep(5000);
    }
    
    /**
     * Test Scenario : Verify whether non-admin user is not able to create a replication only dataset when discovery type is non equal to HDFS and HCAT. 
     */
    private void createReplicationDataSetWithDiscoveryTypeFDI(String dataSetFileName) {
        String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/" + dataSetFileName);
        String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.repDataSetName, dataSetConfigFile);
        String feedName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Parameters" , "value");
        
        dataSetXml = dataSetXml.replaceAll("TARGET1", this.targetGrid1 );
        dataSetXml = dataSetXml.replaceAll("DISCOVERY_TYPE", "FDI" );
        dataSetXml = dataSetXml.replaceAll("GROUP_NAME", this.OPS_DB_GROUP);
        dataSetXml = dataSetXml.replaceAll("DATA_OWNER", this.DATA_OWNER);
        dataSetXml = dataSetXml.replaceAll("OPS_DB_GROUP", this.OPS_DB_GROUP);
        dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", this.repDataSetName);
        dataSetXml = dataSetXml.replaceAll("FEED_NAME", feedName );
        dataSetXml = dataSetXml.replaceAll("FEED_STATS", feedName + "_stats" );
        dataSetXml = dataSetXml.replaceAll("ACQUISITION_SOURCE_NAME", this.SOURCE_NAME );
        dataSetXml = dataSetXml.replace("HCAT_TYPE", this.HCAT_TYPE);
        dataSetXml = dataSetXml.replaceAll("HCAT_TABLE_NAME", this.repDataSetName);
        dataSetXml = dataSetXml.replaceAll("REPLICATION_SOURCE_DATA_PATH", ABF_DATA_PATH );
        dataSetXml = dataSetXml.replaceAll("REPLICATION_SOURCE_COUNT_PATH", ABF_DATA_PATH );
        dataSetXml = dataSetXml.replaceAll("REPLICATION_SOURCE_SCHEMA_PATH", ABF_DATA_PATH);
        dataSetXml = dataSetXml.replaceAll("CUSTOM_DATA_PATH", getCustomPath("data" , this.repDataSetName));
        dataSetXml = dataSetXml.replaceAll("CUSTOM_COUNT_PATH", getCustomPath("count", this.repDataSetName));
        dataSetXml = dataSetXml.replaceAll("CUSTOM_SCHEMA_PATH", getCustomPath("schema", this.repDataSetName));
        dataSetXml = dataSetXml.replaceAll("HCAT_TABLE_NAME", this.repDataSetName);
        Response response = this.consoleHandle.createDataSet(this.repDataSetName, dataSetXml);
        TestSession.logger.info("response code = " + response.getStatusCode());
        assertTrue("Non-Admin user should not be able to create replication only dataset when discovery type is not equal to HDFS and HCAT." , response.getStatusCode() != SUCCESS );
    }
    
    /**
     * Method to create a custom path for the dataset.
     * @param pathType - its a string type which represents either data/count/schema
     * @param dataSet - dataset name
     * @return
     */
    private String getCustomPath(String pathType , String dataSet) {
        return  "/data/daqdev/"+ pathType +"/"+ dataSet + "/%{date}";
    }

}
