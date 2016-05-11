// Copyright 2016, Yahoo Inc.
package hadooptest.gdm.regression;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.cluster.gdm.GeneratePerformanceFeeds;
import hadooptest.cluster.gdm.HTTPHandle;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.XMLConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * 
 * TestCase : Verify whether facet workflow ( acquisition and replication ) are completed, when targets StartType is offset and EndType is offset. 
 *
 */
public class TestDyamicStartTypeOffSetAndEndTypeOffSet extends TestSession {

    private ConsoleHandle consoleHandle;
    private Configuration conf;
    private String dataSetName;
    private String datasetActivationTime;
    private String deploymentSuffixName;
    private WorkFlowHelper workFlowHelperObj = null;
    private GeneratePerformanceFeeds generatePerformanceFeeds  = null;
    private String targetGrid1;
    private String targetGrid2;
    private String cookie;
    private static final String baseDataSetName = "VerifyAcqRepRetWorkFlowExecutionSingleDate";
    private static final int SUCCESS = 200;
    final static Charset ENCODING = StandardCharsets.UTF_8;
    private static final String HCAT_TYPE = "DataOnly";
    private String START_DATE; 
    private String INSTANCE_DATE ;
    private List<String> instanceList;
    private static final String START_DATE_OFFSET_VALUE = "50";
    private static final String END_DATE_OFFSET_VALUE= "10";

    @BeforeClass
    public static void startTestSession() throws Exception {
        TestSession.start();
    }

    @Before
    public void setUp() throws NumberFormatException, Exception {
        String configPath = Util.getResourceFullPath("gdm/conf/config.xml");
        this.conf = new XMLConfiguration(configPath);
        this.deploymentSuffixName = this.conf.getString("hostconfig.console.deployment-suffix-name");
        TestSession.logger.info("Suffix name = " + this.deploymentSuffixName);
        this.consoleHandle = new ConsoleHandle();
        HTTPHandle httpHandle = new HTTPHandle(); 
        this.cookie = httpHandle.getBouncerCookie();
        this.dataSetName = "TestDyamicStartTypeOffSetAndEndTypeOffSet_" + System.currentTimeMillis();
        this.generatePerformanceFeeds  = new GeneratePerformanceFeeds(this.dataSetName , this.deploymentSuffixName.trim());

        this.workFlowHelperObj = new WorkFlowHelper();

        List<String> targetGrids = this.consoleHandle.getUniqueGrids();
        if (targetGrids != null  && targetGrids.size() >= 2) {
            targetGrid1 = targetGrids.get(0);
            targetGrid2 = targetGrids.get(1);
        } else {
            assertTrue("There is no enough grid installed to test this testcase." , true);
        }
        instanceList = new ArrayList<String>();
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        TestSession.logger.info(sdf.format(cal.getTime()));
        this.START_DATE = sdf.format(cal.getTime());

        cal.add(Calendar.DAY_OF_MONTH, -Integer.parseInt(START_DATE_OFFSET_VALUE));
        this.INSTANCE_DATE = sdf.format(cal.getTime());
        instanceList.add(this.INSTANCE_DATE);
    }

    @Test
    public void testWorkFlowWithOutEndDate() throws Exception {

        TestSession.logger.info("start date =  " + this.START_DATE + "     instance date = " + this.INSTANCE_DATE   + "    End date offset value = " + this.END_DATE_OFFSET_VALUE);

        // generate data
        generatePerformanceFeeds.generateFeed(this.INSTANCE_DATE);

        // create a dataset
        createDataSetForAcqRep( );
        
        // check for acquisition workflow
        this.workFlowHelperObj.checkWorkFlow(this.dataSetName, "acquisition", this.datasetActivationTime);

        //check for replication workflow
        this.workFlowHelperObj.checkWorkFlow(this.dataSetName, "replication", this.datasetActivationTime);
    }

    /**
     * Create a dataset & activate it.
     * @throws Exception
     */
    public void createDataSetForAcqRep( ) throws Exception {
        String sourceName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Sources" , "name");
        String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/" + "DynamicStartEndDate.xml");
        String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.dataSetName, dataSetConfigFile);
        TestSession.logger.info("**** DataSet Name = " + this.dataSetName   + " ********** ");
        dataSetXml = dataSetXml.replaceAll("FEED_NAME", this.dataSetName );
        dataSetXml = dataSetXml.replaceAll("FEED_STATS", this.dataSetName + "_stats" );
        dataSetXml = dataSetXml.replaceAll("SOURCE_NAME", sourceName );
        dataSetXml = dataSetXml.replaceAll("TARGET1", this.targetGrid1 );
        dataSetXml = dataSetXml.replaceAll("TARGET2", this.targetGrid2 );
        dataSetXml = dataSetXml.replaceAll("TARGET_ONE_TYPE_START", "offset" );
        dataSetXml = dataSetXml.replaceAll("TARGET_ONE_START_DATE", this.START_DATE_OFFSET_VALUE );
        dataSetXml = dataSetXml.replaceAll("TARGET_ONE_TYPE_END", "offset" );
        dataSetXml = dataSetXml.replaceAll("TARGET_ONE_END_DATE", "0" );
        dataSetXml = dataSetXml.replaceAll("TARGET_TWO_TYPE_START", "offset" );
        dataSetXml = dataSetXml.replaceAll("TARGET_TWO_START_DATE", this.START_DATE_OFFSET_VALUE );
        dataSetXml = dataSetXml.replaceAll("TARGET_TWO_TYPE_END", "offset" );
        dataSetXml = dataSetXml.replaceAll("TARGET_TWO_END_DATE", "0");
        dataSetXml = dataSetXml.replaceAll("TABLE_NAME", this.dataSetName);
        dataSetXml = dataSetXml.replaceAll("GROUP_NAME", "users");
        dataSetXml = dataSetXml.replaceAll("DATA_OWNER", "");

        TestSession.logger.info("**** DataSet Name = " + dataSetXml   + " ********** ");
        Response response = this.consoleHandle.createDataSet(this.dataSetName, dataSetXml);
        if (response.getStatusCode() != SUCCESS) {
            try {
                throw new Exception("Response status code is " + response.getStatusCode() + ", expected 200.");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        this.consoleHandle.sleep(10000);

        // activate the dataset
        response = this.consoleHandle.activateDataSet(dataSetName);
        assertTrue("Failed to activate dataset " + dataSetName , response.getStatusCode() == SUCCESS);
        this.consoleHandle.sleep(30000);
        this.datasetActivationTime = GdmUtils.getCalendarAsString();
    }

    /**
     * returns dataset path of the dataset
     * @param dataSetName
     * @return
     */
    private String getDataSetDataPath(String dataSetName) {
        String path = "/data/daqqe/data/" + dataSetName  + "/%{date}"; 
        TestSession.logger.info("paht == " + path);
        return  path;
    }

    /**
     * returns  count path of the dataset
     * @param dataSetName
     * @return
     */
    private String getDataSetCountPath(String dataSetName) {
        return "/data/daqqe/count/" + dataSetName  + "/%{date}" ;
    }

    /**
     * returns  schema path of the datase
     * @param dataSetName
     * @return
     */
    private String getDataSetSchemaPath(String dataSetName) {
        return "/data/daqqe/schema/" + dataSetName  + "/%{date}" ;
    }   

    /**
     * Method to deactivate the dataset(s)
     */
    @After
    public void tearDown() throws Exception {
        Response response = consoleHandle.deactivateDataSet(this.dataSetName);
        assertEquals("ResponseCode - Deactivate DataSet", 200, response.getStatusCode());
        assertEquals("ActionName.", "terminate", response.getElementAtPath("/Response/ActionName").toString());
        assertEquals("ResponseId", "0", response.getElementAtPath("/Response/ResponseId").toString());
        assertEquals("ResponseMessage.", "Operation on " + this.dataSetName + " was successful.", response.getElementAtPath("/Response/ResponseMessage/[0]").toString());
    }
}
