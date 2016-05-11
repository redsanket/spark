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
import java.util.Calendar;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.XMLConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * Test Scenario : Verify whether generating instance for the future date ( today's date plus two days ) and specifying the current date ( today date) for the start date
 * and end date as blank or not specifying the end date, d't generate the workflow.
 * 
 * Since start date is today date and GDM can't do future discovery so the end date is set to current date i,e in this case both start and end date are same and instance date 
 * is future, workflow will not start, since date filter will filter outs instance. 
 * 
 *
 */
public class TestWorkFlowWithOutEndDate extends TestSession {

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
    private String START_DATE;  // needed for end date
    private String INSTANCE_DATE ;

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
        this.dataSetName = "TestWorkFlowForMissingEndDate_" + System.currentTimeMillis();
        this.generatePerformanceFeeds  = new GeneratePerformanceFeeds(this.dataSetName , this.deploymentSuffixName.trim());

        workFlowHelperObj = new WorkFlowHelper();

        List<String> targetGrids = this.consoleHandle.getUniqueGrids();
        if (targetGrids != null  && targetGrids.size() >= 2) {
            targetGrid1 = targetGrids.get(0);
            targetGrid2 = targetGrids.get(1);
        } else {
            assertTrue("There is no enough grid installed to test this testcase." , true);
        }
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        TestSession.logger.info(sdf.format(cal.getTime()));
        this.START_DATE = sdf.format(cal.getTime());
        cal.add(Calendar.DAY_OF_MONTH, 2);
        this.INSTANCE_DATE = sdf.format(cal.getTime());
    }

    @Test
    public void testWorkFlowWithOutEndDate() throws Exception {

        // generate data
        generatePerformanceFeeds.generateFeed(this.INSTANCE_DATE);
        
        // create a dataset
        createDataSetForAcqRep( );
        
        // check for dataset should come to running  workflow
        boolean isWorkFlowReachedRunningState = workFlowHelperObj.checkWhetherDataSetReachedRunningState(this.dataSetName, "acquisition");
        assertTrue("Since the start date is today's date and GDM can't do future instance discovery, dataset should not come to running state" , isWorkFlowReachedRunningState == false); 
    }
    
    /**
     * Create a dataset & activate it.
     * @throws Exception
     */
    public void createDataSetForAcqRep( ) throws Exception {
        String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/" + "GDMPerformanceDataSetWithoutPartition.xml");
        String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.dataSetName, dataSetConfigFile);
        String sourceName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Sources" , "name");
        TestSession.logger.info("**** DataSet Name = " + this.dataSetName   + " ********** ");
        dataSetXml = dataSetXml.replaceAll("FEED_NAME", this.dataSetName );
        dataSetXml = dataSetXml.replaceAll("FEED_STATS", this.dataSetName + "_stats" );
        dataSetXml = dataSetXml.replaceAll("FDI_SERVER_NAME", sourceName );
        dataSetXml = dataSetXml.replaceAll("TARGET1", this.targetGrid1 );
        dataSetXml = dataSetXml.replaceAll("TARGET2", this.targetGrid2 );
        dataSetXml = dataSetXml.replaceAll("START_DATE", this.START_DATE );
        dataSetXml = dataSetXml.replaceAll("end=\"END_DATE\"", "" );
        dataSetXml = dataSetXml.replaceAll("DATA_PATH", this.getDataSetDataPath(this.dataSetName ));
        dataSetXml = dataSetXml.replaceAll("COUNT_PATH", this.getDataSetCountPath(this.dataSetName ));
        dataSetXml = dataSetXml.replaceAll("SCHEMA_PATH", this.getDataSetSchemaPath(this.dataSetName ));
        dataSetXml = dataSetXml.replaceAll("HCAT_TYPE", this.HCAT_TYPE);
        dataSetXml = dataSetXml.replaceAll("hourly", "daily");
        dataSetXml = dataSetXml.replaceAll("yyyyMMddhh", "yyyyMMdd");

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
