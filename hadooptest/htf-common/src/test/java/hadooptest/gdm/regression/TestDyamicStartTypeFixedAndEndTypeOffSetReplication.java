// Copyright 2016, Yahoo Inc.
package hadooptest.gdm.regression;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;

public class TestDyamicStartTypeFixedAndEndTypeOffSetReplication extends TestSession {
    private ConsoleHandle consoleHandle;
    private WorkFlowHelper workFlowHelperObject;
    private String START_DATE; 
    private String INSTANCE_DATE ;
    private String dataSetName;
    private String targetGrid1;
    private String targetGrid2;
    private String basePath;
    private String datasetActivationTime;
    private static final String START_DATE_OFFSET_VALUE = "50";
    private static final String END_DATE_OFFSET_VALUE= "10";
    private static final String baseDataSetName = "VerifyAcqRepRetWorkFlowExecutionSingleDate";
    private static final String HCAT_TYPE = "DataOnly";
    private static final int SUCCESS = 200;

    @BeforeClass
    public static void startTestSession() throws Exception {
        TestSession.start();
    }

    @Before
    public void setUp() throws NumberFormatException, Exception {
        consoleHandle = new ConsoleHandle();
        workFlowHelperObject = new WorkFlowHelper();
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

        this.dataSetName =  "TestDyamicStartTypeFixedAndEndTypeOffSetRepl_" + System.currentTimeMillis();

        // instance date
        cal.add(Calendar.DAY_OF_MONTH, -Integer.parseInt(START_DATE_OFFSET_VALUE));
        this.INSTANCE_DATE = sdf.format(cal.getTime());

        // start date is before the actual instance date.
        cal.add(Calendar.DAY_OF_MONTH, -10);
        this.START_DATE = sdf.format(cal.getTime());

        List<String> targetGrids = this.consoleHandle.getUniqueGrids();
        if (targetGrids != null  && targetGrids.size() >= 2) {
            targetGrid1 = targetGrids.get(0);
            targetGrid2 = targetGrids.get(1);
        } else {
            assertTrue("There is no enough grid installed to test this testcase." , true);
        }
        
        this.basePath = "/data/daqdev/data/" + this.dataSetName + "/%{date}";
        TestSession.logger.info("Start date = " + this.START_DATE  + "      instanceDate = " + this.INSTANCE_DATE);
    }

    @Test
    public void test() throws IOException, InterruptedException {

        //generate instance on grid
        CreateInstanceOnGrid createInstanceOnGridObj = new CreateInstanceOnGrid(this.targetGrid1 , "/data/daqdev" , "data/" + this.dataSetName , this.INSTANCE_DATE);
        createInstanceOnGridObj.execute();

        // create a dataset
        createDoAsReplicationDataSet();

        //check for replication workflow
        this.workFlowHelperObject.checkWorkFlow(this.dataSetName, "replication", this.datasetActivationTime);
    }

    /**
     * Method that creates a replication dataset
     * @param dataSetFileName - name of the replication dataset
     */
    private void createDoAsReplicationDataSet() {
        String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/OffSetAndFixedReplicationDataSet.xml");
        String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.dataSetName, dataSetConfigFile);
        dataSetXml = dataSetXml.replaceAll("TARGET", this.targetGrid2 );
        dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", this.dataSetName);
        dataSetXml = dataSetXml.replaceAll("FEED_NAME", this.dataSetName );
        dataSetXml = dataSetXml.replaceAll("FEED_STATS", this.dataSetName + "_stats" );
        dataSetXml = dataSetXml.replaceAll("SOURCE", this.targetGrid1 );
        dataSetXml = dataSetXml.replaceAll("START_TYPE", "fixed" );
        dataSetXml = dataSetXml.replaceAll("START_DATE", this.START_DATE );
        dataSetXml = dataSetXml.replaceAll("END_TYPE", "offset" );
        dataSetXml = dataSetXml.replaceAll("END_DATE", "10" );
        dataSetXml = dataSetXml.replaceAll("REPLICATION_DATA_PATH", this.basePath );
        dataSetXml = dataSetXml.replaceAll("REPLICATION_COUNT_PATH", this.basePath);
        dataSetXml = dataSetXml.replaceAll("REPLICATION_SCHEMA_PATH", this.basePath);
        dataSetXml = dataSetXml.replaceAll("CUSTOM_PATH_DATA", this.getCustomPath("data" , this.dataSetName));
        dataSetXml = dataSetXml.replaceAll("CUSTOM_PATH_COUNT", this.getCustomPath("count", this.dataSetName));
        dataSetXml = dataSetXml.replaceAll("CUSTOM_PATH_SCHEMA", this.getCustomPath("schema", this.dataSetName));
        Response response = this.consoleHandle.createDataSet(this.dataSetName, dataSetXml);
        if (response.getStatusCode() != SUCCESS) {
            try {
                throw new Exception("Response status code is " + response.getStatusCode() + ", expected 200.");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        this.consoleHandle.sleep(6000);

        // activate the dataset
        response = this.consoleHandle.activateDataSet(dataSetName);
        assertTrue("Failed to activate dataset " + dataSetName , response.getStatusCode() == SUCCESS);
        this.consoleHandle.sleep(30000);
        this.datasetActivationTime = GdmUtils.getCalendarAsString();
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

    /**
     *  Deactivate the dataset
     * @throws Exception
     */
    @After
    public void tearDown() throws Exception {
        Response response = this.consoleHandle.deactivateDataSet(this.dataSetName);
        assertEquals("ResponseCode - Deactivate DataSet", SUCCESS , response.getStatusCode());
        assertEquals("ActionName.", "terminate", response.getElementAtPath("/Response/ActionName").toString());
        assertEquals("ResponseId", "0", response.getElementAtPath("/Response/ResponseId").toString());
        assertEquals("ResponseMessage.", "Operation on " + this.dataSetName + " was successful.", response.getElementAtPath("/Response/ResponseMessage/[0]").toString());
    }

}
