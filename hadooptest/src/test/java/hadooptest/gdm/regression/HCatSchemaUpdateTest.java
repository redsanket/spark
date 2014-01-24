package hadooptest.gdm.regression;

import java.util.ArrayList;
import java.util.List;

import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.cluster.gdm.Response;

import hadooptest.TestSession;

import org.junit.Before;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import hadooptest.SerialTests;
import hadooptest.Util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Category(SerialTests.class)
public class HCatSchemaUpdateTest extends TestSession {
    private static String dataSetConfigBase;
    private static ConsoleHandle console;
    private static String dataSetName;
    private String grid1;
    
    @Before
    public void setup() throws Exception {
        this.console = new ConsoleHandle();
        this.dataSetName = "HCatSchemaUpdate01_" + System.currentTimeMillis();
        this.grid1 = GdmUtils.getConfiguration("testconfig.HCatSchemaUpdateTest.grid1");

		this.dataSetConfigBase = Util.getResourceFullPath("gdm/datasetconfigs") + "/";
    }
    
    @AfterClass
    public static void tearDown() throws Exception {
        // make dataset inactive
        Response response = console.deactivateDataSet(dataSetName);

        assertEquals("ResponseCode - Deactivate DataSet", 200, response.getStatusCode());
        assertEquals("ActionName.", "terminate", response.getElementAtPath("/Response/ActionName").toString());
        assertEquals("ResponseId", "0", response.getElementAtPath("/Response/ResponseId").toString());
        assertEquals("ResponseMessage.", "Operation on " + dataSetName + " was successful.", response.getElementAtPath("/Response/ResponseMessage/[0]").toString());
    }
    
    @Test
    public void testSchemaUpdates() throws Exception {
        TestSession.logger.info("Creating dataSet " + this.dataSetName);
        
        // create a dataset only up to 20120125.  This instance has version 1 of the HCat schema
        String dataSetXml = this.getDataSetXml("20120125");        
        Response response = this.console.createDataSet(dataSetName, dataSetXml);
        assertEquals("ResponseCode - createDataSet", 200, response.getStatusCode());
        
        this.console.checkAndActivateDataSet(dataSetName);
        String feedSubmisionTime = GdmUtils.getCalendarAsString();  
        
        // make sure 20120125 gets acquired
        validateDataAcquired("20120125", feedSubmisionTime);
        
        // modify the dataset to get 20120126.  This instance has version 2 of the HCat schema.  
        dataSetXml = this.getDataSetXml("20120126");    
        feedSubmisionTime = GdmUtils.getCalendarAsString();
        response = this.console.modifyDataSet(this.dataSetName, dataSetXml);
        assertEquals("modify dataset", 200, response.getStatusCode());
        this.console.checkAndActivateDataSet(this.dataSetName);      
        
        // make sure 20120126 gets acquired
        validateDataAcquired("20120126", feedSubmisionTime);
    }
    
    private void validateDataAcquired(String instance, String feedSubmisionTime) {
        long waitTimeForWorkflowPolling = 15 * 60 * 1000; // 15 minutes
        List<String> targets = new ArrayList<String>();
        targets.add(this.grid1);
        String workflowStatus = this.console.waitForWorkflowExecution(this.dataSetName, feedSubmisionTime, waitTimeForWorkflowPolling);
        assertEquals("Acquisition of data not complete", "COMPLETED", workflowStatus);
        Response response = this.console.getCompletedJobsForDataSet(feedSubmisionTime, GdmUtils.getCalendarAsString(), this.dataSetName);
       
        boolean done = this.console.isWorkflowCompleted(response, "acquisition", targets, instance);
        if (done == true) {
            TestSession.logger.info("Finished acquisition of " + instance + " on " + this.grid1 + ".  Response: " + response);
            return;
        }
        
        fail("failed to acquire data");
    }
    
    private String getDataSetXml(String endDate) {
        String dataSetXml = this.console.createDataSetXmlFromConfig(this.dataSetName, dataSetConfigBase + "FDI_1_Target.xml");
        dataSetXml = dataSetXml.replaceAll("GRID_TARGET", this.grid1);
        dataSetXml = dataSetXml.replaceAll("START_DATE", "20120125");
        dataSetXml = dataSetXml.replaceAll("END_DATE", endDate);
        dataSetXml = dataSetXml.replaceAll("DATA_PATH", this.getDataPath());
        dataSetXml = dataSetXml.replaceAll("SCHEMA_PATH", this.getSchemaPath());
        dataSetXml = dataSetXml.replaceAll("COUNT_PATH", this.getCountPath());
        dataSetXml = dataSetXml.replaceAll("FEED_NAME", "gdm-dataset-ult-schema-changes");
        dataSetXml = dataSetXml.replaceAll("HCAT_USAGE", "<HCatUsage>TRUE</HCatUsage>");
        return dataSetXml;
    }
    
    private String getDataPath() {
        return "/data/gdmqe/auto/data/" + this.dataSetName + "/%{date}";
    }
    
    private String getSchemaPath() {
        return "/data/gdmqe/auto/schema/" + this.dataSetName + "/%{date}";
    }
    
    private String getCountPath() {
        return "/data/gdmqe/auto/count/" + this.dataSetName + "/%{date}";
    }
    

}
