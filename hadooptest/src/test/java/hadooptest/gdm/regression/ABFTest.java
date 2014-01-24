package hadooptest.gdm.regression;

import java.util.ArrayList;
import java.util.List;

import hadooptest.SerialTests;
import hadooptest.Util;

import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.cluster.gdm.Response;

import hadooptest.TestSession;

import org.junit.Before;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

/**
 * This test class validates the replication and retention of ABF data.  You can configure the source and target grid in config.xml.
 * It is assumed that the source ABF daily reference data is on the source grid.  If HCat is enabled on the target grid, it will 
 * publish data into HCat.  You can specify the table name in config.xml.  It is assumed the table exists.
 */
@Category(SerialTests.class)
public class ABFTest extends TestSession {
    private String dataSetConfigBase;
    private static ConsoleHandle console;
    private static String dataSetName;
    private String source1;
    private String target1;
    private String hcatTableName;
    
    @Before
    public void setup() throws Exception {

        this.console = new ConsoleHandle();
        
        // initialize dataset name so we can acquire data to grids to the right directory immediately
        this.dataSetName = "ABF01_" + System.currentTimeMillis();
        
        this.source1 = GdmUtils.getConfiguration("testconfig.ABFTest.source1");
        this.target1 = GdmUtils.getConfiguration("testconfig.ABFTest.target1");
        this.hcatTableName = GdmUtils.getConfiguration("testconfig.ABFTest.hcatTableName");

		this.dataSetConfigBase = Util.getResourceFullPath("gdm/datasetconfigs") + "/";
    }
    
    @AfterClass
    public static void tearDown() {
        // make dataset inactive
        Response response = console.deactivateDataSet(dataSetName);

        assertEquals("ResponseCode - Deactivate DataSet", 200, response.getStatusCode());
        assertEquals("ActionName.", "terminate", response.getElementAtPath("/Response/ActionName").toString());
        assertEquals("ResponseId", "0", response.getElementAtPath("/Response/ResponseId").toString());
        assertEquals("ResponseMessage.", "Operation on " + dataSetName + " was successful.", response.getElementAtPath("/Response/ResponseMessage/[0]").toString());
    }
    
    private String createDataSetXml() {
        String dataSetXml = this.console.createDataSetXmlFromConfig(this.dataSetName, dataSetConfigBase + "ABFDataSet.xml");
        dataSetXml = dataSetXml.replaceAll("SOURCE1_NAME", this.source1);
        dataSetXml = dataSetXml.replaceAll("TARGET1_NAME", this.target1);
        dataSetXml = dataSetXml.replaceAll("HCAT_TABLE_NAME", this.hcatTableName);
        return dataSetXml;
    }
    
    private void validateDataSetInstanceFinished(String feedSubmisionTime, String facet, String instance) {
        
        long waitTimeForWorkflowPolling = 15 * 60 * 1000; // 15 minutes
        List<String> targets = new ArrayList<String>();
        targets.add(this.target1);
        
        while (true) {
            // validate data is replicated
            String workflowStatus = this.console.waitForWorkflowExecution(this.dataSetName, feedSubmisionTime, waitTimeForWorkflowPolling);
            assertEquals("Replication of ABF data complete", "COMPLETED", workflowStatus);
            Response response = this.console.getCompletedJobsForDataSet(feedSubmisionTime, GdmUtils.getCalendarAsString(), this.dataSetName);
           
            boolean done = this.console.isWorkflowCompleted(response, facet, targets, instance);
            if (done == true) {
                TestSession.logger.info("Finished " + facet + " of " + instance + " on " + this.target1 + ".  Response: " + response);
                return;
            }        
            
            this.console.sleep(30000);
        }
    }
    
    @Test
    public void testABF() throws Exception {
        String dataSetXml = this.createDataSetXml();
        Response response = this.console.createDataSet(this.dataSetName, dataSetXml);
        assertEquals("create dataset", 200, response.getStatusCode());
        
        this.console.checkAndActivateDataSet(this.dataSetName);
        String feedSubmisionTime = GdmUtils.getCalendarAsString();
        
        this.validateDataSetInstanceFinished(feedSubmisionTime, "replication", "20130309");
        this.validateDataSetInstanceFinished(feedSubmisionTime, "replication", "20130320");
        this.validateDataSetInstanceFinished(feedSubmisionTime, "replication", "20130321");
        this.validateDataSetInstanceFinished(feedSubmisionTime, "replication", "20130322");
        this.validateDataSetInstanceFinished(feedSubmisionTime, "replication", "20130323");
        this.validateDataSetInstanceFinished(feedSubmisionTime, "replication", "20130324");
        this.validateDataSetInstanceFinished(feedSubmisionTime, "replication", "20130325");
        this.validateDataSetInstanceFinished(feedSubmisionTime, "replication", "20130326");
        this.validateDataSetInstanceFinished(feedSubmisionTime, "replication", "20130327");
        this.validateDataSetInstanceFinished(feedSubmisionTime, "replication", "20130328");
        this.validateDataSetInstanceFinished(feedSubmisionTime, "replication", "20130329");
        
        this.validateDataSetInstanceFinished(feedSubmisionTime, "retention", "20130309");
        this.validateDataSetInstanceFinished(feedSubmisionTime, "retention", "20130320");
        this.validateDataSetInstanceFinished(feedSubmisionTime, "retention", "20130321");
        this.validateDataSetInstanceFinished(feedSubmisionTime, "retention", "20130322");
        this.validateDataSetInstanceFinished(feedSubmisionTime, "retention", "20130323");
        this.validateDataSetInstanceFinished(feedSubmisionTime, "retention", "20130324");
    }
}

