package hadooptest.gdm.regression;

import java.util.ArrayList;
import java.util.List;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.SingleDataSetInstanceAcquirer;

import org.junit.BeforeClass;

import coretest.SerialTests;
import coretest.Util;

import hadooptest.TestSession;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Category(SerialTests.class)
public class SwitchoverTest extends TestSession {
    private ConsoleHandle console;
    private static String dataSetConfigBase = "/home/y/conf/gdm_qe_test/datasetconfigs/";
    private String dataSetName;
    private String grid1 = "gdm-target-denseb-patw";
    private String grid2 = "omegap1";
    private String grid3 = "grima";
    private List<SingleDataSetInstanceAcquirer> acquisitions = new ArrayList<SingleDataSetInstanceAcquirer>();

	@BeforeClass
	public static void startTestSession() {
		TestSession.start();
	}
    
    private class SwitchoverSource {
        String sourceName;
        String dateStart;
        String dateEnd;
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
    
    @Before
    public void setup() throws Exception {
        
        this.console = new ConsoleHandle();
        
        // initialize dataset name so we can acquire data to grids to the right directory immediately
        this.dataSetName = "Switchover01_" + System.currentTimeMillis();
        
        // start fetching a dataset instance 20120125 on grid1
        this.startDataSetInstanceAcquisition(this.grid1, "20120125");
        
        // start fetching a dataset instance 20120126 on grid2
        this.startDataSetInstanceAcquisition(this.grid2, "20120126");

		this.dataSetConfigBase = Util.getResourceFullPath("gdm/datasetconfigs") + "/";
    }
    
    private void startDataSetInstanceAcquisition(String grid, String instanceDate) throws Exception {
        SingleDataSetInstanceAcquirer gridDataAcquirer = new SingleDataSetInstanceAcquirer(grid, instanceDate, this.getDataPath(), this.getSchemaPath(), this.getCountPath(), 
            "gdm-dataset-patw02", false);
        gridDataAcquirer.startAcquisition();    
        acquisitions.add(gridDataAcquirer);
    }
    
    private void finishAcquisitions() throws Exception {
        for (SingleDataSetInstanceAcquirer acquisition : acquisitions) {
            acquisition.finishAcquisition();
        }
        acquisitions.clear();
    }
    
    private String createDataSetXml(SwitchoverSource source1, SwitchoverSource source2) {
        String dataSetXml = this.console.createDataSetXmlFromConfig(this.dataSetName, dataSetConfigBase + "SwitchoverDataSet.xml");
        dataSetXml = dataSetXml.replaceAll("SOURCE1_NAME", source1.sourceName);
        dataSetXml = dataSetXml.replaceAll("SOURCE2_NAME", source2.sourceName);
        if (source1.dateStart != null) {
            dataSetXml = dataSetXml.replaceAll("SOURCE1_DATERANGE", "<DateRange end=\"" + source1.dateEnd + "\" start=\"" + source1.dateStart + "\"/>");
        } else {
            dataSetXml = dataSetXml.replaceAll("SOURCE1_DATERANGE", "");
        }
        if (source2.dateStart != null) {
            dataSetXml = dataSetXml.replaceAll("SOURCE2_DATERANGE", "<DateRange end=\"" + source2.dateEnd + "\" start=\"" + source2.dateStart + "\"/>");
        } else {
            dataSetXml = dataSetXml.replaceAll("SOURCE2_DATERANGE", "");
        }
        dataSetXml = dataSetXml.replaceAll("TARGET1", this.grid2);
        dataSetXml = dataSetXml.replaceAll("TARGET2", this.grid3);
        dataSetXml = dataSetXml.replaceAll("DATA_PATH", this.getDataPath());
        dataSetXml = dataSetXml.replaceAll("SCHEMA_PATH", this.getSchemaPath());
        dataSetXml = dataSetXml.replaceAll("COUNT_PATH", this.getCountPath());

        return dataSetXml;
    }
    
    @Test
    public void testSwitchover() throws Exception {
        Response response;
        
        // setup a switchover dataSet with grid1 active, grid2 standby, replicating to grid2 and grid3
        {
            SwitchoverSource source1 = new SwitchoverSource();
            source1.sourceName = this.grid1;
            source1.dateStart = "20120125";
            source1.dateEnd = "20130125";
            SwitchoverSource source2 = new SwitchoverSource();
            source2.sourceName = this.grid2;
            String dataSetXml = this.createDataSetXml(source1, source2);
            response = this.console.createDataSet(this.dataSetName, dataSetXml);
            assertEquals("create switchover dataset", 200, response.getStatusCode());
        }
        this.console.checkAndActivateDataSet(this.dataSetName);
        String feedSubmisionTime = GdmUtils.getCalendarAsString();
        
        // make sure all the acquisitions we setup are done
        finishAcquisitions();

        // validate dataset instance 20120125 gets on grid2 and grid3
        long waitTimeForWorkflowPolling = 15 * 60 * 1000; // 15 minutes
        String workflowStatus = this.console.waitForWorkflowExecution(this.dataSetName, feedSubmisionTime, waitTimeForWorkflowPolling);
        assertEquals("Replication of 20120125 to grid1 completion", "COMPLETED", workflowStatus);
        response = this.console.getCompletedJobsForDataSet(feedSubmisionTime, GdmUtils.getCalendarAsString(), this.dataSetName);
        
        List<String> targets = new ArrayList<String>();
        targets.add(this.grid2);
        targets.add(this.grid3);
        boolean done = this.console.isWorkflowCompleted(response, "replication", targets, "20120125");
        if (done == true) {
            TestSession.logger.info("Finished repl of 20120125 from " + this.grid1 + " to " + this.grid2 + " and " + this.grid3 + ".  Response: " + response);
        }                
        else {
            fail("initial replication Workflow Failed");
        }

        // perform switchover from grid1 to grid2, starting with dataSetInstance 20120126
        {
            SwitchoverSource source1 = new SwitchoverSource();
            source1.sourceName = this.grid1;
            source1.dateStart = "20120125";
            source1.dateEnd = "20120125";
            SwitchoverSource source2 = new SwitchoverSource();
            source2.sourceName = this.grid2;
            source2.dateStart = "20120126";
            source2.dateEnd = "20130125";
            String dataSetXml = this.createDataSetXml(source1, source2);
            
            // save the next feed submission time
            feedSubmisionTime = GdmUtils.getCalendarAsString();
            
            response = this.console.modifyDataSet(this.dataSetName, dataSetXml);
            assertEquals("modify dataset for switchover", 200, response.getStatusCode());
        }
        
        this.console.checkAndActivateDataSet(this.dataSetName);        

        // validate dataset instance 20120126 gets on grid3
        workflowStatus = this.console.waitForWorkflowExecution(this.dataSetName, feedSubmisionTime, waitTimeForWorkflowPolling);
        assertEquals("Replication of 20120126", "COMPLETED", workflowStatus);
        response = this.console.getCompletedJobsForDataSet(feedSubmisionTime, GdmUtils.getCalendarAsString(), this.dataSetName);
        
        targets.clear();
        targets.add(this.grid3);
        
        done = this.console.isWorkflowCompleted(response, "replication", targets, "20120126");
        if (done == true) {
            TestSession.logger.info("Finished repl of 20120126 from " + this.grid1 + " to " + this.grid2 + " and " + this.grid3 + ".  Response: " + response);
        }                
        else {
            fail("replication Workflow failed");
        }

        // make dataset inactive
        response = this.console.deactivateDataSet(this.dataSetName);
        assertEquals("ResponseCode - Deactivate DataSet", 200, response.getStatusCode());
        assertEquals("ActionName.", "terminate", response.getElementAtPath("/Response/ActionName").toString());
        assertEquals("ResponseId", "0", response.getElementAtPath("/Response/ResponseId").toString());
        assertEquals("ResponseMessage.", "Operation on " + dataSetName + " was successful.", response.getElementAtPath("/Response/ResponseMessage/[0]").toString());
    }
}
