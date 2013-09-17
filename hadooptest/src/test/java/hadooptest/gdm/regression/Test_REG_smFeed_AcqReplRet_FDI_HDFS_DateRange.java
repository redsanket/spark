package hadooptest.gdm.regression;

import coretest.SerialTests;
import coretest.Util;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.cluster.gdm.Response;
import hadooptest.TestSession;

import org.junit.BeforeClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(SerialTests.class)
public class Test_REG_smFeed_AcqReplRet_FDI_HDFS_DateRange extends TestSession
{
	private ConsoleHandle console;
	private Response response;
	private String dataSetName;
	private long waitTimeForWorkflowPolling = 4000000L;
	private String feedSubmisionTime = null;
	private String datasourceconfig_base;

	@BeforeClass
	public static void startTestSession() {
		TestSession.start();
	}

	@Before
	public void init__Setup()
	{
		this.datasourceconfig_base = Util.getResourceFullPath("gdm/datasetconfigs") + "/";

		this.console = new ConsoleHandle();
	}

	@Test
	public void acquire__REG_smFeed_AcqReplRet_FDI_HDFS_DateRange() {
		this.dataSetName = ("REG_smFeed_AcqReplRet_FDI_HDFS_DateRange_" + System.currentTimeMillis());
		long sleepWhileCloneInmSec = 60000L;

		this.response = this.console.cloneDataSet(this.dataSetName, datasourceconfig_base+"REG_smFeed_AcqReplRet_FDI_HDFS_DateRange.xml");
		assertEquals("ResponseCode - cloneDataSet", 200, this.response.getStatusCode());

		TestSession.logger.debug("Data set is being cloned. Sleeping for " + sleepWhileCloneInmSec + " ms.");
		try {
			Thread.sleep(sleepWhileCloneInmSec);
		} catch (InterruptedException ex) {
			TestSession.logger.error(ex.toString());
		}

		this.response = this.console.checkDataSet(this.dataSetName);
		assertEquals("ResponseCode - checkDataSet", 200, this.response.getStatusCode());
		assertEquals("DataSetName does not match.", this.dataSetName, this.response.getElementAtPath("/DatasetsResult/[0]/DatasetName").toString());
		assertEquals("Priority does not match.", "HIGHEST", this.response.getElementAtPath("/DatasetsResult/[0]/Priority").toString());
		assertEquals("Feed is not INACTIVE.", "INACTIVE", this.response.getElementAtPath("/DatasetsResult/[0]/Status").toString());

		this.response = this.console.activateDataSet(this.dataSetName);
		assertEquals("ResponseCode - Activate DataSet", 200, this.response.getStatusCode());
		assertEquals("ActionName.", "unterminate", this.response.getElementAtPath("/Response/ActionName").toString());
		assertEquals("ResponseId", "0", this.response.getElementAtPath("/Response/ResponseId").toString());
		assertEquals("ResponseMessage.", "Operation on " + this.dataSetName + " was successful.", this.response.getElementAtPath("/Response/ResponseMessage/[0]").toString());
		try
		{
			Thread.sleep(15000L);
		} catch (InterruptedException ex) {
			TestSession.logger.error(ex.toString());
		}

		for (int i = 0; i < 5; i++) {
			this.response = this.console.checkDataSet(this.dataSetName);
			if (this.response.getElementAtPath("DatasetsResult/[0]/Status").toString().equals("ACTIVE")) {
				feedSubmisionTime = GdmUtils.getCalendarAsString();
				break;
			}
			try {
				Thread.sleep(5000L);
			} catch (InterruptedException ex) {
				TestSession.logger.error(ex.toString());
			}
		}

		TestSession.logger.debug("Feed Submission Time: " + feedSubmisionTime);

		assertEquals("ResponseCode - checkDataSet", 200, this.response.getStatusCode());
		assertEquals("DataSetName does not match.", this.dataSetName, this.response.getElementAtPath("DatasetsResult/[0]/DatasetName").toString());
		assertEquals("Feed is not ACTIVE.", "ACTIVE", this.response.getElementAtPath("DatasetsResult/[0]/Status").toString());

		String workflowStatus = this.console.pingWorkflowExecution(this.dataSetName, feedSubmisionTime, waitTimeForWorkflowPolling);

		assertEquals("Workflow not Complete.", "COMPLETED", workflowStatus);

		this.response = this.console.getCompletedJobsForDataSet(feedSubmisionTime, GdmUtils.getCalendarAsString(), this.dataSetName);
		assertEquals("It's not an acquisition job", "acquisition", this.response.getElementAtPath("/completedWorkflows/[0]/FacetName"));
		assertEquals("Attempt != 1", Integer.valueOf(1), this.response.getElementAtPath("/completedWorkflows/[0]/Attempt"));
		assertEquals("data.commit not done", "data.commit", this.response.getElementAtPath("/completedWorkflows/[0]/CurrentStep"));

		TestSession.logger.info(this.response.toString());
	}

	@Test
	public void replicate__REG_smFeed_AcqReplRet_FDI_HDFS_DateRange() {

		String workflowStatusRepl = this.console.pingWorkflowExecution(this.dataSetName, feedSubmisionTime, waitTimeForWorkflowPolling);

		assertEquals("Workflow not Completed", "COMPLETED", workflowStatusRepl);

		this.response = this.console.getCompletedJobsForDataSet(feedSubmisionTime, GdmUtils.getCalendarAsString(), this.dataSetName);

		assertEquals("It's not a replication job", "replication", this.response.getElementAtPath("/completedWorkflows/[3]/FacetName"));
		assertEquals("Attempt != 1", Integer.valueOf(1), this.response.getElementAtPath("/completedWorkflows/[3]/Attempt"));
		assertEquals("copy not done", "copy.gdm-target-denseb-patw02.gdm-target-elrond-patw02", this.response.getElementAtPath("/completedWorkflows/[3]/CurrentStep"));
		
		TestSession.logger.info(this.response.toString());

	}


	@Test
	public void integrity__testForFailedWorkflows_DateRange() {
		TestSession.logger.debug("Check for failed workflows, shouldn't be any...");
		TestSession.logger.debug(this.response.getElementAtPath("/failedWorkflows/[0]/FacetName"));
	}

	@Test
	public void deactivate__REG_smFeed_AcqReplRet_FDI_HDFS_DateRange() {
		this.response = this.console.deactivateDataSet(this.dataSetName);

		assertEquals("ResponseCode - Activate DataSet - shouldn't this be DEactivate datset??", 200, this.response.getStatusCode());
		assertEquals("ActionName.", "terminate", this.response.getElementAtPath("/Response/ActionName").toString());
		assertEquals("ResponseId", "0", this.response.getElementAtPath("/Response/ResponseId").toString());
		assertEquals("ResponseMessage.", "Operation on " + this.dataSetName + " was successful.", this.response.getElementAtPath("/Response/ResponseMessage/[0]").toString());
	}
	
}