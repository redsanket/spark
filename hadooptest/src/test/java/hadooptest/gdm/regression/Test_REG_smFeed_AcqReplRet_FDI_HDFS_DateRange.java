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

@Category(SerialTests.class)
public class Test_REG_smFeed_AcqReplRet_FDI_HDFS_DateRange extends TestSession
{
	
/*	
	
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
//		GdmAsserts.assertEquals(this.response.getStatusCode(), 200, "ResponseCode - cloneDataSet");

//		this.log.debug("Data set is being cloned. Sleeping for " + sleepWhileCloneInmSec + " ms.");
		try {
			Thread.sleep(sleepWhileCloneInmSec);
		} catch (InterruptedException ex) {
			TestSession.logger.error(ex.toString());
		}

		this.response = this.console.checkDataSet(this.dataSetName);
//		GdmAsserts.verifyEquals(this.response.getStatusCode(), 200, "ResponseCode - checkDataSet");
//		GdmAsserts.verifyEquals(this.response.getElementAtPath("/DatasetsResult/[0]/DatasetName").toString(), this.dataSetName, "DataSetName matches.");
//		GdmAsserts.verifyEquals(this.response.getElementAtPath("/DatasetsResult/[0]/Priority").toString(), "HIGHEST", "Priority matches.");
//		GdmAsserts.verifyEquals(this.response.getElementAtPath("/DatasetsResult/[0]/Status").toString(), "INACTIVE", "Feed is INACTIVE.");

		this.response = this.console.activateDataSet(this.dataSetName);
//		GdmAsserts.assertEquals(this.response.getStatusCode(), 200, "ResponseCode - Activate DataSet");
//		GdmAsserts.verifyEquals(this.response.getElementAtPath("/Response/ActionName").toString(), "unterminate", "ActionName.");
//		GdmAsserts.verifyEquals(this.response.getElementAtPath("/Response/ResponseId").toString(), "0", "ResponseId");
//		GdmAsserts.verifyEquals(this.response.getElementAtPath("/Response/ResponseMessage/[0]").toString(), "Operation on " + this.dataSetName + " was successful.", "ResponseMessage.");
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

//		GdmAsserts.verifyEquals(this.response.getStatusCode(), 200, "ResponseCode - checkDataSet");
//		GdmAsserts.verifyEquals(this.response.getElementAtPath("DatasetsResult/[0]/DatasetName").toString(), this.dataSetName, "DataSetName matches.");
//		GdmAsserts.verifyEquals(this.response.getElementAtPath("DatasetsResult/[0]/Status").toString(), "ACTIVE", "Feed is ACTIVE.");

		String workflowStatus = this.console.pingWorkflowExecution(this.dataSetName, feedSubmisionTime, waitTimeForWorkflowPolling);

//		GdmAsserts.verifyEquals(workflowStatus, "COMPLETED", "Workflow Completed");

		this.response = this.console.getCompletedJobsForDataSet(feedSubmisionTime, GdmUtils.getCalendarAsString(), this.dataSetName);
//		GdmAsserts.verifyEquals(this.response.getElementAtPath("/completedWorkflows/[0]/FacetName"), "acquisition", "It's an acquisition job");
//		GdmAsserts.verifyEquals(this.response.getElementAtPath("/completedWorkflows/[0]/Attempt"), Integer.valueOf(1), "Attempt = 1");
//		GdmAsserts.verifyEquals(this.response.getElementAtPath("/completedWorkflows/[0]/CurrentStep"), "data.commit", "data.commit done");

		TestSession.logger.info(this.response.toString());
	}

	@Test
	public void replicate__REG_smFeed_AcqReplRet_FDI_HDFS_DateRange() {

		String workflowStatusRepl = this.console.pingWorkflowExecution(this.dataSetName, feedSubmisionTime, waitTimeForWorkflowPolling);

//		GdmAsserts.verifyEquals(workflowStatusRepl, "COMPLETED", "Workflow Completed");

		this.response = this.console.getCompletedJobsForDataSet(feedSubmisionTime, GdmUtils.getCalendarAsString(), this.dataSetName);

		//System.out.println("PHW: dumping a bunch of debugging...\nresponse.getElementAtPath(\"/completedWorkflows/[0]/FacetName\") is : " + response.getElementAtPath("/completedWorkflows/[0]/FacetName") );
		//System.out.println("response.getElementAtPath(\"/completedWorkflows/[1]/FacetName\") is : " + this.response.getElementAtPath("/completedWorkflows/[1]/FacetName") );
		//System.out.println("response.getElementAtPath(\"/completedWorkflows/[2]/FacetName\") is : " + this.response.getElementAtPath("/completedWorkflows/[2]/FacetName") );
		//System.out.println("response.getElementAtPath(\"/completedWorkflows/[3]/FacetName\") is : " + this.response.getElementAtPath("/completedWorkflows/[3]/FacetName") );
		//System.out.println("response.getElementAtPath(\"/completedWorkflows/[4]/FacetName\") is : " + this.response.getElementAtPath("/completedWorkflows/[4]/FacetName") );
		//System.out.println("response.getElementAtPath(\"/completedWorkflows/[5]/FacetName\") is : " + this.response.getElementAtPath("/completedWorkflows/[5]/FacetName") );
		//System.out.println("response.getElementAtPath(\"/completedWorkflows/[6]/FacetName\") is : " + this.response.getElementAtPath("/completedWorkflows/[6]/FacetName") );
		//System.out.println("response.getElementAtPath(\"/completedWorkflows/[7]/FacetName\") is : " + this.response.getElementAtPath("/completedWorkflows/[7]/FacetName") );
		//System.out.println("response.getElementAtPath(\"/completedWorkflows/[8]/FacetName\") is : " + this.response.getElementAtPath("/completedWorkflows/[8]/FacetName") );
		//System.out.println("response.getElementAtPath(\"/completedWorkflows/[9]/FacetName\") is : " + this.response.getElementAtPath("/completedWorkflows/[9]/FacetName") );
		//System.out.println("PHW done" );
		//
//		GdmAsserts.verifyEquals(this.response.getElementAtPath("/completedWorkflows/[3]/FacetName"), "replication", "It's a replication job");
//		GdmAsserts.verifyEquals(this.response.getElementAtPath("/completedWorkflows/[3]/Attempt"), Integer.valueOf(1), "Attempt = 1");
//		GdmAsserts.verifyEquals(this.response.getElementAtPath("/completedWorkflows/[3]/CurrentStep"), "copy.gdm-target-denseb-patw02.gdm-target-elrond-patw02", "copy done");

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

//		GdmAsserts.assertEquals(this.response.getStatusCode(), 200, "ResponseCode - Activate DataSet - shouldn't this be DEactivate datset??");
//		GdmAsserts.verifyEquals(this.response.getElementAtPath("/Response/ActionName").toString(), "terminate", "ActionName.");
//		GdmAsserts.verifyEquals(this.response.getElementAtPath("/Response/ResponseId").toString(), "0", "ResponseId");
//		GdmAsserts.verifyEquals(this.response.getElementAtPath("/Response/ResponseMessage/[0]").toString(), "Operation on " + this.dataSetName + " was successful.", "ResponseMessage.");
	}
	
*/	
}