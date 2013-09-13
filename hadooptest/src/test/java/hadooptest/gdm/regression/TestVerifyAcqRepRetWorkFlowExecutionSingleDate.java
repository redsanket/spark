package hadooptest.gdm.regression;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import hadooptest.TestSession;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.cluster.gdm.Response;

import org.junit.BeforeClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import coretest.SerialTests;
import coretest.Util;

@Category(SerialTests.class)
public class TestVerifyAcqRepRetWorkFlowExecutionSingleDate extends TestSession {

	private ConsoleHandle console;
	private Response response;
	private String dataSetName;
	private String baseDataSetName;
	private long waitTimeBeforeWorkflowPollingInMs = 180000L;
	private long waitTimeBetweenWorkflowPollingInMs = 60000L;
	private long waitTimeForRepostoryUpdateInMs = 45000L;
	private long timeoutInMs = 300000L;
	private String datasetActivationTime = null;
	//private String datasourceconfig_base = "/home/y/conf/gdm_qe_test/datasetconfigs/";
	private String datasourceconfig_base;

	@BeforeClass
	public static void startTestSession() {
		TestSession.start();
		
		
	}
	
	/**
	 * A test to initialize setup for the Acquisition, Replication, and 
	 * Retention workflow execution test.
	 */
	@Before
	public void init__Setup()
	{
		this.datasourceconfig_base = Util.getResourceFullPath("gdm/datasetconfigs") + "/";
		
		this.console = new ConsoleHandle();
		//this.baseDataSetName = console.getConf().getString("testconfig.VerifyAcqRepRetWorkFlowExecutionSingleDate.basedataset");
		this.baseDataSetName = GdmUtils.getConfiguration("testconfig.VerifyAcqRepRetWorkFlowExecutionSingleDate.basedataset");

		if(baseDataSetName == null || baseDataSetName.equals("")) {
			fail("testconfig.VerifyAcqRepRetWorkFlowExecutionSingleDate.basedataset is not set!");
		}
		this.dataSetName = ( baseDataSetName + "_" + System.currentTimeMillis());
		logger.info("Base DataSet Name: " + baseDataSetName);
		logger.info("DataSet Name: " + dataSetName);

		String waitTimeString;
		waitTimeString = GdmUtils.getConfiguration("testconfig.VerifyAcqRepRetWorkFlowExecutionSingleDate.timeoutInMs");
		if (waitTimeString != null && !waitTimeString.isEmpty()) {
			this.timeoutInMs = Long.parseLong(waitTimeString.trim());
		}
		logger.info("timeoutInMs: " + timeoutInMs);

		waitTimeString = null;
		waitTimeString = GdmUtils.getConfiguration("testconfig.VerifyAcqRepRetWorkFlowExecutionSingleDate.waitTimeBeforeWorkflowPollingInMs");
		if (waitTimeString != null && !waitTimeString.isEmpty()) {
			this.waitTimeBeforeWorkflowPollingInMs = Long.parseLong(waitTimeString.trim());
		}
		logger.info("waitTimeBeforeWorkflowPollingInMs: " + waitTimeBeforeWorkflowPollingInMs);

		waitTimeString = null;
		waitTimeString = GdmUtils.getConfiguration("testconfig.VerifyAcqRepRetWorkFlowExecutionSingleDate.waitTimeBetweenWorkflowPollingInMs");
		if (waitTimeString != null && !waitTimeString.isEmpty()) {
			this.waitTimeBetweenWorkflowPollingInMs = Long.parseLong(waitTimeString.trim());
		}
		logger.info("waitTimeBetweenWorkflowPollingInMs: " + waitTimeBetweenWorkflowPollingInMs);

		waitTimeString = null;
		waitTimeString = GdmUtils.getConfiguration("testconfig.VerifyAcqRepRetWorkFlowExecutionSingleDate.waitTimeForRepostoryUpdateInMs");
		if (waitTimeString != null && !waitTimeString.isEmpty()) {
			this.waitTimeForRepostoryUpdateInMs = Long.parseLong(waitTimeString.trim());
		}
		logger.info("waitTimeForRepostoryUpdateInMs: " + waitTimeForRepostoryUpdateInMs);
	}

	/**
	 * Test the Acquisition, Replication, and Retention workflow execution.
	 */
	@Test
	public void testAcqRepRetWorkFlowExecution() {

		String returnCode = "200";
		
		//****************
		TestSession.logger.info("datatSetName: " + this.dataSetName);
		TestSession.logger.info("specification.xml: " + datasourceconfig_base + baseDataSetName + "_specification.xml");
		TestSession.logger.info("baseDataSetName: " + baseDataSetName);
		TestSession.logger.info("console obj:" + this.console.toString());
		//****************
		
		// Clone the base data set
		
		this.response = this.console.cloneDataSet(this.dataSetName, "/home/y/conf/gdm_qe_test/datasetconfigs/VerifyAcqRepRetWorkFlowExecutionSingleDate_specification.xml", baseDataSetName);
		//this.response = this.console.cloneDataSet(this.dataSetName, datasourceconfig_base + baseDataSetName + "_specification.xml", baseDataSetName);
		// Verify the response code
		assertEquals("Verify Response Code - Clone DataSet", returnCode, Integer.toString(this.response.getStatusCode()));
		
		// Wait for the data set to be cloned.
		logger.info("Data set is being cloned. Sleeping for " + waitTimeForRepostoryUpdateInMs + " ms.");
		try {
			Thread.sleep(waitTimeForRepostoryUpdateInMs);
		} catch (InterruptedException ex) {
			logger.error(ex.toString());
		}

		// Activate the data set
		this.response = this.console.activateDataSet(this.dataSetName);
		// Verify the response code
		assertEquals("Verify Response Code - Activate DataSet", returnCode, Integer.toString(this.response.getStatusCode()));

		datasetActivationTime = GdmUtils.getCalendarAsString();
		logger.info("DataSet Activation Time: " + datasetActivationTime);

		// Wait for the data set to be activated.
		logger.info("Data set is being activated. Sleeping for " + waitTimeForRepostoryUpdateInMs + " ms.");
		try {
			Thread.sleep(waitTimeForRepostoryUpdateInMs);
		} catch (InterruptedException ex) {
			logger.error(ex.toString());
		}

		// Check the data set.
		this.response = this.console.checkDataSet(this.dataSetName);
		// Verify the response code.
		assertEquals("Verify Response Code - checkDataSet", returnCode, Integer.toString(this.response.getStatusCode()));
		// Verify the feed is active.
		assertEquals("Verify the Feed is ACTIVE.", "ACTIVE", this.response.getElementAtPath("DatasetsResult/[0]/Status").toString());

		// Sleep before we begin to poll the workflow.
		long currentTotalWaitingTime = waitTimeBeforeWorkflowPollingInMs - waitTimeBetweenWorkflowPollingInMs;
		logger.info("Sleeping for " + currentTotalWaitingTime + " ms before checking workflow status");
		try {
			Thread.sleep(currentTotalWaitingTime);
		} catch (InterruptedException ex) {
			logger.error(ex.toString());
		}

		// Verify that Acquisition, Replication, and Retention completed successfully.
		boolean acqSuccess=false, repSuccess=false, retSuccess=false;
		while (currentTotalWaitingTime < timeoutInMs) {
			logger.info("Sleeping for " + waitTimeBetweenWorkflowPollingInMs + " ms before checking workflow status");
			try {
				Thread.sleep(waitTimeBetweenWorkflowPollingInMs);
			} catch (InterruptedException ex) {
				logger.error(ex.toString());
			}
			currentTotalWaitingTime = currentTotalWaitingTime + waitTimeBetweenWorkflowPollingInMs;

			this.response = this.console.getCompletedJobsForDataSet(datasetActivationTime, GdmUtils.getCalendarAsString(), this.dataSetName);

			if(!acqSuccess){
				acqSuccess = this.console.isWorkflowCompleted(this.response, "acquisition");
				if(acqSuccess){
					assertTrue("Acquisition Workflow Successful", acqSuccess);
				}                
				else if(!acqSuccess && this.console.isWorkflowFailed(this.response, "acquisition")){
					fail("Acquisition Workflow Failed");
				}
			}
			if(acqSuccess && !repSuccess){
				repSuccess = this.console.isWorkflowCompleted(this.response, "replication");
				if(repSuccess){
					assertTrue("Replication Workflow Successful", repSuccess);
				}                
				else if(!repSuccess && this.console.isWorkflowFailed(this.response, "replication")){
					fail("Replication Workflow Failed");
				}
			}
			if(repSuccess && !retSuccess){
				retSuccess = this.console.isWorkflowCompleted(this.response, "retention");
				if(retSuccess){
					assertTrue("Retention Workflow for Replication Successful", acqSuccess);
				}                
				else if(!retSuccess && this.console.isWorkflowFailed(this.response, "retention")){
					fail("Retention Workflow for Replication Failed");
				}
			}

			if(acqSuccess && repSuccess && retSuccess){
				break;   
			}
		}

		boolean notAllSuccessful = !acqSuccess || !repSuccess || !retSuccess;
		if((currentTotalWaitingTime >= timeoutInMs) && notAllSuccessful)
			fail("The test has timed out. Task completion status - " + "Acquistion: " + acqSuccess + ", Replication: " + 
					repSuccess + ", Retention for Replication: " + retSuccess);
	}
}
