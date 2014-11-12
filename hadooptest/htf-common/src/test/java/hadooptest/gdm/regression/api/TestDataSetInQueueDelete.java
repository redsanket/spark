package hadooptest.gdm.regression.api;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import hadooptest.TestSession;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.HTTPHandle;
import hadooptest.cluster.gdm.Response;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test Case : Verify whether dataset is placed in queue and they are aborted.
 * Bug id : http://bug.corp.yahoo.com/show_bug.cgi?id=7050744
 *
 */
public class TestDataSetInQueueDelete  extends TestSession {

	private String cookie;
	private ConsoleHandle consoleHandle;
	private String dataSetName;
	private HTTPHandle httpHandle ;
	private String baseDataSetName = "VerifyAcqRepRetWorkFlowExecutionSingleDate";
	private List<String> dataSets = null;
	private static final int SUCCESS = 200;

	@BeforeClass
	public static void startTestSession() throws Exception {
		TestSession.start();
	}

	@Before
	public void setUp() throws NumberFormatException, Exception {
		this.httpHandle = new HTTPHandle();
		this.consoleHandle = new ConsoleHandle();
		this.cookie = httpHandle.getBouncerCookie();
		this.dataSetName = "TestDataSetInQueueDelete_" + System.currentTimeMillis();
		this.dataSets = new ArrayList<String>();
	}

	@Test
	public void testAbortDatasetFromQueue() {
		// create a dataset.
		createDataSet(this.dataSetName);

		// activate the dataset.
		Response  response = this.consoleHandle.activateDataSet(this.dataSetName);
		assertTrue("Failed to activate dataset " + dataSetName , response.getStatusCode() == SUCCESS);
		TestSession.logger.info("********  " +  this.dataSetName  + "  dataset got activated. ******* ");

		this.consoleHandle.sleep(3000);

		// check whether there is any item in the queue
		JSONArray jsonArray = isItemInQueue();

		// abort the item in the queue if its the dataset creaetd by this testcase.
		abortItemInQueue(jsonArray , this.dataSetName);

		// once again check whether the dataset is placed in queue.
		jsonArray = isItemInQueue();
		String workflowName = getItemInQueue(jsonArray , this.dataSetName);
		assertTrue("Failed to place the dataset in queue = " , workflowName.startsWith(this.dataSetName));
		TestSession.logger.info(this.dataSetName + "  was onace again placed in queue.");
	}
	
	/**
	 * Check whether item(s) are in queue.
	 * @return - return the item in the queue.
	 */
	private JSONArray isItemInQueue() {
		com.jayway.restassured.response.Response response = null;
		JSONArray jsonArray = null;
		long waitTimeForWorkflowPolling = 10 * 60 * 1000; // 10 minutes
		long sleepTime = 100; // 5 sec  sleep time.
		long waitTime = 0;
		boolean flag = false;

		while (waitTime <= waitTimeForWorkflowPolling) {
			String testURL = this.consoleHandle.getConsoleURL().replace("9999", this.consoleHandle.getFacetPortNo("acquisition")) + "/acquisition/api/workflow/queue";
			TestSession.logger.info(" Queue testURL   - " + testURL);
			response = given().cookie(this.cookie).get(testURL);

			String queueResponseString = response.asString();
			TestSession.logger.info("queueResponseString   = " + queueResponseString);

			// if dataset is not in queue, go for next iteration.
			if (! queueResponseString.equals("{\"QueueResponse\":[]}")) {

				// convert String to jsonObject
				JSONObject obj =  (JSONObject) JSONSerializer.toJSON(queueResponseString);
				TestSession.logger.info("obj = " + obj.toString());

				// convert string to jsonArray
				jsonArray = obj.getJSONArray("QueueResponse");
				flag = true;
				break;
			}
			waitTime += sleepTime;
			this.consoleHandle.sleep(sleepTime);
		}
		if (waitTime >= waitTimeForWorkflowPolling) {
			fail("Time out. " + this.dataSetName  + "  was inserted into the queue.");
		}
		if (flag == false) {
			TestSession.logger.info(this.dataSetName  + "  was inserted into the queue.");
			fail(this.dataSetName  + "  was inserted into the queue.");
		}
		return jsonArray;
	}

	/**
	 * Get the workflow name from the queue
	 * @param jsonArray
	 * @param dataSetName
	 * @return
	 */
	private String getItemInQueue(JSONArray jsonArray , String dataSetName)  {
		String workflowName = null;
		if (jsonArray.size() > 0) {
			Iterator iterator  = jsonArray.iterator();
			while ( iterator.hasNext() ) {
				JSONObject runningJsonObject = (JSONObject) iterator.next();
				workflowName = runningJsonObject.getString("WorkflowName");
				if ( workflowName.equals(dataSetName)) {
					break;
				}
			}

		}
		return workflowName;
	}

	/**
	 * Abort an item in the queue.
	 * @param jsonArray
	 */
	private void abortItemInQueue(JSONArray jsonArray, String datasetName) {
		com.jayway.restassured.response.Response response = null;
		String workflowName = getItemInQueue(jsonArray , datasetName);
		if (workflowName != null) {
			String queueAbortURL = this.consoleHandle.getConsoleURL().replace("9999", this.consoleHandle.getFacetPortNo("acquisition")) + "/acquisition/api/admin/workflow/queue/" +  workflowName ;
			TestSession.logger.info("queueAbortURL  = " + queueAbortURL);
			response = given().cookie(this.cookie).delete(queueAbortURL);
			String queueAbortStr = response.asString();
			TestSession.logger.info("response ================= " + queueAbortStr);
			assertTrue("Failed to delete the queue." ,response.getStatusCode() == 200);
			assertTrue("Failed to kill the queue, got response as " +queueAbortStr  , queueAbortStr.endsWith("successfully removed"));
		}
	}

	/**
	 * Create a new dataset.
	 * @param datasetName
	 */
	private void createDataSet(String datasetName) {
		// Read dataset and replace source and target values
		String dataSetXml = this.consoleHandle.getDataSetXml(this.baseDataSetName);
		dataSetXml = dataSetXml.replaceAll(this.baseDataSetName, datasetName);

		Response response = this.consoleHandle.createDataSet(datasetName, dataSetXml);
		TestSession.logger.info("Response code = " + response.getStatusCode());
		assertTrue("Failed to create a dataset " + datasetName , response.getStatusCode() == SUCCESS);

		this.consoleHandle.sleep(30000);
	}

	/**
	 * deactivate the dataset(s)	
	 */
	@After
	public void tearDown() {
		TestSession.logger.info("Deactivate "+ this.dataSetName  +"  dataset ");
		Response response = this.consoleHandle.deactivateDataSet(this.dataSetName);
		assertTrue("Failed to deactivate dataset " + dataSetName , response.getStatusCode() == SUCCESS);
		assertTrue("Expected terminate action name but got " + response.getElementAtPath("/Response/ActionName").toString() , response.getElementAtPath("/Response/ActionName").toString().equals("terminate") );
		assertTrue("Expected to get 0 response id, but got " + response.getElementAtPath("/Response/ResponseId").toString() , response.getElementAtPath("/Response/ResponseId").toString().equals("0"));
	}
}
