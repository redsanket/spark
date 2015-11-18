package hadooptest.gdm.regression;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import hadooptest.TestSession;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.cluster.gdm.HTTPHandle;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.jayway.restassured.path.json.JsonPath;


/**
 * Test Case : To check whether Hadoop MapReduce end notification is triggered for the a given workflow. 
 *
 */
public class TestDataSetHadoopJobEndNotification extends TestSession {

	private String cookie;
	private ConsoleHandle consoleHandle;
	private String consoleURL;
	private String dataSetName;
	private String datasetActivationTime;
	private WorkFlowHelper workFlowHelper;
	private HTTPHandle httpHandle ;	
	private WorkFlowHelper workFlowHelperObj = null;
	private static final String baseDataSetName = "VerifyAcqRepRetWorkFlowExecutionDateRange";
	private static final int SUCCESS = 200;
	private final static String LOG_FILE = "/home/y/logs/ygrid_gdm_FACET_NAME_server/FACET_NAME-application.log";

	@BeforeClass
	public static void startTestSession() throws Exception {
		TestSession.start();
	}

	@Before
	public void setUp() throws NumberFormatException, Exception {
		this.consoleHandle = new ConsoleHandle();
		this.httpHandle = new HTTPHandle();
		this.workFlowHelperObj = new WorkFlowHelper();
		this.cookie = httpHandle.getBouncerCookie();
		this.consoleURL = this.consoleHandle.getConsoleURL();
		this.dataSetName = "Test_DataSet_Hadoop_Job_End_Notification_" + System.currentTimeMillis();
	}

	@Test
	public void test() throws IOException {

		// creates a dataset
		createDataSet();

		// check for acquisition workflow completed successfully.
		workFlowHelperObj.checkWorkFlow(this.dataSetName, "acquisition", this.datasetActivationTime);

		// check for hadoop job end notification.
		checkHadoopJobEndNotification(this.dataSetName , "acquisition");

		workFlowHelperObj.checkWorkFlow(this.dataSetName, "replication", this.datasetActivationTime);
		checkHadoopJobEndNotification(this.dataSetName , "replication");
	}

	/**
	 * Create a dataset for testing workflow
	 */
	private void createDataSet() {
		String dataSetXml = this.consoleHandle.getDataSetXml(this.baseDataSetName);
		dataSetXml = dataSetXml.replaceAll(this.baseDataSetName, this.dataSetName);
		Response response = this.consoleHandle.createDataSet(this.dataSetName, dataSetXml);
		assertTrue("Failed to create a dataset " +this.dataSetName , response.getStatusCode() == 200);

		// activate the dataset.
		response = this.consoleHandle.activateDataSet(dataSetName);
		assertTrue("Failed to activate dataset " + dataSetName , response.getStatusCode() == SUCCESS);
		this.consoleHandle.sleep(30000);
		this.datasetActivationTime = GdmUtils.getCalendarAsString();
	}

	/**
	 * Method that checks for dataset workflow completion and then gets the detail step information for the dataset and constructs a 
	 * 
	 * @param datasetName
	 * @param facetName
	 * @return
	 */	
	private Map<String,String> constructHadoopJobEndNotificationString(String datasetName , String facetName) {
		Map<String,String> jobs = new HashMap<String,String>();
		String completedWorkFlowURL = this.consoleHandle.getConsoleURL() + "/console/api/workflows/completed?datasetname=" + this.dataSetName + "&instancessince=F&joinType=innerJoin&facet=" + facetName ;
		TestSession.logger.info("completedWorkFlowURL  = " + completedWorkFlowURL);
		this.consoleHandle.sleep(30000);

		// Get the required field from completed workflow response
		com.jayway.restassured.response.Response completedResponse =  given().cookie(this.cookie).get(completedWorkFlowURL);
		JSONArray jsonArray = this.consoleHandle.convertResponseToJSONArray(completedResponse, "completedWorkflows");
		if (jsonArray.size() > 0) {
			Iterator iterator = jsonArray.iterator();
			while (iterator.hasNext()) {
				JSONObject completedJsonObject = (JSONObject) iterator.next();
				String executionId = completedJsonObject.getString("ExecutionID");
				String colo = completedJsonObject.getString("FacetColo");
				String fName = completedJsonObject.getString("FacetName");
				String wflowName = completedJsonObject.getString("WorkflowName");
				List<String> workFlowInstance = Arrays.asList(wflowName.split("/"));
				String dsName = workFlowInstance.get(0);
				String instanceId = workFlowInstance.get(1);

				this.consoleHandle.sleep(30000);

				// Get details information about the completed workflow i,e get step details and fetch only map reduce url
				String testDetailInfoOnDataSetURL = this.consoleHandle.getConsoleURL() + "/console/api/workflows/" + executionId + "/view?facet=" + fName + "&colo=" + colo;
				System.out.println("testDetailInfoOnDataSetURL   - " + testDetailInfoOnDataSetURL);
				com.jayway.restassured.response.Response response =  given().cookie(this.cookie).get(testDetailInfoOnDataSetURL);
				String str = response.getBody().asString();
				JSONObject obj =  (JSONObject) JSONSerializer.toJSON(str.toString());
				TestSession.logger.info("obj ===== " + obj.toString());
				JSONObject jsonObject =  obj.getJSONObject("WorkflowExecution");
				TestSession.logger.info("workflow name = " + jsonObject.getString("Workflow Name"));
				jsonArray = jsonObject.getJSONArray("Step Executions");
				iterator = jsonArray.iterator();
				String jobId = null;

				// build the notification string that has to be searched in facet application.log file.
				while (iterator.hasNext()) {
					JSONObject tempJsonObj = (JSONObject) iterator.next();
					String mapReduceURL = tempJsonObj.getString("Workflow Map/Reduce Url");
					if ( ! (mapReduceURL.contains("#") || mapReduceURL.contains("loadproxy"))) {
						jobId = getJobId(mapReduceURL);
						String urlTemp = "Got job end notification from hadoop: /" + fName + "/jobEndNotificationjobId=" + jobId + "&dataSetName=" + dsName +  "&instanceID=" + instanceId;
						TestSession.logger.info("*******************  urlTemp   = " + urlTemp);
						jobs.put(instanceId, urlTemp);
					}
				}
			}
		}
		return jobs;
	}

	/**
	 * returns hadoop mapreduce job id. 
	 * @param workFlowMapReduceURL - value of mapreduce value
	 * @return String
	 */
	private String getJobId(String workFlowMapReduceURL) {
		String jobId  = null;
		workFlowMapReduceURL = workFlowMapReduceURL.replace("<a href=", "").replace("'>", "").replace("</a>", "");
		List<String> lst = Arrays.asList(workFlowMapReduceURL.split("/"));
		for (String val : lst) {
			if (val.startsWith("job")) {
				jobId = val;
				break;
			}
		}
		return jobId;
	}

	/**
	 * Search a given string in the log file.
	 * @param datasetName - name of the dataset for which user is search for Hadoop Job notification.
	 * @param facetName - either acquisition or replication facet.
	 * @throws IOException
	 */
	private void checkHadoopJobEndNotification(String datasetName , String facetName) throws IOException  {
		Map<String,String> notificationValueList  = this.constructHadoopJobEndNotificationString(this.dataSetName , facetName);

		for (String key: notificationValueList.keySet()) {
			String value = notificationValueList.get(key).trim();

			TestSession.logger.info("notificationValue  = " + value);	
			Pattern pattern = Pattern.compile(value);

			// read the log file.
			String facetApplicationLogFile = null , hostName = null;

			org.apache.commons.configuration.Configuration configuration = consoleHandle.getConf();
			String environmentType = configuration.getString("hostconfig.console.test_environment_type");
			if (environmentType.equals("oneNode")) {
				TestSession.logger.info("****** QE or Dev test Environment ******** ");
				hostName = configuration.getString("hostconfig.console.base_url");
				facetApplicationLogFile = "/grid/0/yroot/var/yroots/"+ facetName.trim() +"/"+LOG_FILE.replaceAll("FACET_NAME", facetName);

				// Read the facet application log file.
				FileInputStream input = new FileInputStream(facetApplicationLogFile);
				FileChannel channel = input.getChannel();

				ByteBuffer bbuf = channel.map(FileChannel.MapMode.READ_ONLY, 0, (int) channel.size());
				CharBuffer cbuf = Charset.forName("8859_1").newDecoder().decode(bbuf);
				Matcher matcher = pattern.matcher(cbuf);
				long count = 0;
				while (matcher.find()) {
					String match = matcher.group().trim();
					System.out.println(match);
					assertTrue("Failed : " + value + " dn't match with  " +  match , value.equals(match));
					count++;
				}
				TestSession.logger.info("count = " + count);
			} else if (environmentType.equals("staging")) {
				TestSession.logger.info("****** Staging test Environment ******** ");
				String tempHostName = configuration.getString("hostconfig.console.staging_console_url");
				TestSession.logger.info("tempHostName  = " +   tempHostName   );
				facetApplicationLogFile = LOG_FILE.replaceAll("FACET_NAME", facetName);
				String temp =  workFlowHelperObj.getFacetHostName(tempHostName , facetName);
				hostName = Arrays.asList(temp.split(":")).get(1).replaceAll("//", "");
				String command = "ssh " + hostName  + "  \"cat  " +  facetApplicationLogFile  + " | grep \"\\\"Got job end notification from hadoop\"\\\""  + "  | grep " + dataSetName + "\"";
				String output = workFlowHelperObj.executeCommand(command);
				assertTrue("Expected job end notification from hadoop for " + this.dataSetName + " but got " + output , output.indexOf(this.dataSetName) > 0);
			} else  {
				TestSession.logger.info("****** Specified invalid test environment ******** ");
				fail("You have specified a Unknow execution environment ( oneNode or stage) ");
			}
		}
	}

	@After
	public void tearDown() throws Exception {
		// deactivate the dataset
		Response response = this.consoleHandle.deactivateDataSet(this.dataSetName);
		assertEquals("ResponseCode - Deactivate DataSet", 200, response.getStatusCode());
		assertEquals("ActionName.", "terminate", response.getElementAtPath("/Response/ActionName").toString());
		assertEquals("ResponseId", "0", response.getElementAtPath("/Response/ResponseId").toString());
		assertEquals("ResponseMessage.", "Operation on " + this.dataSetName + " was successful.", response.getElementAtPath("/Response/ResponseMessage/[0]").toString());
	}
}
