package hadooptest.gdm.regression.staging.hcat;

import static com.jayway.restassured.RestAssured.given;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.httpclient.HttpStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.jayway.restassured.path.json.JsonPath;

import hadooptest.TestSession;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.DataSetTarget;
import hadooptest.cluster.gdm.DataSetXmlGenerator;
import hadooptest.cluster.gdm.HTTPHandle;
import hadooptest.cluster.gdm.JSONUtil;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import static org.junit.Assert.assertTrue;


/**
 * TestCase : To test whether hcat metadata only works on staging
 *
 */
public class TestHCatNoDirectoryCreatedForMetadataOnlyReplOnStaging extends TestSession {

	private ConsoleHandle consoleHandle;
	private WorkFlowHelper workFlowHelper;
	private JSONUtil jsonUtil;
	private String cookie;
	private String dataSetName;
	private String datasetActivationTime;
	private final static String SOURCE_CLUSTER_NAME = "AxoniteRed";
	private final static String TARGET_CLUSTER_NAME = "KryptoniteRed";
	private static final String DBNAME = "gdmstgtesting";
	private static final String TABLE_NAME = "metadata_only";
	private static final String DATA_PATH = "/user/hitusr_1/metadata_only";
	private static final String HEALTH_CHECKUP_API = "/console/api/proxy/health";
	private static final String HCAT_TABLE_LIST_API = "/replication/api/admin/hcat/table/list";
	private static int SUCCESS = 200;
	private static int SLEEP_TIME = 50000;
	private static final String START_INSTANCE_RANGE = "20150101";
	private static final String END_INSTANCE_RANGE = "20160721";

	@BeforeClass
	public static void startTestSession() {
		TestSession.start();
	}

	@Before
	public void setup() throws Exception {
		this.consoleHandle = new ConsoleHandle();
		HTTPHandle httpHandle = new HTTPHandle();
		jsonUtil = new JSONUtil();
		this.cookie = httpHandle.getBouncerCookie();
		dataSetName = "TestHCatMetadataOnlyReplOnStg_" + System.currentTimeMillis();
		workFlowHelper = new WorkFlowHelper();
	}

	@Test
	public void test() {
		String hostName = getReplicationHostName();
		if (hostName != "") {
			TestSession.logger.info("replication hostName - " + hostName);
			if ( checkTableExistsOnSource(hostName) ) {
				TestSession.logger.info(this.TABLE_NAME + " exists on " + this.SOURCE_CLUSTER_NAME);
				checkDataSetExistForGivenPath();
				createDataset();

				this.consoleHandle.activateDataSet(this.dataSetName);
				TestSession.logger.info("wait for some time, so that dataset can get activated.");
				this.consoleHandle.sleep(SLEEP_TIME);

				// check for replication workflow
				workFlowHelper.checkWorkFlow(this.dataSetName, "replication", this.datasetActivationTime);

				// deactivate the dataset before applying retention to dataset
				Response response = this.consoleHandle.deactivateDataSet(this.dataSetName);
				Assert.assertEquals("ResponseCode - Deactivate DataSet failed", HttpStatus.SC_OK, response.getStatusCode());

				// set retention policy to zero
				this.consoleHandle.setRetentionPolicyToAllDataSets(this.dataSetName , "0");
				
				TestSession.logger.info("wait for some time, so that dataset changes are applied.");
				this.consoleHandle.sleep(SLEEP_TIME);

				// check for retention workflow
				workFlowHelper.checkWorkFlow(this.dataSetName, "retention", this.datasetActivationTime);
			}
		}
	}

	/**
	 * Check whether any dataset(s) exists for the given path, if exists delete those dataset(s).
	 * This is to avoid path collision when creating a new dataset.
	 */
	public void checkDataSetExistForGivenPath() {
		String url = this.consoleHandle.getConsoleURL() + "/console/api/datasets/view?prefix=" + DATA_PATH +  "&dataSource=" + SOURCE_CLUSTER_NAME;
		TestSession.logger.info("url - " + url);
		com.jayway.restassured.response.Response response = given().cookie(this.cookie).get(url);
		if (response != null ) {
			List<String> dataSetNames = response.getBody().jsonPath().getList("DatasetsResult.DatasetName");

			// disable & delete dataset(s)
			for ( String dsName : dataSetNames) {
				TestSession.logger.info("dataSetName - " + dsName);
				deactivateAndRemoveDataSet(dsName);
			}
		}
	}

	/**
	 * Create a replication dataset for metadata only
	 */
	private void createDataset() {
		DataSetXmlGenerator generator = new DataSetXmlGenerator();
		generator.setName(this.dataSetName);
		generator.setDescription(this.dataSetName);
		generator.setCatalog(this.dataSetName);
		generator.setActive("FALSE");
		generator.setOwner("dfsload");
		generator.setGroup("users");
		generator.setPermission("750");
		generator.setRetentionEnabled("TRUE");
		generator.setPriority("NORMAL");
		generator.setFrequency("daily");
		generator.setDiscoveryFrequency("30");
		generator.setDiscoveryInterface("HCAT");
		generator.addSourcePath("data", DATA_PATH + "/instancedate=%{date}");
		generator.setSource(SOURCE_CLUSTER_NAME);

		// hcat specifics 
		generator.setHcatDbName(DBNAME);
		generator.setHcatForceExternalTables("FALSE");
		generator.setHcatInstanceKey("instancedate");
		generator.setHcatRunTargetFilter("FALSE");
		generator.setHcatTableName(TABLE_NAME);
		generator.setHcatTablePropagationEnabled("TRUE");

		DataSetTarget target = new DataSetTarget();
		target.setName(TARGET_CLUSTER_NAME);
		target.setDateRangeStart(true, START_INSTANCE_RANGE);
		target.setDateRangeEnd(true, END_INSTANCE_RANGE);
		target.setHCatType("HCatOnly");
		target.setNumInstances("5");
		generator.setTarget(target);
		String dataSetXml = generator.getXml();

		Response response = this.consoleHandle.createDataSet(this.dataSetName, dataSetXml);
		if (response.getStatusCode() != HttpStatus.SC_OK) {
			TestSession.logger.error("Failed to create dataset, xml: " + dataSetXml);
			Assert.fail("Response status code is " + response.getStatusCode() + ", expected 200.");
		}
	}

	/**
	 * Deactivate and delete the dataset
	 * @param dataSetName
	 */
	public void deactivateAndRemoveDataSet(String dataSetName) {
		// deactivate dataset
		Response response = this.consoleHandle.deactivateDataSet(dataSetName);
		assertTrue("Failed to deactivate the dataset " +dataSetName  , response.getStatusCode() == SUCCESS);
		TestSession.logger.info("deactivate   " +  dataSetName  + "  dataset");

		// wait for some time, so that changes are reflected in the dataset specification file i,e active to inactive
		this.consoleHandle.sleep(SLEEP_TIME);

		String resource = this.jsonUtil.constructResourceNamesParameter(Arrays.asList(dataSetName));
		TestSession.logger.info("resource = "+this.jsonUtil.formatString(resource));

		// remove the dataset
		com.jayway.restassured.response.Response res = given().cookie(this.cookie).param("resourceNames", resource).param("command","remove")
				.post(this.consoleHandle.getConsoleURL() + "/console/rest/config/dataset/actions");

		String resString = res.asString();
		TestSession.logger.info("response *****"+this.jsonUtil.formatString(resString));

		// Check for Response
		JsonPath jsonPath = new JsonPath(resString);
		String actionName = jsonPath.getString("Response.ActionName");
		String responseId = jsonPath.getString("Response.ResponseId");
		assertTrue("Expected remove action, but got " + actionName , actionName.equals("remove"));
		assertTrue("Expected 0, but found " + responseId , responseId.equals("0"));
		String responseMessage = jsonPath.getString("Response.ResponseMessage");
		boolean flag = responseMessage.contains(dataSetName) && responseMessage.contains("successful");
		assertTrue("failed to get the correct message, but found " + responseMessage , flag == true);

		TestSession.logger.info("Deleted " + dataSetName  + " dataset");
	}

	/**
	 * Check whether table exists on the source cluster.
	 * @param replicationHostName
	 * @return return true if table exists
	 */
	public boolean checkTableExistsOnSource(String replicationHostName) {
		boolean isTableExists = false , isDataPathExists = false;
		String url = "http://" + replicationHostName  + ":4080" + HCAT_TABLE_LIST_API + "?dataSource=" + SOURCE_CLUSTER_NAME + "&dbName=" + DBNAME  + "&tablePattern="  + TABLE_NAME;
		TestSession.logger.info("url - " + url);
		com.jayway.restassured.response.Response response = given().cookie(this.cookie).get(url);
		JSONArray jsonArray = this.consoleHandle.convertResponseToJSONArray(response, "Tables");
		if (jsonArray.size() > 0) {
			Iterator iterator = jsonArray.iterator();	
			while (iterator.hasNext()) {
				JSONObject dSObject = (JSONObject) iterator.next();

				// check for table name
				String  tableName = dSObject.getString("TableName");
				TestSession.logger.info("tableName  - " + tableName);
				isTableExists = tableName.equalsIgnoreCase(TABLE_NAME);

				// check for path 
				String location = dSObject.getString("Location");
				TestSession.logger.info("location - " + location);
				isDataPathExists = location.indexOf(DATA_PATH) > 0;

				if (isTableExists && isDataPathExists) {
					return true;
				}
			}
		}
		return false;
	}

	/**
	 * Query health checkup on console and get red replication hostname
	 * @return
	 */
	public String getReplicationHostName() {
		String healthCheckUpURL = this.consoleHandle.getCurrentConsoleURL() + HEALTH_CHECKUP_API + "?facet=console&colo=ne1&type=health";
		String replicationHostName= "";
		TestSession.logger.info("health checkup api - " + healthCheckUpURL);
		com.jayway.restassured.response.Response response = given().cookie(this.cookie).get(healthCheckUpURL);
		if (response != null) {
			String resString = response.asString();
			TestSession.logger.info("response = " + resString);
			JsonPath jsonPath = new JsonPath(resString);
			Map<String , String>applicationSummary = new HashMap<String, String>();
			List<String> keys = jsonPath.get("ApplicationSummary.Parameter");
			List<String> values = jsonPath.get("ApplicationSummary.Value");
			for(int i = 0;i<keys.size() ; i++){
				applicationSummary.put(keys.get(i), values.get(i));
			}
			List<String> hostNames = Arrays.asList(applicationSummary.get("Facet Endpoints").split(" "));
			List<String> hostName = hostNames.stream().filter(hostname -> hostname.indexOf("red") > -1 && hostname.indexOf("replication") > -1).collect(Collectors.toList());
			if (hostName.size() > 0) {
				replicationHostName = hostName.get(0).replaceAll("https://" , "").replaceAll(":4443/replication", "");
			}
		}
		return replicationHostName;
	}
}
