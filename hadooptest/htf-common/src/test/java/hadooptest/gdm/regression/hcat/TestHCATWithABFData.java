package hadooptest.gdm.regression.hcat;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.cluster.gdm.HCatHelper;
import hadooptest.cluster.gdm.HTTPHandle;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * 
 * Test Case : Verify whether ABF data is copied successfully i,e replication is succesfull for ABF data
 * 			and verify whether HCAT table and partition is created for ABF data.
 *
 */
public class TestHCATWithABFData  extends TestSession {

	private ConsoleHandle consoleHandle;
	private List<String> hcatSupportedGrid;
	private String dataSetName;
	private String cookie;
	private String targetGrid1;
	private String datasetActivationTime;
	private String baseDataSetName = "VerifyAcqRepRetWorkFlowExecutionSingleDate";
	private WorkFlowHelper workFlowHelper;
	private HCatHelper hcatHelperObject = null;
	private static final String HCAT_TYPE = "Mixed";
	private static final int SUCCESS = 200;
	private static final String SOURCE_NAME= "elrond";
	private final static String HADOOP_LS_PATH = "/console/api/admin/hadoopls?dataSource=";
	private final static String ABF_DATA_PATH = "/data/SOURCE_ABF/ABF_DAILY/";
	private static final String DATABASE_NAME = "gdm";

	@BeforeClass
	public static void startTestSession() throws Exception {
		TestSession.start();
	}

	@Before
	public void setUp() throws NumberFormatException, Exception {
		this.hcatHelperObject = new HCatHelper();
		this.consoleHandle = new ConsoleHandle();
		HTTPHandle httpHandle = new HTTPHandle();
		this.workFlowHelper = new WorkFlowHelper();
		this.dataSetName = "TestHCatWithABFData_REP_DS_" + System.currentTimeMillis();

		this.cookie = httpHandle.getBouncerCookie();

		// get all the hcat supported clusters
		hcatSupportedGrid = this.consoleHandle.getHCatEnabledGrid();

		// check whether we have two hcat cluster one for acquisition
		if (hcatSupportedGrid.size() < 1) {
			throw new Exception("Unable to run " + this.dataSetName  +" 2 grid datasources are required.");
		}

		this.targetGrid1 = hcatSupportedGrid.get(0).trim();
		TestSession.logger.info("Using grids " + this.targetGrid1  );

		// check whether hcat is enabled on target1 cluster
		boolean targetHCatSupported = this.consoleHandle.isHCatEnabledForDataSource(this.targetGrid1);
		if (!targetHCatSupported) {
			this.consoleHandle.modifyDataSource(this.targetGrid1, "HCatSupported", "FALSE", "TRUE");
		}

	}

	@Test
	public void testHCATWithABFData() throws Exception {
		
		// check whether ABF data is available on the specified source
		List<String>dates = getInstanceFiles();
		assertTrue("ABF data is missing so testing can't be done, make sure whether the ABF data path is correct...!" , dates != null);

		// create the dataset
		createDataSet();

		// activate the dataset
		this.consoleHandle.checkAndActivateDataSet(this.dataSetName);
		this.consoleHandle.sleep(40000);
		String datasetActivationTime = GdmUtils.getCalendarAsString();

		// check whether each instance date workflow
		for (String date : dates ) {
			this.workFlowHelper.checkWorkFlow(this.dataSetName , "replication" , datasetActivationTime , date);
		}
		
		String tableOwner = this.hcatHelperObject.getHCatTableName(this.targetGrid1 , this.dataSetName , "replication");
		String tableName = this.dataSetName.toLowerCase().replaceAll("-", "_");
		assertTrue("Expected " + tableOwner + " data owner but got " + tableOwner , tableOwner.equals(tableName) == true);
		
		this.workFlowHelper.checkStepExistsInWorkFlowExecutionSteps(this.dataSetName, datasetActivationTime , "completed", "Step Name" , "publish." + this.SOURCE_NAME.trim() + "." + this.targetGrid1.trim()  );
	}

	/**
	 * Create a dataset specification configuration file.
	 */
	private void createDataSet() {
		String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/ABFHcatDataSet.xml");
		String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.dataSetName, dataSetConfigFile);
		String feedName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Parameters" , "value");
		dataSetXml = dataSetXml.replaceAll("TARGET1_NAME", this.targetGrid1 );
		dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", this.dataSetName);
		dataSetXml = dataSetXml.replaceAll("FEED_NAME", feedName );
		dataSetXml = dataSetXml.replaceAll("FEED_STATS", feedName + "_stats" );
		dataSetXml = dataSetXml.replaceAll("SOURCE_NAME", this.SOURCE_NAME );
		dataSetXml = dataSetXml.replace("HCAT_TYPE", this.HCAT_TYPE);
		dataSetXml = dataSetXml.replace("ABF-DATA-PATH", this.ABF_DATA_PATH + "%{date}");
		dataSetXml = dataSetXml.replace("HCAT_TABLE_NAME", this.dataSetName);
		dataSetXml = dataSetXml.replaceAll("DATABASE_NAME", this.DATABASE_NAME);

		Response response = this.consoleHandle.createDataSet(this.dataSetName, dataSetXml);
		if (response.getStatusCode() != SUCCESS) {
			try {
				throw new Exception("Response status code is " + response.getStatusCode() + ", expected 200.");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		this.consoleHandle.sleep(5000);
	}

	/**
	 * First checks whether ABF data exists on the grid for a given path, if exists returns instance date(s) 
	 * @return
	 */
	public List<String>getInstanceFiles() {
		JSONArray jsonArray = null;
		List<String>instanceDate = new ArrayList<String>();
		String testURL = this.consoleHandle.getConsoleURL() + this.HADOOP_LS_PATH + this.SOURCE_NAME + "&path=" + ABF_DATA_PATH + "&format=json";
		TestSession.logger.info("Test url = " + testURL);
		com.jayway.restassured.response.Response res = given().cookie(this.cookie).get(testURL);
		assertTrue("Failed to get the respons  " + res , (res != null ) );

		jsonArray = this.consoleHandle.convertResponseToJSONArray(res , "Files");
		TestSession.logger.info("********size = " + jsonArray.size());
		if ( jsonArray.size() > 0 ) {
			Iterator iterator = jsonArray.iterator();
			while (iterator.hasNext()) {
				JSONObject dSObject = (JSONObject) iterator.next();
				String  directory = dSObject.getString("Directory");
				TestSession.logger.info("######directory == " + directory);
				if (directory.equals("yes")) {
					String path = dSObject.getString("Path");
					List<String>instanceFile = Arrays.asList(path.split("/"));
					if (instanceFile != null ) {
						String dt = instanceFile.get(instanceFile.size() - 1);
						TestSession.logger.info("^^^^^^ date = " + dt);
						instanceDate.add(dt);
					}	
				}
			}
			return instanceDate;
		}
		return null;
	}

	@After
	public  void tearDown() {
		// make dataset inactive
		Response response = this.consoleHandle.deactivateDataSet(this.dataSetName);
		assertEquals("ResponseCode - Deactivate DataSet", 200, response.getStatusCode());
		assertEquals("ActionName.", "terminate", response.getElementAtPath("/Response/ActionName").toString());
		assertEquals("ResponseId", "0", response.getElementAtPath("/Response/ResponseId").toString());
		assertEquals("ResponseMessage.", "Operation on " + this.dataSetName + " was successful.", response.getElementAtPath("/Response/ResponseMessage/[0]").toString());
	}
}
