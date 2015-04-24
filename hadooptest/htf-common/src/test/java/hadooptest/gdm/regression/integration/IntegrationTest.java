package hadooptest.gdm.regression.integration;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.cluster.gdm.HCatHelper;
import hadooptest.cluster.gdm.HTTPHandle;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * TestCase : To create a set of dataset for a given duration like a day(s).
 *
 */
public class IntegrationTest  extends TestSession {

	private ConsoleHandle consoleHandle;
	private List<String> hcatSupportedGrid;
	private List<String> installedGrids;
	private String dataSetName;
	private String cookie;
	private String datasetActivationTime;
	private String enableHCAT;
	private String sourceCluster;
	private String destinationCluster;
	private int duration;
	private int noOfFeeds;
	private List<String> feedList;
	private String baseDataSetName = "VerifyAcqRepRetWorkFlowExecutionSingleDate";
	private WorkFlowHelper workFlowHelper;
	private HCatHelper hcatHelperObject = null;
	private String HCAT_TYPE =  "DataOnly" ;
	private static final String TARGET_START_TYPE_MIXED = "Mixed";
	private static final String TARGET_START_TYPE_DATAONLY = "DataOnly";
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
		this.consoleHandle = new ConsoleHandle();
		HTTPHandle httpHandle = new HTTPHandle();

		// get all the cluster that is currently GDM deployed
		this.installedGrids = this.consoleHandle.getAllInstalledGridName();

		// get the source cluster
		this.sourceCluster = GdmUtils.getConfiguration("testconfig.IntegrationTest.sourceCluster");
		TestSession.logger.info("sourceCluster  = " + sourceCluster);
		if ( ! (this.sourceCluster != null) && (this.installedGrids.contains(this.sourceCluster)) )  {
			fail("Source cluster is null or Specified a wrong source cluster that is not configured.");
		}

		// get the destination cluster
		this.destinationCluster = GdmUtils.getConfiguration("testconfig.IntegrationTest.destinationCluster");
		TestSession.logger.info("destinationCluster = " + destinationCluster);
		if ( ! (this.destinationCluster != null) && (this.installedGrids.contains(this.destinationCluster)) )  {
			fail("Destination cluster is null or Specified a wrong destination cluster that is not configured.");
		}

		// get hcat enabled for the dataset
		this.enableHCAT = GdmUtils.getConfiguration("testconfig.IntegrationTest.enable-hcat");
		if (this.enableHCAT != null && this.enableHCAT.toUpperCase() == "TRUE") {

			// check whether destination cluster is hcat enabled.
			this.hcatSupportedGrid = this.consoleHandle.getHCatEnabledGrid();
			if ( this.hcatSupportedGrid.contains(this.destinationCluster) == true ) {

				// check whether destination cluster is hcat enabled.
				boolean targetHCatSupported = this.consoleHandle.isHCatEnabledForDataSource(this.destinationCluster);
				if (!targetHCatSupported) {

					// enable destination cluster.
					this.consoleHandle.modifyDataSource(this.destinationCluster, "HCatSupported", "FALSE", "TRUE");
				}

				// since hcat is enabled, TARGET TYPE is MIXED
				this.HCAT_TYPE = this.TARGET_START_TYPE_MIXED; 
			} else {
				TestSession.logger.info("Hive is not installed on " + this.destinationCluster + " , will replication only DATA.");
			}

			// create instance of hcat helper class
			this.hcatHelperObject = new HCatHelper();
		} else if (this.enableHCAT != null && this.enableHCAT.toUpperCase() == "FALSE") {

			// since hcat is enabled, TARGET TYPE is DataOnly
			this.HCAT_TYPE = this.TARGET_START_TYPE_DATAONLY;
		}
		TestSession.logger.info("HCAT_TYPE = " + HCAT_TYPE);

		String dur = GdmUtils.getConfiguration("testconfig.IntegrationTest.duration");
		if ( dur != null ) {
			this.duration = Integer.parseInt(dur);
		}

		// get cookie for the headless user.
		this.cookie = httpHandle.getBouncerCookie();
		this.workFlowHelper = new WorkFlowHelper();
	}

	public void iamSleepgin() {
		this.consoleHandle.sleep(60);
	}

	@Test
	public void testHCATWithABFData() throws Exception {

		// check whether ABF data is available on the specified source
		List<String>dates = getInstanceFiles();
		assertTrue("ABF data is missing so testing can't be done, make sure whether the ABF data path is correct...!" , dates != null);

		SimpleDateFormat sdf = new SimpleDateFormat("yyMMddHHmm");
		Calendar todayCal = Calendar.getInstance();
		Calendar LastdayCal = Calendar.getInstance();
		Calendar currentCal = Calendar.getInstance();

		long toDay = Long.parseLong(sdf.format(todayCal.getTime()));

		// set the duration for how long the data has to generate.
		LastdayCal.add(Calendar.DAY_OF_MONTH , this.duration);
		long lastDay = Long.parseLong(sdf.format(LastdayCal.getTime()));
		System.out.println(" Current date - "+ sdf.format(todayCal.getTime()));
		System.out.println(" Next date - "+ sdf.format(LastdayCal.getTime()));
		long currentDay;
		while(toDay <= lastDay) {

			this.dataSetName = "Integration_Testing_DS_" + System.currentTimeMillis();

			// create  a dataset
			this.createDataSet();

			// activate the dataset
			this.consoleHandle.checkAndActivateDataSet(this.dataSetName);
			this.consoleHandle.sleep(40000);
			String datasetActivationTime = GdmUtils.getCalendarAsString();

			// check whether each instance date workflow
			for (String date : dates ) {
				this.workFlowHelper.checkWorkFlow(this.dataSetName , "replication" , datasetActivationTime , date);
			}

			toDay = Long.parseLong(sdf.format(currentCal.getTime()));

			// TODO : Need to find the API to query HIVE and HCat for table creation and partition. We can use GDM REST API or Data discovery REST API

			// deactivate the dataset
			this.tearDown();
		}
	}

	/**
	 * Create a dataset specification configuration file.
	 */
	private void createDataSet() {
		String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/ABFHcatDataSet.xml");
		String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.dataSetName, dataSetConfigFile);
		String feedName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Parameters" , "value");
		dataSetXml = dataSetXml.replaceAll("TARGET1_NAME", this.destinationCluster );
		dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", this.dataSetName);
		dataSetXml = dataSetXml.replaceAll("FEED_NAME", feedName );
		dataSetXml = dataSetXml.replaceAll("FEED_STATS", feedName + "_stats" );
		dataSetXml = dataSetXml.replaceAll("SOURCE_NAME", this.sourceCluster );
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

	public void tearDown() {
		// make dataset inactive
		Response response = this.consoleHandle.deactivateDataSet(this.dataSetName);
		assertEquals("ResponseCode - Deactivate DataSet", 200, response.getStatusCode());
		assertEquals("ActionName.", "terminate", response.getElementAtPath("/Response/ActionName").toString());
		assertEquals("ResponseId", "0", response.getElementAtPath("/Response/ResponseId").toString());
		assertEquals("ResponseMessage.", "Operation on " + this.dataSetName + " was successful.", response.getElementAtPath("/Response/ResponseMessage/[0]").toString());
	}
}
