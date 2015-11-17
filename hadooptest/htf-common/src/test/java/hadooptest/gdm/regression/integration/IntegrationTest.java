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
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;

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
	private int frequency;
	private String freq;
	private List<String> feedList;
	private String baseDataSetName = "VerifyAcqRepRetWorkFlowExecutionSingleDate";
	private WorkFlowHelper workFlowHelper;
	private HCatHelper hcatHelperObject = null;
	private String HCAT_TYPE =  "DataOnly" ;
	private static final String TARGET_START_TYPE_MIXED = "Mixed";
	private static final String TARGET_START_TYPE_DATAONLY = "DataOnly";
	private static final int SUCCESS = 200;
	private static final String SOURCE_NAME= "qe9blue";
	private final static String HADOOP_LS_PATH = "/console/api/admin/hadoopls?dataSource=";
	private final static String ABF_DATA_PATH = "/data/SOURCE_ABF/ABF_Daily/";
	private static final String DATABASE_NAME = "gdm";
	private static final String HADOOPQA_AS_HDFSQA_IDENTITY_FILE = "/homes/hadoopqa/.ssh/flubber_hadoopqa_as_hdfsqa";

	@BeforeClass
	public static void startTestSession() throws Exception {
		TestSession.start();
	}

	@Before
	public void setUp() throws NumberFormatException, Exception {
		this.consoleHandle = new ConsoleHandle();
		HTTPHandle httpHandle = new HTTPHandle();

		// Get all the clusters that GDM knows about
		this.installedGrids = this.consoleHandle.getAllInstalledGridName();

		// get the source cluster
		this.sourceCluster = GdmUtils.getConfiguration("testconfig.IntegrationTest.sourceCluster");
		TestSession.logger.info("sourceCluster  = " + sourceCluster);
		if ( (this.sourceCluster != null) && ( ! this.installedGrids.contains(this.sourceCluster)) )  {
			fail("Source cluster is null or Specified a wrong source cluster that is not configured.");
		}

		// get the destination cluster
		this.destinationCluster = GdmUtils.getConfiguration("testconfig.IntegrationTest.destinationCluster");
		TestSession.logger.info("destinationCluster = " + destinationCluster);
		if ( (this.destinationCluster != null) && ( ! this.installedGrids.contains(this.destinationCluster)) )  {
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

			// since hcat is disabled, will do data-only replication
			this.HCAT_TYPE = this.TARGET_START_TYPE_DATAONLY;
		}
		TestSession.logger.info("HCAT_TYPE = " + HCAT_TYPE);

		String dur = GdmUtils.getConfiguration("testconfig.IntegrationTest.duration");
		if ( dur != null ) {
			this.duration = Integer.parseInt(dur);
		}
		
		this.freq  =  GdmUtils.getConfiguration("testconfig.IntegrationTest.frequency");
		if (this.freq != null) {
			if (this.freq.equals("hourly")) {
				this.frequency = 1;
			}
		} 
		
		// get cookie for the headless user.
		this.cookie = httpHandle.getBouncerCookie();
		this.workFlowHelper = new WorkFlowHelper();
	}
	
	@Test
	public void integrationTest() throws Exception {

		// check whether instance files are available on the specified source
		List<String> dates = getInstanceFileDates();
		assertTrue("Instance files dn't exists at " + ABF_DATA_PATH  +  "  on  " + this.sourceCluster , dates != null);

		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
		sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
		Calendar todayCal = Calendar.getInstance();
		Calendar LastdayCal = Calendar.getInstance();
		Calendar currentCal = Calendar.getInstance();

		long toDay = Long.parseLong(sdf.format(todayCal.getTime()));

		// set the duration for how long the data has to generate.
		LastdayCal.add(Calendar.DAY_OF_WEEK_IN_MONTH , this.duration);
		
		long lastDay = Long.parseLong(sdf.format(LastdayCal.getTime()));
		System.out.println(" Current date - "+ sdf.format(todayCal.getTime()));
		System.out.println(" Next date - "+ sdf.format(LastdayCal.getTime()));

		Calendar initialCal = Calendar.getInstance();
		Calendar futureCal = Calendar.getInstance();

		long intialMin = Long.parseLong(sdf.format(initialCal.getTime()));
		initialCal.add(Calendar.MINUTE, 1);
		long futureMin =  Long.parseLong(sdf.format(initialCal.getTime()));
		System.out.println(" intialMin   = " +  intialMin   + "  futureMin  =  "  + futureMin);
		SimpleDateFormat feed_sdf = new SimpleDateFormat("yyyyMMddHH");
		feed_sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
		while (toDay <= lastDay) {
			Date d = new Date();
			long initTime = Long.parseLong(sdf.format(d));
			
			if (initTime >= futureMin ) {
				initialCal = Calendar.getInstance();
				intialMin = Long.parseLong(feed_sdf.format(initialCal.getTime()));
				initialCal.add(Calendar.HOUR, this.frequency);
				futureMin =  Long.parseLong(feed_sdf.format(initialCal.getTime()));
				
				Calendar dataSetCal = Calendar.getInstance();
				long dataSetHourlyTimeStamp =  Long.parseLong(feed_sdf.format(dataSetCal.getTime()));
				this.dataSetName = "Integration_Testing_DS_" + dataSetHourlyTimeStamp + "00";
				System.out.println("dataSetName  = " + this.dataSetName);
				
				initialCal = null;
				
				// create  a dataset
				this.createDataSet();

				// activate the dataset
				this.consoleHandle.checkAndActivateDataSet(this.dataSetName);
				this.consoleHandle.sleep(40000);
				String datasetActivationTime = GdmUtils.getCalendarAsString();

				// check for replication workflow is success for each instance
				for (String date : dates ) {
					this.workFlowHelper.checkWorkFlow(this.dataSetName , "replication" , datasetActivationTime , date);
				}
				// TODO : Need to find the API to query HIVE and HCat for table creation and partition. We can use GDM REST API or Data discovery REST API

				// create a done file
				String finalDataPath = "/data/daqdev/abf/data/" + this.dataSetName ;
				CreateDoneFile createDoneFile = new CreateDoneFile( this.destinationCluster , finalDataPath);
				createDoneFile.execute();
				
				// check whether DONE file is created.
				assertTrue("Failed  to create done for " + finalDataPath , createDoneFile.isDoneFileCreated() == true );
				
				// deactivate the dataset
				this.tearDown();
			}
			
			this.consoleHandle.sleep(60000);
			d = new Date();
			initTime = Long.parseLong(feed_sdf.format(d));
			d = null;
			toDay = Long.parseLong(feed_sdf.format(currentCal.getTime()));
			TestSession.logger.info("This is " + this.freq  + "  feed. Feed will be started " + this.freq);
			TestSession.logger.info("Next workflow will start @ " + futureMin   + "  Current  time = " + initTime);
		}
	}
	
	private void prepareJobFile(String propertyFileName) {
		Calendar cal = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
		long pipeLineInstance = Long.parseLong(sdf.format(cal.getTime()));
		
	}

	/**
	 * Create a dataset specification configuration file.
	 */
	private void createDataSet() {
		String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/ABFHcatDataSet.xml");
		String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.dataSetName, dataSetConfigFile);
		
		// removed the dependency on base dataset.
		//String feedName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Parameters" , "value");
		dataSetXml = dataSetXml.replaceAll("TARGET1_NAME", this.destinationCluster );
		dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", this.dataSetName);
		dataSetXml = dataSetXml.replaceAll("FEED_NAME", "temp" );
		dataSetXml = dataSetXml.replaceAll("FEED_STATS", "temp" + "_stats" );
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
	public List<String> getInstanceFileDates() {
		JSONArray jsonArray = null;
		List<String> instanceDates = new ArrayList<String>();
		String testURL = this.consoleHandle.getConsoleURL() + this.HADOOP_LS_PATH + this.SOURCE_NAME + "&path=" + ABF_DATA_PATH + "&format=json";
		TestSession.logger.info("Test url = " + testURL);
		com.jayway.restassured.response.Response res = given().cookie(this.cookie).get(testURL);
		assertTrue("Failed to get the response  " + res , (res != null ) );

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
						instanceDates.add(dt);
					}	
				}
			}
			return instanceDates;
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
