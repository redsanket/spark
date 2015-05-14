package hadooptest.gdm.regression;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.cluster.gdm.GeneratePerformanceFeeds;
import hadooptest.cluster.gdm.HTTPHandle;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.XMLConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * TestCase : Verify whether instance files are copied to the targets( should copy all the instance files before the start date, 
 * any instance available after the start date should be ignored ). 
 *
 */
public class TestDyamicStartTypeOffSetAndEndTypeFixedWithInstaceAfterStartDate extends TestSession {

	private ConsoleHandle consoleHandle;
	private Configuration conf;
	private String dataSetName;
	private String datasetActivationTime;
	private String deploymentSuffixName;
	private WorkFlowHelper workFlowHelperObj = null;
	private GeneratePerformanceFeeds generatePerformanceFeeds  = null;
	private String targetGrid1;
	private String targetGrid2;
	private String cookie;
	private String url;
	private static final String baseDataSetName = "VerifyAcqRepRetWorkFlowExecutionSingleDate";
	private static final int SUCCESS = 200;
	final static Charset ENCODING = StandardCharsets.UTF_8;
	private static final String HCAT_TYPE = "DataOnly";
	private String START_DATE;
	private String INSTANCE_DATE_1;
	private String INSTANCE_DATE_2;
	private String INSTANCE_DATE_3;
	private List<String> instanceList;
	private Map<String, String> instanceDateMap = new HashMap<String,String>();
	
	@BeforeClass
	public static void startTestSession() throws Exception {
		TestSession.start();
	}

	@Before
	public void setUp() throws NumberFormatException, Exception {
		String configPath = Util.getResourceFullPath("gdm/conf/config.xml");
		this.conf = new XMLConfiguration(configPath);
		this.deploymentSuffixName = this.conf.getString("hostconfig.console.deployment-suffix-name");
		TestSession.logger.info("Suffix name = " + this.deploymentSuffixName);
		this.consoleHandle = new ConsoleHandle();
		HTTPHandle httpHandle = new HTTPHandle(); 
		this.cookie = httpHandle.getBouncerCookie();
		this.url = this.consoleHandle.getConsoleURL();
		this.dataSetName = "TestDyamicStartTypeFixedAndEndTypeOffSet_" + System.currentTimeMillis();
		this.generatePerformanceFeeds  = new GeneratePerformanceFeeds(this.dataSetName , this.deploymentSuffixName.trim());

		this.workFlowHelperObj = new WorkFlowHelper();

		List<String> targetGrids = this.consoleHandle.getUniqueGrids();
		if (targetGrids != null  && targetGrids.size() >= 2) {
			targetGrid1 = targetGrids.get(0);
			targetGrid2 = targetGrids.get(1);
		} else {
			assertTrue("There is no enough grid installed to test this testcase." , true);
		}
		instanceList = new ArrayList<String>();
		Calendar cal = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

		// instance 1 date , 50 days instance are generated
		cal.add(Calendar.DAY_OF_MONTH, -50);	// 50 days
		this.INSTANCE_DATE_1 = sdf.format(cal.getTime());
		instanceList.add(this.INSTANCE_DATE_1);
		instanceDateMap.put("50-Days", this.INSTANCE_DATE_1);
		
		// reset the value to current day
		cal.add(Calendar.DAY_OF_MONTH, 50);

		// instance 2 date , 40 days instance are generated
		cal.add(Calendar.DAY_OF_MONTH, -40); // 40 days
		this.INSTANCE_DATE_2 = sdf.format(cal.getTime());
		instanceList.add(this.INSTANCE_DATE_2);
		instanceDateMap.put("40-Days", this.INSTANCE_DATE_2);
		
		// reset the value to current day
		cal.add(Calendar.DAY_OF_MONTH, 40);

		// 30 days, but instance are not generated , but where as this date is considered as start date
		cal.add(Calendar.DAY_OF_MONTH, -30);  
		this.START_DATE = sdf.format(cal.getTime());
		instanceDateMap.put("30-Days - startDate", this.START_DATE );
		
		// reset the value to current day
		cal.add(Calendar.DAY_OF_MONTH, 30);

		// instance 3 date, 20 days instance are generated
		cal.add(Calendar.DAY_OF_MONTH, -20);
		this.INSTANCE_DATE_3 = sdf.format(cal.getTime());
		instanceList.add(this.INSTANCE_DATE_3);
		instanceDateMap.put("20-Days", this.INSTANCE_DATE_3);
	}

	@Test
	public void testWorkFlowWithOutEndDate() throws Exception {

		// generate data
		for ( String instance : this.instanceList) {
			generatePerformanceFeeds.generateFeed(instance);
		}
		
		TestSession.logger.info("************************************************************************");
		Calendar cal = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		TestSession.logger.info("Start Date - " + this.START_DATE);
		TestSession.logger.info("Current date - " + sdf.format(cal.getTime()));
		 cal.add(Calendar.DAY_OF_MONTH, -1);
		 String endDateStr = sdf.format(cal.getTime());
		TestSession.logger.info("End date ( offset value is 1) " + endDateStr);
		TestSession.logger.info("Following are the dates that instances are generated");
		List<String> logList = new ArrayList<String> ();
		for (String key : instanceDateMap.keySet()) {
			if (!key.contains("startDate")) {
				logList.add( instanceDateMap.get(key));
				TestSession.logger.info( key + " - "  + instanceDateMap.get(key));
			}
		}
		
		// collect all the instance that are going to get discovered.
		List<String> discoveredInstanceList = new ArrayList<String>();
		
		TestSession.logger.info("Discovert should happen between " + this.START_DATE  + "  and " + endDateStr );
		Date startDate = sdf.parse(this.START_DATE);
		Date endDate = sdf.parse(endDateStr);
		for (String instance : logList) {
			Date dt = sdf.parse(instance);
			if ( dt.compareTo(startDate) > 0 && dt.compareTo(endDate) < 0) {
				discoveredInstanceList.add(instance);
				TestSession.logger.info(instance);
			} else if ( dt.compareTo(startDate) == 0) {
				discoveredInstanceList.add(instance);
				TestSession.logger.info(instance);
			} else if ( dt.compareTo(endDate ) == 0) {
				discoveredInstanceList.add(instance);
				TestSession.logger.info(instance);
			}
		}
		TestSession.logger.info("************************************************************************");
		
		// create a dataset
		createDataSetForAcqRep( );

		// check for acquisition workflow
		for ( String instance : discoveredInstanceList ) {
			this.workFlowHelperObj.checkWorkFlow(this.dataSetName, "acquisition", this.datasetActivationTime , instance);
			
			// check whether instance files are available on target1
			this.checkInstanceFiles(this.targetGrid1 , "acquisition" , this.dataSetName , instance);
		}
		
		//check for replication workflow
		for ( String instance : discoveredInstanceList ) {
			this.workFlowHelperObj.checkWorkFlow(this.dataSetName, "replication", this.datasetActivationTime, instance);
			
			// check whether instance files are available on target2
			this.checkInstanceFiles(this.targetGrid2 , "replication" , this.dataSetName , instance);
		}
	}

	/**
	 * Check whether instance are availabe on the target
	 * @param targetName
	 * @param facetName
	 * @param dataSetName
	 */
	private void checkInstanceFiles(String targetName , String facetName , String dataSetName , String instance) {
		String hadoopLSURL =  this.url + "/console/api/admin/hadoopls?dataSource=" + targetName + "&dataSet=" + dataSetName + "&format=json";
		TestSession.logger.info("testURL = " + hadoopLSURL);
		com.jayway.restassured.response.Response response = given().cookie( this.cookie).get(hadoopLSURL);
		assertTrue("Failed to get the respons = " + hadoopLSURL , (response != null) );
		boolean instanceFound = false;
		JSONArray hadoopLSJsonArray = this.consoleHandle.convertResponseToJSONArray(response, "Files");
		if (hadoopLSJsonArray.size() > 0) {
			Iterator iterator = hadoopLSJsonArray.iterator();
			while (iterator.hasNext()) {
				JSONObject hadoopLSJsonObject = (JSONObject) iterator.next();
				String isDirectory = hadoopLSJsonObject.getString("Directory");
				if( isDirectory.equals("yes")) {
					String path = hadoopLSJsonObject.getString("Path").trim();
					if (path.contains(instance) == true) {
						instanceFound = true;
					} 
				}
			}
			assertTrue( instance + " instance failed to copy to " + facetName , instanceFound == true );
		}
	}

	/**
	 * Create a dataset & activate it.
	 * @throws Exception
	 */
	public void createDataSetForAcqRep( ) throws Exception {
		String sourceName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Sources" , "name");
		String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/" + "DynamicStartEndDate.xml");
		String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.dataSetName, dataSetConfigFile);
		TestSession.logger.info("**** DataSet Name = " + this.dataSetName   + " ********** ");
		dataSetXml = dataSetXml.replaceAll("FEED_NAME", this.dataSetName );
		dataSetXml = dataSetXml.replaceAll("FEED_STATS", this.dataSetName + "_stats" );
		dataSetXml = dataSetXml.replaceAll("SOURCE_NAME", sourceName );
		dataSetXml = dataSetXml.replaceAll("TARGET1", this.targetGrid1 );
		dataSetXml = dataSetXml.replaceAll("TARGET2", this.targetGrid2 );
		dataSetXml = dataSetXml.replaceAll("TARGET_ONE_TYPE_START", "Fixed" );
		dataSetXml = dataSetXml.replaceAll("TARGET_ONE_START_DATE", this.START_DATE );
		dataSetXml = dataSetXml.replaceAll("TARGET_ONE_TYPE_END", "Offset" );
		dataSetXml = dataSetXml.replaceAll("TARGET_ONE_END_DATE", "1" );
		dataSetXml = dataSetXml.replaceAll("TARGET_TWO_TYPE_START", "Fixed" );
		dataSetXml = dataSetXml.replaceAll("TARGET_TWO_START_DATE", this.START_DATE );
		dataSetXml = dataSetXml.replaceAll("TARGET_TWO_TYPE_END", "Offset" );
		dataSetXml = dataSetXml.replaceAll("TARGET_TWO_END_DATE", "1");
		dataSetXml = dataSetXml.replaceAll("TABLE_NAME", this.dataSetName);
		dataSetXml = dataSetXml.replaceAll("GROUP_NAME", "users");
		dataSetXml = dataSetXml.replaceAll("DATA_OWNER", "");

		TestSession.logger.info("**** DataSet Name = " + dataSetXml   + " ********** ");
		Response response = this.consoleHandle.createDataSet(this.dataSetName, dataSetXml);
		if (response.getStatusCode() != SUCCESS) {
			try {
				throw new Exception("Response status code is " + response.getStatusCode() + ", expected 200.");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		this.consoleHandle.sleep(10000);

		// activate the dataset
		response = this.consoleHandle.activateDataSet(dataSetName);
		assertTrue("Failed to activate dataset " + dataSetName , response.getStatusCode() == SUCCESS);
		this.consoleHandle.sleep(30000);
		this.datasetActivationTime = GdmUtils.getCalendarAsString();
	}

	/**
	 * returns dataset path of the dataset
	 * @param dataSetName
	 * @return
	 */
	private String getDataSetDataPath(String dataSetName) {
		String path = "/data/daqqe/data/" + dataSetName  + "/%{date}"; 
		TestSession.logger.info("paht == " + path);
		return  path;
	}

	/**
	 * returns  count path of the dataset
	 * @param dataSetName
	 * @return
	 */
	private String getDataSetCountPath(String dataSetName) {
		return "/data/daqqe/count/" + dataSetName  + "/%{date}" ;
	}

	/**
	 * returns  schema path of the datase
	 * @param dataSetName
	 * @return
	 */
	private String getDataSetSchemaPath(String dataSetName) {
		return "/data/daqqe/schema/" + dataSetName  + "/%{date}" ;
	}	

	/**
	 * Method to deactivate the dataset(s)
	 */
	@After
	public void tearDown() throws Exception {
		Response response = consoleHandle.deactivateDataSet(this.dataSetName);
		assertEquals("ResponseCode - Deactivate DataSet", 200, response.getStatusCode());
		assertEquals("ActionName.", "terminate", response.getElementAtPath("/Response/ActionName").toString());
		assertEquals("ResponseId", "0", response.getElementAtPath("/Response/ResponseId").toString());
		assertEquals("ResponseMessage.", "Operation on " + this.dataSetName + " was successful.", response.getElementAtPath("/Response/ResponseMessage/[0]").toString());
	}
}

