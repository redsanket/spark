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
import java.util.Iterator;
import java.util.List;

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
	private String INSTANCE_DATE_1 = "50";
	private String INSTANCE_DATE_2 = "40";
	private String INSTANCE_DATE_3  = "20";
	private List<String> instanceList;
	
	@BeforeClass
	public static void startTestSession() throws Exception {
		TestSession.start();
	}

	@Before
	public void setUp() throws NumberFormatException, Exception {
		String configPath = Util.getResourceFullPath("gdm/conf/config.xml");
		this.conf = new XMLConfiguration(configPath);
		this.deploymentSuffixName = this.conf.getString("hostconfig.console.deployment-suffix-name");
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

		// instance 1 date
		cal.add(Calendar.DAY_OF_MONTH, -Integer.parseInt(this.INSTANCE_DATE_1));
		this.INSTANCE_DATE_1 = sdf.format(cal.getTime());
		instanceList.add(this.INSTANCE_DATE_1);

		// instance 2 date
		cal.add(Calendar.DAY_OF_MONTH, -Integer.parseInt(this.INSTANCE_DATE_2));
		this.INSTANCE_DATE_2 = sdf.format(cal.getTime());
		instanceList.add(this.INSTANCE_DATE_2);

		// start date ( instance will not be generated for start date.
		cal.add(Calendar.DAY_OF_MONTH, -20);
		this.START_DATE = sdf.format(cal.getTime());

		// instance 3 date
		cal.add(Calendar.DAY_OF_MONTH, -Integer.parseInt(this.INSTANCE_DATE_3));
		this.INSTANCE_DATE_3 = sdf.format(cal.getTime());
		instanceList.add(this.INSTANCE_DATE_3);

	}

	@Test
	public void testWorkFlowWithOutEndDate() throws Exception {

		// generate data
		for ( String instance : this.instanceList) {
			generatePerformanceFeeds.generateFeed(instance);
		}

		// create a dataset
		createDataSetForAcqRep( );

		// check for acquisition workflow
		this.workFlowHelperObj.checkWorkFlow(this.dataSetName, "acquisition", this.datasetActivationTime , this.INSTANCE_DATE_1);
		this.workFlowHelperObj.checkWorkFlow(this.dataSetName, "acquisition", this.datasetActivationTime , this.INSTANCE_DATE_2);

		// check whether instance files are available on target1
		this.checkInstanceFiles(this.targetGrid1 , "acquisition" , this.dataSetName);

		//check for replication workflow
		this.workFlowHelperObj.checkWorkFlow(this.dataSetName, "replication", this.datasetActivationTime, this.INSTANCE_DATE_1);
		this.workFlowHelperObj.checkWorkFlow(this.dataSetName, "replication", this.datasetActivationTime, this.INSTANCE_DATE_2);
		
		// check whether instance files are available on target2
		this.checkInstanceFiles(this.targetGrid2 , "replication" , this.dataSetName);
	}

	/**
	 * Check whether instance are availabe on the target
	 * @param targetName
	 * @param facetName
	 * @param dataSetName
	 */
	private void checkInstanceFiles(String targetName , String facetName , String dataSetName) {
		String hadoopLSURL =  this.url + "/console/api/admin/hadoopls?dataSource=" + targetName + "&dataSet=" + dataSetName + "&format=json";
		TestSession.logger.info("testURL = " + hadoopLSURL);
		com.jayway.restassured.response.Response response = given().cookie( this.cookie).get(hadoopLSURL);
		assertTrue("Failed to get the respons = " + hadoopLSURL , (response != null) );
		boolean instance_1_Found = false,instance_2_Found = false , instance_3_Found = false;
		JSONArray hadoopLSJsonArray = this.consoleHandle.convertResponseToJSONArray(response, "Files");
		if (hadoopLSJsonArray.size() > 0) {
			Iterator iterator = hadoopLSJsonArray.iterator();
			while (iterator.hasNext()) {
				JSONObject hadoopLSJsonObject = (JSONObject) iterator.next();
				String isDirectory = hadoopLSJsonObject.getString("Directory");
				if( isDirectory.equals("yes")) {
					String path = hadoopLSJsonObject.getString("Path").trim();
					if (path.contains(this.INSTANCE_DATE_1) == true) {
						instance_1_Found = true;
					} else if (path.contains(this.INSTANCE_DATE_2) == true ) {
						instance_2_Found = true;
					} else if (path.contains(this.INSTANCE_DATE_3) == true) {
						instance_3_Found = true;
					}
				}
			}
			assertTrue( this.INSTANCE_DATE_1 + " instance failed to copy to " + facetName , instance_1_Found == true );
			assertTrue( this.INSTANCE_DATE_2 + " instance failed to copy to " + facetName , instance_2_Found == true );
			assertTrue( this.INSTANCE_DATE_3 + " instance should have not copied to " + facetName , instance_3_Found == false );
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
		dataSetXml = dataSetXml.replaceAll("TARGET_ONE_START_DATE_TYPE", "fixed" );
		dataSetXml = dataSetXml.replaceAll("TARGET_ONE_START_DATE", this.START_DATE );
		dataSetXml = dataSetXml.replaceAll("TARGET_ONE_END_DATE_TYPE", "offset" );
		dataSetXml = dataSetXml.replaceAll("TARGET_ONE_END_DATE", "1" );
		dataSetXml = dataSetXml.replaceAll("TARGET_TWO_START_DATE_TYPE", "fixed" );
		dataSetXml = dataSetXml.replaceAll("TARGET_TWO_START_DATE", this.START_DATE );
		dataSetXml = dataSetXml.replaceAll("TARGET_TWO_END_DATE_TYPE", "offset" );
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

