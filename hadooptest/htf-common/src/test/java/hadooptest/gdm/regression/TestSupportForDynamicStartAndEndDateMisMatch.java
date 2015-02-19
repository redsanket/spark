package hadooptest.gdm.regression;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;

import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.HTTPHandle;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSupportForDynamicStartAndEndDateMisMatch  extends TestSession {

	private String cookie;
	private ConsoleHandle consoleHandle;
	private String consoleURL;
	private String dataSetName;
	private String targetGrid1;
	private String targetGrid2;
	private List <String> gridNamesList;
	private HTTPHandle httpHandle ;	
	private static final String baseDataSetName = "VerifyAcqRepRetWorkFlowExecutionDateRange";
	private static final int SUCCESS = 200;
	private static final int FAILED = 500;

	@BeforeClass
	public static void startTestSession() throws Exception {
		TestSession.start();
	}

	@Before
	public void setUp() throws NumberFormatException, Exception {
		this.consoleHandle = new ConsoleHandle();
		this.httpHandle = new HTTPHandle();
		this.cookie = httpHandle.getBouncerCookie();
		this.consoleURL = this.consoleHandle.getConsoleURL();
		this.dataSetName = "Test_Dynamic_StartEndDateType_" + System.currentTimeMillis();
		
		this.gridNamesList = this.consoleHandle.getAllGridNames();
		assertTrue("Expected minimum two grids , but got " + this.gridNamesList , this.gridNamesList.size() >= 2);
		this.targetGrid1 = this.gridNamesList.get(0);
		this.targetGrid2 = this.gridNamesList.get(1);
		TestSession.logger.info("Target1 = " + this.targetGrid1   + "    Target2 = " + this.targetGrid2);
	}

	/**
	 * TestCase : Verify whether dataset is not saved when target1 start date type is Fixed and target2 start date type is Offset.
	 * Expected Result : Http 500 error code is thrown
	 */
	@Test
	public void testTargetsStartDateTypeMisMatch1() {
		String feedName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Parameters" , "value");
		String sourceName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Sources" , "name");
		
		String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/DynamicStartEndDate.xml");
		String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.dataSetName, dataSetConfigFile);
		dataSetXml = dataSetXml.replaceAll("TARGET1", this.targetGrid1 );
		dataSetXml = dataSetXml.replaceAll("TARGET2", this.targetGrid2 );
		dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", this.dataSetName);
		dataSetXml = dataSetXml.replaceAll("FEED_NAME", feedName );
		dataSetXml = dataSetXml.replaceAll("FEED_STATS", feedName + "_stats" );
		dataSetXml = dataSetXml.replaceAll("SOURCE_NAME", sourceName );
		dataSetXml = dataSetXml.replace("TABLE_NAME", this.dataSetName);
		dataSetXml = dataSetXml.replace("HCAT_TYPE", "DataOnly");
		dataSetXml = dataSetXml.replace("TARGET_ONE_START_DATE_TYPE", "Fixed");
		dataSetXml = dataSetXml.replace("TARGET_ONE_END_DATE_TYPE", "Offset");
		dataSetXml = dataSetXml.replace("TARGET_TWO_START_DATE_TYPE", "Offset");
		dataSetXml = dataSetXml.replace("TARGET_TWO_END_DATE_TYPE", "Fixed");
		dataSetXml = dataSetXml.replace("TARGET_ONE_START_DATE", "20130725");
		dataSetXml = dataSetXml.replace("TARGET_ONE_END_DATE", "20220131");
		dataSetXml = dataSetXml.replace("TARGET_TWO_START_DATE", "20130725");
		dataSetXml = dataSetXml.replace("TARGET_TWO_END_DATE", "20220131");
		TestSession.logger.info(dataSetXml);
		Response res = this.consoleHandle.createDataSet(this.dataSetName, dataSetXml);
		if (res.getStatusCode() != FAILED) {
			fail("Expected to get HTTP 500, but got HTTP code = "  + res.getStatusCode()  + "   Reason = " + res.getResponseBodyAsString());
		}
	}
	
	/**
	 * TestCase : Verify whether dataset is not saved when target1 start date type is Offset and target2 start date type is Fixed.
	 * Expected Result : Http 500 error code is thrown
	 */
	@Test
	public void testTargetsStartDateTypeMisMatch2() {
		String feedName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Parameters" , "value");
		String sourceName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Sources" , "name");
		String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/DynamicStartEndDate.xml");
		String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.dataSetName, dataSetConfigFile);
		dataSetXml = dataSetXml.replaceAll("TARGET1", this.targetGrid1 );
		dataSetXml = dataSetXml.replaceAll("TARGET2", this.targetGrid2 );
		dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", this.dataSetName);
		dataSetXml = dataSetXml.replaceAll("FEED_NAME", feedName );
		dataSetXml = dataSetXml.replaceAll("FEED_STATS", feedName + "_stats" );
		dataSetXml = dataSetXml.replaceAll("SOURCE_NAME", sourceName );
		dataSetXml = dataSetXml.replace("TABLE_NAME", this.dataSetName);
		dataSetXml = dataSetXml.replace("HCAT_TYPE", "DataOnly");
		dataSetXml = dataSetXml.replace("TARGET_ONE_START_DATE_TYPE", "Offset");
		dataSetXml = dataSetXml.replace("TARGET_ONE_END_DATE_TYPE", "Fixed");
		dataSetXml = dataSetXml.replace("TARGET_TWO_START_DATE_TYPE", "Fixed");
		dataSetXml = dataSetXml.replace("TARGET_TWO_END_DATE_TYPE", "Offset");
		dataSetXml = dataSetXml.replace("TARGET_ONE_START_DATE", "20130725");
		dataSetXml = dataSetXml.replace("TARGET_ONE_END_DATE", "20220131");
		dataSetXml = dataSetXml.replace("TARGET_TWO_START_DATE", "20130725");
		dataSetXml = dataSetXml.replace("TARGET_TWO_END_DATE", "20220131");
		TestSession.logger.info(dataSetXml);
		Response res = this.consoleHandle.createDataSet(this.dataSetName, dataSetXml);
		if (res.getStatusCode() != FAILED) {
			fail("Expected to get HTTP 500, but got HTTP code = "  + res.getStatusCode()  + "   Reason = " + res.getResponseBodyAsString());
		}
	}
	
	/**
	 * TestCase : Verify whether dataset is not saved when target1 end date type is Fixed and target2 end date type is Offset.
	 * Expected Result : Http 500 error code is thrown
	 */
	@Test
	public void testTargetsEndDateTypeMisMatch1() {
		String feedName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Parameters" , "value");
		String sourceName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Sources" , "name");
		String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/DynamicStartEndDate.xml");
		String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.dataSetName, dataSetConfigFile);
		dataSetXml = dataSetXml.replaceAll("TARGET1", this.targetGrid1 );
		dataSetXml = dataSetXml.replaceAll("TARGET2", this.targetGrid2 );
		dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", this.dataSetName);
		dataSetXml = dataSetXml.replaceAll("FEED_NAME", feedName );
		dataSetXml = dataSetXml.replaceAll("FEED_STATS", feedName + "_stats" );
		dataSetXml = dataSetXml.replaceAll("SOURCE_NAME", sourceName );
		dataSetXml = dataSetXml.replace("TABLE_NAME", this.dataSetName);
		dataSetXml = dataSetXml.replace("HCAT_TYPE", "DataOnly");
		dataSetXml = dataSetXml.replace("TARGET_ONE_START_DATE_TYPE", "Fixed");
		dataSetXml = dataSetXml.replace("TARGET_ONE_END_DATE_TYPE", "Offset");
		dataSetXml = dataSetXml.replace("TARGET_TWO_START_DATE_TYPE", "Fixed");
		dataSetXml = dataSetXml.replace("TARGET_TWO_END_DATE_TYPE", "Fixed");
		dataSetXml = dataSetXml.replace("TARGET_ONE_START_DATE", "20130725");
		dataSetXml = dataSetXml.replace("TARGET_ONE_END_DATE", "20220131");
		dataSetXml = dataSetXml.replace("TARGET_TWO_START_DATE", "20130725");
		dataSetXml = dataSetXml.replace("TARGET_TWO_END_DATE", "20220131");
		TestSession.logger.info(dataSetXml);
		Response res = this.consoleHandle.createDataSet(this.dataSetName, dataSetXml);
		if (res.getStatusCode() != FAILED) {
			fail("Expected to get HTTP 500, but got HTTP code = "  + res.getStatusCode()  + "   Reason = " + res.getResponseBodyAsString());
		}
	}
	
	
	/**
	 * TestCase : Verify whether dataset is not saved when target1 end date type is Offset and target2 end date type is Fixed.
	 * Expected Result : Http 500 error code is thrown
	 */
	@Test
	public void testTargetsEndDateTypeMisMatch2() {
		String feedName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Parameters" , "value");
		String sourceName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Sources" , "name");
		String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/DynamicStartEndDate.xml");
		String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.dataSetName, dataSetConfigFile);
		dataSetXml = dataSetXml.replaceAll("TARGET1", this.targetGrid1 );
		dataSetXml = dataSetXml.replaceAll("TARGET2", this.targetGrid2 );
		dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", this.dataSetName);
		dataSetXml = dataSetXml.replaceAll("FEED_NAME", feedName );
		dataSetXml = dataSetXml.replaceAll("FEED_STATS", feedName + "_stats" );
		dataSetXml = dataSetXml.replaceAll("SOURCE_NAME", sourceName );
		dataSetXml = dataSetXml.replace("TABLE_NAME", this.dataSetName);
		dataSetXml = dataSetXml.replace("HCAT_TYPE", "DataOnly");
		dataSetXml = dataSetXml.replace("TARGET_ONE_START_DATE_TYPE", "Fixed");
		dataSetXml = dataSetXml.replace("TARGET_ONE_END_DATE_TYPE", "Fixed");
		dataSetXml = dataSetXml.replace("TARGET_TWO_START_DATE_TYPE", "Fixed");
		dataSetXml = dataSetXml.replace("TARGET_TWO_END_DATE_TYPE", "Offset");
		dataSetXml = dataSetXml.replace("TARGET_ONE_START_DATE", "20130725");
		dataSetXml = dataSetXml.replace("TARGET_ONE_END_DATE", "20220131");
		dataSetXml = dataSetXml.replace("TARGET_TWO_START_DATE", "20130725");
		dataSetXml = dataSetXml.replace("TARGET_TWO_END_DATE", "20220131");
		TestSession.logger.info(dataSetXml);
		Response res = this.consoleHandle.createDataSet(this.dataSetName, dataSetXml);
		if (res.getStatusCode() != FAILED) {
			fail("Expected to get HTTP 500, but got HTTP code = "  + res.getStatusCode()  + "   Reason = " + res.getResponseBodyAsString());
		}
	}
	
	/**
	 * TestCase : Verify whether dataset is not saved, when target one start date is missing.
	 * Expected Result : Http 500 error code is thrown
	 */
	@Test
	public void testTargetStartDateIsMissing() {
		String feedName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Parameters" , "value");
		String sourceName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Sources" , "name");
		String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/DynamicStartEndDate.xml");
		String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.dataSetName, dataSetConfigFile);
		
		dataSetXml = dataSetXml.replaceAll("TARGET1", this.targetGrid1 );
		dataSetXml = dataSetXml.replaceAll("TARGET2", this.targetGrid2 );
		dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", this.dataSetName);
		dataSetXml = dataSetXml.replaceAll("FEED_NAME", feedName );
		dataSetXml = dataSetXml.replaceAll("FEED_STATS", feedName + "_stats" );
		dataSetXml = dataSetXml.replaceAll("SOURCE_NAME", sourceName );
		dataSetXml = dataSetXml.replace("TABLE_NAME", this.dataSetName);
		dataSetXml = dataSetXml.replace("HCAT_TYPE", "DataOnly");
		dataSetXml = dataSetXml.replace("TARGET_ONE_START_DATE_TYPE", "Fixed");
		dataSetXml = dataSetXml.replace("TARGET_ONE_END_DATE_TYPE", "Fixed");
		dataSetXml = dataSetXml.replace("TARGET_TWO_START_DATE_TYPE", "Fixed");
		dataSetXml = dataSetXml.replace("TARGET_TWO_END_DATE_TYPE", "Fixed");
		dataSetXml = dataSetXml.replace("TARGET_ONE_START_DATE", "");
		dataSetXml = dataSetXml.replace("TARGET_ONE_END_DATE", "20220131");
		dataSetXml = dataSetXml.replace("TARGET_TWO_START_DATE", "20130725");
		dataSetXml = dataSetXml.replace("TARGET_TWO_END_DATE", "20220131");
		TestSession.logger.info(dataSetXml);
		Response res = this.consoleHandle.createDataSet(this.dataSetName, dataSetXml);
		if (res.getStatusCode() != FAILED) {
			fail("Expected to get HTTP 500, but got HTTP code = "  + res.getStatusCode()  + "   Reason = " + res.getResponseBodyAsString());
		}
	}
	
	
	/**
	 * TestCase : Verify whether dataset is not saved, when target one end date is missing.
	 * Expected Result : Http 500 error code is thrown
	 */
	@Test
	public void testTargetEndDateIsMissing() {
		String feedName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Parameters" , "value");
		String sourceName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Sources" , "name");
		String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/DynamicStartEndDate.xml");
		String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.dataSetName, dataSetConfigFile);
		dataSetXml = dataSetXml.replaceAll("TARGET1", this.targetGrid1 );
		dataSetXml = dataSetXml.replaceAll("TARGET2", this.targetGrid2 );
		dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", this.dataSetName);
		dataSetXml = dataSetXml.replaceAll("FEED_NAME", feedName );
		dataSetXml = dataSetXml.replaceAll("FEED_STATS", feedName + "_stats" );
		dataSetXml = dataSetXml.replaceAll("SOURCE_NAME", sourceName );
		dataSetXml = dataSetXml.replace("TABLE_NAME", this.dataSetName);
		dataSetXml = dataSetXml.replace("HCAT_TYPE", "DataOnly");
		dataSetXml = dataSetXml.replace("TARGET_ONE_START_DATE_TYPE", "Fixed");
		dataSetXml = dataSetXml.replace("TARGET_ONE_END_DATE_TYPE", "Fixed");
		dataSetXml = dataSetXml.replace("TARGET_TWO_START_DATE_TYPE", "Fixed");
		dataSetXml = dataSetXml.replace("TARGET_TWO_END_DATE_TYPE", "Fixed");
		dataSetXml = dataSetXml.replace("TARGET_ONE_START_DATE", "20130725");
		dataSetXml = dataSetXml.replace("TARGET_ONE_END_DATE", "");
		dataSetXml = dataSetXml.replace("TARGET_TWO_START_DATE", "20130725");
		dataSetXml = dataSetXml.replace("TARGET_TWO_END_DATE", "20220131");
		TestSession.logger.info(dataSetXml);
		Response res = this.consoleHandle.createDataSet(this.dataSetName, dataSetXml);
		if (res.getStatusCode() != FAILED) {
			fail("Expected to get HTTP 500, but got HTTP code = "  + res.getStatusCode()  + "   Reason = " + res.getResponseBodyAsString());
		}
	}
	
	@Test
	public void testOldStyleStartEndDate() {
		String feedName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Parameters" , "value");
		String sourceName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Sources" , "name");
		String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/DynamicStartEndDate.xml");
		String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.dataSetName, dataSetConfigFile);
		dataSetXml = dataSetXml.replaceAll("TARGET1", this.targetGrid1 );
		dataSetXml = dataSetXml.replaceAll("TARGET2", this.targetGrid2 );
		dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", this.dataSetName);
		dataSetXml = dataSetXml.replaceAll("FEED_NAME", feedName );
		dataSetXml = dataSetXml.replaceAll("FEED_STATS", feedName + "_stats" );
		dataSetXml = dataSetXml.replaceAll("SOURCE_NAME", sourceName );
		dataSetXml = dataSetXml.replace("TABLE_NAME", this.dataSetName);
		dataSetXml = dataSetXml.replace("HCAT_TYPE", "DataOnly");
		dataSetXml = dataSetXml.replace("TARGET_ONE_START_DATE_TYPE", "Fixed");
		dataSetXml = dataSetXml.replace("TARGET_ONE_END_DATE_TYPE", "Fixed");
		dataSetXml = dataSetXml.replace("TARGET_TWO_START_DATE_TYPE", "Fixed");
		dataSetXml = dataSetXml.replace("TARGET_TWO_END_DATE_TYPE", "Fixed");
		dataSetXml = dataSetXml.replace("TARGET_ONE_START_DATE", "20130725");
		dataSetXml = dataSetXml.replace("TARGET_ONE_END_DATE", "");
		dataSetXml = dataSetXml.replace("TARGET_TWO_START_DATE", "20130725");
		dataSetXml = dataSetXml.replace("TARGET_TWO_END_DATE", "20220131");
		TestSession.logger.info(dataSetXml);
		Response res = this.consoleHandle.createDataSet(this.dataSetName, dataSetXml);
		if (res.getStatusCode() != FAILED) {
			fail("Expected to get HTTP 500, but got HTTP code = "  + res.getStatusCode()  + "   Reason = " + res.getResponseBodyAsString());
		}
	}
}
