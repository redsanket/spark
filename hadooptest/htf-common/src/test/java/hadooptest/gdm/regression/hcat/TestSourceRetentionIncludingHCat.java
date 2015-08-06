package hadooptest.gdm.regression.hcat;

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
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * 
 * Test Case : Support HCat retention on replication sources
 * https://jira.corp.yahoo.com/browse/GDM-304
 *
 */
public class TestSourceRetentionIncludingHCat  extends TestSession {

	private ConsoleHandle consoleHandle;
	private List<String> hcatSupportedGrid;
	private String acquisitionDataSetName;
	private String retentionDataSetName;
	private HTTPHandle httpHandle = null; 
	private String targetGrid1;
	private String targetGrid2;
	private String datasetActivationTime;
	private String baseDataSetName = "VerifyAcqRepRetWorkFlowExecutionSingleDate";
	private WorkFlowHelper workFlowHelper;
	private HCatHelper hcatHelperObject = null;
	private List<String> dataSetNames;
	private static final String HCatList = "/api/admin/hcat/table/list";
	private static final String HCAT_ENABLED = "TRUE";
	private static final String DB_NAME = "gdm";
	private static final int SUCCESS = 200;
	private static final int SLEEP_TIME = 50000;
	private static final String DATABASE_NAME = "gdm";
	private static final String HCAT_TYPE = "Mixed";

	@BeforeClass
	public static void startTestSession() throws Exception {
		TestSession.start();
	}

	@Before
	public void setUp() throws NumberFormatException, Exception {

		workFlowHelper = new WorkFlowHelper();
		this.hcatHelperObject = new HCatHelper();
		this.consoleHandle = new ConsoleHandle();
		this.httpHandle = new HTTPHandle();
		dataSetNames = new ArrayList<String>();

		// get all the hcat supported clusters
		hcatSupportedGrid = this.consoleHandle.getHCatEnabledGrid();

		this.acquisitionDataSetName = "TestHCatAcquisition_" + System.currentTimeMillis();
		this.retentionDataSetName = "TestHCatSourceRetention_" + System.currentTimeMillis();

		// check whether we have two hcat cluster one for acquisition and replication
		if (hcatSupportedGrid.size() < 2) {
			throw new Exception("Unable to run " + this.acquisitionDataSetName  +" 2 grid datasources are required.");
		}

		this.targetGrid1 = hcatSupportedGrid.get(0).trim();
		this.targetGrid2 = hcatSupportedGrid.get(1).trim();
		TestSession.logger.info("Using grids " + this.targetGrid1 + " , " + this.targetGrid2 );

		// check whether hcat is enabled on target cluster
		boolean targetHCatSupported = this.consoleHandle.isHCatEnabledForDataSource(this.targetGrid1);
		if (!targetHCatSupported) {
			this.consoleHandle.modifyDataSource(this.targetGrid1, "HCatSupported", "FALSE", "TRUE");
		}
	
	}

	@Test
	public void testHCatDiscovery( ) throws Exception {
		// acquisition
		{	
			// create acquisition dataset.
			createAcquisitionDataSet("DoAsAcquisitionDataSet.xml");
			this.consoleHandle.sleep(SLEEP_TIME);

			// activate acquisition dataset
			this.consoleHandle.checkAndActivateDataSet(this.acquisitionDataSetName);
			this.datasetActivationTime = GdmUtils.getCalendarAsString();

			// check for acquisition workflow.
			workFlowHelper.checkWorkFlow(this.acquisitionDataSetName , "acquisition" , this.datasetActivationTime);		

			// Get target1 hcat sever name
			String acquisitionHCatServerName = this.hcatHelperObject.getHCatServerHostName(this.targetGrid1);
			assertTrue("Failed to get the HCatServer Name for " + this.targetGrid1 , acquisitionHCatServerName != null);
			TestSession.logger.info("Hcat Server for " + this.targetGrid1  + "  is " + acquisitionHCatServerName);

			String tableOwner = this.hcatHelperObject.getHCatTableName(this.targetGrid1 , this.acquisitionDataSetName , "acquisition");
            String tableName = this.acquisitionDataSetName.toLowerCase().replaceAll("-", "_");
            assertTrue("Expected " + tableOwner  + "  data owner but got " + tableOwner ,   tableOwner.equals(tableName) == true);
		}

		// retention
		{
			// create retention dataset.
			createRetentionDataSet("RetentionOnSourceDataSet.xml");
			this.consoleHandle.sleep(SLEEP_TIME);

			// activate retention dataset
			this.consoleHandle.checkAndActivateDataSet(this.retentionDataSetName);
			this.datasetActivationTime = GdmUtils.getCalendarAsString();

			// check for replication workflow
			this.workFlowHelper.checkWorkFlow(this.retentionDataSetName , "retention" , this.datasetActivationTime  );
		}
	}

	/**
	 * Method that creates a acquisition dataset
	 * @param dataSetFileName - name of the acquisition dataset
	 */
	private void createAcquisitionDataSet( String dataSetFileName) {

		String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/" + dataSetFileName);
		String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.acquisitionDataSetName, dataSetConfigFile);
		String feedName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Parameters" , "value");
		String sourceName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Sources" , "name");

		dataSetXml = dataSetXml.replaceAll("TARGET1", this.targetGrid1 );
		dataSetXml = dataSetXml.replaceAll("GROUP_NAME","users");
		dataSetXml = dataSetXml.replaceAll("owner=\"DATA_OWNER\"", "");
		dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", this.acquisitionDataSetName);
		dataSetXml = dataSetXml.replaceAll("FEED_NAME", feedName );
		dataSetXml = dataSetXml.replaceAll("FEED_STATS", feedName + "_stats" );
		dataSetXml = dataSetXml.replaceAll("FDI_SERVER_NAME", sourceName );
		dataSetXml = dataSetXml.replaceAll("HCAT_TABLE_NAME", this.acquisitionDataSetName.trim());
		dataSetXml = dataSetXml.replace("HCAT_TYPE", this.HCAT_TYPE);
		dataSetXml = dataSetXml.replaceAll("DATABASE_NAME", this.DATABASE_NAME);
		dataSetXml = dataSetXml.replaceAll("CUSTOM_DATA_PATH", getCustomPath("data", this.acquisitionDataSetName) );
		dataSetXml = dataSetXml.replaceAll("CUSTOM_COUNT_PATH", getCustomPath("count", this.acquisitionDataSetName) );
		dataSetXml = dataSetXml.replaceAll("CUSTOM_SCHEMA_PATH", getCustomPath("schema", this.acquisitionDataSetName));
		
		// this is doAs specific no need of this tag
		dataSetXml = dataSetXml.replaceAll("<RunAsOwner>acquisition</RunAsOwner>", "");

		Response response = this.consoleHandle.createDataSet(this.acquisitionDataSetName, dataSetXml);
		if (response.getStatusCode() != SUCCESS) {
			try {
				throw new Exception("Response status code is " + response.getStatusCode() + ", expected 200.");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		// add the aquisition dataset name, so that it can be use to deactivated dataset in tearDown method 
		dataSetNames.add(this.acquisitionDataSetName);
	}

	/**
	 * Method to create a replication dataset
	 * @param dataSetFileName - name of the replication dataset
	 */
	private void createRetentionDataSet(String dataSetFileName) {
		String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/" + dataSetFileName);
		String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.retentionDataSetName, dataSetConfigFile);
		String feedName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Parameters" , "value");

		dataSetXml = dataSetXml.replaceAll("TARGET1", this.targetGrid1 );
		dataSetXml = dataSetXml.replaceAll("TARGET2", this.targetGrid2 );
		dataSetXml = dataSetXml.replaceAll("GROUP_NAME", "users");
		dataSetXml = dataSetXml.replaceAll("owner=\"DATA_OWNER\"", "");
		dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", this.retentionDataSetName);
		dataSetXml = dataSetXml.replaceAll("FEED_NAME", feedName );
		dataSetXml = dataSetXml.replaceAll("FEED_STATS", feedName + "_stats" );
		dataSetXml = dataSetXml.replace("HCAT_TYPE", this.HCAT_TYPE);
		dataSetXml = dataSetXml.replaceAll("DATABASE_NAME", this.DATABASE_NAME);
		dataSetXml = dataSetXml.replaceAll("HCAT_TABLE_NAME", this.acquisitionDataSetName.toLowerCase());
		dataSetXml = dataSetXml.replaceAll("CUSTOM_DATA_PATH", getCustomPath("data" , this.acquisitionDataSetName));
		dataSetXml = dataSetXml.replaceAll("CUSTOM_COUNT_PATH", getCustomPath("count", this.acquisitionDataSetName));
		dataSetXml = dataSetXml.replaceAll("CUSTOM_SCHEMA_PATH", getCustomPath("schema", this.acquisitionDataSetName));

		Response response = this.consoleHandle.createDataSet(this.retentionDataSetName, dataSetXml);
		if (response.getStatusCode() != SUCCESS) {
			try {
				throw new Exception("Response status code is " + response.getStatusCode() + ", expected 200.");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		// add the retention dataset name, so that it can be use to deactivated dataset in tearDown method
		dataSetNames.add(this.retentionDataSetName);
	}

	/**
	 * Method to create a custom path for the dataset.
	 * @param pathType - its a string type which represents either data/count/schema
	 * @param dataSet - dataset name
	 * @return
	 */
	private String getCustomPath(String pathType , String dataSet) {
		return  "/data/daqdev/"+ pathType +"/"+ dataSet + "/%{date}";
	}

	/**
	 * deactivate the dataset(s)	
	 */
	@After
	public void tearDown() {
		for (String dataSetName : dataSetNames) {
			TestSession.logger.info("Deactivate "+ dataSetName  +"  dataset ");
			Response response = this.consoleHandle.deactivateDataSet(dataSetName);
			assertTrue("Failed to deactivate dataset " + dataSetName , response.getStatusCode() == SUCCESS);
			assertTrue("Expected terminate action name but got " + response.getElementAtPath("/Response/ActionName").toString() , response.getElementAtPath("/Response/ActionName").toString().equals("terminate") );
			assertTrue("Expected to get 0 response id, but got " + response.getElementAtPath("/Response/ResponseId").toString() , response.getElementAtPath("/Response/ResponseId").toString().equals("0"));
		}
	}
}
