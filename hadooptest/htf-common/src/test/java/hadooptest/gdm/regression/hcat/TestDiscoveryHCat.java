package hadooptest.gdm.regression.hcat;


import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.cluster.gdm.HCatHelper;
import hadooptest.cluster.gdm.HTTPHandle;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;

/**
 * 
 * Test Scenario  : Testcase to  test Discovery HCat feature.
 * Verify the following :
 * 	    a) Acquisition is workflow is successfully completed
 *  	b) Verify whether hcat table for acquisition is created.
 *  	c) Create a replication dataset with discovery type set to HCAT
 *  	c) Verify whether replication workflow is successfully completed
 *  	d) Verify whether hcat table for replication is created but with acquisition dataset table name
 *
 */
public class TestDiscoveryHCat extends TestSession {

	private ConsoleHandle consoleHandle;
	private List<String> hcatSupportedGrid;
	private String acquisitionDataSetName;
	private String replicationDataSetName;
	private HTTPHandle httpHandle = null; 
	private String targetGrid1;
	private String targetGrid2;
	private String cookie;
	private String datasetActivationTime;
	private String baseDataSetName = "VerifyAcqRepRetWorkFlowExecutionSingleDate";
	private WorkFlowHelper workFlowHelper;
	private HCatHelper hcatHelperObject = null;
	private List<String>dataSetNames;
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
		this.cookie = this.httpHandle.getBouncerCookie();

		dataSetNames = new ArrayList<String>();

		// get all the hcat supported clusters
		hcatSupportedGrid = this.consoleHandle.getHCatEnabledGrid();

		this.acquisitionDataSetName = "TestHCatDiscoveryAcquisition_" + System.currentTimeMillis();
		this.replicationDataSetName = "TestHCatDiscoveryReplication_" + System.currentTimeMillis();

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
		targetHCatSupported = this.consoleHandle.isHCatEnabledForDataSource(this.targetGrid2);
		if (!targetHCatSupported) {
			this.consoleHandle.modifyDataSource(this.targetGrid2, "HCatSupported", "FALSE", "TRUE");
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

			// check whether hcat table is created for Mixed HCatTargetType on acquisition facet's HCat server
			boolean isAcqusitionTableCreated = this.hcatHelperObject.isTableExists(acquisitionHCatServerName, this.acquisitionDataSetName , this.DATABASE_NAME);
			assertTrue("Failed to HCAT create table for " + this.acquisitionDataSetName , isAcqusitionTableCreated == true);
		}

		// replication
		{
			// create replication dataset.
			createReplicationDataSet("DoAsReplicationDataSet.xml");
			this.consoleHandle.sleep(SLEEP_TIME);

			// activate replication dataset
			this.consoleHandle.checkAndActivateDataSet(this.replicationDataSetName);
			this.datasetActivationTime = GdmUtils.getCalendarAsString();

			// check for replication workflow
			this.workFlowHelper.checkWorkFlow(this.replicationDataSetName , "replication" , this.datasetActivationTime  );

			// get Hcat server name for targetGrid2
			String replicationHCatServerName = this.hcatHelperObject.getHCatServerHostName(this.targetGrid2);
			assertTrue("Failed to get the HCatServer Name for " + this.targetGrid1 , replicationHCatServerName != null);
			TestSession.logger.info("Hcat Server for " + this.targetGrid1  + "  is " + replicationHCatServerName);

			// check whether hcat table is created for Mixed HCatTargetType on replication facet's HCat server.
			boolean isReplicationTableCreated = this.hcatHelperObject.isTableExists(replicationHCatServerName, this.acquisitionDataSetName , this.DATABASE_NAME);
			assertTrue("Failed to create HCAT table for " + this.replicationDataSetName , isReplicationTableCreated == true);
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
	private void createReplicationDataSet(String dataSetFileName) {
		String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/" + dataSetFileName);
		String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.replicationDataSetName, dataSetConfigFile);
		String feedName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Parameters" , "value");

		dataSetXml = dataSetXml.replaceAll("TARGET1", this.targetGrid2 );
		dataSetXml = dataSetXml.replaceAll("GROUP_NAME", "users");
		dataSetXml = dataSetXml.replaceAll("owner=\"DATA_OWNER\"", "");
		dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", this.replicationDataSetName);
		dataSetXml = dataSetXml.replaceAll("FEED_NAME", feedName );
		dataSetXml = dataSetXml.replaceAll("FEED_STATS", feedName + "_stats" );
		dataSetXml = dataSetXml.replaceAll("ACQUISITION_SOURCE_NAME", this.targetGrid1 );
		dataSetXml = dataSetXml.replace("HCAT_TYPE", this.HCAT_TYPE);
		dataSetXml = dataSetXml.replaceAll("DATABASE_NAME", this.DATABASE_NAME);
		
		// this is doAs specific , so no need of this tag
		dataSetXml = dataSetXml.replaceAll("<RunAsOwner>replication</RunAsOwner>", "");
		dataSetXml = dataSetXml.replaceAll("HCAT_TABLE_NAME", this.acquisitionDataSetName.toLowerCase());

		dataSetXml = dataSetXml.replaceAll("REPLICATION_SOURCE_DATA_PATH", getCustomPath("data" , this.acquisitionDataSetName) );
		dataSetXml = dataSetXml.replaceAll("REPLICATION_SOURCE_COUNT_PATH", getCustomPath("count" , this.acquisitionDataSetName) );
		dataSetXml = dataSetXml.replaceAll("REPLICATION_SOURCE_SCHEMA_PATH", getCustomPath("schema" , this.acquisitionDataSetName));

		dataSetXml = dataSetXml.replaceAll("CUSTOM_DATA_PATH", getCustomPath("data" , this.replicationDataSetName));
		dataSetXml = dataSetXml.replaceAll("CUSTOM_COUNT_PATH", getCustomPath("count", this.replicationDataSetName));
		dataSetXml = dataSetXml.replaceAll("CUSTOM_SCHEMA_PATH", getCustomPath("schema", this.replicationDataSetName));

		// Change the default hcat table name to the acquisition table name, this is to achieve HCAT discovery feature.
		dataSetXml = dataSetXml.replace("TestDoAsReplication-1", this.acquisitionDataSetName );

		// enable hcat discovery feature.
		dataSetXml = dataSetXml.replaceAll("<DiscoveryInterface>HDFS</DiscoveryInterface>", "<DiscoveryInterface>HCat</DiscoveryInterface>");

		Response response = this.consoleHandle.createDataSet(this.replicationDataSetName, dataSetXml);
		if (response.getStatusCode() != SUCCESS) {
			try {
				throw new Exception("Response status code is " + response.getStatusCode() + ", expected 200.");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		// add the replication dataset name, so that it can be use to deactivated dataset in tearDown method
		dataSetNames.add(this.replicationDataSetName);
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
