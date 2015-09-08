package hadooptest.gdm.regression.hcat;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.cluster.gdm.HCatHelper;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * TestCase  : Verify whether both data and HCAT table is created for <HCatTargetType>Mixed</HCatTargetType> tag.
 * 			   HCat table should be created for both acquisition and replication workflow on HCat server and 
 * 			   instance files are copied to on HDFS.
 * 
 * 			Acquisition : Copy the data from FDI to Grid and check whether HCat table & partition are created on  HCat server.
 * 			Replication : Copy the data from acquisition Grid and check whether HCat table & partition are created on HCat server.
 */
public class TestHCatTargetTypeMixed extends TestSession {

	private ConsoleHandle consoleHandle;
	private List<String> hcatSupportedGrid;
	private String dataSetName;
	private String targetGrid1;
	private String targetGrid2;
	private String datasetActivationTime;
	private String baseDataSetName = "VerifyAcqRepRetWorkFlowExecutionSingleDate";
	private WorkFlowHelper workFlowHelper;
	private HCatHelper hcatHelperObject = null;
	private static final String HCAT_TYPE = "Mixed";
	private static final String DATABASE_NAME = "gdm";
	private static final int SUCCESS = 200;

	@BeforeClass
	public static void startTestSession() throws Exception {
		TestSession.start();
	}

	@Before
	public void setUp() throws NumberFormatException, Exception {
		this.hcatHelperObject = new HCatHelper();
		this.consoleHandle = new ConsoleHandle();
		this.workFlowHelper = new WorkFlowHelper();
		this.dataSetName = "TestHCatTargetTypeMixed_" + System.currentTimeMillis();

		// get all the hcat supported clusters
		hcatSupportedGrid = this.consoleHandle.getHCatEnabledGrid();

		// check whether we have two hcat cluster one for acquisition and replication
		if (hcatSupportedGrid.size() < 2) {
			throw new Exception("Unable to run " + this.dataSetName  +" 2 grid datasources are required.");
		}

		this.targetGrid1 = hcatSupportedGrid.get(0).trim();
		TestSession.logger.info("Using grids " + this.targetGrid1  );

		this.targetGrid2 = hcatSupportedGrid.get(1).trim();
		TestSession.logger.info("Using grids " + this.targetGrid2  );

		// check whether hcat is enabled on target1 cluster
		boolean targetHCatSupported = this.consoleHandle.isHCatEnabledForDataSource(this.targetGrid1);
		if (!targetHCatSupported) {
			this.consoleHandle.modifyDataSource(this.targetGrid1, "HCatSupported", "FALSE", "TRUE");
		}

		// check whether hcat is enabled on target2 cluster
		targetHCatSupported = this.consoleHandle.isHCatEnabledForDataSource(this.targetGrid2);
		if (!targetHCatSupported) {
			this.consoleHandle.modifyDataSource(this.targetGrid2, "HCatSupported", "FALSE", "TRUE");
		}
	}

	@Test
	public void testHCatTargetTypeMixed() throws Exception {

		// create acquisition dataset
		createDataSet();

		// activate the dataset
		this.consoleHandle.checkAndActivateDataSet(this.dataSetName);
		this.datasetActivationTime = GdmUtils.getCalendarAsString();

		// acquisition
		{
			// check for acquisition workflow
			this.workFlowHelper.checkWorkFlow(this.dataSetName , "acquisition" , this.datasetActivationTime  );
			String acqHCatTableName = this.hcatHelperObject.getHCatTableName(this.targetGrid1 , this.dataSetName, "acquisition");
			String tempDataSetName = this.dataSetName.trim().toLowerCase().replaceAll("-", "_");
			assertTrue("Expected " + tempDataSetName   + "  but got " + acqHCatTableName , acqHCatTableName.equals(tempDataSetName));
		}

		// replication
		{	
			// check for replication workflow
			this.workFlowHelper.checkWorkFlow(this.dataSetName , "replication" , this.datasetActivationTime  );
			String repHCatTableName = this.hcatHelperObject.getHCatTableName(this.targetGrid1 , this.dataSetName, "replication");
			String tempDataSetName = this.dataSetName.trim().toLowerCase().replaceAll("-", "_");
			assertTrue("Expected " + tempDataSetName   + "  but got " + repHCatTableName , repHCatTableName.equals(tempDataSetName));
		}
	}

	/**
	 * Create a dataset specification configuration file.
	 */
	public void createDataSet() {
		String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/HCATDataSet.xml");
		String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.dataSetName, dataSetConfigFile);
		String feedName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Parameters" , "value");
		String sourceName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Sources" , "name");

		dataSetXml = dataSetXml.replaceAll("TARGET1", this.targetGrid1 );
		dataSetXml = dataSetXml.replaceAll("TARGET2", this.targetGrid2 );
		dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", this.dataSetName);
		dataSetXml = dataSetXml.replaceAll("FEED_NAME", feedName );
		dataSetXml = dataSetXml.replaceAll("FEED_STATS", feedName + "_stats" );
		dataSetXml = dataSetXml.replaceAll("FDI_SERVER_NAME", sourceName );
		dataSetXml = dataSetXml.replace("HCAT_TYPE", this.HCAT_TYPE);
		dataSetXml = dataSetXml.replace("GROUP_NAME", "users");
		dataSetXml = dataSetXml.replaceAll("owner=\"DATA_OWNER\"", "");
		dataSetXml = dataSetXml.replace("<RunAsOwner>FACETS</RunAsOwner>", "");
		dataSetXml = dataSetXml.replaceAll("DATABASE_NAME", this.DATABASE_NAME);
		dataSetXml = dataSetXml.replaceAll("TABLE_NAME", this.dataSetName);

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

