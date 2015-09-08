package hadooptest.gdm.regression.hcat;

import static org.junit.Assert.assertTrue;
import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.cluster.gdm.HCatHelper;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;

import java.util.List;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * TestCase : Verify whether HCat table and HCat partition is created when <HCatTargetType>HCatOnly</HCatTargetType> is set for replication dataset.
 *             
 *  Acquisition : Copy data from FDI server to GRID and create HCat table and partition in acquisition HCat server once the acquisition workflow is successful.
 *  Replication : Make sure data is not copied replication facet, instead just copy the meta data on the replication HCat server, data location still pointing to acquisition GRID.
 *
 */
public class TestHCatTargetTypeHCatOnly extends TestSession {

	private ConsoleHandle consoleHandle;
	private List<String> hcatSupportedGrid;
	private String acqDataSetName;
	private String repDataSetName;
	private String targetGrid1;
	private String targetGrid2;
	private String datasetActivationTime;
	private String baseDataSetName = "VerifyAcqRepRetWorkFlowExecutionSingleDate";
	private WorkFlowHelper workFlowHelper;
	private HCatHelper hcatHelperObject = null;
	private static final String ACQ_HCAT_TYPE = "Mixed";
	private static final String REP_HCAT_TYPE = "HCatOnly";
	private static final int SUCCESS = 200;
	private static final String DATABASE_NAME = "gdm";

	@BeforeClass
	public static void startTestSession() throws Exception {
		TestSession.start();
	}

	@Before
	public void setUp() throws NumberFormatException, Exception {
		this.hcatHelperObject = new HCatHelper();
		this.consoleHandle = new ConsoleHandle();
		this.workFlowHelper = new WorkFlowHelper();
		this.acqDataSetName = "TestHCatTargetTypeHCat_Acq_" + System.currentTimeMillis();
		this.repDataSetName = "TestHCatTargetTypeHCat_Rep_" + System.currentTimeMillis();
		
		// get all the hcat supported clusters
		hcatSupportedGrid = this.consoleHandle.getHCatEnabledGrid();

		// check whether we have two hcat cluster one for acquisition and
		// replication
		if (hcatSupportedGrid.size() < 2) {
			throw new Exception("Unable to run " + this.acqDataSetName + " 2 grid datasources are required.");
		}

		this.targetGrid1 = hcatSupportedGrid.get(0).trim();
		TestSession.logger.info("Using grids " + this.targetGrid1);

		this.targetGrid2 = hcatSupportedGrid.get(1).trim();
		TestSession.logger.info("Using grids " + this.targetGrid2);

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
	public void testHCatTargetTypeHCatOnly() throws Exception {
		// acquisition
		{	
			// create a dataset
			createAquisitionDataSet("DoAsAcquisitionDataSet.xml");

			// activate the dataset
			this.consoleHandle.checkAndActivateDataSet(this.acqDataSetName);
			this.datasetActivationTime = GdmUtils.getCalendarAsString();

			// check for workflow
			this.workFlowHelper.checkWorkFlow(this.acqDataSetName , "acquisition" , this.datasetActivationTime  );
			String acqHCatTableName = this.hcatHelperObject.getHCatTableName(this.targetGrid1 , this.acqDataSetName, "replication");
			String tempDataSetName = this.acqDataSetName.trim().toLowerCase().replaceAll("-", "_");
			assertTrue("Expected " + tempDataSetName   + "  but got " + acqHCatTableName , acqHCatTableName.equals(tempDataSetName));
		}

		// replication
		{
			createReplicationDataSet("DoAsReplicationDataSet.xml");

			// activate the dataset
			this.consoleHandle.checkAndActivateDataSet(this.repDataSetName);
			this.datasetActivationTime = GdmUtils.getCalendarAsString();

			// check for replication workflow
			this.workFlowHelper.checkWorkFlow(this.repDataSetName , "replication" , this.datasetActivationTime  );
			String repHCatTableName = this.hcatHelperObject.getHCatTableName(this.targetGrid1 , this.repDataSetName, "replication");
			String tempDataSetName = this.repDataSetName.trim().toLowerCase().replaceAll("-", "_");
			assertTrue("Expected " + tempDataSetName   + "  but got " + repHCatTableName , repHCatTableName.equals(tempDataSetName));
		}
	}

	/**
	 * creates a acquisition dataset to test DataOnly HCatTargetType
	 * @param dataSetFileName - name of the acquisition dataset
	 */
	private void createAquisitionDataSet( String dataSetFileName) {
		String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/" + dataSetFileName);
		String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.acqDataSetName, dataSetConfigFile);
		String feedName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Parameters" , "value");
		String sourceName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Sources" , "name");

		dataSetXml = dataSetXml.replaceAll("TARGET1", this.targetGrid1 );
		dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", this.acqDataSetName);
		dataSetXml = dataSetXml.replaceAll("FEED_NAME", feedName );
		dataSetXml = dataSetXml.replaceAll("FEED_STATS", feedName + "_stats" );
		dataSetXml = dataSetXml.replaceAll("FDI_SERVER_NAME", sourceName );
		dataSetXml = dataSetXml.replace("HCAT_TYPE", this.ACQ_HCAT_TYPE);
		dataSetXml = dataSetXml.replaceAll("DATABASE_NAME", this.DATABASE_NAME);
		dataSetXml = dataSetXml.replace("<UGI group=\"GROUP_NAME\" owner=\"DATA_OWNER\" permission=\"755\"/>", "<UGI group=\"users\" permission=\"755\"/>");
		dataSetXml = dataSetXml.replaceAll("<RunAsOwner>acquisition</RunAsOwner>", "");
		dataSetXml = dataSetXml.replaceAll("CUSTOM_DATA_PATH", getCustomPath("data", this.acqDataSetName) );
		dataSetXml = dataSetXml.replaceAll("CUSTOM_COUNT_PATH", getCustomPath("count", this.acqDataSetName) );
		dataSetXml = dataSetXml.replaceAll("CUSTOM_SCHEMA_PATH", getCustomPath("schema", this.acqDataSetName));
		dataSetXml = dataSetXml.replaceAll("HCAT_TABLE_NAME", this.acqDataSetName);

		Response response = this.consoleHandle.createDataSet(this.acqDataSetName, dataSetXml);
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
	 * Method that creates a replication dataset
	 * @param dataSetFileName - name of the replication dataset
	 */
	private void createReplicationDataSet(String dataSetFileName) {
		String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/" + dataSetFileName);
		String feedName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Parameters" , "value");
		
		StringBuffer dataSetXmlBuffer = new StringBuffer(this.consoleHandle.createDataSetXmlFromConfig(this.repDataSetName, dataSetConfigFile));
		
		String dataSetXml = dataSetXmlBuffer.toString();
		dataSetXml = dataSetXml.replaceAll("TARGET1", this.targetGrid2 );
		dataSetXml = dataSetXml.replace("<UGI group=\"GROUP_NAME\" owner=\"DATA_OWNER\" permission=\"755\"/>", "<UGI group=\"users\" permission=\"755\"/>");
		dataSetXml = dataSetXml.replaceAll("<RunAsOwner>replication</RunAsOwner>", "");
		dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", this.repDataSetName);
		dataSetXml = dataSetXml.replaceAll("FEED_NAME", feedName );
		dataSetXml = dataSetXml.replaceAll("FEED_STATS", feedName + "_stats" );
		dataSetXml = dataSetXml.replaceAll("ACQUISITION_SOURCE_NAME", this.targetGrid1 );
		dataSetXml = dataSetXml.replaceAll("HDFS", "HCAT" );
		dataSetXml = dataSetXml.replaceAll("HCAT_TABLE_PROPAGATION", "TRUE" );
		dataSetXml = dataSetXml.replaceAll("DATABASE_NAME", this.DATABASE_NAME);
		dataSetXml = dataSetXml.replace("HCAT_TYPE", this.REP_HCAT_TYPE);
		String acqTableName = this.acqDataSetName.toLowerCase().replace("-", "_").trim();
		dataSetXml = dataSetXml.replace("HCAT_TABLE_NAME", acqTableName);
		dataSetXml = dataSetXml.replaceAll("REPLICATION_SOURCE_DATA_PATH", getCustomPath("data" , this.acqDataSetName) );
		dataSetXml = dataSetXml.replaceAll("REPLICATION_SOURCE_COUNT_PATH", getCustomPath("count" , this.acqDataSetName) );
		dataSetXml = dataSetXml.replaceAll("REPLICATION_SOURCE_SCHEMA_PATH", getCustomPath("schema" , this.acqDataSetName));

		dataSetXml = dataSetXml.replaceAll("CUSTOM_DATA_PATH", getCustomPath("data" , this.repDataSetName));
		dataSetXml = dataSetXml.replaceAll("CUSTOM_COUNT_PATH", getCustomPath("count", this.repDataSetName));
		dataSetXml = dataSetXml.replaceAll("CUSTOM_SCHEMA_PATH", getCustomPath("schema", this.repDataSetName));
		

		Response response = this.consoleHandle.createDataSet(this.repDataSetName, dataSetXml);
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
	 * Method to create a custom path for the dataset.
	 * @param pathType - its a string type which represents either data/count/schema
	 * @param dataSet - dataset name
	 * @return
	 */
	private String getCustomPath(String pathType , String dataSet) {
		return  "/data/daqdev/"+ pathType +"/"+ dataSet + "/%{date}";
	}


}
