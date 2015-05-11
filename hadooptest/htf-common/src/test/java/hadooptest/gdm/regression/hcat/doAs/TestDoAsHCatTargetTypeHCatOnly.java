package hadooptest.gdm.regression.hcat.doAs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.cluster.gdm.HCatHelper;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDoAsHCatTargetTypeHCatOnly extends TestSession {

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
	private List<String> dataSetList ;
	private static final String ACQ_HCAT_TYPE = "Mixed";
	private static final String REP_HCAT_TYPE = "HCatOnly";
	private static final int SUCCESS = 200;
	private static final String GROUP_NAME = "users";
	private static final String DATA_OWNER = "lawkp";
	private static  final String DATABASE_NAME = "law_doas_gdm";

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
		
		this.dataSetList = new ArrayList<String> ();

		// get all the hcat supported clusters
		hcatSupportedGrid = this.consoleHandle.getHCatEnabledGrid();

		// check whether we have two hcat cluster one for acquisition and replication
		if (hcatSupportedGrid.size() < 2) {
			throw new Exception("Unable to run test since it requires two grid hcat enabled datasource.");
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
	public void testHCatTargetTypeHCatOnly() throws Exception {
		// acquisition
		{	
			// create a dataset
			createAquisitionDataSet("DoAsAcquisitionDataSet.xml");

			// activate the dataset
			this.consoleHandle.checkAndActivateDataSet(this.acqDataSetName);
			this.datasetActivationTime = GdmUtils.getCalendarAsString();
			
			this.dataSetList.add(this.acqDataSetName);

			// check for workflow
			this.workFlowHelper.checkWorkFlow(this.acqDataSetName , "acquisition" , this.datasetActivationTime  );

			// get Hcat server name for targetGrid1
			String acquisitionHCatServerName = this.hcatHelperObject.getHCatServerHostName(this.targetGrid1);
			TestSession.logger.info("Hcat Server for " + this.targetGrid1  + "  is " + acquisitionHCatServerName);

			// check whether hcat table is created for Mixed HCatTargetType.
			boolean isAcqusitionTableCreated = this.hcatHelperObject.isTableExists(acquisitionHCatServerName, this.acqDataSetName , this.DATABASE_NAME);
			assertTrue("Failed : Expected that HCAT table is created " + this.acqDataSetName , isAcqusitionTableCreated == true);
			
			
			// check for table owner
			String tableOwner = this.hcatHelperObject.getHCatTableOwner(this.targetGrid1 , this.acqDataSetName , "acquisition");
			boolean flag =  tableOwner.contains(this.DATA_OWNER);
			assertTrue("Expected " + this.DATA_OWNER  + "  data owner but got " + tableOwner ,   flag == true);
		}

		// replication
		{
			createReplicationDataSet("DoAsReplicationDataSet.xml");

			// activate the dataset
			this.consoleHandle.checkAndActivateDataSet(this.repDataSetName);
			this.datasetActivationTime = GdmUtils.getCalendarAsString();
			
			this.dataSetList.add(this.repDataSetName);

			// check for replication workflow
			this.workFlowHelper.checkWorkFlow(this.repDataSetName , "replication" , this.datasetActivationTime  );

			// get Hcat server name for targetGrid2
			String replicationHCatServerName = this.hcatHelperObject.getHCatServerHostName(this.targetGrid2);
			TestSession.logger.info("Hcat Server for " + this.targetGrid1  + "  is " + replicationHCatServerName);

			// check whether hcat table is created for HCatOnly HCatTargetType.
			boolean isTableCreated = this.hcatHelperObject.isTableExists(replicationHCatServerName, this.acqDataSetName , this.DATABASE_NAME);
			assertTrue("Failed : Expected that HCAT table is  created  " + this.repDataSetName , isTableCreated == true);
			
			// check for table owner
			String tableOwner = this.hcatHelperObject.getHCatTableOwner(this.targetGrid2 , this.repDataSetName , "replication");
			boolean flag = tableOwner.contains(this.DATA_OWNER);
			assertTrue("Expected " + this.DATA_OWNER  + "  data owner but got " + tableOwner , flag == true );
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
		dataSetXml = dataSetXml.replaceAll("GROUP_NAME", this.GROUP_NAME);
		dataSetXml = dataSetXml.replaceAll("DATA_OWNER", this.DATA_OWNER);
		dataSetXml = dataSetXml.replace("HCAT_TYPE", this.ACQ_HCAT_TYPE);
		dataSetXml = dataSetXml.replaceAll("DATABASE_NAME", this.DATABASE_NAME);
		dataSetXml = dataSetXml.replace("<UGI group=\"GROUP_NAME\" owner=\"DATA_OWNER\" permission=\"755\"/>", "<UGI group=\"users\" permission=\"755\"/>");
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
	//	int index = dataSetXmlBuffer.indexOf("</HCatTableName>") + "</HCatTableName>".length() + 2;
	//	dataSetXmlBuffer.insert(index, "<HCatTablePropagationEnabled>TRUE</HCatTablePropagationEnabled>");
		
		String dataSetXml = dataSetXmlBuffer.toString();
		dataSetXml = dataSetXml.replaceAll("TARGET1", this.targetGrid2 );
		//dataSetXml = dataSetXml.replace("<UGI group=\"GROUP_NAME\" owner=\"DATA_OWNER\" permission=\"755\"/>", "<UGI group=\"users\" permission=\"755\"/>");
		dataSetXml = dataSetXml.replaceAll("GROUP_NAME", this.GROUP_NAME);
		dataSetXml = dataSetXml.replaceAll("DATA_OWNER", this.DATA_OWNER);
		dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", this.repDataSetName);
		dataSetXml = dataSetXml.replaceAll("GROUP_NAME", this.GROUP_NAME);
		dataSetXml = dataSetXml.replaceAll("DATA_OWNER", this.DATA_OWNER);
		dataSetXml = dataSetXml.replaceAll("HCAT_TABLE_PROPAGATION", "TRUE");
		dataSetXml = dataSetXml.replaceAll("FEED_NAME", feedName );
		dataSetXml = dataSetXml.replaceAll("FEED_STATS", feedName + "_stats" );
		dataSetXml = dataSetXml.replaceAll("ACQUISITION_SOURCE_NAME", this.targetGrid1 );
		dataSetXml = dataSetXml.replaceAll("HDFS", "HCAT" );
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
	
	/**
	 * Method to deactivate the dataset(s)
	 */
	@After
	public void tearDown() throws Exception {

		List<String> allDatasetList =  consoleHandle.getAllDataSetName();

		// Deactivate all three facet datasets
		for ( String dataset : this.dataSetList)  {
			if (allDatasetList.contains(dataset)) {
				Response response = consoleHandle.deactivateDataSet(dataset);
				assertEquals("ResponseCode - Deactivate DataSet", 200, response.getStatusCode());
				assertEquals("ActionName.", "terminate", response.getElementAtPath("/Response/ActionName").toString());
				assertEquals("ResponseId", "0", response.getElementAtPath("/Response/ResponseId").toString());
				assertEquals("ResponseMessage.", "Operation on " + dataset + " was successful.", response.getElementAtPath("/Response/ResponseMessage/[0]").toString());		
			}
		}		
	}

}

