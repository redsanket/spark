package hadooptest.gdm.regression;


import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.Response;


import java.util.ArrayList;
import java.util.List;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Verify whether HTTP Error code 500 is thrown when %date is missing data path.
 *
 */
public class TestDiscoveryPathDateMissing extends TestSession {

	private ConsoleHandle consoleHandle;
	private String dataSetName;
	private String baseDataSetName = "VerifyAcqRepRetWorkFlowExecutionDateRange";
	private String targetGrid1;
	private String targetGrid2;
	private List<String> grids = new ArrayList<String>();
	private static final int FAILURE = 500;
	private static final String GROUP_NAME = "jaggrp";
	private static final String DATA_OWNER = "jagpip";
	private static final String HCAT_TYPE = "DataOnly";

	@BeforeClass
	public static void startTestSession() {
		TestSession.start();
	}

	@Before
	public void setup() throws Exception {
		this.consoleHandle = new ConsoleHandle();

		List<String> tempGrids = this.consoleHandle.getAllInstalledGridName();

		// remove target that starting with gdm-target
		for ( String gridName : tempGrids) {
			if ( !gridName.startsWith("gdm-target") )
				grids.add(gridName);
		}

		// check whether secure cluster exists
		if (grids.size() >= 2 ) {
			this.targetGrid1 = grids.get(0);
			this.targetGrid2 = grids.get(1);
		} else {
			fail("There are only " + grids.size() + " grids; need at least two to run tests. ");
		}
	}

	@Test
	public void testDoAcqAndRep() throws Exception {
		// Dataset name
		this.dataSetName =  "TestCreatingDataSetWithOutDateInDataPath_"  + System.currentTimeMillis();

		// create replication dataset
		createDataSetWithOutDateInPath("DoAsReplicationDataSet.xml");
	}

	/**
	 * Create a dataset without %date specified in the datapath, creating of dataset will fail with HTTP error code 500
	 * @param dataSetFileName - name of the replication dataset
	 */
	private void createDataSetWithOutDateInPath(String dataSetFileName) {
		String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/" + dataSetFileName);
		String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.dataSetName, dataSetConfigFile);
		String feedName = this.consoleHandle.getDataSetTagsAttributeValue(this.baseDataSetName , "Parameters" , "value");

		dataSetXml = dataSetXml.replaceAll("TARGET1", this.targetGrid2 );
		dataSetXml = dataSetXml.replaceAll("GROUP_NAME", this.GROUP_NAME);
		dataSetXml = dataSetXml.replaceAll("DATA_OWNER", this.DATA_OWNER);
		dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", this.dataSetName);
		dataSetXml = dataSetXml.replaceAll("FEED_NAME", feedName );
		dataSetXml = dataSetXml.replaceAll("FEED_STATS", feedName + "_stats" );
		dataSetXml = dataSetXml.replaceAll("ACQUISITION_SOURCE_NAME", this.targetGrid1 );
		dataSetXml = dataSetXml.replace("HCAT_TYPE", this.HCAT_TYPE);
		dataSetXml = dataSetXml.replaceAll("HCAT_TABLE_NAME", this.dataSetName);
		dataSetXml = dataSetXml.replaceAll("REPLICATION_SOURCE_DATA_PATH", getCustomPath("data" , "dataPath") );
		dataSetXml = dataSetXml.replaceAll("REPLICATION_SOURCE_COUNT_PATH", getCustomPath("count" , "countPath") );
		dataSetXml = dataSetXml.replaceAll("REPLICATION_SOURCE_SCHEMA_PATH", getCustomPath("schema" , "schemaPath"));

		dataSetXml = dataSetXml.replaceAll("CUSTOM_DATA_PATH", getCustomPath("data" , "dataPath"));
		dataSetXml = dataSetXml.replaceAll("CUSTOM_COUNT_PATH", getCustomPath("count", "countPath"));
		dataSetXml = dataSetXml.replaceAll("CUSTOM_SCHEMA_PATH", getCustomPath("schema", "schemaPath"));

		Response response = this.consoleHandle.createDataSet(this.dataSetName, dataSetXml);
		assertTrue("Response status code is " + response.getStatusCode() + ", expected 500." , response.getStatusCode() == FAILURE);
	}


	/**
	 * Method to create a custom path without date in path.
	 * @param pathType - its a string type which represents either data/count/schema
	 * @param dataSet - dataset name
	 * @return
	 */
	private String getCustomPath(String pathType , String dataSet) {
		return  "/data/daqdev/"+ pathType +"/"+ dataSet + "/";
	}
}
