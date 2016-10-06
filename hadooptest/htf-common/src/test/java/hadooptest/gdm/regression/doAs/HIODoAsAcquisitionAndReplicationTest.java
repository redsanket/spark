package hadooptest.gdm.regression.doAs;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.cluster.gdm.HTTPHandle;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.jayway.restassured.path.json.JsonPath;
import com.jayway.restassured.path.xml.XmlPath;

/**
 * TestCase : Verify whether HIO acquisition and replication in DoAs context
 *
 */
public class HIODoAsAcquisitionAndReplicationTest extends TestSession {

	private ConsoleHandle consoleHandle;
	private String dataSetName;
	private String cookie;
	private String url;
	private String datasetActivationTime;
	private String targetGrid1;
	private String targetGrid2;
	private WorkFlowHelper helper = null;
	private static final int SUCCESS = 200;
	private static String startDate = "20110607" ;
	private static String endDate = "20220131" ;
	private static final String PATH = "/data/daqdev/";
	private String targetGrid1_NameNode;
	private String targetGrid2_NameNode;
	private List<String> grids = new ArrayList<String>();
	private boolean eligibleForDelete = false;

	@BeforeClass
	public static void startTestSession() {
		TestSession.start();
	}

	@Before
	public void setup() throws Exception {
		this.dataSetName = "TestHIO_DoAs_WorkFlow_" + System.currentTimeMillis();
		this.consoleHandle = new ConsoleHandle();
		HTTPHandle httpHandle = new HTTPHandle();
		this.cookie = httpHandle.getBouncerCookie();
		this.url =  this.consoleHandle.getConsoleURL();
		helper = new WorkFlowHelper(); 	
		
		this.grids = this.consoleHandle.getAllGridNames();
		TestSession.logger.info("Grids = " + grids);
		// check whether secure cluster exists
		if (this.grids.size() > 2 ) {
			this.targetGrid1 = this.grids.get(0);
			this.targetGrid2 = this.grids.get(1);
		} else {
			fail("There are only " + grids.size() + " grid and its not sufficient to test.. ");
		}
		
		// Get namenode name of target cluster
		this.targetGrid1_NameNode = this.consoleHandle.getClusterNameNodeName(this.targetGrid1);
		this.targetGrid2_NameNode = this.consoleHandle.getClusterNameNodeName(this.targetGrid2);
		
		// check and change the group, owner and permission if they did n't meet the following requirement
		// Permission should be 777 for the destination path, group = users and owner = dfsload
		this.helper.checkAndSetPermision(this.targetGrid1_NameNode, this.PATH); 
		this.helper.checkAndSetPermision(this.targetGrid2_NameNode, this.PATH);
	}

	@Test
	public void testHIO() throws Exception {
		createHIODataSet("HIODoAs.xml");

		// activate the dataset
		this.consoleHandle.checkAndActivateDataSet(this.dataSetName);
		this.datasetActivationTime = GdmUtils.getCalendarAsString();
		
		// acquisition workflow
		helper.checkWorkFlow(this.dataSetName, "acquisition", this.datasetActivationTime );
		
		// replication workflow
		helper.checkWorkFlow(this.dataSetName, "replication", this.datasetActivationTime);
		
		eligibleForDelete = true;
	}

	@After
	public void tearDown() throws Exception {
		List<String> datasetList =  consoleHandle.getAllDataSetName();
		if (datasetList.contains(this.dataSetName)) {
			
			// make dataset inactive
			Response response = consoleHandle.deactivateDataSet(this.dataSetName);
			assertEquals("ResponseCode - Deactivate DataSet", 200, response.getStatusCode());
			assertEquals("ActionName.", "terminate", response.getElementAtPath("/Response/ActionName").toString());
			assertEquals("ResponseId", "0", response.getElementAtPath("/Response/ResponseId").toString());
			assertEquals("ResponseMessage.", "Operation on " + this.dataSetName + " was successful.", response.getElementAtPath("/Response/ResponseMessage/[0]").toString());	
			
			// remove dataset
			if (eligibleForDelete) {
			    this.consoleHandle.removeDataSet(this.dataSetName);    
			}
		} else {
			TestSession.logger.info(this.dataSetName + " does not exists.");
		}
	}

	/**
	 * Create a dataset with the specified dataset file name
	 * @param dataSetFileName
	 */
	private void createHIODataSet( String dataSetFileName ) {
		String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/" + dataSetFileName);
		String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.dataSetName, dataSetConfigFile);
		
		String testURL = this.url + "/console/query/config/dataset/getDatasets";
		JsonPath jsonPath = given().cookie(cookie).get(testURL).jsonPath();
		List<String> hio = jsonPath.getList("DatasetsResult.findAll { ! it.DatasetName.contains('hio') }.DatasetName");
		if ( hio == null) {
			fail("No HIO dataset exist");
		}
		TestSession.logger.info("hio dataset name = " + hio.get(0));
		
		// Get the source name
		String sourceName  = this.consoleHandle.getDataSetTagsAttributeValue(hio.get(0) , "Sources" , "name");
		TestSession.logger.info("*****************************SourceName = " + sourceName);
		dataSetXml = dataSetXml.replaceAll("GDM_HIO_SOURCE", sourceName );
		dataSetXml = dataSetXml.replaceAll("START_DATE", this.startDate );
		dataSetXml = dataSetXml.replaceAll("END_DATE", this.endDate );
		dataSetXml = dataSetXml.replaceAll("TARGET1", this.targetGrid1 );
		dataSetXml = dataSetXml.replaceAll("TARGET2", this.targetGrid2 );
		dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", this.dataSetName);
		dataSetXml = dataSetXml.replaceAll("HCAT_TABLE_NAME", this.dataSetName);

		Response response = this.consoleHandle.createDataSet(this.dataSetName, dataSetXml);
		if (response.getStatusCode() != SUCCESS) {
			try {
				throw new Exception("Response status code is " + response.getStatusCode() + ", expected 200.");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
}
