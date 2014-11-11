package hadooptest.gdm.regression.api;

import static org.junit.Assert.assertTrue;
import hadooptest.TestSession;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.cluster.gdm.HTTPHandle;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;

import java.util.Iterator;
import java.util.List;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * Test Case : To Verify whether discovery monitor return the correct discovered value for non DSD partition.
 *
 */
public class TestDiscoveryMonitorWithNoPartitionDataSet extends TestSession {

	private ConsoleHandle consoleHandle;
	private String dataSetName;
	private WorkFlowHelper workFlowHelper;
	private String datasetActivationTime;
	private String newDataSourceName ;
	private String dataSetWithErrMessage;
	private HTTPHandle httpHandle ;
	private String url;
	private static final String baseDataSetName = "VerifyAcqRepRetWorkFlowExecutionSingleDate";
	public static final int SUCCESS = 200;

	@BeforeClass
	public static void startTestSession() {
		TestSession.start();
	}

	@Before
	public void setup() throws Exception {
		this.consoleHandle = new ConsoleHandle();
		this.url = this.consoleHandle.getConsoleURL();
		TestSession.logger.info("url = " + this.url);
		this.dataSetName = "TestDiscoverMonitoringWithNonDataSet_"  + System.currentTimeMillis();
		this.workFlowHelper = new WorkFlowHelper();
	}

	@Test
	public void testDiscoveryMonitorWithNoPartitionDataSet() {
		
		// create a dataset
		createDataSet(this.dataSetName);
		
		//  Monitor for dataset discovery
		JSONArray jsonArray = this.workFlowHelper.isDiscoveryMonitoringStarted("acquisition", this.dataSetName);
		if (jsonArray.size() > 0) {
			Iterator iterator = jsonArray.iterator();
			while (iterator.hasNext()) {
				JSONObject jsonObject = (JSONObject) iterator.next();
				String dataSet = jsonObject.getString("Dataset");
				String partition = jsonObject.getString("Partition");
				String last_Discovery_Date = jsonObject.getString("Last Discovery Date");
				assertTrue("Failed to get the  " + dataSet  , jsonObject.getString("Dataset").equals(this.dataSetName));
				assertTrue("Failed to get the srcid & partition id " + partition  , partition.equals("None") );
				assertTrue("Failed to get the srcid & partition id " + partition  , partition.equals("None") );
			}
		}
		assertTrue("Failed to get discovery monitoring on acquisition for " + this.dataSetName , jsonArray.size() > 0);
	}

	/**
	 * Create a dataset and activate it.
	 */
	private void createDataSet(String datasetName) {
		// Read dataset and replace source and target values
		String dataSetXml = this.consoleHandle.getDataSetXml(this.baseDataSetName);

		// Replace the dataset name
		dataSetXml = dataSetXml.replaceAll(this.baseDataSetName, datasetName);

		// Create a new dataset
		Response response = this.consoleHandle.createDataSet(datasetName, dataSetXml);
		assertTrue("Failed to create a dataset " + datasetName , response.getStatusCode() == 200);

		// wait for some time so that specification files are created
		this.consoleHandle.sleep(40000);

		// activate the dataset
		try {
			this.consoleHandle.checkAndActivateDataSet(datasetName);
			this.datasetActivationTime = GdmUtils.getCalendarAsString();
		} catch (Exception e) { 
			e.printStackTrace();
		}
		this.consoleHandle.sleep(4000);
	}

}
