package hadooptest.gdm.regression.crossHadoopVersion;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import hadooptest.TestSession;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.cluster.gdm.HTTPHandle;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.gdm.WorkFlowHelper;


/**
 * Test Scenario : Verify whether acquisition and replication workflow are successful between different 
 * versions of hadoop.
 * Steps :
 *   1) Get all the installed grid names.
 *   2) Create a matrix for the installed grid
 *   3) Create datasets based on the matrix.
 *   4) Check for acquistion and replication workflow.
 *
 */
public class VerifyAllFacetsWorkFlowAcrossHadoopVersionsTest extends TestSession {

	private ConsoleHandle console;
	private int numberOfTargets;
	private String cookie;
	private String url;
	private WorkFlowHelper workFlowHelperObj = null;
	private List<String> targets = new ArrayList<String>();
	private List<String> testMatrixList ;
	private String datasetActivationTime = null;
	private Response response;
	private List<String> datasets = new ArrayList<String>();
	private List<String> datasetActivationTimeList = new ArrayList<String>();
	private List<String> testDatasets = new ArrayList<String>();
	private static String baseDataSetName = "VerifyAcqRepRetWorkFlowExecutionSingleDate";
	private static final int SUCCESS = 200;

	@BeforeClass
	public static void startTestSession() throws Exception {
		TestSession.start();
	}

	@Before
	public void setUp() throws NumberFormatException, Exception {
		HTTPHandle httpHandle = new HTTPHandle();
		this.console = new ConsoleHandle();
		this.cookie = httpHandle.getBouncerCookie();
		this.url = this.console.getConsoleURL();
		this.workFlowHelperObj = new WorkFlowHelper();

		// create test matrix
		testMatrixList = createTestMatrix( );
		if (testMatrixList == null) {
			fail("Unable to create the test matrix");
		}
		TestSession.logger.info(testMatrixList);

		// create a dataset
		createAndActivateTestDataSet();
	}

	/**
	 * Method that creates the new dataset from the basedataset and activates.
	 * The number of dataset(s) that gets created are dependent upon the number of targets specified on the config.xml
	 */
	private void createAndActivateTestDataSet() {

		// Navigate the testMatrix, where each element in the testmatrix is the targets for the  dataset
		for (String t : testMatrixList) {
			String tar[] = t.split(":");
			String targetCluster1 = tar[0].trim();
			String targetCluster2 = tar[1].trim();
			TestSession.logger.info(" cluster1 = " + targetCluster1  + "    cluster2 = " + targetCluster2);

			// create dataset Name
			String dataSetName = "GDMWorkFlowTestBw_" + targetCluster1 + "_" + targetCluster2 + "_" + System.currentTimeMillis();
			TestSession.logger.info("dataSetName  = "+dataSetName);
			datasets.add(dataSetName);
			
			// Read the base dataset file 
			String dataSetXml = this.console.getDataSetXml(this.baseDataSetName);

			// get target names
			List<String> baseDataSetTargets = this.console.getDataSource(this.baseDataSetName, "target", "name");

			// read the first target from the base dataset
			String sourceTarget = baseDataSetTargets.get(0);
			TestSession.logger.info("SourceTarget1 = " + sourceTarget);

			// replace first target with targetCluster1 value
			dataSetXml = dataSetXml.replaceAll(sourceTarget, targetCluster1);

			// read the second target from the base dataset
			sourceTarget = baseDataSetTargets.get(1);
			TestSession.logger.info("SourceTarget2 = "+sourceTarget);

			dataSetXml = dataSetXml.replaceAll(sourceTarget, targetCluster2);

			// replace basedatasetName with the new datasetname
			dataSetXml = dataSetXml.replaceAll(this.baseDataSetName, dataSetName);
			TestSession.logger.info("newDataSetXml = "+dataSetXml);

			// Create a new dataset
			Response response = this.console.createDataSet(dataSetName, dataSetXml);
			assertTrue("Failed to create a dataset " + dataSetName , response.getStatusCode() == SUCCESS);

			testDatasets.add(dataSetName);

			// wait for some time so that dataset specification file gets created 
			this.console.sleep(40000);

			// activate the dataset.
			this.response = this.console.activateDataSet(dataSetName);
			assertTrue("Failed to activate dataset " + dataSetName , response.getStatusCode() == SUCCESS);

			this.datasetActivationTime = GdmUtils.getCalendarAsString();
			datasetActivationTimeList.add(datasetActivationTime);
			TestSession.logger.info("DataSet Activation Time: " + datasetActivationTime);

			// wait for some time so that active data get eligible for workflow
			this.console.sleep(30000);
		}
	}

	@Test
	public void testAcqRepWorkFlowExecution() {
		int count = 0;
		for (String dataSetName : testDatasets) {

			//get the dataset activation time
			datasetActivationTime = datasetActivationTimeList.get(count++);

			TestSession.logger.info("Verifying acquisition workflow for " + dataSetName);
			this.workFlowHelperObj.checkWorkFlow(dataSetName , "acquisition" , datasetActivationTime);

			TestSession.logger.info("Verifying replication workflow for " + dataSetName);
			this.workFlowHelperObj.checkWorkFlow(dataSetName , "replication" , datasetActivationTime);
		}
	}

	/**
	 * deactivate the dataset(s)	
	 */
	@After
	public void tearDown() {
		if (datasets != null && datasets.size() > 0) {
			for (String dataSetName : datasets) {
				TestSession.logger.info("Deactivate "+ dataSetName);
				Response response = this.console.deactivateDataSet(dataSetName);
				assertTrue("Failed to deactivate dataset " + dataSetName , response.getStatusCode() == SUCCESS);
				assertTrue("Expected terminate action name but got " + response.getElementAtPath("/Response/ActionName").toString() , response.getElementAtPath("/Response/ActionName").toString().equals("terminate") );
				assertTrue("Expected to get 0 response id, but got " + response.getElementAtPath("/Response/ResponseId").toString() , response.getElementAtPath("/Response/ResponseId").toString().equals("0"));
			}
		}
	}

	/**
	 * Method to create the test matrix
	 * example : If suppose i have cluster1(grima) and cluster2(densea)
	 * 			then  i have to following test matrix 
	 * 			cluster1(grima) and cluster2(densea)
	 * 			cluster2(densea) and cluster1(grima)
	 * Note : this is not a full fledged matrix, but testing between two hadoop version should be good.
	 * @param target
	 * @return
	 */
	public List<String> createTestMatrix() {
		
		List<String> grids = this.console.getAllGridNames();
		List<String> targets = new ArrayList<String>();

		TestSession.logger.info("Installed grids = " + grids);

		for ( int i=0;i<grids.size() - 1 ; i++) {
			for ( int j = i + 1; j< grids.size() ; j++) {
				TestSession.logger.info(grids.get(i) + ":" +grids.get(j));
				if (! (grids.get(i).trim().equals((grids.get(j))))) {
					targets.add(grids.get(i) + ":" +grids.get(j));
				}
			}
		}
		
		// create matrix
		List<String> testMatrixList = new ArrayList<String>();
		for (String tar : targets){
			testMatrixList.add(tar);
			List<String>tar1 = Arrays.asList(tar.split(":"));

			// swap the targets
			testMatrixList.add(tar1.get(1) + ":" + tar1.get(0));
		}

		TestSession.logger.info("testMatrixList  = " + testMatrixList);
		return testMatrixList;
	}

}

