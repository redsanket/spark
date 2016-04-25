package hadooptest.gdm.regression.stackIntegration;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import hadooptest.TestSession;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.gdm.regression.stackIntegration.lib.CommonFunctions;

public class HadoopStackComponentsTest extends TestSession {

	// Since hbase is deployed outside the cluster , hbase is not in the following list.
	private static final String stackComponentsArray[ ] = {"namenode" , "jobTracker" , "gateway" , "hive" , "oozie"};
	private StackComponent [] stackComponents;
	private String clusterName;
	private CommonFunctions commonFunctionsObject;
	private TestDataAvailabilityOnCluster testDataAvailabilityOnCluster; 
	private static final int TEST_ITERATION_COUNT = 3;

	@BeforeClass
	public static void startTestSession() throws Exception {
		TestSession.start();
	}

	@Before
	public void setUp() {
	}

	public HadoopStackComponentsTest() throws IOException {
		stackComponents = new StackComponent[stackComponentsArray.length];
		String cName = GdmUtils.getConfiguration("testconfig.TestWatchForDataDrop.clusterName");
		setClusterName(cName);
		this.commonFunctionsObject = new CommonFunctions(cName);
		this.commonFunctionsObject.createDB();
		this.commonFunctionsObject.setStackComponentList(Arrays.asList(stackComponentsArray));
	}

	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	@Test
	public void checkStackComponentsHealth() throws InterruptedException, ExecutionException, IOException {
		testStack();
	}
	
	public void testStack() throws InterruptedException, ExecutionException, IOException {
		for ( int iteration=1 ; iteration<=TEST_ITERATION_COUNT ; iteration++) {
			String currentDataSetName = this.commonFunctionsObject.getCurrentHourPath() + "_" + iteration;
			this.commonFunctionsObject.setDataSetName(currentDataSetName);
			
			// check component health
			this.commonFunctionsObject.checkClusterHealth();
			this.testDataAvailabilityOnCluster = new TestDataAvailabilityOnCluster(this.commonFunctionsObject.getNameNodeName());

			// check for data avaiable for the current hr 
			boolean isDataAvailable = this.testDataAvailabilityOnCluster.pollForDataAvaiability();
			TestSession.logger.info("isDataAvailable  = " + isDataAvailable);

			if (isDataAvailable ) {
				this.commonFunctionsObject.preInit();
				this.commonFunctionsObject.initComponents();
				this.commonFunctionsObject.testStackComponent();
			}
		}
	}
}
