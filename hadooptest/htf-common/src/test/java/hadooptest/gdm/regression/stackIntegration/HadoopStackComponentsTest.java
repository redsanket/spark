package hadooptest.gdm.regression.stackIntegration;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import javax.mail.MessagingException;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import hadooptest.TestSession;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.gdm.regression.stackIntegration.lib.CommonFunctions;

import org.apache.hadoop.util.VersionInfo;

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
	public void setUp() { }

	public HadoopStackComponentsTest() throws IOException {
		String currentStackComponentTestList =  GdmUtils.getConfiguration("testconfig.TestWatchForDataDrop.stackComponents");
		TestSession.logger.info("test list - " + currentStackComponentTestList);
		List<String> tempStackComponentList = Arrays.asList(currentStackComponentTestList.split(" "));
		if (tempStackComponentList == null || tempStackComponentList.size() == 0) {
			try {
				throw new Exception("Please specify atleast one stack component.");
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(0);
			}
		}

		stackComponents = new StackComponent[stackComponentsArray.length];
		String cName = GdmUtils.getConfiguration("testconfig.TestWatchForDataDrop.clusterName");
		setClusterName(cName);
		this.commonFunctionsObject = new CommonFunctions(cName);
		this.commonFunctionsObject.createDB();
		this.commonFunctionsObject.setStackComponentList(Arrays.asList(stackComponentsArray));
		this.commonFunctionsObject.setCurrentStackComponentTestList(tempStackComponentList);
	}

	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	@Test
	public void checkStackComponentsHealth() throws InterruptedException, ExecutionException, IOException, ClassNotFoundException, InstantiationException, IllegalAccessException, SQLException, MessagingException {
		testStack();
	}

	public void testStack() throws InterruptedException, ExecutionException, IOException {
		String prefixName = VersionInfo.getVersion(); 
		TestSession.logger.debug("Using HADOOP_VERSION " + prefixName + " for dataset prefixName");

		// set the total run's start date and time
		java.text.SimpleDateFormat sdfStartDateTime = new java.text.SimpleDateFormat("yyyyMMddhhmmss");
		String currentStartDateTime = sdfStartDateTime.format(calendar.getTime());
		TestSession.logger.info("GRIDCI-1667, populate startDateTime for total run, startDateTime is: " +
        		currentStartDateTime);

		String tmpDATASETNAME = prefixName + "_"+ this.commonFunctionsObject.getCurrentHourPath();

		TestSession.logger.info("GRIDCI-1667, tmpDATASETNAME is: " + tmpDATASETNAME);
		this.commonFunctionsObject.updateDB(tmpDATASETNAME, "startDateTime", currentStartDateTime);

		if (prefixName != null) {
			for ( int iteration=1 ; iteration<=TEST_ITERATION_COUNT ; iteration++) {
				String currentDataSetName = prefixName + "_"+ this.commonFunctionsObject.getCurrentHourPath() + "_" + iteration;
				this.commonFunctionsObject.setDataSetName(currentDataSetName); 

				// check component health
				this.commonFunctionsObject.checkClusterHealth();
				this.testDataAvailabilityOnCluster = new TestDataAvailabilityOnCluster(this.commonFunctionsObject.getNameNodeName() , this.getClusterName());

				// check for data avaiable for the current hr 
				boolean isDataAvailable = this.testDataAvailabilityOnCluster.pollForDataAvaiability();
				TestSession.logger.info("isDataAvailable  = " + isDataAvailable);

				if (isDataAvailable ) {
					this.commonFunctionsObject.preInit();
					this.commonFunctionsObject.initComponents();
					this.commonFunctionsObject.testStackComponent();
				}
			}
		}else {
			TestSession.logger.error("Could not determine HADOOP_VERSION for dataset prefixName!!");
		}
		
	}
}
