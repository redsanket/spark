package hadooptest.gdm.regression.integration;

import static org.junit.Assert.fail;
import hadooptest.TestSession;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.GdmUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * Class that initiates polling for data at the specified target cluster.
 */
public class TestWatchForDataDrop extends TestSession {

	private String clusterName;
	private ConsoleHandle consoleHandle;
	private String cookie;
	private String oozieHostName;
	private String hcatHostName;
	private int duration;
	private String freq;
	private String pullOozieJobLength;
	private int frequency;
	private String pathPattern;
	private static final String BASEPATH = "/data/daqdev/abf/data/";
	public DataAvailabilityPoller watcher;

	@BeforeClass
	public static void startTestSession() throws Exception {
		TestSession.start();
	}

	@Before
	public void setUp() throws NumberFormatException, Exception {

		this.consoleHandle = new ConsoleHandle();
		this.clusterName = GdmUtils.getConfiguration("testconfig.TestWatchForDataDrop.clusterName");

		// Get all the clusters that GDM knows about
		List<String> gridList  = this.consoleHandle.getAllInstalledGridName();

		// verify whether specified cluster name matches with the data dropped cluster.
		if ( (this.clusterName != null) && (gridList.contains(this.clusterName) == true) ) {
			TestSession.logger.info(this.clusterName + " exists in current one node deployment list.");
		} else {
			fail(this.clusterName  + " is either null or specified a wrong cluster.");
		}

		String dur = GdmUtils.getConfiguration("testconfig.TestWatchForDataDrop.duration");
		if ( dur != null ) {
			this.duration = Integer.parseInt(dur);
		}

		this.freq  =  GdmUtils.getConfiguration("testconfig.TestWatchForDataDrop.frequency");
		if (this.freq != null) {
			if (this.freq.equals("hourly")) {
				this.frequency = 1;
			}
		}

		String pattern =   GdmUtils.getConfiguration("testconfig.TestWatchForDataDrop.dataSetPattern");
		if (pattern != null ) {
			List<String> tempList = Arrays.asList(pattern.split("_"));
			pattern = tempList.get(tempList.size() - 1);
			if (pattern.toUpperCase().equals("HOURLY")) {
				this.pathPattern = "yyyyMMddHH";
			} else if (pattern.toUpperCase().equals("MINS")) {
				this.pathPattern = "yyyyMMddHHmm";
			} else if (pattern.toUpperCase().equals("DAILY")) {
				this.pathPattern = "yyyyMMdd";
			} else if (pattern.toUpperCase().equals("MONTHLY")) {
				this.pathPattern = "yyyyMM";
			} 
		}

		String jobPullLength = GdmUtils.getConfiguration("testconfig.TestWatchForDataDrop.pullOozieJobLength");
		if (jobPullLength != null) {
			TestSession.logger.info("jobPullLength  = " + jobPullLength);
			this.pullOozieJobLength = jobPullLength;
		}
		
		this.watcher = new DataAvailabilityPoller(this.duration , this.clusterName , this.BASEPATH , this.pathPattern , "polling" , this.pullOozieJobLength);
	}

	@Test
	public void test() throws Exception {
		TestSession.logger.info("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
		this.watcher.dataPoller();
		TestSession.logger.info("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
	}
}
