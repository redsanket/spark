package hadooptest.monitoring.monitors;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.monitoring.exceptionParsing.ExceptionPeel;
import hadooptest.monitoring.listeners.MonitoringListener;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class TestStatusMonitor extends AbstractMonitor {

	public TestStatusMonitor(String clusterName,
			HashMap<String, ArrayList<String>> sentComponentToHostMapping,
			Class<?> testClass, String testMethodName) {
		super(clusterName, sentComponentToHostMapping, testClass,
				testMethodName, 0,
				HadooptestConstants.Miscellaneous.TEST_STATUS);

	}

	public void logTestStatus() {
		TestSession.logger
				.trace("````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````");
		String dumpLocation;

		// File dirHandle = new File(baseDirPathToDump);
		// dirHandle.mkdirs();
		dumpLocation = baseDirPathToDump + this.kind;

		File testStatusFileHandle = new File(dumpLocation);
		PrintWriter printWriter = null;

		try {

			testStatusFileHandle.getParentFile().mkdirs();
			testStatusFileHandle.createNewFile();

			printWriter = new PrintWriter(testStatusFileHandle);
			for (String aKey : MonitoringListener.testStatusesWhereTrueMeansPassAndFalseMeansFailed
					.keySet()) {
				TestSession.logger
						.trace("testStatusesWhereTrueMeansPassAndFalseMeansFailed has key:"
								+ aKey);
			}
			TestSession.logger
					.trace(MonitoringListener.testStatusesWhereTrueMeansPassAndFalseMeansFailed);
			if (MonitoringListener.testStatusesWhereTrueMeansPassAndFalseMeansFailed
					.get(testMethodBeingMonitored) == true) {
				/*
				 * means test passed
				 */
				TestSession.logger.info("TestStatusMonitor sees "
						+ testMethodBeingMonitored + " as passed");
				printWriter.println("PASSED");
			} else {
				/*
				 * means test failed
				 */
				TestSession.logger.info("TestStatusMonitor sees "
						+ testMethodBeingMonitored + " as failed");
				printWriter.println("FAILED");
			}
			printWriter.flush();

			MonitoringListener.testStatusesWhereTrueMeansPassAndFalseMeansFailed
					.remove(testMethodBeingMonitored);

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (printWriter != null) {

				printWriter.close();
			}
		}

	}

	@Override
	public void monitorDoYourThing(int tick) throws IOException {
		/*
		 * Not applicable, because TestStatusMonitor is not a monitor in a true
		 * sense
		 */

	}

	public void startMonitoring() {
		/*
		 * Not applicable, because all the action happens in stop Monitoring. By
		 * default a test status is "Pass"
		 */

	}

	@Override
	public void stopMonitoring() {
		TestSession.logger.info("Stop monitoring received for Test Status for"
				+ testMethodBeingMonitored);
		logTestStatus();
	}

	@Override
	synchronized public void run() {
		/*
		 * Not applicable, because TestStatusMonitor is not a monitor in a true
		 * sense
		 */
	}

	@Override
	public void dumpData() {

	}

}
