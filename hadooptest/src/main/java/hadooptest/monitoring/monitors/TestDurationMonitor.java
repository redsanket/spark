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

public class TestDurationMonitor extends AbstractMonitor {
	long testStartTime;
	long testEndTime;
	public TestDurationMonitor(String clusterName,
			HashMap<String, ArrayList<String>> sentComponentToHostMapping,
			Class<?> testClass, String testMethodName) {
		super(clusterName, sentComponentToHostMapping, testClass,
				testMethodName, 0,
				HadooptestConstants.Miscellaneous.TEST_DURATION);

	}

	public void logTestStatus() {
		TestSession.logger
				.trace("````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````");
		String dumpLocation;

		dumpLocation = baseDirPathToDump + this.kind;

		File testDurationFileHandle = new File(dumpLocation);
		PrintWriter printWriter = null;

		try {

			testDurationFileHandle.getParentFile().mkdirs();
			testDurationFileHandle.createNewFile();

			printWriter = new PrintWriter(testDurationFileHandle);
			//Log the time in seconds
			printWriter.println((testEndTime-testStartTime)/1000);
			printWriter.flush();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (printWriter != null) {
				printWriter.close();
			}
		}
	}

	@Override
	public void fetchResourceUsageIntoMemory(int tick) throws IOException {
		/*
		 * Not applicable, because TestStatusMonitor is not a monitor in a true
		 * sense
		 */

	}

	public void startMonitoring() {
		testStartTime = System.currentTimeMillis();
	}

	@Override
	public void stopMonitoring() {
		testEndTime = System.currentTimeMillis();
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
