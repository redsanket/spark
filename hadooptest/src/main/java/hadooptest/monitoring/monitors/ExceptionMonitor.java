package hadooptest.monitoring.monitors;

import hadooptest.automation.utils.exceptionParsing.ExceptionPeel;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ExceptionMonitor extends AbstractMonitor {
	List<ExceptionPeel> exceptionPeels;

	ExceptionMonitor(String clusterName,
			HashMap<String, ArrayList<String>> sentComponentToHostMapping,
			Class<?> testClass, String testMethodName, int periodicity,
			String resourceKind) {
		super(clusterName, sentComponentToHostMapping, testClass,
				testMethodName, periodicity, resourceKind);
		
	}

	public void setExceptionPeels(List<ExceptionPeel> exceptionPeels){
		this.exceptionPeels = exceptionPeels;
	}
	public void logExceptions() {
		String dumpLocation;
		HashMap<String, Integer> exceptionCountHashMap = new HashMap<String, Integer>();
		for (ExceptionPeel exceptionPeel : exceptionPeels) {
			if (exceptionCountHashMap.containsKey(exceptionPeel.getExceptionName())) {
				int exceptionCount = exceptionCountHashMap.get(exceptionPeel.getExceptionName());
				exceptionCountHashMap.put(exceptionPeel.getExceptionName(),
						++exceptionCount);
			} else {
				exceptionCountHashMap.put(exceptionPeel.getExceptionName(), 1);
			}
		}

		dumpLocation = baseDirPathToDump + this.kind;
		File exceptionFileHandle = new File(dumpLocation);

		PrintWriter printWriter = null;

		try {
			exceptionFileHandle.createNewFile();
			printWriter = new PrintWriter(exceptionFileHandle);
			for (String exceptionName : exceptionCountHashMap.keySet()) {
				printWriter.print(exceptionName + ":"
						+ exceptionCountHashMap.get(exceptionName));
				printWriter.println();
			}
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
	public void fetchResourceUsageIntoMemory(int tick) throws IOException {
		/*
		 * Not applicable, because ExceptionMonitor is not a monitor in a true
		 * sense
		 */

	}

	public void startMonitoring() {
		/*
		 * Not applicable, because ExceptionMonitor is not a monitor in a true
		 * sense
		 */

	}

	@Override
	public void stopMonitoring() {
		/*
		 * Not applicable, because ExceptionMonitor is not a monitor in a true
		 * sense
		 */
	}

	@Override
	synchronized public void run() {
		/*
		 * Not applicable, because ExceptionMonitor is not a monitor in a true
		 * sense
		 */
	}

}
