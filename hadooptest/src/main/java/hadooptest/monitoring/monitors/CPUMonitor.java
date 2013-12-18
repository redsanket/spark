package hadooptest.monitoring.monitors;

import hadooptest.automation.constants.HadooptestConstants;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.log4j.Logger;

/**
 * <p>
 * Class extends {@link AbstractMonitor} and provides for an actual
 * implementation of the CPUMonitor. The default frequency of CPU util
 * collection is 10 secods as defined in the {@link Monitorable} annotation, but
 * can be overridden when decorated over a test method.
 * </p>
 * 
 * @author tiwari
 * 
 */
public class CPUMonitor extends AbstractMonitor {
	Logger logger = Logger.getLogger(CPUMonitor.class);

	public CPUMonitor(String clusterName,
			HashMap<String, ArrayList<String>> sentComponentToHostMapping,
			Class<?> testClass, String testMethodName, int periodicity) {
		super(clusterName, sentComponentToHostMapping, testClass,
				testMethodName, periodicity,
				HadooptestConstants.Miscellaneous.CPU);
		commandStrings = new String[] { "bash", "-c",
				"pdsh -w " + commaSeperatedHosts + " mpstat -u 1 1" };
	}

	/**
	 * At the set periodicity, the record is fetched and read in memory. The
	 * memory is dumped into a file, once the test finishes.
	 */
	@Override
	public void fetchResourceUsageIntoMemory(int tick) throws IOException {
		String responseLine;
		Process p = Runtime.getRuntime().exec(commandStrings);
		BufferedReader r = new BufferedReader(new InputStreamReader(
				p.getInputStream()));
		while ((responseLine = r.readLine()) != null) {
			if (!responseLine.contains("Linux")
					&& !responseLine.contains("CPU")
					&& !responseLine.contains("Average")) {
				logger.debug("            CPU    %usr   %nice    %sys %iowait    %irq   %soft  %steal  %guest   %idle");
				logger.debug("(CPU) Monitoring response:" + responseLine);
				String[] splits = responseLine.split("\\s+");
				if (splits.length < 2)
					continue;
				String hostThatResponded = splits[0].split(":")[0];
				String cpuUtilReading = splits[4];
				if (hostwiseReadings.containsKey(hostThatResponded)) {
					HashMap<Integer, String> timelapsedReding = hostwiseReadings
							.get(hostThatResponded);
					timelapsedReding.put(tick, cpuUtilReading);
				} else {
					HashMap<Integer, String> timelapsedReding = new HashMap<Integer, String>();
					timelapsedReding.put(tick, cpuUtilReading);
					hostwiseReadings.put(hostThatResponded, timelapsedReding);
				}
			}
		}
	}
}
