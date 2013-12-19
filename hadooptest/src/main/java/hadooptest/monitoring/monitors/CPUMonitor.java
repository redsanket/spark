package hadooptest.monitoring.monitors;

import hadooptest.automation.constants.HadooptestConstants;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;

import hadooptest.TestSession;

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

	private String outputBaseDirPath = TestSession.conf.getProperty("WORKSPACE") + "/target/surefire-reports";
	private String outputFilePath = outputBaseDirPath + "/CPU_Utilization.txt";
	
	public CPUMonitor(String clusterName,
			HashMap<String, ArrayList<String>> sentComponentToHostMapping,
			Class<?> testClass, String testMethodName, int periodicity) {
		super(clusterName, sentComponentToHostMapping, testClass,
				testMethodName, periodicity,
				HadooptestConstants.Miscellaneous.CPU);
		commandStrings = new String[] { "bash", "-c",
				"pdsh -w " + commaSeperatedHosts + " mpstat -u 1 1" };
		
		File outputDir = new File(outputBaseDirPath);
		outputDir.mkdirs();
		File outputFile = new File(outputFilePath);
		try {
			outputFile.createNewFile();
		}
		catch (IOException ioe) {
			TestSession.logger.error("Could not create CPU monitoring output file", ioe);
		}
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
		boolean logHeader = false;
		
	    PrintWriter artifactFile = new PrintWriter(new BufferedWriter(new FileWriter(outputFilePath, true)));
		String header = "(CPU) utilization:" + 
				"                         CPU    %usr   %nice    %sys %iowait    %irq   %soft  %steal  %guest   %idle";
		String utilizationOutput;
	    
		while ((responseLine = r.readLine()) != null) {
			if (!responseLine.contains("Linux")
					&& !responseLine.contains("CPU")
					&& !responseLine.contains("Average")
					&& responseLine.contains("all")) {
				
				if (logHeader == false) {
					TestSession.logger.debug(header);
				    artifactFile.println(header);
					logHeader = true;
				}
				
				utilizationOutput = "(CPU) utilization: " + responseLine;
				TestSession.logger.debug(utilizationOutput);
			    artifactFile.println(utilizationOutput);
			    
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
		
	    artifactFile.close();
	}
}
