package hadooptest.monitoring.monitors;

import hadooptest.automation.constants.HadooptestConstants;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.text.SimpleDateFormat;
import java.util.HashMap;

import hadooptest.TestSession;

/**
 * <p>
 * Class extends {@link AbstractMonitor} and provides for an actual
 * implementation of the MemoryMonitor. The default frequency of Memory util
 * collection is 10 secods as defined in the {@link Monitorable} annotation, but
 * can be overridden when decorated over a test method.
 * </p>
 * 
 * @author tiwari
 * 
 */

public class MemoryMonitor extends AbstractMonitor {

	private String outputBaseDirPath = TestSession.conf.getProperty("WORKSPACE") + "/target/surefire-reports";
	private String outputFilePath = outputBaseDirPath + "/Memory_Utilization.txt";
	
	public MemoryMonitor(String clusterName,
			HashMap<String, ArrayList<String>> sentComponentToHostMapping,
			Class<?> testClass, String testMethodName, int periodicity) {
		super(clusterName, sentComponentToHostMapping, testClass,
				testMethodName, periodicity,
				HadooptestConstants.Miscellaneous.MEMORY);
		commandStrings = new String[] { "bash", "-c",
				"pdsh -w " + commaSeperatedHosts + " free -m" };

		File outputDir = new File(outputBaseDirPath);
		outputDir.mkdirs();
		File outputFile = new File(outputFilePath);
		try {
			outputFile.createNewFile();
		}
		catch (IOException ioe) {
			TestSession.logger.error("Could not create memory monitoring output file", ioe);
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

	    PrintWriter artifactFile = new PrintWriter(new BufferedWriter(new FileWriter(outputFilePath, true)));
		String header = "(MEMORY) utilization:";
		String utilizationOutput;
		boolean logHeader = false;
		
		while ((responseLine = r.readLine()) != null) {
			if (!responseLine.contains("cache")
					&& !responseLine.contains("Swap")) {
				String[] splits = responseLine.split("\\s+");
				if (splits.length < 2)
					continue; // Empty line
				String hostThatResponded = splits[0].split(":")[0];
				String totalMemString = splits[2];
				String usedMemString = splits[3];
				float totalMem = Float.parseFloat(totalMemString);
				float usedMem = Float.parseFloat(usedMemString);
				float memUsagePercentage = ((float) (usedMem / totalMem)) * 100;
				DecimalFormat df = new DecimalFormat("##.##");
				memUsagePercentage = Float.parseFloat(df
						.format(memUsagePercentage));
				
				if (logHeader == false) {
					TestSession.logger.debug(header);
				    artifactFile.println(header);
					logHeader = true;
				}
				
				String timeStamp = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(Calendar.getInstance().getTime());
				utilizationOutput = "(MEMORY) % utilization: " + hostThatResponded + ": " + memUsagePercentage;
				TestSession.logger.debug(utilizationOutput);
			    artifactFile.println(timeStamp + " : " + utilizationOutput);
				
				if (hostwiseReadings.containsKey(hostThatResponded)) {
					HashMap<Integer, String> timelapsedReding = hostwiseReadings
							.get(hostThatResponded);
					timelapsedReding.put(tick,
							Float.toString(memUsagePercentage));
				} else {
					HashMap<Integer, String> timelapsedReding = new HashMap<Integer, String>();
					timelapsedReding.put(tick,
							Float.toString(memUsagePercentage));
					hostwiseReadings.put(hostThatResponded, timelapsedReding);
				}
			}
		}
		artifactFile.close();
	}
}
