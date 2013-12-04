package hadooptest.monitoring.monitors;

import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.automation.constants.LoggingNomenclature;
import hadooptest.automation.utils.exceptionParsing.ExceptionParsingOrchestrator;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

/**
 * <p>
 * Fetches the logs relevant from the start to finish of a test run. When the
 * test starts, this fetches the metadata {@link LogMetadata} for each file.
 * There is a predictable format of the logs, so if a log exists on the host the
 * start timestamp is marked and logs are fetched from that point onwards, until
 * the end of the test.
 * </p>
 * 
 * @author tiwari
 * 
 */
public class LogMonitor extends AbstractMonitor {
	Logger logger = Logger.getLogger(LogMonitor.class);
	List<LogMetadata> fileMetadataList;
	static HashMap<String, HashMap<String, String>> supportingData = new HashMap<String, HashMap<String, String>>();
	static String USER = "user";
	static String PREFIX = "prefix";
	private ArrayList<String> completeDirPathsToDumpedFiles;

	public LogMonitor(String clusterName,
			HashMap<String, ArrayList<String>> sentComponentToHostMapping,
			Class<?> testClass, String testMethodName) {
		super(clusterName, sentComponentToHostMapping, testClass,
				testMethodName, 0, HadooptestConstants.Miscellaneous.FILES);

		commandStrings = null;
		fileMetadataList = new ArrayList<LogMetadata>();
		initializeFileMetadata();
		completeDirPathsToDumpedFiles = new ArrayList<String>();

	}

	void initializeFileMetadata() {
		for (String aComponent : componentToHostMapping.keySet()) {
			logger.info("Log processing for component:" + aComponent);
			for (String aHostname : componentToHostMapping.get(aComponent)) {
				LogMetadata fileMetadata = new LogMetadata(
						LoggingNomenclature.getLogPrefix(aComponent),
						LoggingNomenclature.getUser(aComponent), aComponent,
						aHostname);
				if (fileMetadata.fileExists) {
					fileMetadataList.add(fileMetadata);
				}
			}
		}
	}

	/**
	 * Since we are collecting logs here, a one time operation. There is nothing
	 * to dump to memory, hence stop the thread. The logs would be pulled at the
	 * end of the test run.
	 */
	@Override
	public void fetchResourceUsageIntoMemory(int tick) throws IOException {
		// Just so that the thread stops, since logs just need to be collected
		// at the end as a one time thinggy.
		stopMonitoring();
	}

	/**
	 * This method overrides the {@link AbstractMonitor} dumpData, because
	 * unlike the other monitors (CPU, Memory) the log files are fetched at the
	 * end of a test run.
	 */
	@Override
	public void dumpData() {
		for (LogMetadata fileMetadata : fileMetadataList) {
			if (fileMetadata.fileExists) {

				String[] pdshCmd = { HadooptestConstants.ShellCommand.PDSH,
						"-N", "-w", fileMetadata.hostname };
				ArrayList<String> temp = new ArrayList<String>();

				String cmdStrng = "sed -n '/" + fileMetadata.loggingStartTime
						+ "/,/EOF/p' " + fileMetadata.fileName;
				logger.info("CMD STR:" + cmdStrng);
				String testCmd[] = { cmdStrng };

				temp.addAll(Arrays.asList(pdshCmd));
				temp.addAll(Arrays.asList(testCmd));
				String[] commandStrings = temp
						.toArray(new String[pdshCmd.length + testCmd.length]);
				String responseLine;
				Process p;

				StringBuffer sb = new StringBuffer();
				for (String aFrag : commandStrings) {
					sb.append(aFrag);
					sb.append(" ");
				}
				logger.info("Running command:" + sb.toString());
				try {
					p = Runtime.getRuntime().exec(commandStrings);
					String dumpLocation;
					dumpLocation = baseDirPathToDump +  this.kind;
					int lastIndexOfBackSlash = fileMetadata.fileName
							.lastIndexOf('/');
					String dirAddition = fileMetadata.fileName.substring(0,
							lastIndexOfBackSlash);
					dumpLocation += dirAddition;
					logger.info("New directory path:" + dumpLocation);
					new File(dumpLocation).mkdirs();
					completeDirPathsToDumpedFiles.add(dumpLocation);
					fileHandleOnCompletePathToOutputFile = new File(
							dumpLocation
									+ "/"
									+ fileMetadata.fileName
											.substring(lastIndexOfBackSlash + 1));

					fw = new FileWriter(
							fileHandleOnCompletePathToOutputFile
									.getCanonicalFile());
					bw = new BufferedWriter(fw);
					BufferedReader r = new BufferedReader(
							new InputStreamReader(p.getInputStream()));
					while ((responseLine = r.readLine()) != null) {
						bw.write(responseLine);
						bw.write('\n');
					}
					bw.close();

				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		}

	}

}
