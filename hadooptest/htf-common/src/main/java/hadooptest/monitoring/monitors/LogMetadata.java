package hadooptest.monitoring.monitors;

import hadooptest.automation.constants.HadooptestConstants;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.log4j.Logger;

/**
 * <p>
 * Before a test starts, this method goes and fetches the last timestamp in the
 * file, and sets it as a marker. Once the test ends the records are 'cut' from
 * the marked location through the end of the file and are copied by the
 * {@link LogMonitor}
 * </p>
 * 
 * @author tiwari
 * 
 */
public class LogMetadata {
	String fileName;
	String loggingStartTime;
	boolean fileExists;
	String hostname;
	String logPrefix;
	Logger logger = Logger.getLogger(LogMetadata.class);

	LogMetadata(String logPrefix, String user, String componentName,
			String componentHostname) {
		this.hostname = componentHostname;
		this.logPrefix = logPrefix;
		this.fileName = HadooptestConstants.Log.LOG_LOCATION + user + "/"
				+ logPrefix + "-" + user + "-" + componentName + "-"
				+ componentHostname + HadooptestConstants.Log.DOT_LOG;
		this.fileExists = doesFileExistOnHost(componentHostname);
		if (this.fileExists) {
			updateLogginStartTime(componentHostname);
		}
	}

	synchronized boolean doesFileExistOnHost(String hostname) {
		String responseLine;
		boolean itExists = false;

		String[] pdshCmd = { HadooptestConstants.ShellCommand.PDSH, "-w",
				hostname };
		ArrayList<String> temp = new ArrayList<String>();

		String testCmd[] = { "/bin/ls", fileName };
		temp.addAll(Arrays.asList(pdshCmd));
		temp.addAll(Arrays.asList(testCmd));
		String[] commandStrings = temp.toArray(new String[pdshCmd.length
				+ testCmd.length]);

		Process p;
		try {
			p = Runtime.getRuntime().exec(commandStrings);
			p.waitFor();

			BufferedReader r = new BufferedReader(new InputStreamReader(
					p.getInputStream()));
			while ((responseLine = r.readLine()) != null) {
				logger.info("RESPO:" + responseLine);
				if (responseLine.contains(hostname)) {
					itExists = true;
					break;
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (itExists) {
			logger.info(fileName + " exists");
		}
		return itExists;
	}

	/**
	 * Marks the location from where onwards the logs would be 'cut' and
	 * retrieved.
	 * 
	 * @param hostname
	 */
	void updateLogginStartTime(String hostname) {
		String[] pdshCmd = { HadooptestConstants.ShellCommand.PDSH, "-w",
				hostname };
		ArrayList<String> temp = new ArrayList<String>();

		String testCmd[] = { HadooptestConstants.ShellCommand.TAC, fileName,
				HadooptestConstants.ShellCommand.PIPE,
				HadooptestConstants.ShellCommand.GREP, "^[0-9][0-9][0-9][0-9]",
				"-m1" };
		temp.addAll(Arrays.asList(pdshCmd));
		temp.addAll(Arrays.asList(testCmd));
		String[] commandStrings = temp.toArray(new String[pdshCmd.length
				+ testCmd.length]);

		String responseLine;
		Process p;
		try {
			p = Runtime.getRuntime().exec(commandStrings);
			BufferedReader r = new BufferedReader(new InputStreamReader(
					p.getInputStream()));
			while ((responseLine = r.readLine()) != null) {

				logger.info("(BEGIN TIMESTAMP) Monitoring response:"
						+ responseLine);
				String[] words = responseLine.split("\\s+");
				if (words.length < 2)
					continue;
				String dateStamp = words[1];
				String timeStamp = words[2];
				this.loggingStartTime = dateStamp + " " + timeStamp;
			}

		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		logger.info("Updated start time as:" + loggingStartTime + " for file:"
				+ this.fileName);

	}

}
