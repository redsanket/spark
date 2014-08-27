package hadooptest.monitoring.exceptionParsing;

import hadooptest.TestSession;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.List;

import org.apache.log4j.Logger;

/**
 * <p>
 * This thread runs per input file that needs to be combed for exceptions. On
 * running into an exception "peel" it into an {@link ExceptionPeel}.
 * </p>
 * 
 * @author tiwari
 * 
 */
public class ExceptionPeeler implements Runnable {
	Logger logger = Logger.getLogger(ExceptionPeeler.class);
	File aFile;
	ExceptionBucketer exceptionBucketer;
	List<ExceptionPeel> exceptionPeels;
	private String dateTimeStampAtLineBeginningPattern = "^(\\d{4}-\\d{2}-\\d{2}\\s+\\d{2}:\\d{2}:\\d{2},\\d{3}).*";

	ExceptionPeeler(File aFile, ExceptionBucketer exceptionBucketer) {
		this.aFile = aFile;
		this.exceptionBucketer = exceptionBucketer;
	}

	ExceptionPeeler(File aFile, List<ExceptionPeel> exceptionPeels) {
		this.aFile = aFile;
		this.exceptionPeels = exceptionPeels;
		this.exceptionBucketer = null;
	}

	public void run() {
		logger.trace("Now scouring file[" + aFile.getName()
				+ "] for exceptions.");
		boolean inTheMiddleOfRecordingExceptionLines = false;
		BufferedReader br = null;
		String exceptionOneLiner = null;
		String dateTimestampString = null;
		String loggedLine;
		StringBuilder sb = new StringBuilder();

		try {
			br = new BufferedReader(new FileReader(aFile));
			while ((loggedLine = br.readLine()) != null) {
				if (!inTheMiddleOfRecordingExceptionLines) {
					if (loggedLine.contains("WARN")) {
						/*
						 * This is to do away with false positives like this
						 * line 2014-02-01 18:00:00,295 [Trash Emptier] WARN
						 * security.UserGroupInformation:
						 * PriviledgedActionException
						 * as:hdfs/gsbl90929.blue.ygrid
						 * .yahoo.com@DEV.YGRID.YAHOO.COM (auth:KERBEROS)
						 * cause:javax.security.sasl.SaslException: GSS initiate
						 * failed [Caused by GSSException: No valid credentials
						 * provided (Mechanism level: Failed to find any
						 * Kerberos tgt)]
						 */
						continue;
					}
				}
				if ((loggedLine.toLowerCase().contains("exception") || loggedLine
						.toLowerCase().contains("error"))
						&& loggedLine
								.matches(dateTimeStampAtLineBeginningPattern)) {
					TestSession.logger.trace("TRIPPED ON EXCEPTION LINE:" + loggedLine);
					inTheMiddleOfRecordingExceptionLines = true;
					dateTimestampString = loggedLine.replaceAll(
							dateTimeStampAtLineBeginningPattern, "$1");
					sb.append(loggedLine);
					exceptionOneLiner = loggedLine;
					continue;
				}

				if (inTheMiddleOfRecordingExceptionLines
						&& loggedLine
								.matches(dateTimeStampAtLineBeginningPattern)) {
					inTheMiddleOfRecordingExceptionLines = false;
					sb.append("\n");
					if (exceptionBucketer != null) {
						/*
						 * We are bucketing exceptions instead of logging.
						 */
						createAndRegisterExceptionPeel(aFile.getName(),
								dateTimestampString, exceptionOneLiner,
								sb.toString());
					} else {
						/*
						 * We are logging/remembering exceptions instead of
						 * bucketing
						 */
						createAndRememberExceptionPeel(aFile.getName(),
								dateTimestampString, exceptionOneLiner,
								sb.toString());
					}
					sb = new StringBuilder();
				}

				if (inTheMiddleOfRecordingExceptionLines) {
					sb.append("\n");
					sb.append(loggedLine);
				}
			}
			br.close();

		} catch (FileNotFoundException e) {

			e.printStackTrace();
		} catch (IOException e) {

			e.printStackTrace();
		}
	}

	void createAndRegisterExceptionPeel(String fileName, String timestamp,
			String exceptionOneLiner, String blurb) {
		try {
			ExceptionPeel exceptionPeel = new ExceptionPeel(fileName,
					timestamp, exceptionOneLiner, blurb);
			exceptionBucketer.addException(exceptionPeel);

		} catch (ParseException e) {
			throw new RuntimeException(e);
		}
	}

	void createAndRememberExceptionPeel(String fileName, String timestamp,
			String exceptionOneLiner, String blurb) {
		try {
			ExceptionPeel exceptionPeel = new ExceptionPeel(fileName,
					timestamp, exceptionOneLiner, blurb);
			exceptionPeels.add(exceptionPeel);
			TestSession.logger
					.debug("In exceptionPeeler, concurrent size is now:"
							+ exceptionPeels.size());

		} catch (ParseException e) {
			throw new RuntimeException(e);
		}
	}

	boolean newLogLineStarted(String loggedLine) {
		boolean newLogLineStarted = false;
		String dateTimestampString = loggedLine.replaceAll(
				dateTimeStampAtLineBeginningPattern, "$1");
		if (!dateTimestampString.isEmpty() && !dateTimestampString.equals("")) {
			logger.info("MAtched new line hence stopping "
					+ dateTimestampString);
			newLogLineStarted = true;
		}
		return newLogLineStarted;
	}
}
