package hadooptest.monitoring.exceptionParsing;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;

/**
 * <p>
 * This is a Singleton class, as only one instance of this is expected to be
 * running. Once presented with an exception to bucket, it fetches its timestamp
 * and buckets it in the appropriate {@link ExceptionBucket}.
 * </p>
 * <p> In addition this class provides some housekeeping functions around listing
 * the exceptions and printing stats around them.
 * @author tiwari
 * 
 */
public class ExceptionBucketer {
	private static ExceptionBucketer singletonExceptionBucket = new ExceptionBucketer();
	Logger logger = Logger.getLogger(ExceptionBucketer.class);
	List<ExceptionBucket> exceptionBuckets;

	private ExceptionBucketer() {
		exceptionBuckets = Collections
				.synchronizedList(new ArrayList<ExceptionBucket>());

	}

	static public ExceptionBucketer getInstance() {
		return singletonExceptionBucket;
	}

	synchronized void addException(ExceptionPeel exceptionPeel) {
		ExceptionBucket homeBucket = findHomeBucket(exceptionPeel);
		if (homeBucket == null) {
			Timestamp startTimestamp = getTimeStampSansMilliseconds(exceptionPeel
					.getTimestamp());
			Timestamp endTimestamp = new Timestamp(
					startTimestamp.getTime() + 1000);
			homeBucket = new ExceptionBucket(startTimestamp, endTimestamp);
			exceptionBuckets.add(homeBucket);
		}
		homeBucket.addExceptionPeel(exceptionPeel);

	}

	ExceptionBucket findHomeBucket(ExceptionPeel exceptionPeel) {
		ExceptionBucket homeBucket = null;
		for (ExceptionBucket anExceptionBucket : exceptionBuckets) {
			if (anExceptionBucket.startTime.getTime() <= exceptionPeel
					.getTimestamp().getTime()
					&& anExceptionBucket.endTime.getTime() > exceptionPeel
							.getTimestamp().getTime()) {
				homeBucket = anExceptionBucket;
				break;
			}
		}
		return homeBucket;
	}

	Timestamp getTimeStampSansMilliseconds(Timestamp startTimestamp) {
		long startTime = startTimestamp.getTime();
		long milliseconds = startTime % 1000;
		startTime = startTime - milliseconds;
		return new Timestamp(startTime);
	}

	void showExceptions() {
		Collections.sort(exceptionBuckets);
		for (ExceptionBucket exceptionBucket : exceptionBuckets) {
			for (ExceptionPeel exceptionPeel : exceptionBucket
					.getExceptionPeels()) {
				logger.info(exceptionPeel.getFilename());
				logger.info(exceptionPeel.getTimestamp());
				logger.info(exceptionPeel.getBlurb());
				logger.info("******************************************************************************************");
				logger.info("******************************************************************************************");
			}
		}
	}

	int countExceptions() {
		int exceptionCnt = 0;
		for (ExceptionBucket anExBuck : exceptionBuckets) {
			exceptionCnt += anExBuck.getNumberOfExceptions();
		}
		return exceptionCnt;
	}

	void showBucketUtilization() {
		Collections.sort(exceptionBuckets);
		System.out
				.println("Total Number of buckets:" + exceptionBuckets.size());
		logger.info("Total Number of exceptions:" + countExceptions());
		for (ExceptionBucket anExBuck : exceptionBuckets) {
			System.out
					.println("Bucket starttime(" + anExBuck.startTime
							+ ") end time(" + anExBuck.endTime + ") has "
							+ anExBuck.getNumberOfExceptions()
							+ " count of exceptions");
			for (ExceptionPeel exp : anExBuck.exceptionPeels) {
				logger.info(exp.printSummary() + "\n");
			}
			logger.info("..............................................................................................");
		}

	}
	
	public void logExceptionsInFile(String completePathToFile) throws IOException{
		File logIntoThisFile = new File(completePathToFile);
		if (!logIntoThisFile.exists()){
			logIntoThisFile.createNewFile();
		}
		FileWriter fileWriter = new FileWriter(logIntoThisFile);
		BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
		bufferedWriter.write("--------------------------------------SUMMARY---------------------------------------------\n");
		bufferedWriter.write("Total count of Exception buckets:" + exceptionBuckets.size() + "\n");
		bufferedWriter.write("Total count of exceptions:" + countExceptions() + "\n");
		bufferedWriter.write("------------------------------------------------------------------------------------------\n");
		for (ExceptionBucket exceptionBucket : exceptionBuckets) {
			for (ExceptionPeel exceptionPeel : exceptionBucket
					.getExceptionPeels()) {
				
				bufferedWriter.write("vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv\n");				
				bufferedWriter.write("File Name: " + exceptionPeel.getFilename() +"\n");
				bufferedWriter.write("Time Stamp:" + exceptionPeel.getTimestamp() +"\n");
				bufferedWriter.write("Blurb:" + exceptionPeel.getBlurb() +"\n");
				bufferedWriter.write("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n");
			}
		}
		bufferedWriter.close();
		
	}

}
