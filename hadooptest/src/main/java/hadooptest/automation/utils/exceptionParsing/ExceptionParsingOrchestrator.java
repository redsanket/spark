package hadooptest.automation.utils.exceptionParsing;

import hadooptest.TestSession;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

//import org.apache.log4j.TestSession.logger;
/**
 * The main "orchestrator" class for exception parsing/bucketing. Given a list
 * of directories it loops through each directory and collates the files. Then
 * for each file it fires off a thread to parse/bucket the exceptions.
 * 
 * @author tiwari
 * 
 */
public class ExceptionParsingOrchestrator {
	// TestSession.logger TestSession.logger =
	// TestSession.logger.getLogger(ExceptionParsingOrchestrator.class);
	List<File> exceptionFiles;
	ExecutorService executor;
	ExceptionBucketer exceptionBucketer;
	List<ExceptionPeel> exceptionPeelsStorage = Collections
			.synchronizedList(new ArrayList<ExceptionPeel>());

	public ExceptionParsingOrchestrator(List<String> files) {
		exceptionBucketer = ExceptionBucketer.getInstance();
		exceptionFiles = new ArrayList<File>();
		for (String aLogFileThatMayContainExceptions : files) {
			exceptionFiles.add(new File(aLogFileThatMayContainExceptions));
			TestSession.logger.trace("ADDED:"
					+ aLogFileThatMayContainExceptions);
		}

		executor = Executors.newFixedThreadPool(exceptionFiles.size());
	}

	public void peelAndBucketExceptions() throws InterruptedException {

		for (int xx = 0; xx < exceptionFiles.size(); xx++) {
			Runnable exceptionPeeler = new ExceptionPeeler(
					exceptionFiles.get(xx), exceptionBucketer);
			executor.execute(exceptionPeeler);
		}
		executor.shutdown();
		// Wait until all threads finish
		executor.awaitTermination(120, TimeUnit.SECONDS);
		TestSession.logger.trace("Finished all threads");
	}

	public void peelAndRememberExceptions() throws InterruptedException {

		for (int xx = 0; xx < exceptionFiles.size(); xx++) {
			Runnable exceptionPeeler = new ExceptionPeeler(
					exceptionFiles.get(xx), exceptionPeelsStorage);
			executor.execute(exceptionPeeler);
		}
		executor.shutdown();
		// Wait until all threads finish
		executor.awaitTermination(10, TimeUnit.SECONDS);
		TestSession.logger
				.trace("Finished all threads in peelAndRememberExceptions!");
	}

	public List<ExceptionPeel> fetchExceptionPeels() {
		return Collections.unmodifiableList(exceptionPeelsStorage);
	}

	public void showExceptions() {
		exceptionBucketer.showExceptions();
	}

	public void showBucketUtilization() {
		exceptionBucketer.showBucketUtilization();
	}

	public void logExceptionsInFile(String completePathToFile)
			throws IOException {
		exceptionBucketer.logExceptionsInFile(completePathToFile);
	}
}
