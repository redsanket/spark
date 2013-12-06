package hadooptest.automation.utils.exceptionParsing;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
/**
 * The main "orchestrator" class for exception parsing/bucketing. Given a list
 * of directories it loops through each directory and collates the files. Then
 * for each file it fires off a thread to parse/bucket the exceptions.
 * @author tiwari
 *
 */
public class ExceptionParsingOrchestrator {
	Logger logger = Logger.getLogger(ExceptionParsingOrchestrator.class);
	List<File> exceptionFiles;
	ExecutorService executor;
	ExceptionBucketer exceptionBucketer;
	public ExceptionParsingOrchestrator(List<String> dirs){
		exceptionBucketer = ExceptionBucketer.getInstance();
		exceptionFiles = new ArrayList<File>();
		for(String aDir:dirs){
			File aFolder = new File(aDir);
			for (File aLogFileThatMayContainExceptions:aFolder.listFiles()){
				exceptionFiles.add(aLogFileThatMayContainExceptions);
				logger.info("ADDED:" + aLogFileThatMayContainExceptions.getName());
				logger.info("Absolute path:" + aLogFileThatMayContainExceptions.getAbsolutePath());
			}			
		}	
		
		executor = Executors.newFixedThreadPool(exceptionFiles.size());
	}
	
	public void peelAndBucketExceptions() throws InterruptedException{

		for (int xx=0; xx<exceptionFiles.size(); xx++){
			Runnable exceptionPeeler = new ExceptionPeeler(exceptionFiles.get(xx), exceptionBucketer);
			executor.execute(exceptionPeeler);
		}
		executor.shutdown();
	    // Wait until all threads are finish
	    executor.awaitTermination(120, TimeUnit.SECONDS);
	    logger.info("Finished all threads");
	}
	
	public void showExceptions(){
		exceptionBucketer.showExceptions();
	}
	
	public void showBucketUtilization(){
		exceptionBucketer.showBucketUtilization();
	}
}
