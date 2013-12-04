package hadooptest.hadoop.stress;

import java.util.ArrayList;
import java.util.List;

import hadooptest.automation.utils.exceptionParsing.ExceptionParsingOrchestrator;

import org.junit.Test;

public class TestExceptionParsingAndBucketing {
	@Test
	public void testExceptionParsingAndBucketing() {
		List<String> completeDirPathsToDumpedFiles = new ArrayList<String>();
		String cluster = System.getProperty("CLUSTER_NAME").trim();
        completeDirPathsToDumpedFiles
                        .add("/grid/0/tmp/stressMonitoring/" + cluster + "/hadooptest.hadoop.stress/"
                                        + "TestMonitoring/secondTestBeingMonitored/Dec_03_2013_22_06_51/FILES/"
                                        + "home/gs/var/log/hdfsqa/");
        completeDirPathsToDumpedFiles
                        .add("/grid/0/tmp/stressMonitoring/" + cluster + "/hadooptest.hadoop.stress/"
                                        + "TestMonitoring/secondTestBeingMonitored/Dec_03_2013_22_06_51/FILES/"
                                        + "home/gs/var/log/mapredqa/");
		ExceptionParsingOrchestrator exceptionParsingO10r = new ExceptionParsingOrchestrator(
				completeDirPathsToDumpedFiles);
		try {
			exceptionParsingO10r.peelAndBucketExceptions();
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
		exceptionParsingO10r.showExceptions();
		exceptionParsingO10r.showBucketUtilization();

	}
}
