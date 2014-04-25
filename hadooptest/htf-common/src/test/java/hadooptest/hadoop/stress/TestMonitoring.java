package hadooptest.hadoop.stress;

import java.io.File;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import junit.framework.Assert;
import hadooptest.ParallelMethodTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.monitoring.Monitorable;
import hadooptest.monitoring.exceptionParsing.ExceptionParsingOrchestrator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.RandomWriter;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import hadooptest.SerialTests;

//@Category(SerialTests.class)
@Category(ParallelMethodTests.class)
public class TestMonitoring extends TestSession {
	public static final String BYTES_PER_MAP = "mapreduce.randomwriter.bytespermap";
	Logger logger = Logger.getLogger(TestMonitoring.class);

	@Rule
	public TemporaryFolder aTempFolderCreatedByJunit = new TemporaryFolder();

	@Test
	@Monitorable(cpuPeriodicity = 30, memPeriodicity = 30)
	public void testWithMonitoring() throws InterruptedException {
		
		
		logger.info("Beginning Stress test.............sleeping for 10 secs");
		Thread.sleep(30000);
		logger.info("Test waking up, to finish!!!!!!!");
	}

	@Test
	@Monitorable(cpuPeriodicity = 10, memPeriodicity = 10)
	public void testWithMonitoringBuddy() throws InterruptedException {
		logger.info("Beginning 2nd Stress test.............for the 2nd test!");
		Thread.sleep(30000);
		logger.info("2ns stress test finished");
	}
	
	@Test
	@Monitorable
	public void fireJobsInTheCourseOfTest1(){
		try {
			File tempRandomWriterFolder = aTempFolderCreatedByJunit.newFolder("randomWriter");
			runStdHadoopRandomWriter(tempRandomWriterFolder.getPath());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	@Monitorable
	public void fireJobsInTheCourseOfTest2(){
		try {
			File tempRandomWriterFolder = aTempFolderCreatedByJunit.newFolder("randomWriter2");
			runStdHadoopRandomWriter(tempRandomWriterFolder.getPath());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Test
	@Monitorable
	public void fireJobsInTheCourseOfTest3(){
		try {
			File tempRandomWriterFolder = aTempFolderCreatedByJunit.newFolder("randomWriter3");
			runStdHadoopRandomWriter(tempRandomWriterFolder.getPath());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Test
	@Monitorable
	public void fireJobsInTheCourseOfTest4(){
		try {
			File tempRandomWriterFolder = aTempFolderCreatedByJunit.newFolder("randomWriter3");
			runStdHadoopRandomWriter(tempRandomWriterFolder.getPath());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void runStdHadoopRandomWriter(String randomWriterOutputDirOnHdfs)
			throws Exception {
		logger.info("running RandomWriter.............");
		Configuration conf = TestSession.cluster.getConf();
		conf.setLong(BYTES_PER_MAP, 256000);
		int res = ToolRunner.run(conf, new RandomWriter(),
				new String[] { randomWriterOutputDirOnHdfs });

	}


//	@AfterClass
	static public void collateExceptions() throws ParseException, IOException {
		String cluster = System.getProperty("CLUSTER_NAME");
		Class<?> clazzz = new Object() {
		}.getClass().getEnclosingClass();
		String clazz = clazzz.getSimpleName();
		String packaze = clazzz.getPackage().getName();

		List<String> directoriesToScour = new ArrayList<String>();
		for (Method aMethod : clazzz.getMethods()) {
			Annotation[] annotations = aMethod.getAnnotations();
			for (Annotation anAnnotation : annotations) {
				if (anAnnotation.annotationType().isAssignableFrom(Test.class)) {
					String latestRun = getLatestRun(cluster, packaze, clazz,
							aMethod.getName());
					directoriesToScour.add(latestRun
							+ HadooptestConstants.UserNames.HDFSQA + "/");
					directoriesToScour.add(latestRun
							+ HadooptestConstants.UserNames.MAPREDQA + "/");

				}
			}
		}
		ExceptionParsingOrchestrator exceptionParsingO10r = new ExceptionParsingOrchestrator(
				directoriesToScour);
		try {
			exceptionParsingO10r.peelAndBucketExceptions();
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
		String outputBaseDirPath = TestSession.conf.getProperty("WORKSPACE")
				+ "/htf-common/target/surefire-reports";
		String outputFilePath = outputBaseDirPath + "/" + cluster + "_"
				+ packaze + "_" + clazz + "_" + "loggedExceptions.txt";

		TestSession.logger.info("Exceptions going into:" + outputFilePath);
		exceptionParsingO10r.logExceptionsInFile(outputFilePath);

	}

	static private String getLatestRun(String cluster, String packaze,
			String clazz, String testName) throws ParseException {
		TestSession.logger.info("PAckaze:" + packaze + " Clazz:" + clazz
				+ " TestName:" + testName);
		List<Date> dates = new ArrayList<Date>();
		String datesHangFromHere = "/grid/0/tmp/stressMonitoring/" + cluster
				+ "/" + packaze + "/" + clazz + "/" + testName + "/";
		File folder = new File(datesHangFromHere);
		File[] listOfFilesOrDirs = folder.listFiles();
		for (File aFileOrDirectory : listOfFilesOrDirs) {
			if (aFileOrDirectory.isDirectory()) {
				Date date = new SimpleDateFormat("MMM_dd_yyyy_HH_mm_ss")
						.parse(aFileOrDirectory.getName());
				dates.add(date);
			}
		}
		Collections.sort(dates);
		SimpleDateFormat sdf = new SimpleDateFormat("MMM_dd_yyyy_HH_mm_ss");
		
		String formattedDateAndTime = sdf.format(dates.get(dates.size()-1));
		return datesHangFromHere + formattedDateAndTime
				+ "/FILES/home/gs/var/log/";
	}
	
	@Override
	@After
	public void logTaskReportSummary() {
		// Override to hide the Test Session logs
	}


}
