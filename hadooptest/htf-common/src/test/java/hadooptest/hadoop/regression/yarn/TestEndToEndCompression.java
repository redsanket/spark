package hadooptest.hadoop.regression.yarn;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import hadooptest.TestSession;
import hadooptest.cluster.hadoop.HadoopCluster;
import hadooptest.cluster.hadoop.dfs.DFS;
import hadooptest.config.hadoop.HadoopConfiguration;
import hadooptest.workflow.hadoop.job.GenericJob;
import hadooptest.workflow.hadoop.job.JobClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.SortValidator;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import hadooptest.ParallelMethodTests;

@Category(ParallelMethodTests.class)
public class TestEndToEndCompression extends TestSession {

    @After
    public void logTaskReportSummary() 
            throws InterruptedException, IOException {
        TestSession.logger.info("--------- @After: TestSession: logTaskReportSummary ----------------------------");
        TestSession.logger.debug(
                "logTaskReportSummary currently does not support " +
                "parallel method tests.");
    }

    /*
     * After each test, fetch the job task reports.
     */
    @AfterClass
    public static void logTaskReportSummaryAfterClass() 
            throws InterruptedException, IOException {
        TestSession.logger.info("--------- @AfterClass: TestSession: logTaskReportSummary ----------------------------");

        // Log the tasks report summary for jobs that ran as part of this test 
        JobClient jobClient = TestSession.cluster.getJobClient();
        int numAcceptableNonCompleteMapTasks = 20;
        int numAcceptableNonCompleteReduceTasks = 20;
        jobClient.validateTaskReportSummary(
                jobClient.logTaskReportSummary(
                        TestSession.TASKS_REPORT_LOG, 
                        TestSession.startTime),
                        numAcceptableNonCompleteMapTasks,
                        numAcceptableNonCompleteReduceTasks);        
    }
    
	private static final String[] CODECS = {
		"org.apache.hadoop.io.compress.GzipCodec",
		"org.apache.hadoop.io.compress.DefaultCodec",
		"org.apache.hadoop.io.compress.BZip2Codec",
		"org.apache.hadoop.io.compress.LzoCodec"
	};
	private static final String[] COMPRESSION_TYPES =
		{"NONE", "BLOCK", "RECORD"};
	private static final String[] DATA_TYPES = {"byte", "text"};
	private static int numReduces = 2;	
	private static String relativeTestDataDir = "Compression/";
	
	@BeforeClass
	public static void startTestSession() throws Exception{
		TestSession.start();
		TestSession.cluster.setupSingleQueueCapacity();
		TestSession.cluster.setupRwRtwTestData(relativeTestDataDir);
	}
	
	public static String[] runHadoopExampleJar(String[] jarCmd)
		throws Exception {
		String[] jobCmd = {
			TestSession.cluster.getConf().getHadoopProp("HADOOP_BIN"),
			"--config",
			TestSession.cluster.getConf().getHadoopConfDir(),
			"jar",
			TestSession.cluster.getConf().getHadoopProp("HADOOP_EXAMPLE_JAR"),
		};
		ArrayList<String> temp = new ArrayList<String>();
		temp.addAll(Arrays.asList(jobCmd));
		temp.addAll(Arrays.asList(jarCmd));
		String[] cmd = temp.toArray(new String[0]);
		return TestSession.exec.runProcBuilderSecurity(cmd);
	}

	/*
	 * Test running compression jobs
	 */

	/*
	 * The following code block is commented out because it will not allow
	 *  parallel test execution.
	 *  

	@Test
	public void runCompressionTest() throws Exception {
		TestSession.logger.info("Run EndToEnd Compression Test");
		setup();
		generateRandomByteData();
		generateRandomTextData();		
		testCompression();
	}

	public void testCompression() throws Exception {
		int index = 1;
		for (String jobCodec : CODECS) {
			for (String mapCodec : CODECS) {
				for (String compressionType : COMPRESSION_TYPES) {
					for (String dataType : DATA_TYPES) {
						TestSession.logger.info("****** Compression TC # " +
								index + " ********");
						testCompression(
								jobCodec, mapCodec, compressionType, dataType);
						index++;
					}
				}
			}
		}
	}
	 */

	// job codec = CODECS[0]
	@Test public void testCompression01() throws Exception{ testCompression(CODECS[0], CODECS[0], COMPRESSION_TYPES[0], DATA_TYPES[0]); }
	@Test public void testCompression02() throws Exception{ testCompression(CODECS[0], CODECS[0], COMPRESSION_TYPES[0], DATA_TYPES[1]); }
	@Test public void testCompression03() throws Exception{ testCompression(CODECS[0], CODECS[0], COMPRESSION_TYPES[1], DATA_TYPES[0]); }
	@Test public void testCompression04() throws Exception{ testCompression(CODECS[0], CODECS[0], COMPRESSION_TYPES[1], DATA_TYPES[1]); }
	@Test public void testCompression05() throws Exception{ testCompression(CODECS[0], CODECS[0], COMPRESSION_TYPES[2], DATA_TYPES[0]); }
	@Test public void testCompression06() throws Exception{ testCompression(CODECS[0], CODECS[0], COMPRESSION_TYPES[2], DATA_TYPES[1]); }

	@Test public void testCompression07() throws Exception{ testCompression(CODECS[0], CODECS[1], COMPRESSION_TYPES[0], DATA_TYPES[0]); }
	@Test public void testCompression08() throws Exception{ testCompression(CODECS[0], CODECS[1], COMPRESSION_TYPES[0], DATA_TYPES[1]); }
	@Test public void testCompression09() throws Exception{ testCompression(CODECS[0], CODECS[1], COMPRESSION_TYPES[1], DATA_TYPES[0]); }
	@Test public void testCompression10() throws Exception{ testCompression(CODECS[0], CODECS[1], COMPRESSION_TYPES[1], DATA_TYPES[1]); }
	@Test public void testCompression11() throws Exception{ testCompression(CODECS[0], CODECS[1], COMPRESSION_TYPES[2], DATA_TYPES[0]); }
	@Test public void testCompression12() throws Exception{ testCompression(CODECS[0], CODECS[1], COMPRESSION_TYPES[2], DATA_TYPES[1]); }

	@Test public void testCompression13() throws Exception{ testCompression(CODECS[0], CODECS[2], COMPRESSION_TYPES[0], DATA_TYPES[0]); }
	@Test public void testCompression14() throws Exception{ testCompression(CODECS[0], CODECS[2], COMPRESSION_TYPES[0], DATA_TYPES[1]); }
	@Test public void testCompression15() throws Exception{ testCompression(CODECS[0], CODECS[2], COMPRESSION_TYPES[1], DATA_TYPES[0]); }
	@Test public void testCompression16() throws Exception{ testCompression(CODECS[0], CODECS[2], COMPRESSION_TYPES[1], DATA_TYPES[1]); }
	@Test public void testCompression17() throws Exception{ testCompression(CODECS[0], CODECS[2], COMPRESSION_TYPES[2], DATA_TYPES[0]); }
	@Test public void testCompression18() throws Exception{ testCompression(CODECS[0], CODECS[2], COMPRESSION_TYPES[2], DATA_TYPES[1]); }

	@Test public void testCompression19() throws Exception{ testCompression(CODECS[0], CODECS[3], COMPRESSION_TYPES[0], DATA_TYPES[0]); }
	@Test public void testCompression20() throws Exception{ testCompression(CODECS[0], CODECS[3], COMPRESSION_TYPES[0], DATA_TYPES[1]); }
	@Test public void testCompression21() throws Exception{ testCompression(CODECS[0], CODECS[3], COMPRESSION_TYPES[1], DATA_TYPES[0]); }
	@Test public void testCompression22() throws Exception{ testCompression(CODECS[0], CODECS[3], COMPRESSION_TYPES[1], DATA_TYPES[1]); }
	@Test public void testCompression23() throws Exception{ testCompression(CODECS[0], CODECS[3], COMPRESSION_TYPES[2], DATA_TYPES[0]); }
	@Test public void testCompression24() throws Exception{ testCompression(CODECS[0], CODECS[3], COMPRESSION_TYPES[2], DATA_TYPES[1]); }

	// job codec = CODECS[1]
	@Test public void testCompression25() throws Exception{ testCompression(CODECS[1], CODECS[0], COMPRESSION_TYPES[0], DATA_TYPES[0]); }
	@Test public void testCompression26() throws Exception{ testCompression(CODECS[1], CODECS[0], COMPRESSION_TYPES[0], DATA_TYPES[1]); }
	@Test public void testCompression27() throws Exception{ testCompression(CODECS[1], CODECS[0], COMPRESSION_TYPES[1], DATA_TYPES[0]); }
	@Test public void testCompression28() throws Exception{ testCompression(CODECS[1], CODECS[0], COMPRESSION_TYPES[1], DATA_TYPES[1]); }
	@Test public void testCompression29() throws Exception{ testCompression(CODECS[1], CODECS[0], COMPRESSION_TYPES[2], DATA_TYPES[0]); }
	@Test public void testCompression30() throws Exception{ testCompression(CODECS[1], CODECS[0], COMPRESSION_TYPES[2], DATA_TYPES[1]); }

	@Test public void testCompression31() throws Exception{ testCompression(CODECS[1], CODECS[1], COMPRESSION_TYPES[0], DATA_TYPES[0]); }
	@Test public void testCompression32() throws Exception{ testCompression(CODECS[1], CODECS[1], COMPRESSION_TYPES[0], DATA_TYPES[1]); }
	@Test public void testCompression33() throws Exception{ testCompression(CODECS[1], CODECS[1], COMPRESSION_TYPES[1], DATA_TYPES[0]); }
	@Test public void testCompression34() throws Exception{ testCompression(CODECS[1], CODECS[1], COMPRESSION_TYPES[1], DATA_TYPES[1]); }
	@Test public void testCompression35() throws Exception{ testCompression(CODECS[1], CODECS[1], COMPRESSION_TYPES[2], DATA_TYPES[0]); }
	@Test public void testCompression36() throws Exception{ testCompression(CODECS[1], CODECS[1], COMPRESSION_TYPES[2], DATA_TYPES[1]); }

	@Test public void testCompression37() throws Exception{ testCompression(CODECS[1], CODECS[2], COMPRESSION_TYPES[0], DATA_TYPES[0]); }
	@Test public void testCompression38() throws Exception{ testCompression(CODECS[1], CODECS[2], COMPRESSION_TYPES[0], DATA_TYPES[1]); }
	@Test public void testCompression39() throws Exception{ testCompression(CODECS[1], CODECS[2], COMPRESSION_TYPES[1], DATA_TYPES[0]); }
	@Test public void testCompression40() throws Exception{ testCompression(CODECS[1], CODECS[2], COMPRESSION_TYPES[1], DATA_TYPES[1]); }
	@Test public void testCompression41() throws Exception{ testCompression(CODECS[1], CODECS[2], COMPRESSION_TYPES[2], DATA_TYPES[0]); }
	@Test public void testCompression42() throws Exception{ testCompression(CODECS[1], CODECS[2], COMPRESSION_TYPES[2], DATA_TYPES[1]); }

	@Test public void testCompression43() throws Exception{ testCompression(CODECS[1], CODECS[3], COMPRESSION_TYPES[0], DATA_TYPES[0]); }
	@Test public void testCompression44() throws Exception{ testCompression(CODECS[1], CODECS[3], COMPRESSION_TYPES[0], DATA_TYPES[1]); }
	@Test public void testCompression45() throws Exception{ testCompression(CODECS[1], CODECS[3], COMPRESSION_TYPES[1], DATA_TYPES[0]); }
	@Test public void testCompression46() throws Exception{ testCompression(CODECS[1], CODECS[3], COMPRESSION_TYPES[1], DATA_TYPES[1]); }
	@Test public void testCompression47() throws Exception{ testCompression(CODECS[1], CODECS[3], COMPRESSION_TYPES[2], DATA_TYPES[0]); }
	@Test public void testCompression48() throws Exception{ testCompression(CODECS[1], CODECS[3], COMPRESSION_TYPES[2], DATA_TYPES[1]); }

	// job codec = CODECS[2]
	@Test public void testCompression49() throws Exception{ testCompression(CODECS[2], CODECS[0], COMPRESSION_TYPES[0], DATA_TYPES[0]); }
	@Test public void testCompression50() throws Exception{ testCompression(CODECS[2], CODECS[0], COMPRESSION_TYPES[0], DATA_TYPES[1]); }
	@Test public void testCompression51() throws Exception{ testCompression(CODECS[2], CODECS[0], COMPRESSION_TYPES[1], DATA_TYPES[0]); }
	@Test public void testCompression52() throws Exception{ testCompression(CODECS[2], CODECS[0], COMPRESSION_TYPES[1], DATA_TYPES[1]); }
	@Test public void testCompression53() throws Exception{ testCompression(CODECS[2], CODECS[0], COMPRESSION_TYPES[2], DATA_TYPES[0]); }
	@Test public void testCompression54() throws Exception{ testCompression(CODECS[2], CODECS[0], COMPRESSION_TYPES[2], DATA_TYPES[1]); }

	@Test public void testCompression55() throws Exception{ testCompression(CODECS[2], CODECS[1], COMPRESSION_TYPES[0], DATA_TYPES[0]); }
	@Test public void testCompression56() throws Exception{ testCompression(CODECS[2], CODECS[1], COMPRESSION_TYPES[0], DATA_TYPES[1]); }
	@Test public void testCompression57() throws Exception{ testCompression(CODECS[2], CODECS[1], COMPRESSION_TYPES[1], DATA_TYPES[0]); }
	@Test public void testCompression58() throws Exception{ testCompression(CODECS[2], CODECS[1], COMPRESSION_TYPES[1], DATA_TYPES[1]); }
	@Test public void testCompression59() throws Exception{ testCompression(CODECS[2], CODECS[1], COMPRESSION_TYPES[2], DATA_TYPES[0]); }
	@Test public void testCompression60() throws Exception{ testCompression(CODECS[2], CODECS[1], COMPRESSION_TYPES[2], DATA_TYPES[1]); }

	@Test public void testCompression61() throws Exception{ testCompression(CODECS[2], CODECS[2], COMPRESSION_TYPES[0], DATA_TYPES[0]); }
	@Test public void testCompression62() throws Exception{ testCompression(CODECS[2], CODECS[2], COMPRESSION_TYPES[0], DATA_TYPES[1]); }
	@Test public void testCompression63() throws Exception{ testCompression(CODECS[2], CODECS[2], COMPRESSION_TYPES[1], DATA_TYPES[0]); }
	@Test public void testCompression64() throws Exception{ testCompression(CODECS[2], CODECS[2], COMPRESSION_TYPES[1], DATA_TYPES[1]); }
	@Test public void testCompression65() throws Exception{ testCompression(CODECS[2], CODECS[2], COMPRESSION_TYPES[2], DATA_TYPES[0]); }
	@Test public void testCompression66() throws Exception{ testCompression(CODECS[2], CODECS[2], COMPRESSION_TYPES[2], DATA_TYPES[1]); }

	@Test public void testCompression67() throws Exception{ testCompression(CODECS[2], CODECS[3], COMPRESSION_TYPES[0], DATA_TYPES[0]); }
	@Test public void testCompression68() throws Exception{ testCompression(CODECS[2], CODECS[3], COMPRESSION_TYPES[0], DATA_TYPES[1]); }
	@Test public void testCompression69() throws Exception{ testCompression(CODECS[2], CODECS[3], COMPRESSION_TYPES[1], DATA_TYPES[0]); }
	@Test public void testCompression70() throws Exception{ testCompression(CODECS[2], CODECS[3], COMPRESSION_TYPES[1], DATA_TYPES[1]); }
	@Test public void testCompression71() throws Exception{ testCompression(CODECS[2], CODECS[3], COMPRESSION_TYPES[2], DATA_TYPES[0]); }
	@Test public void testCompression72() throws Exception{ testCompression(CODECS[2], CODECS[3], COMPRESSION_TYPES[2], DATA_TYPES[1]); }

	// job codec = CODECS[3]
	@Test public void testCompression73() throws Exception{ testCompression(CODECS[3], CODECS[0], COMPRESSION_TYPES[0], DATA_TYPES[0]); }
	@Test public void testCompression74() throws Exception{ testCompression(CODECS[3], CODECS[0], COMPRESSION_TYPES[0], DATA_TYPES[1]); }
	@Test public void testCompression75() throws Exception{ testCompression(CODECS[3], CODECS[0], COMPRESSION_TYPES[1], DATA_TYPES[0]); }
	@Test public void testCompression76() throws Exception{ testCompression(CODECS[3], CODECS[0], COMPRESSION_TYPES[1], DATA_TYPES[1]); }
	@Test public void testCompression77() throws Exception{ testCompression(CODECS[3], CODECS[0], COMPRESSION_TYPES[2], DATA_TYPES[0]); }
	@Test public void testCompression78() throws Exception{ testCompression(CODECS[3], CODECS[0], COMPRESSION_TYPES[2], DATA_TYPES[1]); }

	@Test public void testCompression79() throws Exception{ testCompression(CODECS[3], CODECS[1], COMPRESSION_TYPES[0], DATA_TYPES[0]); }
	@Test public void testCompression80() throws Exception{ testCompression(CODECS[3], CODECS[1], COMPRESSION_TYPES[0], DATA_TYPES[1]); }
	@Test public void testCompression81() throws Exception{ testCompression(CODECS[3], CODECS[1], COMPRESSION_TYPES[1], DATA_TYPES[0]); }
	@Test public void testCompression82() throws Exception{ testCompression(CODECS[3], CODECS[1], COMPRESSION_TYPES[1], DATA_TYPES[1]); }
	@Test public void testCompression83() throws Exception{ testCompression(CODECS[3], CODECS[1], COMPRESSION_TYPES[2], DATA_TYPES[0]); }
	@Test public void testCompression84() throws Exception{ testCompression(CODECS[3], CODECS[1], COMPRESSION_TYPES[2], DATA_TYPES[1]); }

	@Test public void testCompression85() throws Exception{ testCompression(CODECS[3], CODECS[2], COMPRESSION_TYPES[0], DATA_TYPES[0]); }
	@Test public void testCompression86() throws Exception{ testCompression(CODECS[3], CODECS[2], COMPRESSION_TYPES[0], DATA_TYPES[1]); }
	@Test public void testCompression87() throws Exception{ testCompression(CODECS[3], CODECS[2], COMPRESSION_TYPES[1], DATA_TYPES[0]); }
	@Test public void testCompression88() throws Exception{ testCompression(CODECS[3], CODECS[2], COMPRESSION_TYPES[1], DATA_TYPES[1]); }
	@Test public void testCompression89() throws Exception{ testCompression(CODECS[3], CODECS[2], COMPRESSION_TYPES[2], DATA_TYPES[0]); }
	@Test public void testCompression90() throws Exception{ testCompression(CODECS[3], CODECS[2], COMPRESSION_TYPES[2], DATA_TYPES[1]); }

	@Test public void testCompression91() throws Exception{ testCompression(CODECS[3], CODECS[3], COMPRESSION_TYPES[0], DATA_TYPES[0]); }
	@Test public void testCompression92() throws Exception{ testCompression(CODECS[3], CODECS[3], COMPRESSION_TYPES[0], DATA_TYPES[1]); }
	@Test public void testCompression93() throws Exception{ testCompression(CODECS[3], CODECS[3], COMPRESSION_TYPES[1], DATA_TYPES[0]); }
	@Test public void testCompression94() throws Exception{ testCompression(CODECS[3], CODECS[3], COMPRESSION_TYPES[1], DATA_TYPES[1]); }
	@Test public void testCompression95() throws Exception{ testCompression(CODECS[3], CODECS[3], COMPRESSION_TYPES[2], DATA_TYPES[0]); }
	@Test public void testCompression96() throws Exception{ testCompression(CODECS[3], CODECS[3], COMPRESSION_TYPES[2], DATA_TYPES[1]); }
	
	public void testCompression(String jobCodec, String mapCodec,
			String compressionType, String dataType) throws Exception {
		
		StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
		StackTraceElement e = stacktrace[2];//maybe this number needs to be corrected
		String methodName = e.getMethodName();
		TestSession.logger.info("Test Case: " + methodName);
		
		
		String testDesc=
			"job codec=" + jobCodec + ", " +
			"map codec=" + mapCodec + ", " +
			"compression type=" + compressionType + ", " +
			"data type=" + dataType;
		TestSession.logger.debug(testDesc);
					
		String sortInput = relativeTestDataDir + "/" + dataType + "Input";

		String sortOutput =
		    relativeTestDataDir + "/" +
			jobCodec + "/" +
			mapCodec + "/" +
			compressionType + "/" +
			dataType + "/" +
			"output-" + TestSession.getFileDateFormat(new Date());
		/*
		String prefix = "hdfs://gsbl90269.blue.ygrid.yahoo.com/user/hadoopqa/";
		sortInput = prefix+sortInput;
		sortOutput = prefix+sortOutput;
		*/

		// Submit a sort job
		GenericJob job = new GenericJob();
        job.setJobJar(TestSession.cluster.getConf().getHadoopProp("HADOOP_EXAMPLE_JAR"));
        job.setJobName("sort");
        ArrayList<String> jobArgs = new ArrayList<String>();
		jobArgs.addAll(Arrays.asList(HadoopCluster.YARN_OPTS));
		jobArgs.addAll(Arrays.asList(HadoopCluster.COMPRESS_ON));
		jobArgs.add("-Dmapreduce.output.fileoutputformat.compress.codec=" +
				jobCodec);
		jobArgs.add("-Dmapreduce.map.output.compress.codec=" + mapCodec);
		jobArgs.add("-Dmapreduce.output.fileoutputformat.compress.type=" +
				compressionType);
		if (dataType.equals("text")) {
			jobArgs.add("-outKey");
			jobArgs.add("org.apache.hadoop.io.Text");
			jobArgs.add("-outValue");
			jobArgs.add("org.apache.hadoop.io.Text");
		}
		jobArgs.add(sortInput);
		jobArgs.add(sortOutput);
		jobArgs.add("-r");
		jobArgs.add(Integer.toString(numReduces));
		job.setJobArgs(jobArgs.toArray(new String[0]));
        job.start();
        job.waitForID(600);
		boolean isSuccessful = job.waitForSuccess(20);
		assertTrue(
			"Unable to run SORT job for job codec " + jobCodec +
			", map codec " + mapCodec + ", compression type " +
			compressionType + ": cmd=" + StringUtils.join(job.getCommand(), " "), 
			isSuccessful);
		
		// if (output[0].equals("0")) {
		if (isSuccessful) {
			/*
			DFS dfs = new DFS();
			dfs.fsls("/user/" + System.getProperty("user.name") + "/" + sortInput);
			dfs.fsls("/user/" + System.getProperty("user.name") + "/" + sortOutput);
			*/
			validateSortResults(jobCodec, mapCodec,
				compressionType, dataType, sortInput, sortOutput);
		}
	}
		
	/* 
	 * validate sort was successful
	 */
	public void validateSortResults(String jobCodec, String mapCodec,
			String compressionType, String dataType,
			String sortInput, String sortOutput) throws Exception {
		TestSession.logger.debug("Validate Sort Results : ");

		// Submit a sort validate job

		// API calls
        ArrayList<String> jobArgs = new ArrayList<String>();
        jobArgs.addAll(Arrays.asList(HadoopCluster.YARN_OPTS));
		jobArgs.add("-sortInput");
		jobArgs.add(sortInput);
		jobArgs.add("-sortOutput");
		jobArgs.add(sortOutput);
		String[] args = jobArgs.toArray(new String[0]);
		HadoopConfiguration conf = TestSession.getCluster().getConf();

                // getting NPE from ToolRunner (gridci-1395), adding debugging
                TestSession.logger.debug("XXXXXXXXXXXXX  DEBUGGING for gridci-1395 XXXXXXXXXXXXXXXXXX");
                if ( conf instanceof HadoopConfiguration )  TestSession.logger.debug("conf is instanceof HadoopConfiguration"); 
                TestSession.logger.debug("args is: " + Arrays.toString(args));
                TestSession.logger.debug("XXXXXXXXXXXXX  calling ToolRunner XXXXXXXXXXXXXXXXXX");

		int rc = ToolRunner.run(conf, new SortValidator(), args);
		if (rc != 0) {
			TestSession.logger.error("SortValidator failed!!!");
		}
		
		
		/*
		// CLI call
		
		GenericJob job = new GenericJob();
        job.setJobJar(TestSession.cluster.getConf().getHadoopProp("HADOOP_TEST_JAR"));
        job.setJobName("testmapredsort");
        ArrayList<String> jobArgs = new ArrayList<String>();
		jobArgs.addAll(Arrays.asList(YARN_OPTS));
		jobArgs.add("-sortInput");
		jobArgs.add(sortInput);
		jobArgs.add("-sortOutput");
		jobArgs.add(sortOutput);
		job.setJobArgs(jobArgs.toArray(new String[0]));
		job.start();
        job.waitForID(600);
		boolean isSuccessful = job.waitForSuccess(20);
		*/

		boolean isSuccessful = (rc == 0) ? true : false;
		assertTrue(
				"Unable to run SORT validation job for job codec " + jobCodec +
				", map codec " + mapCodec + ", compression type " +
				compressionType, isSuccessful);

		/*
		assertTrue(
			"Unable to run SORT validation job for job codec " + jobCodec +
			", map codec " + mapCodec + ", compression type " +
			compressionType + ": cmd=" + StringUtils.join(job.getCommand(), " "), 
			isSuccessful);
		 */	
		
		if (isSuccessful) {
			validateCompression(compressionType, jobCodec, sortOutput);
		}
		
	}
	
	/* 
	 * Verify that the data is really compressed
	 */
	public void validateCompression(String compressionType, String jobCodec,
			String sortOutput) throws Exception{
		TestSession.logger.debug("Validate Compression: checking output " +
			"partitions : ");

		DFS dfs = new DFS();
		String URL = dfs.getBaseUrl();
		FileSystem fs = TestSession.cluster.getFS();
		FileStatus[] elements = fs.listStatus(new Path(URL+"/user/" +
			System.getProperty("user.name") + "/" + sortOutput));
		
		for (FileStatus element : elements) {
			TestSession.logger.info("Checking part file: " + element.getPath());

			HadoopConfiguration conf = TestSession.getCluster().getConf();
			SequenceFile.Reader reader =
				new SequenceFile.Reader(conf,
						SequenceFile.Reader.file(element.getPath()));
			// Get Compression Type and Codec
			String elementType = reader.getCompressionType().toString();
			CompressionCodec codec = reader.getCompressionCodec();
			String elementCodec = (codec != null) ? 
					codec.getClass().getName().toString() : "";		

			// Check expected and actual type matches.
			assertEquals("Compression type does not match.", 
				compressionType, elementType);

			if (elementType.equals("NONE")) {
				// If type is NONE then there should be no codec.				
				assertEquals("Compression codec does not match.", 
					"", elementCodec);
			} else {
				// Check for codec only if type is not NONE.
				assertEquals("Compression codec does not match.", 
					jobCodec, elementCodec);
			}				
		}
	}		
}
