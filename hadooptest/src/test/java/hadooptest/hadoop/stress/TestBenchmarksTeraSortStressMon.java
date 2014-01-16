package hadooptest.hadoop.stress;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.automation.utils.exceptionParsing.ExceptionParsingOrchestrator;
import hadooptest.cluster.hadoop.DFS;
import hadooptest.cluster.hadoop.HadoopCluster;
import hadooptest.cluster.hadoop.HadoopCluster.Action;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.config.hadoop.HadoopConfiguration;
import hadooptest.monitoring.Monitorable;
import hadooptest.workflow.hadoop.job.GenericJob;
import hadooptest.workflow.hadoop.job.JobClient;

import java.io.File;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.io.FileInputStream;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.examples.terasort.TeraValidate;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.QueueInfo;
//import org.apache.hadoop.yarn.client.YarnClientImpl; // H0.23
import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl; // H2.x
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import coretest.SerialTests;

/*
 *  Runs the teragen and terasort test. Takes about 2 minutes to run.
 *  Added another test 'testSortRunForSpecifiedTime', which is based on 'testSort' only
 *  it takes a time to run in minutes from the properties conf file. 
 */

@Category(SerialTests.class)
public class TestBenchmarksTeraSortStressMon extends TestSession {
    
    @BeforeClass
    public static void startTestSession() throws Exception {
        TestSession.start();
        setupTestConf();
        setupTestDir();
    }

	public static void setupTestConf() throws Exception {
		FullyDistributedCluster cluster = (FullyDistributedCluster) TestSession.cluster;
		String component = HadoopCluster.RESOURCE_MANAGER;

		/*
		 * NOTE: Add a check via the Hadoop API or jmx to determine if a single
		 * queue is already in place. If so, skip the following as to not waste
		 * time.
		 */
		YarnClientImpl yarnClient = new YarnClientImpl();
		yarnClient.init(TestSession.getCluster().getConf());
		yarnClient.start();

		List<QueueInfo> queues = yarnClient.getAllQueues();
		assertNotNull("Expected cluster queue(s) not found!!!", queues);
		TestSession.logger.info("queues='" + Arrays.toString(queues.toArray())
				+ "'");
		if ((queues.size() == 1)
				&& (Float.compare(queues.get(0).getCapacity(), 1.0f) == 0)) {
			TestSession.logger.debug("Cluster is already setup properly."
					+ "Nothing to do.");
			return;
		}

		// Backup the default configuration directory on the Resource Manager
		// component host.
		cluster.getConf(component).backupConfDir();

		// Copy files to the custom configuration directory on the
		// Resource Manager component host.
		String sourceFile = TestSession.conf.getProperty("WORKSPACE")
				+ "/conf/SingleQueueConf/single-queue-capacity-scheduler.xml";
		cluster.getConf(component).copyFileToConfDir(sourceFile,
				"capacity-scheduler.xml");
		cluster.hadoopDaemon(Action.STOP, component);
		cluster.hadoopDaemon(Action.START, component);
	}

	public static void setupTestDir() throws Exception {
		// Define the test directory
		DFS dfs = new DFS();
		String testDir = dfs.getBaseUrl() + "/user/"
				+ System.getProperty("user.name") + "/terasort";

		// Delete it existing test directory if exists
		FileSystem fs = TestSession.cluster.getFS();
		FsShell fsShell = TestSession.cluster.getFsShell();
		if (fs.exists(new Path(testDir))) {
			TestSession.logger.info("Delete existing test directory: "
					+ testDir);
			fsShell.run(new String[] { "-rm", "-r", testDir });
		}

		// Create or re-create the test directory.
		TestSession.logger.info("Create new test directory: " + testDir);
		fsShell.run(new String[] { "-mkdir", "-p", testDir });
	}

	// Create test data
	private void createData(String outputDir) throws Exception {

		String jobName = "teragen";
		String jar = TestSession.cluster.getConf().getHadoopProp(
				"HADOOP_EXAMPLE_JAR");
		String dataSize = "2";

		GenericJob job = new GenericJob();
		job.setJobJar(jar);
		job.setJobName(jobName);
		ArrayList<String> jobArgs = new ArrayList<String>();
		jobArgs.add("-Dmapreduce.job.acl-view-job=*");
		jobArgs.add(dataSize);
		jobArgs.add(outputDir);
		job.setJobArgs(jobArgs.toArray(new String[0]));
		job.start();
		job.waitForID(600);
		boolean isSuccessful = job.waitForSuccess(20);
		assertTrue(
				"Unable to run job '" + jobName + "': cmd='"
						+ StringUtils.join(job.getCommand(), " ") + "'.",
				isSuccessful);
	}

	// Run a sort job
	private void runSortJob(String sortInput, String sortOutput)
			throws Exception {

		String jobName = "terasort";
		String jar = TestSession.cluster.getConf().getHadoopProp(
				"HADOOP_EXAMPLE_JAR");

		GenericJob job;
		job = new GenericJob();
		job.setJobJar(jar);
		job.setJobName(jobName);
		ArrayList<String> jobArgs = new ArrayList<String>();
		jobArgs.add("-Dmapreduce.job.acl-view-job=*");
		jobArgs.add("-Dmapreduce.reduce.input.limit=-1");
		jobArgs.add(sortInput);
		jobArgs.add(sortOutput);

		/*
		 * int numReduces = 2; jobArgs.add("-r");
		 * jobArgs.add(Integer.toString(numReduces));
		 */

		job.setJobArgs(jobArgs.toArray(new String[0]));
		job.start();
		job.waitForID(600);
		boolean isSuccessful = job.waitForSuccess(20);
		assertTrue(
				"Unable to run SORT job: cmd="
						+ StringUtils.join(job.getCommand(), " "), isSuccessful);
	}

	/*
	 * validate sort was successful
	 */
	public void validateSortResults(String sortOutput, String sortReport)
			throws Exception {
		TestSession.logger.debug("Validate Sort Results : ");

		/*
		 * Submit a sort validate job:
		 * 
		 * CLI: CMD="$HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR jar
		 * $HADOOP_MAPRED_TEST_JAR testmapredsort ${YARN_OPTIONS} -sortInput
		 * sortInputDir -sortOutput sortOutputDir"
		 * 
		 * API Call:
		 */
		ArrayList<String> jobArgs = new ArrayList<String>();
		jobArgs.add("-Dmapreduce.job.acl-view-job=*");
		jobArgs.add("-Dmapreduce.reduce.input.limit=-1");
		jobArgs.add(sortOutput);
		jobArgs.add(sortReport);
		String[] args = jobArgs.toArray(new String[0]);
		HadoopConfiguration conf = TestSession.getCluster().getConf();
		int rc = ToolRunner.run(conf, new TeraValidate(), args);
		if (rc != 0) {
			TestSession.logger.error("SortValidator failed!!!");
		}

		boolean isSuccessful = (rc == 0) ? true : false;
		assertTrue("Unable to run terasort validation job.", isSuccessful);
	}

    @Test 
    @Monitorable(cpuPeriodicity = 3, memPeriodicity = 3)
    public void testSort() throws Exception{
        String tcDesc = "Runs hadoop terasort and verifies that " + 
                "the data is properly sorted";
        TestSession.logger.info("Run test: " + tcDesc);

        DFS dfs = new DFS();
        String testDir = dfs.getBaseUrl() + "/user/" +
                System.getProperty("user.name") + "/terasort";
        
        // Generate sort data
        String dataDir = testDir + "/teraInputDir";
        createData(dataDir);
        
        // Sort the data
        String sortInputDir = dataDir;
        String sortOutputDir = testDir + "/teraOutputDir";
        runSortJob(sortInputDir, sortOutputDir);

        // Validate the data
        String sortReport = testDir + "/teraReport";
        validateSortResults(sortOutputDir, sortReport);
    }
    
    /*
     * After each test, fetch the job task reports.
     */
    @After
    public void logTaskResportSummary() 
            throws InterruptedException, IOException {
        JobClient jobClient = TestSession.cluster.getJobClient();
        jobClient.validateTaskReportSummary(
                jobClient.logTaskReportSummary(
                        "tasks_report.log", TestSession.testStartTime), 0, 0);        
    }

	@AfterClass
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
				+ "/target/surefire-reports";
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

    @Test 
    @Monitorable(cpuPeriodicity = 10, memPeriodicity = 10)
    public void testSortRunForSpecifiedTime() throws Exception{
    	
    	// get timeToRunMinutes from the .properties file.
    	Properties prop = new Properties();
    	String workingDir = System.getProperty("user.dir");
    	try {
            //load properties file
    		prop.load(new FileInputStream(workingDir+"/conf/StressConf/StressTestProp.properties"));
    	} catch (IOException ex) {
    		TestSession.logger.error("Problem loading StressTestProp.properties file");
    		ex.printStackTrace();
        }
    	int timeToRunMinutes  = Integer.parseInt(prop.getProperty("Stress.stressTimeToRunMinutes"));
        TestSession.logger.debug("Property file sets time to run as: " + timeToRunMinutes + " minutes");
    	
        String tcDesc = "Runs hadoop terasort for specified time and " +
        		"verifies that the data is properly sorted";
        TestSession.logger.info("Run test for " + timeToRunMinutes + " minutes:\n" + tcDesc);

        DFS dfs = new DFS();
        String testDir = dfs.getBaseUrl() + "/user/" +
                System.getProperty("user.name") + "/terasort";
        
        // run the set of sort jobs (data-gen/sort/validate) for specified time,
        // runtime resolution is driven by the job's runtimes, which is about a
        // minute, so the actual runtime may be higher than specified by as much 
        // as this amount (~1 minute)
        long currentTime = (System.currentTimeMillis()/1000); // start time in seconds
        long startTime = currentTime;
        long runTime = timeToRunMinutes*60; // get runtime in seconds
        long endTime = currentTime + runTime; // get time to stop
        TestSession.logger.debug("Time test needs to run in seconds: " + runTime);
        TestSession.logger.debug("Start time in seconds: " + currentTime);
        TestSession.logger.debug("End time in seconds: " + endTime);
        
        // while wrapping the original terasort job, controlled by the desired
        // runtime, tracks and logs the number of job sets we've run in the given
        // runtime, job sets being; data-gen, sorting, and validation.
        int passcount = 0;
        while (currentTime < endTime) {
        	
        	// Generate sort data
            String dataDir = testDir + "/teraInputDir"+passcount;
            createData(dataDir);
            
            // Sort the data
            String sortInputDir = dataDir;
            String sortOutputDir = testDir + "/teraOutputDir"+passcount;
        	runSortJob(sortInputDir, sortOutputDir);

        	// Validate the data
        	String sortReport = testDir + "/teraReport"+passcount;
        	validateSortResults(sortOutputDir, sortReport);
                	
        	// update the loop control and job counter
        	currentTime = (System.currentTimeMillis()/1000);
        	passcount++;
        	TestSession.logger.debug("currentTime is: " + currentTime);
        	TestSession.logger.info("Ran sort job set, passcount is: " + passcount);
        }
        // log the overall run's job's info
        TestSession.logger.info("Completed, ran " + passcount + " job sets (generate data, sort, and validate) in " + 
        		(currentTime-startTime) + " seconds");
    }  
    
}
