package hadooptest.hadoop.stress;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import hadooptest.TestSession;
import hadooptest.cluster.hadoop.DFS;
import hadooptest.cluster.hadoop.HadoopCluster;
import hadooptest.cluster.hadoop.HadoopCluster.Action;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.config.hadoop.HadoopConfiguration;
import hadooptest.monitoring.Monitorable;
import hadooptest.workflow.hadoop.job.GenericJob;
import hadooptest.workflow.hadoop.job.JobClient;
import hadooptest.workflow.hadoop.job.TaskReportSummary;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.examples.terasort.TeraValidate;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.QueueInfo;
//import org.apache.hadoop.yarn.client.YarnClientImpl; // H0.23
import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl; // H2.x
import org.junit.BeforeClass;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import coretest.SerialTests;

/*
 *  Runs the teragen and terasort test. Takes about 2 minutes to run.
 */

@Category(SerialTests.class)
public class TestBenchmarksTeraSortStressMon extends TestSession {

    @BeforeClass
    public static void startTestSession() throws Exception{
        TestSession.start();
        setupTestConf();
        setupTestDir();
    }

    public static void setupTestConf() throws Exception {
        FullyDistributedCluster cluster =
                (FullyDistributedCluster) TestSession.cluster;
        String component = HadoopCluster.RESOURCE_MANAGER;

        /* 
         * NOTE: Add a check via the Hadoop API or jmx to determine if a single
         * queue is already in place. If so, skip the following as to not waste
         *  time.
         */
        YarnClientImpl yarnClient = new YarnClientImpl();
        yarnClient.init(TestSession.getCluster().getConf());
        yarnClient.start();

        List<QueueInfo> queues =  yarnClient.getAllQueues(); 
        assertNotNull("Expected cluster queue(s) not found!!!", queues);        
        TestSession.logger.info("queues='" +
            Arrays.toString(queues.toArray()) + "'");
        if ((queues.size() == 1) &&
            (Float.compare(queues.get(0).getCapacity(), 1.0f) == 0)) {
                TestSession.logger.debug("Cluster is already setup properly." +
                        "Nothing to do.");
                return;
        }
        
        // Backup the default configuration directory on the Resource Manager
        // component host.
        cluster.getConf(component).backupConfDir(); 

        // Copy files to the custom configuration directory on the
        // Resource Manager component host.
        String sourceFile = TestSession.conf.getProperty("WORKSPACE") +
                "/conf/SingleQueueConf/single-queue-capacity-scheduler.xml";
        cluster.getConf(component).copyFileToConfDir(sourceFile,
                "capacity-scheduler.xml");
        cluster.hadoopDaemon(Action.STOP, component);
        cluster.hadoopDaemon(Action.START, component);
    }

    public static void setupTestDir() throws Exception {
        // Define the test directory
        DFS dfs = new DFS();
        String testDir = dfs.getBaseUrl() + "/user/" +
            System.getProperty("user.name") + "/terasort";
        
        // Delete it existing test directory if exists
        FileSystem fs = TestSession.cluster.getFS();
        FsShell fsShell = TestSession.cluster.getFsShell();                
        if (fs.exists(new Path(testDir))) {
            TestSession.logger.info("Delete existing test directory: " +
                testDir);
            fsShell.run(new String[] {"-rm", "-r", testDir});           
        }
        
        // Create or re-create the test directory.
        TestSession.logger.info("Create new test directory: " + testDir);
        fsShell.run(new String[] {"-mkdir", "-p", testDir});
    }
    
    // Create test data
    private void createData(String outputDir) throws Exception {

        String jobName = "teragen";
        String jar =
                TestSession.cluster.getConf().getHadoopProp("HADOOP_EXAMPLE_JAR");
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
        assertTrue("Unable to run job '" + jobName + "': cmd='" +
                StringUtils.join(job.getCommand(), " ") + "'.", isSuccessful);
    }
    

    // Run a sort job
    private void runSortJob(String sortInput, String sortOutput)
            throws Exception {

        String jobName = "terasort";
        String jar =
                TestSession.cluster.getConf().getHadoopProp("HADOOP_EXAMPLE_JAR");

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
        int numReduces = 2;
        jobArgs.add("-r");
        jobArgs.add(Integer.toString(numReduces));
        */
        
        job.setJobArgs(jobArgs.toArray(new String[0]));
        job.start();
        job.waitForID(600);
        boolean isSuccessful = job.waitForSuccess(20);
        assertTrue("Unable to run SORT job: cmd=" + 
                StringUtils.join(job.getCommand(), " "), isSuccessful);        
    }

    
    /* 
     * validate sort was successful
     */
    public void validateSortResults(String sortOutput, String sortReport)
            throws Exception {
        TestSession.logger.debug("Validate Sort Results : ");

        /* Submit a sort validate job: 
         * 
         * CLI:
         * CMD="$HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR 
         * jar $HADOOP_MAPRED_TEST_JAR testmapredsort
         *  ${YARN_OPTIONS} -sortInput sortInputDir -sortOutput sortOutputDir"
         *  
         *  API Call:
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
    public void getTaskResportSummary() 
            throws InterruptedException, IOException {
        JobClient js = TestSession.cluster.getJobClient();
        
        // LOG TO FILE
        Logger logger = TestSession.logger;
        FileAppender fileAppender = new FileAppender();
        fileAppender.setName("FileLogger");        
        fileAppender.setFile(
                TestSession.conf.getProperty("WORKSPACE_SF_REPORTS") +
                "/tasks_report.log");
        fileAppender.setLayout(new PatternLayout("%d %-5p %m%n"));
        fileAppender.setThreshold(Level.INFO);
        fileAppender.setAppend(true);
        fileAppender.activateOptions();
        logger.addAppender(fileAppender);

        TestSession.logger.info("********************************************");
        TestSession.logger.info("---> Display Jobs:");
        TestSession.logger.info("********************************************");
        JobStatus[] jobsStatus = js.getJobs(TestSession.startTime);
        js.displayJobList(jobsStatus);
        TestSession.logger.info("Total Number of jobs = " + jobsStatus.length);
        
        TestSession.logger.info("********************************************");
        TestSession.logger.info("--> Aggregate Task Summary For Each Job:");
        TestSession.logger.info("********************************************");
        TaskReportSummary taskReportSummary = js.getTaskReportSummary(jobsStatus);
        
        TestSession.logger.info("********************************************");
        TestSession.logger.info("--> Print Task Summary For Jobs:");
        TestSession.logger.info("********************************************");        
        taskReportSummary.printSummary();
        
        logger.removeAppender(fileAppender);
        
        // Check that there are no non-complete tasks
        int numNonCompleteMapTasks = taskReportSummary.getNonCompleteMapTasks();
        int numNonCompleteReduceTasks = taskReportSummary.getNonCompleteReduceTasks();
        
        int acceptableMapFailure = 0;
        int acceptableRedFailure = 0;
        String mapMsg = "There are " +
                (numNonCompleteMapTasks - acceptableMapFailure) + 
                " more map task failures than the acceptable failure of " +
                acceptableMapFailure;
        String redMsg = "There are " + 
                (numNonCompleteReduceTasks - acceptableRedFailure) + 
                " more reduce task failures than the acceptable failure of " + 
                acceptableRedFailure;
        assertTrue(mapMsg, numNonCompleteMapTasks <= acceptableMapFailure);
        assertTrue(redMsg, numNonCompleteReduceTasks <= acceptableRedFailure);           
    }
    
}
