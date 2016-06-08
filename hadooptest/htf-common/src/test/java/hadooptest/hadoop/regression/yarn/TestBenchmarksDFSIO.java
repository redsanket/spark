package hadooptest.hadoop.regression.yarn;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import hadooptest.TestSession;
import hadooptest.cluster.hadoop.HadoopCluster;
import hadooptest.cluster.hadoop.HadoopCluster.Action;
import hadooptest.cluster.hadoop.dfs.DFS;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.workflow.hadoop.job.GenericJob;
import hadooptest.workflow.hadoop.job.JobClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.yarn.api.records.QueueInfo;
//import org.apache.hadoop.yarn.client.YarnClientImpl; // H0.23
import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl; // H2.x
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import hadooptest.ParallelMethodTests;

/*
 *  Runs the TestDFSIO test. Takes about 5 minutes to run.
 */

@Category(ParallelMethodTests.class)
public class TestBenchmarksDFSIO extends TestSession {

    private static final int FILE_SIZE = 320;
    private static int ttCount;
    private static final int[] PERCENTAGE = {
        25,
        100,
        200
    };
    private static final String[] OPERATION = {
        "write",
        "read"
    };

    @BeforeClass
    public static void startTestSession() throws Exception{
        TestSession.start();
        setupTestConf();
        setupTestDir();
        initNumTT();        
    }

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

        TestSession.logger.info("NOTE:  @AfterClass check disabled until remaining tasks count is re-evaluated, see GRIDCI-1146 ");
        /* gridci-1146, since over-commit went in, more uncompleted tasks could remain but
           not sure how many, need to find this and update the taskreport check

        jobClient.validateTaskReportSummary(
                jobClient.logTaskReportSummary(
                        TestSession.TASKS_REPORT_LOG, 
                        TestSession.startTime),
                        numAcceptableNonCompleteMapTasks,
                        numAcceptableNonCompleteReduceTasks);        
        */
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
        FileSystem fs = TestSession.cluster.getFS();
        FsShell fsShell = TestSession.cluster.getFsShell();
        DFS dfs = new DFS();
        String testDir = dfs.getBaseUrl() + "/user/" +
            System.getProperty("user.name") + "/benchmarks_dfsio";
        if (fs.exists(new Path(testDir))) {
            TestSession.logger.info("Delete existing test directory: " +
                testDir);
            fsShell.run(new String[] {"-rm", "-r", testDir});           
        }
        TestSession.logger.info("Create new test directory: " + testDir);
        fsShell.run(new String[] {"-mkdir", "-p", testDir});
    }

    public static void initNumTT() throws Exception {
        Cluster clusterInfo = TestSession.cluster.getClusterInfo();
        ttCount = clusterInfo.getClusterStatus().getTaskTrackerCount();
        TestSession.logger.info("tasktracker count = " +
                Integer.toString(ttCount));
    }

    private void testDFSIO(int percentage, String operation) throws Exception {
        int numFiles = (ttCount * percentage)/100;

        String testcaseName = "DFSIO_" + operation + "_" + percentage; 
        String testcaseDesc = "Mapreduce Benchmark - DFSIO with " + operation +
                " Operation and " + percentage + " %.";
        TestSession.logger.info("TC='" + testcaseName + "': Desc='" +
                testcaseDesc + "'.");
        
        // Run DFSIO Test job
        GenericJob job;
        job = new GenericJob();
        job.setJobJar(
                TestSession.cluster.getConf().getHadoopProp("HADOOP_TEST_JAR"));
        job.setJobName("TestDFSIO");

        ArrayList<String> jobArgs = new ArrayList<String>();
        
        // Custom Output Directory
        DFS dfs = new DFS();
        String outputDir = 
                dfs.getBaseUrl() + "/user/" + System.getProperty("user.name") + 
                "/benchmarks_dfsio/" + percentage;
        jobArgs.add("-D");
        jobArgs.add("test.build.data=" + outputDir);
        
        jobArgs.add("-" + operation);
        jobArgs.add("-nrFiles");
        jobArgs.add(Integer.toString(numFiles));
        jobArgs.add("-fileSize");
        jobArgs.add(Integer.toString(FILE_SIZE));
        
        job.setJobArgs(jobArgs.toArray(new String[0]));

        job.start();
        job.waitForID(600);
        boolean isSuccessful = job.waitForSuccess(20);

        assertTrue(
            "Unable to run TC='" + testcaseName + "': Desc='" + testcaseDesc +
            "': cmd='" + StringUtils.join(job.getCommand(), " ") + "'.", 
            isSuccessful);
        
        if (isSuccessful) {

        }
    }

    private void testDFSIO_WriteAndRead(int percentage) throws Exception {
        testDFSIO(percentage, OPERATION[0]);
        testDFSIO(percentage, OPERATION[1]);
    }

    // Temporarily removing test as it doesn't work with small clusters with less than 4 task trackers
    // DFSIO 25% write & read
    //@Test 
    //public void testDFSIO_WriteAndRead_25percent() throws Exception{
    //    testDFSIO_WriteAndRead(PERCENTAGE[0]);
    //}
    
    // DFSIO 50% write & read
    @Test 
    public void testDFSIO_WriteAndRead_50percent() throws Exception{
        testDFSIO_WriteAndRead(PERCENTAGE[1]);
    }

    // DFSIO 100% write & read
    @Test 
    public void testDFSIO_WriteAndRead_100percent() throws Exception{
        testDFSIO_WriteAndRead(PERCENTAGE[2]); 
    }

}
