package hadooptest.hadoop.regression.yarn;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import hadooptest.TestSession;
import hadooptest.cluster.hadoop.DFS;
import hadooptest.cluster.hadoop.HadoopCluster;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.config.hadoop.HadoopConfiguration;
import hadooptest.workflow.hadoop.job.GenericJob;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.SortValidator;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.client.YarnClientImpl;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import hadooptest.ParallelMethodTests;

@Category(ParallelMethodTests.class)
public class TestBenchmarksSort extends TestSession {

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
        cluster.hadoopDaemon("stop", component);
        cluster.hadoopDaemon("start", component);
    }
    
    public static void setupTestDir() throws Exception {
        // Define the test directory
        DFS dfs = new DFS();
        String testDir = dfs.getBaseUrl() + "/user/" +
            System.getProperty("user.name") + "/sort";
        
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
    
    // Run a randomwriter job to generate Random Byte Data
    private void runRandomWriterJob(String outputDir) throws Exception {
        GenericJob job = new GenericJob();
        job.setJobJar(
                TestSession.cluster.getConf().getHadoopProp("HADOOP_EXAMPLE_JAR"));
        job.setJobName("randomwriter");
        ArrayList<String> jobArgs = new ArrayList<String>();
        jobArgs.add("-Dmapreduce.job.acl-view-job=*");
        jobArgs.add(outputDir);
        job.setJobArgs(jobArgs.toArray(new String[0]));
        job.start();
        job.waitForID(600);
        boolean isSuccessful = job.waitForSuccess(20);
        assertTrue("Unable to run randomwriter: cmd='" +
                StringUtils.join(job.getCommand(), " ") + "'.", isSuccessful);
    }
    

    // Run a sort job
    private void runSortJob(String sortInput, String sortOutput) throws Exception {
        GenericJob job;
        job = new GenericJob();
        job.setJobJar(
                TestSession.cluster.getConf().getHadoopProp("HADOOP_EXAMPLE_JAR"));
        job.setJobName("sort");
        ArrayList<String> jobArgs = new ArrayList<String>();
        jobArgs.add("-Dmapreduce.job.acl-view-job=*");
        jobArgs.add(sortInput);
        jobArgs.add(sortOutput);
        
        /*
        int numReduces = 2;
        jobArgs.add("-r");
        jobArgs.add(Integer.toString(numReduces));
        */
        
        job.setJobArgs(jobArgs.toArray(new String[0]));
        job.start();
        job.waitForID(1800);
        boolean isSuccessful = job.waitForSuccess(20);
        assertTrue("Unable to run SORT job: cmd=" + 
                StringUtils.join(job.getCommand(), " "), isSuccessful);        
    }

    
    /* 
     * validate sort was successful
     */
    public void validateSortResults(String sortInput, String sortOutput)
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
        jobArgs.add("-sortInput");
        jobArgs.add(sortInput);
        jobArgs.add("-sortOutput");
        jobArgs.add(sortOutput);
        String[] args = jobArgs.toArray(new String[0]);
        HadoopConfiguration conf = TestSession.getCluster().getConf();
        int rc = ToolRunner.run(conf, new SortValidator(), args);
        if (rc != 0) {
            TestSession.logger.error("SortValidator failed!!!");
        }
        
        boolean isSuccessful = (rc == 0) ? true : false;
        assertTrue("Unable to run SORT validation job.", isSuccessful);
    }

    
    @Test 
    public void testSort() throws Exception{
        String tcDesc = "Runs hadoop sort on random data and verifies that " + 
                "the data is properly sorted";
        TestSession.logger.info("Run test: " + tcDesc);
        
        setupTestDir();
        
        DFS dfs = new DFS();
        String testDir = dfs.getBaseUrl() + "/user/" +
                System.getProperty("user.name") + "/sort";
        
        // Generate sort data
        String dataDir = testDir + "/rwOutputDir";
        runRandomWriterJob(dataDir);
        
        // Sort the data
        String sortInputDir = dataDir;
        String sortOutputDir = testDir + "/sortOutputDir";
        runSortJob(sortInputDir, sortOutputDir);

        // Validate the data
        validateSortResults(sortInputDir, sortOutputDir);
    }

}
