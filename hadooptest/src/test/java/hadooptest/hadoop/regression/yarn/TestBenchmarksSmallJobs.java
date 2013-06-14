package hadooptest.hadoop.regression.yarn;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import hadooptest.TestSession;
import hadooptest.cluster.hadoop.DFS;
import hadooptest.cluster.hadoop.HadoopCluster;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.workflow.hadoop.job.GenericJob;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.client.YarnClientImpl;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import hadooptest.ParallelMethodTests;

/*
 *  Runs the mrbench test. Takes about 2 minutes to run.
 */

@Category(ParallelMethodTests.class)
public class TestBenchmarksSmallJobs extends TestSession {

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
        TestSession.cluster.setSecurityAPI("keytab-hdfsqa", "user-hdfsqa");

        DFS dfs = new DFS();
        // String testDir = dfs.getBaseUrl() + "/user/" +
        // System.getProperty("user.name") + "/smalljobs";
        String testDir = dfs.getBaseUrl() + "/benchmarks";
        
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
        fsShell.run(new String[] {"-chmod", "777", testDir});
        dfs.fsls(testDir, new String[] {"-d"});
        
        TestSession.cluster.setSecurityAPI("keytab-hadoopqa", "user-hadoopqa");
    }
    
    @Test 
    public void testSmallJobs() throws Exception{
        String tcDesc = "Runs hadoop small jobs";
        TestSession.logger.info("Run test: " + tcDesc);

        GenericJob job;
        job = new GenericJob();
        job.setJobJar(
                TestSession.cluster.getConf().getHadoopProp("HADOOP_TEST_JAR"));
        job.setJobName("mrbench");
        ArrayList<String> jobArgs = new ArrayList<String>();
        jobArgs.add("-Dmapreduce.job.acl-view-job=*");
        
        String numRuns = "30";
        String lines = "10000";        
        Cluster clusterInfo = TestSession.cluster.getClusterInfo();
        int ttCount = clusterInfo.getActiveTaskTrackers().length;
        TestSession.logger.info("tasktracker count = " +
                Integer.toString(ttCount));
        int mapper = ttCount * 90 / 100 * 2;
        int reducer = ttCount * 90 / 100;

        /* NOTE:
         * 
         * -baseDir <base DFS path. default is /becnhmarks/NNBench.
         * This is not mandatory>
         * 
         * There is a known bug MAPREDUCE-2398 tracking the issue that the
         * “-baseDir“ parameter has no effect. This means that multiple 
         * parallel “MRBench“ runs might interfere with each other.  Also, the
         * /benchmarks on the QE Cluster is writable to only hdfs, so this will
         * also cause the test to fail if thi directory does not exists. 
         * 
         */        
        DFS dfs = new DFS();
        String testDir = dfs.getBaseUrl() + "/benchmarks";
        jobArgs.add("-baseDir");
        jobArgs.add(testDir);
        
        jobArgs.add("-numRuns");
        jobArgs.add(numRuns);
        jobArgs.add("-maps");
        jobArgs.add(Integer.toString(mapper));
        jobArgs.add("-reduces");
        jobArgs.add(Integer.toString(reducer));
        jobArgs.add("-inputLines");
        jobArgs.add(lines);
        jobArgs.add("-inputType");
        jobArgs.add("ascending");
        
        job.setJobArgs(jobArgs.toArray(new String[0]));
        job.start();
        job.waitForID(600);
        boolean isSuccessful = job.waitForSuccess(20);
        assertTrue("Unable to run small jobs job: cmd=" + 
                StringUtils.join(job.getCommand(), " "), isSuccessful);        
    }
}
