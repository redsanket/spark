package hadooptest.hadoop.regression.yarn;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import hadooptest.TestSession;
import hadooptest.cluster.hadoop.HadoopCluster;
import hadooptest.cluster.hadoop.HadoopCluster.Action;
import hadooptest.cluster.hadoop.dfs.DFS;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.workflow.hadoop.job.LoadgenJob;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.QueueInfo;
//import org.apache.hadoop.yarn.client.YarnClientImpl; // 0.23
import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl; // 2.x
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import hadooptest.ParallelMethodTests;

/*
 *  Runs the loadgen test. Takes about 8 minutes to run.
 */

@Category(ParallelMethodTests.class)
public class TestBenchmarksShuffle extends TestSession {

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
            System.getProperty("user.name") + "/shuffle";
        
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
    
    @Test 
    public void testShuffle() throws Exception{
        String tcDesc =
                "Runs hadoop shuffle by having more maps than reduce tasks";
        TestSession.logger.info("Run test: " + tcDesc);
        
        LoadgenJob loadgenJob = new LoadgenJob();
        loadgenJob.setNumMappers(18);
        loadgenJob.setNumReducers(9);
        loadgenJob.start();

        loadgenJob.waitForID(600);
        boolean isSuccessful = loadgenJob.waitForSuccess(20);
        assertTrue("Unable to run shuffle job: cmd=" + 
                StringUtils.join(loadgenJob.getCommand(), " "), isSuccessful);   
    }

}
