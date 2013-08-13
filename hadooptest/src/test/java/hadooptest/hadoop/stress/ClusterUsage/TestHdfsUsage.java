package hadooptest.hadoop.stress.ClusterUsage;

import static org.junit.Assert.assertNotNull;
import hadooptest.TestSession;
import hadooptest.cluster.hadoop.HadoopCluster;
import hadooptest.cluster.hadoop.HadoopCluster.Action;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.client.YarnClientImpl;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestHdfsUsage extends TestSession {
	
	@BeforeClass
    public static void startTestSession() throws Exception{
        TestSession.start();
        setupTestConf();
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
    
    @Test 
    public void HdfsUsage() throws Exception{
    	FileSystem fs = TestSession.cluster.getFS();
    	FsStatus status  =fs.getStatus();
    	long capacity = status.getCapacity();
    	long remain = status.getRemaining();
    	long used = status.getUsed();
    	TestSession.logger.info("getCapacity	= "+capacity);
    	TestSession.logger.info("getRemaining	= "+remain);
    	TestSession.logger.info("getUsed		= "+used);
    	TestSession.logger.info("DFS Remaining	= "+((double)remain/(double)capacity));
    	TestSession.logger.info("DFS Used	= "+((double)used/(double)capacity));
    }
}


