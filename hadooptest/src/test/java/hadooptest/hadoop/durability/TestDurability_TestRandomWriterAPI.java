package hadooptest.hadoop.durability;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import hadooptest.TestSession;
import hadooptest.cluster.hadoop.DFS;
import hadooptest.cluster.hadoop.HadoopCluster;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.client.YarnClientImpl;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDurability_TestRandomWriterAPI extends TestSession {
	
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
        cluster.hadoopDaemon("stop", component);
        cluster.hadoopDaemon("start", component);
    }
    
    @Test 
    public void TestRandomWriterAPI(){
    	//<out-dir> <level> <jobNum><MAPS_PER_HOST_VAL><BYTES_PER_MAP_VAL>
		String TargetLevel = System.getProperty("RandomWriterAPI.TargetLevel");
		if(TargetLevel == null) TargetLevel = "";
		
		String JobNum = System.getProperty("RandomWriterAPI.JobNum");
		if(JobNum == null) JobNum = "";
		
		String MAPS_PER_HOST = System.getProperty("RandomWriterAPI.MAPS_PER_HOST");
		if(MAPS_PER_HOST == null) MAPS_PER_HOST = "";
		
		String BYTES_PER_MAP = System.getProperty("RandomWriterAPI.BYTES_PER_MAP");
		if(BYTES_PER_MAP == null) BYTES_PER_MAP = "";

    	try {
			Configuration conf = TestSession.cluster.getConf();
			TestSession.cluster.setSecurityAPI("keytab-hadoopqa", "user-hadoopqa");

			DateFormat dateFormat = new SimpleDateFormat("yyyy_MM_dd___HH_mm_ss");
			Date date = new Date();
	    	String outdir = new DFS().getBaseUrl() + "/user/" + System.getProperty("user.name") + "/LargeFile/"+dateFormat.format(date).toString();
	    	TestSession.logger.info("outdir = "+outdir);
			String[] args = {outdir,TargetLevel,JobNum,MAPS_PER_HOST,BYTES_PER_MAP };
				
			int rc = ToolRunner.run(conf, new RandomWriterAPI(), args);
			if (rc != 0) 
				TestSession.logger.error("Job failed!!!");
		}
		catch (Exception e) {
			TestSession.logger.error("Exception failure.", e);
			fail();
		}
    }
}


