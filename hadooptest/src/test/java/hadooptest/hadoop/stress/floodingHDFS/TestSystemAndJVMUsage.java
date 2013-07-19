package hadooptest.hadoop.stress.floodingHDFS;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import hadooptest.TestSession;
import hadooptest.cluster.hadoop.DFS;
import hadooptest.cluster.hadoop.HadoopCluster;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
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

public class TestSystemAndJVMUsage extends TestSession {
	
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
    	TestSession.logger.info("================== System level stats ======================");
    	OperatingSystemMXBean operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();
    	for (Method method : operatingSystemMXBean.getClass().getDeclaredMethods()) {
    		method.setAccessible(true);
		    if (method.getName().startsWith("get") && Modifier.isPublic(method.getModifiers())) {
		            Object value;
		        try {
		            value = method.invoke(operatingSystemMXBean);
		        } catch (Exception e) {
		            value = e;
		        }
		        TestSession.logger.info(method.getName() + " = " + value);
		    }
    	}
    	TestSession.logger.info("================== JVM level stats ======================");
    	int mb = 1024*1024;
        Runtime runtime = Runtime.getRuntime();
        TestSession.logger.info("##### Heap utilization statistics [MB] #####");
        TestSession.logger.info("Used Memory:"+ (runtime.totalMemory() - runtime.freeMemory()) / mb);
        TestSession.logger.info("Free Memory:"+ runtime.freeMemory() / mb);
        TestSession.logger.info("Total Memory:" + runtime.totalMemory() / mb);
        TestSession.logger.info("Max Memory:" + runtime.maxMemory() / mb);
    }
}


