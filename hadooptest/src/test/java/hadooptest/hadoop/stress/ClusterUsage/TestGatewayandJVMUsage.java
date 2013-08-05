package hadooptest.hadoop.stress.ClusterUsage;

import static org.junit.Assert.assertNotNull;
import hadooptest.TestSession;
import hadooptest.cluster.hadoop.HadoopCluster;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.client.YarnClientImpl;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestGatewayandJVMUsage extends TestSession {
	
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
        if (queues.size() < 1){
			cluster.hadoopDaemon("stop", component);
			cluster.hadoopDaemon("start", component);
        }
    }
    
    @Test 
    public void SystemAndJVMUsage(){
    	long mb = 1024*1024;
		long kb = 1024;
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
		        Long v = (Long)value;
		        if(v > mb)
		        	TestSession.logger.info(method.getName() + " = " + v/mb +"M");
		        else if(v > kb)
		        	TestSession.logger.info(method.getName() + " = " + v/kb +"K");
		        else
		        	TestSession.logger.info(method.getName() + " = " + value);		    }
    	}
    	
    	TestSession.logger.info("================== JVM level stats ======================");
        Runtime runtime = Runtime.getRuntime();
        TestSession.logger.info("##### Heap utilization statistics [MB] #####");
        TestSession.logger.info("Used Memory:"+ (runtime.totalMemory() - runtime.freeMemory()) / mb);
        TestSession.logger.info("Free Memory:"+ runtime.freeMemory() / mb);
        TestSession.logger.info("Total Memory:" + runtime.totalMemory() / mb);
        TestSession.logger.info("Max Memory:" + runtime.maxMemory() / mb);
    }
}


