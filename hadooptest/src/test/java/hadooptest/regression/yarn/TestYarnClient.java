package hadooptest.regression.yarn;

import static org.junit.Assert.*;
	
import hadooptest.TestSession;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.client.YarnClientImpl;
import org.junit.BeforeClass;
import org.junit.Test;


public class TestYarnClient extends TestSession {
	
	@BeforeClass
	public static void startTestSession() {
		TestSession.start();
	}

	/*
	 * A test for the Yarn Client
	 */
	@Test
	public void runYarnClient() throws Exception {

		YarnClientImpl yarnClient = new YarnClientImpl();
		yarnClient.init(TestSession.getCluster().getConf());
		yarnClient.start();

		List<QueueInfo> queues =  yarnClient.getAllQueues(); 
        TestSession.logger.info("queues='" +
        	Arrays.toString(queues.toArray()) + "'");
 
        List<NodeReport>  nodeReports = yarnClient.getNodeReports();
        TestSession.logger.info("nodes='" +
        	Arrays.toString(nodeReports.toArray()) + "'");
        
		for (QueueInfo queue : queues) {
			List<ApplicationReport> apps = queue.getApplications();
	        TestSession.logger.info("apps='" +
	            	Arrays.toString(apps.toArray()) + "'");			
		}
				
	}
	
}
