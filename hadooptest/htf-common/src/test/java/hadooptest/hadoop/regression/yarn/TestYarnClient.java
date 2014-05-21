package hadooptest.hadoop.regression.yarn;

import static org.junit.Assert.*;
	
import hadooptest.TestSession;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.QueueInfo;
//import org.apache.hadoop.yarn.client.YarnClientImpl; // H0.23
import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl; // H2.x
import org.junit.Test;


public class TestYarnClient extends TestSession {
	
	/*
	 * A test for the Yarn Client
	 */
	@Test
	public void runYarnClient() throws Exception {
		YarnClientImpl yarnClient = TestSession.cluster.getYarnClient();
		
		List<QueueInfo> queues =  yarnClient.getAllQueues(); 
		assertNotNull("Expected cluster queue(s) not found!!!", queues);		
		TestSession.logger.info("queues='" +
        	Arrays.toString(queues.toArray()) + "'");
        
        List<NodeReport>  nodeReports = yarnClient.getNodeReports();
		assertNotNull("Expected reports for cluster node(s) not found!!!",
				queues);		
        TestSession.logger.info("nodes='" +
        	Arrays.toString(nodeReports.toArray()) + "'");
        
		for (QueueInfo queue : queues) {
			List<ApplicationReport> apps = queue.getApplications();
	        TestSession.logger.info("apps='" +
	            	Arrays.toString(apps.toArray()) + "'");			
		}
				
	}
	
}

/*
assertTrue("Version has invalid format!!!", testConfVersionAPI.matches("\\d+[.\\d+]+"));
assertTrue("API and CLI versions do not match!!!", testConfVersionAPI.equals(testConfVersionCLI));
assertTrue("Cluster Object version and Cluster Conf Object version do not match!!!", clusterVersion.equals(testConfVersionAPI));
*/