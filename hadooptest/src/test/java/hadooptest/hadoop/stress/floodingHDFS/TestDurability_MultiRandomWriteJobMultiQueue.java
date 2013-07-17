package hadooptest.hadoop.stress.floodingHDFS;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import hadooptest.TestSession;
import hadooptest.cluster.hadoop.DFS;
import hadooptest.cluster.hadoop.HadoopCluster;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.workflow.hadoop.job.RandomWriterJob;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.client.YarnClientImpl;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDurability_MultiRandomWriteJobMultiQueue extends TestSession {
	
	static List<QueueInfo> queues;
	
	@BeforeClass
	public static void startTestSession() throws Exception {
		TestSession.start();
		setupQueue();
	}
	public static void setupQueue() throws Exception  {
		
		FullyDistributedCluster cluster =
				(FullyDistributedCluster) TestSession.cluster;
		String component = HadoopCluster.RESOURCE_MANAGER;

		YarnClientImpl yarnClient = new YarnClientImpl();
		yarnClient.init(TestSession.getCluster().getConf());
		yarnClient.start();

		queues =  yarnClient.getAllQueues(); 
		assertNotNull("Expected cluster queue(s) not found!!!", queues);		
		TestSession.logger.info("================= queues ='" +	Arrays.toString(queues.toArray()) + "'");
		// we need to detect whether there are two queues running
		if (queues.size() >= 2) {
			TestSession.logger.debug("Cluster is already setup properly." 
									+ "Multi-queues are Running." + "Nothing to do.");
		} else {
			cluster.hadoopDaemon("stop", component);
			cluster.hadoopDaemon("start", component);
		}
		TestSession.logger.info("================= queues ='" +	Arrays.toString(queues.toArray()) + "'");

	}
	

	@Test
	public void runTestDurability() throws IOException, InterruptedException {
		
		DateFormat dateFormat = new SimpleDateFormat("yyyy_MM_dd___HH_mm_ss___");
		RandomWriterJob[] jobs = new RandomWriterJob[3];
		int queueIndex = 0;
		try {
			for(int i = 0; i < jobs.length; i++){
				jobs[i] = new RandomWriterJob();
				queueIndex = queueIndex % queues.size();
				jobs[i].setQueue(queues.get(queueIndex).getQueueName());
				logger.info("job "+i+" is using Queue "+queues.get(queueIndex).getQueueName()+", index = "+queueIndex);
				queueIndex++;
				
				Date date = new Date();
				String outputDir = new DFS().getBaseUrl() + "/user/" + System.getProperty("user.name") + "/LargeFile/"+
		    			dateFormat.format(date).toString()+ Integer.toString(i);
				logger.info("=============== outputDir = "+outputDir);
				jobs[i].setOutputDir(outputDir);
			}
			for(int i = 0; i < jobs.length; i++){	
				jobs[i].start();
				assertTrue("jobs["+i+"] was not assigned an ID within 10 seconds.", 
						jobs[i].waitForID(60));
				assertTrue("job ID for WordCount jobs["+i+"] is invalid.", 
						jobs[i].verifyID());
			}
		}catch (Exception e) {
			TestSession.logger.error("Exception failure.", e);
			fail();
		}
	}
}
