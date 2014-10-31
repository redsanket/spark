package hadooptest.hadoop.regression.yarn.capacityScheduler;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.automation.utils.http.CustomNameValuePair;
import hadooptest.automation.utils.http.HTTPHandle;
import hadooptest.cluster.gdm.Response;
import hadooptest.cluster.hadoop.HadoopComponent;
import hadooptest.node.hadoop.HadoopNode;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Properties;

import net.sf.json.JSONObject;

import org.apache.commons.httpclient.HttpMethod;

public class RuntimeRESTStatsBO {
	public ArrayList<SchedulerRESTStatsSnapshot> listOfRESTSnapshotsAcrossAllLeafQueues = null;
	public volatile boolean keepCollecting = false;
	int periodicityInMilliseconds;

	RuntimeRESTStatsBO(int periodicity) {
		listOfRESTSnapshotsAcrossAllLeafQueues = new ArrayList<SchedulerRESTStatsSnapshot>();
		this.periodicityInMilliseconds = periodicity;
	}

	void start() {
		keepCollecting = true;
		RESTStatCollector restStatCollector = new RESTStatCollector(periodicityInMilliseconds);
		Thread t = new Thread(restStatCollector);
		t.start();
	}

	void stop() {
		keepCollecting = false;
	}

	class RESTStatCollector implements Runnable {
		String workingDir;
		int periodicityInMilliseconds;

		public RESTStatCollector(int periodicity) {
			this.periodicityInMilliseconds = periodicity;
			workingDir = System
					.getProperty(HadooptestConstants.Miscellaneous.USER_DIR);

		}

		public void run() {
			while (keepCollecting) {
				String rmHost = "http://" + getRMForLocalCluster() + ":8088";
				String resource = "/ws/v1/cluster/scheduler";
				HTTPHandle httpHandle = new HTTPHandle();
				HttpMethod getMethod = httpHandle.makeGET(rmHost,
						resource, new ArrayList<CustomNameValuePair>());
				Response response = new Response(getMethod);

				TestSession.logger.info(response.toString());
				JSONObject rmResponseJson = response.getJsonObject();
				SchedulerRESTStatsSnapshot schedulerRESTStatsSnapshot = new SchedulerRESTStatsSnapshot(
						rmResponseJson);
				schedulerRESTStatsSnapshot.rootQueue.recursivelyAssimilateLeafQueues();
				listOfRESTSnapshotsAcrossAllLeafQueues.add(schedulerRESTStatsSnapshot);
				try {
					Thread.sleep(this.periodicityInMilliseconds);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

			}
		}
		 String getRMForLocalCluster() {
			// This is the same cluster, no need to lookup the config file
			String rmHostName = null;
			HadoopComponent hadoopComp = TestSession.cluster.getComponents().get(
					HadooptestConstants.NodeTypes.RESOURCE_MANAGER);

			Hashtable<String, HadoopNode> nodesHash = hadoopComp.getNodes();
			for (String key : nodesHash.keySet()) {
				TestSession.logger.info("Key:" + key);
				TestSession.logger.info("The associated RM host is:"
						+ nodesHash.get(key).getHostname());
				rmHostName = nodesHash.get(key).getHostname();
				break;
			}
			return rmHostName;

		}


	}
}
