package hadooptest.hadoop.regression.yarn.capacityScheduler;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.automation.utils.http.HTTPHandle;
import hadooptest.cluster.gdm.Response;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
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
		Properties crossClusterProperties;
		String workingDir;
		int periodicityInMilliseconds;

		public RESTStatCollector(int periodicity) {
			this.periodicityInMilliseconds = periodicity;
			workingDir = System
					.getProperty(HadooptestConstants.Miscellaneous.USER_DIR);
			crossClusterProperties = new Properties();
			try {
				crossClusterProperties.load(new FileInputStream(workingDir
						+ "/conf/CrossCluster/Resource.properties"));
			} catch (IOException ex) {
				ex.printStackTrace();
			}

		}

		@Override
		public void run() {
			while (keepCollecting) {
				String resourceManager = crossClusterProperties
						.getProperty(System.getProperty("CLUSTER_NAME")
								+ "."
								+ HadooptestConstants.NodeTypes.RESOURCE_MANAGER);

				String resource = "/ws/v1/cluster/scheduler";
				HTTPHandle httpHandle = new HTTPHandle();
				HttpMethod getMethod = httpHandle.makeGET(resourceManager,
						resource, null);
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

	}
}
