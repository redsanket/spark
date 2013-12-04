package hadooptest.monitoring.listeners;

import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.automation.utils.http.HTTPHandle;
import hadooptest.automation.utils.http.Response;
import hadooptest.monitoring.Monitorable;
import hadooptest.monitoring.monitors.CPUMonitor;
import hadooptest.monitoring.monitors.LogMonitor;
import hadooptest.monitoring.monitors.MemoryMonitor;
import hadooptest.monitoring.monitors.MonitorGeneral;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.commons.httpclient.HttpMethod;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.junit.runner.Description;
import org.junit.runner.notification.RunListener;

/**
 * <p>
 * MonitoringListener is invoked from the pom-ci.xml file when a test is
 * started/finished. It has been registered there as a listener for maven
 * sure-fire-plugin. This is where the {@link Monitorable} annotation is
 * connected with the test code.
 * </p>
 * 
 * @author tiwari
 * 
 */
public class MonitoringListener extends RunListener {
	Logger logger = Logger.getLogger(MonitoringListener.class);
	MonitorGeneral monitorGeneral = new MonitorGeneral();

	@Override
	/**
	 * Overridden Method, provided by the RunListener. Invoked when a test is about to start.
	 */
	public void testStarted(Description description) {
		logger.info("Monitoring listener invoked..on testStart");
		String cluster = System.getProperty("CLUSTER_NAME");
		Class<?> descriptionOfTestClass = description.getTestClass();
		logger.info("Method that has this annotation:"
				+ description.getMethodName());
		Collection<Annotation> annotations = description.getAnnotations();
		for (Annotation annotation : annotations) {
			if (annotation.annotationType().isAssignableFrom(Monitorable.class)) {
				logger.info("Doing Monitoring..since annotation is "
						+ annotation.annotationType().getCanonicalName());
				Configuration conf = new Configuration(true);
				conf.addResource(HadooptestConstants.Location.CORE_SITE_XML);
				conf.addResource(HadooptestConstants.Location.HDFS_SITE_XML);
				conf.addResource(HadooptestConstants.Location.YARN_SITE_XML);
				conf.addResource(HadooptestConstants.Location.MAPRED_SITE_XML);
				conf.addResource(new Path(HadooptestConstants.Location.CORE_SITE_XML));
				conf.addResource(new Path(HadooptestConstants.Location.HDFS_SITE_XML));
				conf.addResource(new Path(HadooptestConstants.Location.YARN_SITE_XML));
				conf.addResource(new Path(HadooptestConstants.Location.MAPRED_SITE_XML));
				ClassLoader classLoader = Configuration.class.getClassLoader();
				conf.addResource(classLoader
						.getResourceAsStream(HadooptestConstants.ConfFileNames.CORE_SITE_XML));
				conf.addResource(classLoader
						.getResourceAsStream(HadooptestConstants.ConfFileNames.HDFS_SITE_XML));
				conf.addResource(classLoader
						.getResourceAsStream(HadooptestConstants.ConfFileNames.YARN_SITE_XML));
				conf.addResource(classLoader
						.getResourceAsStream(HadooptestConstants.ConfFileNames.MAPRED_SITE_XML));
				HashMap<String, ArrayList<String>> componentToHostMapping = new HashMap<String, ArrayList<String>>();

				// Resource Manager
				String rm = conf
						.get("yarn.resourcemanager.resource-tracker.address");
				String rmWithoutPort = rm.split(":")[0];
				ArrayList<String> components = new ArrayList<String>();
				components.add(rmWithoutPort);
				componentToHostMapping.put(
						HadooptestConstants.NodeTypes.RESOURCE_MANAGER,
						components);
				// History Server (shares the details with RM)
				components = new ArrayList<String>();
				components.add(rmWithoutPort);
				componentToHostMapping.put(
						HadooptestConstants.NodeTypes.HISTORY_SERVER,
						components);

				// Namenodes
				String nn = conf.get("dfs.namenode.https-address");
				components = new ArrayList<String>();
				components.add(nn.split(":")[0]);
				componentToHostMapping.put(
						HadooptestConstants.NodeTypes.NAMENODE, components);

				// Secondary namenodes
				String snn = conf.get("dfs.namenode.secondary.http-address");
				components = new ArrayList<String>();
				components.add(snn.split(":")[0]);
				componentToHostMapping.put(
						HadooptestConstants.NodeTypes.SECONDARY_NAMENODE,
						components);

				// Data Nodes
				String rmWebAppAddress = conf
						.get("yarn.resourcemanager.webapp.address");
				componentToHostMapping.put(
						HadooptestConstants.NodeTypes.DATANODE,
						getDataNodes(rmWebAppAddress));

				logger.info("COMPS:" + componentToHostMapping);

				// Add the CPU Monitor
				CPUMonitor cpuMonitor = new CPUMonitor(cluster,
						componentToHostMapping, descriptionOfTestClass,
						description.getMethodName(),
						((Monitorable) annotation).cpuPeriodicity());
				monitorGeneral.registerMonitor(cpuMonitor);

				// Add the memory Monitor
				MemoryMonitor memoryMonitor = new MemoryMonitor(cluster,
						componentToHostMapping, descriptionOfTestClass,
						description.getMethodName(),
						((Monitorable) annotation).memPeriodicity());
				monitorGeneral.registerMonitor(memoryMonitor);

				// Add the Log Monitor
				LogMonitor logMonitor = new LogMonitor(cluster,
						componentToHostMapping, descriptionOfTestClass,
						description.getMethodName());
				monitorGeneral.registerMonitor(logMonitor);

				// Start 'em monitors
				monitorGeneral.startMonitors();
			} else {
				logger.info("Skipping annotation..since annotation is "
						+ annotation.annotationType().getCanonicalName()
						+ " in test named " + description.getMethodName());
			}
		}
	}

	@Override
	/**
	 * This method invokes the {@link MonitorGeneral} to stop and remove all the monitors.
	 */
	public void testFinished(Description description) {
		monitorGeneral.stopMonitors();
		monitorGeneral.removeAllMonitors();
	}

	/**
	 * Dynamically constructs the list of data Nodes, by fetching the "alive'
	 * nodes from the resource manager, via a HTTP call.
	 * 
	 * @param rmWebappAddress
	 * @return
	 */
	ArrayList<String> getDataNodes(String rmWebappAddress) {
		ArrayList<String> components = new ArrayList<String>();
		HTTPHandle httpHandle = new HTTPHandle();
		String resource = "/ws/v1/cluster/nodes";
		HttpMethod getMethod = httpHandle.makeGET("http://" + rmWebappAddress,
				resource, null);
		Response response = new Response(getMethod);

		JSONObject rmResponseJson = response.getJsonObject();
		JSONObject nodesJson = (JSONObject) rmResponseJson.get("nodes");
		JSONArray dataNodeArrayJson = (JSONArray) nodesJson.get("node");
		for (int xx = 0; xx < dataNodeArrayJson.size(); xx++) {
			JSONObject dataNodeJson = dataNodeArrayJson.getJSONObject(xx);
			components.add((String) dataNodeJson.get("nodeHostName"));
		}
		return components;
	}

}
