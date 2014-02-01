package hadooptest.monitoring.listeners;

import hadooptest.cluster.hadoop.HadoopCluster;
import hadooptest.monitoring.Monitorable;
import hadooptest.monitoring.monitors.AbstractMonitor;
import hadooptest.monitoring.monitors.CPUMonitor;
import hadooptest.monitoring.monitors.LogMonitor;
import hadooptest.monitoring.monitors.MemoryMonitor;
import hadooptest.monitoring.monitors.MonitorGeneral;
import hadooptest.monitoring.monitors.SshAgentLogMonitor;

import java.lang.annotation.Annotation;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;

import org.junit.runner.Description;
import org.junit.runner.notification.RunListener;

import hadooptest.TestSession;

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
	Date date = new Date();
	static SimpleDateFormat sdf = new SimpleDateFormat("MMM_dd_yyyy_HH_mm_ss");

	HashMap<String, MonitorGeneral> monitorGenerals = new HashMap<String, MonitorGeneral>();

	@Override
	/**
	 * Overridden Method, provided by the RunListener. Invoked when a test is about to start.
	 */
	public synchronized void testStarted(Description description) {
		MonitorGeneral monitorGeneral = new MonitorGeneral();
		TestSession.logger.info("Monitoring listener invoked..on testStart");
		String cluster = System.getProperty("CLUSTER_NAME");
		Class<?> descriptionOfTestClass = description.getTestClass();
		TestSession.logger.info("Method that has this annotation:"
				+ description.getMethodName());
		Collection<Annotation> annotations = description.getAnnotations();

		for (Annotation annotation : annotations) {
			if (annotation.annotationType().isAssignableFrom(Monitorable.class)) {
				monitorGenerals
						.put(description.getMethodName(), monitorGeneral);
				TestSession.logger.trace("START MJ size now:"
						+ monitorGenerals.size());
				TestSession.logger.trace("Doing Monitoring..since annotation is "
						+ annotation.annotationType().getCanonicalName());

				HashMap<String, ArrayList<String>> componentToHostMapping = new HashMap<String, ArrayList<String>>();

				componentToHostMapping.put(
						HadoopCluster.DATANODE,
						new ArrayList<String>(Arrays.asList(TestSession.cluster
								.getNodeNames(HadoopCluster.DATANODE))));
				componentToHostMapping.put(
						HadoopCluster.NAMENODE,
						new ArrayList<String>(Arrays.asList(TestSession.cluster
								.getNodeNames(HadoopCluster.NAMENODE))));
				componentToHostMapping.put(
						HadoopCluster.HISTORYSERVER,
						new ArrayList<String>(Arrays.asList(TestSession.cluster
								.getNodeNames(HadoopCluster.HISTORYSERVER))));
				componentToHostMapping
						.put(HadoopCluster.RESOURCE_MANAGER,
								new ArrayList<String>(
										Arrays.asList(TestSession.cluster
												.getNodeNames(HadoopCluster.RESOURCE_MANAGER))));
				componentToHostMapping.put(
						HadoopCluster.NODEMANAGER,
						new ArrayList<String>(Arrays.asList(TestSession.cluster
								.getNodeNames(HadoopCluster.NODEMANAGER))));

				TestSession.logger.trace("COMPS:" + componentToHostMapping);

//				Date date = new Date();
//				SimpleDateFormat sdf = new SimpleDateFormat("MMM_dd_yyyy_HH_mm_ss_SSS");
				AbstractMonitor.formattedDateAndTime = sdf.format(date);

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
//				LogMonitor logMonitor = new LogMonitor(cluster,
//						componentToHostMapping, descriptionOfTestClass,
//						description.getMethodName());
//				monitorGeneral.registerMonitor(logMonitor);

				// Add the (SSH Agent) Log Monitor
				SshAgentLogMonitor sshAgentlogMonitor = new SshAgentLogMonitor(cluster,
						componentToHostMapping, descriptionOfTestClass,
						description.getMethodName());
				monitorGeneral.registerMonitor(sshAgentlogMonitor);

				// Start 'em monitors
				monitorGeneral.startMonitors();
			} else {
				TestSession.logger.trace("Skipping annotation..since annotation is "
						+ annotation.annotationType().getCanonicalName()
						+ " in test named " + description.getMethodName());
			}
		}
	}

	@Override
	/**
	 * This method invokes the {@link MonitorGeneral} to stop and remove all the monitors.
	 */
	public synchronized void testFinished(Description description) {
		if (monitorGenerals.size() == 0)
			return;

		TestSession.logger.trace("STOP MJ size now:" + monitorGenerals.size()
				+ " called on method:" + description.getMethodName());
		MonitorGeneral monitorGeneral = monitorGenerals.get(description
				.getMethodName());
		TestSession.logger.trace("Got a monitorGeneral" + monitorGeneral + "calling stop Monitors on it");
		monitorGeneral.stopMonitors();
		monitorGeneral.removeAllMonitors();
		monitorGenerals.remove(description.getMethodName());
	}

}
