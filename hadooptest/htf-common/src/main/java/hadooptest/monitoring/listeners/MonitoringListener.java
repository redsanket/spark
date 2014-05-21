package hadooptest.monitoring.listeners;

import hadooptest.cluster.hadoop.HadoopCluster;
import hadooptest.monitoring.Monitorable;
import hadooptest.monitoring.monitors.AbstractMonitor;
import hadooptest.monitoring.monitors.CPUMonitor;
import hadooptest.monitoring.monitors.JobStatusMonitor;
import hadooptest.monitoring.monitors.LogMonitor;
import hadooptest.monitoring.monitors.MemoryMonitor;
import hadooptest.monitoring.monitors.MonitorGeneral;
import hadooptest.monitoring.monitors.SshAgentLogMonitor;
import hadooptest.monitoring.monitors.TestDurationMonitor;
import hadooptest.monitoring.monitors.TestStatusMonitor;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.runner.Description;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;

import hadooptest.TestSession;

import org.junit.rules.TestWatcher;

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
	static Date date = new Date();	
	HashMap<String, MonitorGeneral> monitorGenerals = new HashMap<String, MonitorGeneral>();
	public static ConcurrentHashMap<String, Boolean> testStatusesWhereTrueMeansPassAndFalseMeansFailed = new ConcurrentHashMap<String, Boolean>();

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
				TestSession.logger
						.trace("Doing Monitoring..since annotation is "
								+ annotation.annotationType()
										.getCanonicalName());

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
				
				SimpleDateFormat sdf = new SimpleDateFormat("MMM_dd_yyyy_HH_mm_ss");
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

				// Add the (SSH Agent) Log Monitor
				SshAgentLogMonitor sshAgentlogMonitor = new SshAgentLogMonitor(
						cluster, componentToHostMapping,
						descriptionOfTestClass, description.getMethodName());
				monitorGeneral.registerMonitor(sshAgentlogMonitor);

				// Add the Test Status Monitor
				TestStatusMonitor testStatusMonitor = new TestStatusMonitor(cluster,
						componentToHostMapping, descriptionOfTestClass,
						description.getMethodName());
				monitorGeneral.registerMonitor(testStatusMonitor);

				// Add the Job Status Monitor
				JobStatusMonitor jobStatusMonitor = new JobStatusMonitor(cluster,
						componentToHostMapping, descriptionOfTestClass,
						description.getMethodName());
				monitorGeneral.registerMonitor(jobStatusMonitor);

				// Add the Test Duration Monitor
				TestDurationMonitor testDurationMonitor = new TestDurationMonitor(cluster,
						componentToHostMapping, descriptionOfTestClass,
						description.getMethodName());
				monitorGeneral.registerMonitor(testDurationMonitor);

				// Start 'em monitors
				monitorGeneral.startMonitors();
				/*
				 * By default a test case status is pass, unless it is
				 * overridden by a test failure
				 */
				testStatusesWhereTrueMeansPassAndFalseMeansFailed.put(
						description.getMethodName(), true);
			} else {
				TestSession.logger
						.trace("Skipping annotation..since annotation is "
								+ annotation.annotationType()
										.getCanonicalName() + " in test named "
								+ description.getMethodName());
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
		TestSession.logger.trace("Got a monitorGeneral" + monitorGeneral
				+ "calling stop Monitors on it");
		monitorGeneral.stopMonitors();
		monitorGeneral.removeAllMonitors();

		monitorGenerals.remove(description.getMethodName());
	}

	@Override
	public void testFailure(Failure failure) throws Exception {
		TestSession.logger.trace("Dang the test failed..............\n");
		String testName = failure.getDescription().getMethodName();
		if (testStatusesWhereTrueMeansPassAndFalseMeansFailed
				.containsKey(testName)) {
			testStatusesWhereTrueMeansPassAndFalseMeansFailed.put(testName,
					false);
		}
	}

}
