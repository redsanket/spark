package hadooptest.monitoring.monitors;

import hadooptest.monitoring.IMonitor;
import hadooptest.monitoring.IMonitoringControl;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 * MonitorGeneral is a single point of control for a Monitor's lifecycle.
 * {@link MonitoringListener} instantiates this class.
 * </p>
 * 
 * @author tiwari
 * 
 */
public class MonitorGeneral implements IMonitoringControl {
	private ArrayList<IMonitor> monitors = new ArrayList<IMonitor>();
	ExecutorService executor = Executors.newFixedThreadPool(20);

	/**
	 * This method is invoked from {@link MonitoringListener} once a test
	 * starts.
	 */

	public void registerMonitor(IMonitor monitor) {
		monitors.add(monitor);
	}

	public void removeMonitor(IMonitor monitor) {
		monitors.remove(monitor);

	}

	/**
	 * This method is invoked from {@link MonitoringListener} once a test
	 * starts.
	 */

	public void startMonitors() {
		for (IMonitor aMonitor : monitors) {
			aMonitor.startMonitoring();
			executor.execute((Runnable) aMonitor);
		}
	}

	/**
	 * This method is invoked from {@link MonitoringListener} once a test
	 * finishes.
	 */
	public void stopMonitors() {
		for (IMonitor aMonitor : monitors) {
			aMonitor.stopMonitoring();
			aMonitor.dumpData();
		}
	}

	/**
	 * This method is invoked from {@link MonitoringListener} once a test
	 * finishes.
	 */
	public void removeAllMonitors() {
		monitors = new ArrayList<IMonitor>();
	}
}
