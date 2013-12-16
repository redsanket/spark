package hadooptest.monitoring;

/**
 * <p>
 * This interface is implemented by {@link MonitorGeneral}. This serves as a
 * single point for controlling a Monitor's lifecycle.
 * </p>
 * 
 * @author tiwari
 * 
 */
public interface IMonitoringControl {
	public void registerMonitor(IMonitor monitor);

	public void removeMonitor(IMonitor monitor);

	public void removeAllMonitors();

	public void startMonitors();

	public void stopMonitors();

}
