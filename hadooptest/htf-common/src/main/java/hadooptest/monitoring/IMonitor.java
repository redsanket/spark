package hadooptest.monitoring;

/**
 * <p>
 * The AbstractMonitor implements this interface. All the monitors
 * {@link CPUMonitor}, {@link MemoryMonitor} and {@link LogMonitor} extend the
 * AbstractMonitor.
 * </p>
 * 
 * @author tiwari
 * 
 */
public interface IMonitor {

	public void startMonitoring();

	public void stopMonitoring();

	/**
	 * Depending on the monitor, data is fetched either periodically (as in case
	 * of CPU, Memory) or is fetched onetime (as in the case of a Log File).
	 */
	public void dumpData();
}
