package hadooptest.monitoring.monitors;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import hadooptest.monitoring.IMonitor;
import hadooptest.TestSession;

/**
 * The actual monitors (CPU, Memory, Log) should extend this AbstractMonitor.
 * 
 * @author tiwari
 * 
 */
public abstract class AbstractMonitor implements Runnable, IMonitor {

	int periodicity;
	String testClassBeingMonitored = null;
	String testMethodBeingMonitored = null;
	String testPackageBeingMonitored = null;
	String clusterName = null;
	String startTime = null;
	HashMap<String, HashMap<Integer, String>> hostwiseReadings;
	volatile boolean monitoringUnderway;
	HashMap<String, ArrayList<String>> componentToHostMapping = null;
	String commaSeperatedHosts;
	String[] commandStrings = null;
	String kind;

	protected FileWriter fw = null;
	protected BufferedWriter bw = null;
	protected String baseDirPathToDump;
	protected File fileHandleOnCompletePathToOutputFile;

	AbstractMonitor(String clusterName,
			HashMap<String, ArrayList<String>> sentComponentToHostMapping,
			Class<?> testClass, String testMethodName, int periodicity,
			String resourceKind) {
		this.clusterName = clusterName;
		this.componentToHostMapping = sentComponentToHostMapping;
		this.testClassBeingMonitored = testClass.getSimpleName();
		this.testMethodBeingMonitored = testMethodName;
		this.testPackageBeingMonitored = testClass.getPackage().getName();
		this.periodicity = periodicity;

		Date date = new Date();
		SimpleDateFormat sdf = new SimpleDateFormat("MMM_dd_yyyy_HH_mm_ss");
		String formattedDateAndTime = sdf.format(date);
		this.startTime = formattedDateAndTime;
		baseDirPathToDump = "/grid/0/tmp" + "/" + "stressMonitoring/"
				+ this.clusterName + "/" 
				+ this.testPackageBeingMonitored + "/"
				+ this.testClassBeingMonitored + "/"
				+ this.testMethodBeingMonitored + "/"
				+ formattedDateAndTime + "/";
			synchronized(this){
			hostwiseReadings = new HashMap<String, HashMap<Integer, String>>();
			}

		commaSeperatedHosts = new String();
		for (String aComponent : componentToHostMapping.keySet()) {
			for (String aHost : componentToHostMapping.get(aComponent)) {
				commaSeperatedHosts += aHost + ",";
			}
		}
		commaSeperatedHosts = commaSeperatedHosts.substring(0,
				commaSeperatedHosts.length() - 1);
		this.kind = resourceKind;

	}

	/**
	 * The thread run control variable. Thread stops once this is set to false.
	 */

	public void startMonitoring() {
		monitoringUnderway = true;
	}

	/**
	 * The thread run control variable. Thread stops once this is set.
	 */
	public void stopMonitoring() {
		TestSession.logger.info("Stop monitoring received in AbstractMonotor for "
				+ this.kind);
		monitoringUnderway = false;
	}

	synchronized public void run() {
		int tick = 0;
		while (monitoringUnderway) {
			try {
				fetchResourceUsageIntoMemory(tick++);
				TestSession.logger.info(kind + " monitor sleeping for " + this.periodicity
						+ " secs...");
				Thread.sleep(this.periodicity * 1000);
			} catch (IOException e) {
				throw new RuntimeException(e);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
	}

	/**
	 * The records that have been read into memory thus far are dumped into a
	 * file here. The file hierarchy includes the cluster, the package test
	 * class and the test method name.
	 */
	public void dumpData() {
		TestSession.logger.info("DUMPING:" + hostwiseReadings);
		new File(baseDirPathToDump).mkdirs();
		fileHandleOnCompletePathToOutputFile = new File(baseDirPathToDump
				+ this.kind);

		FileWriter fw = null;
		int maxTick = 0;
		try {
			fw = new FileWriter(
					fileHandleOnCompletePathToOutputFile.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			for (String aHost : hostwiseReadings.keySet()) {
				bw.write(aHost);
				bw.write(",");
				maxTick = hostwiseReadings.get(aHost).keySet().size();
			}
			bw.write('\n');
			for (int tt = 0; tt < maxTick; tt++) {
				for (String aHost : hostwiseReadings.keySet()) {
					bw.write(hostwiseReadings.get(aHost).get(tt));
					bw.write(",");
				}
				bw.write('\n');
			}
			bw.close();
		} catch (IOException e1) {
			throw new RuntimeException(e1);
		}
	}

	abstract public void fetchResourceUsageIntoMemory(int tick)
			throws IOException;

}
