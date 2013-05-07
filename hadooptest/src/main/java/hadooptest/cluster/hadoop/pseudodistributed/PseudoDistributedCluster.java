/*
 * YAHOO!
 */

package hadooptest.cluster.hadoop.pseudodistributed;

import hadooptest.cluster.ClusterState;
import hadooptest.cluster.hadoop.HadoopCluster;
import hadooptest.config.hadoop.HadoopConfiguration;
import hadooptest.config.hadoop.pseudodistributed.PseudoDistributedConfiguration;
import hadooptest.Util;
import hadooptest.TestSession;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A Cluster subclass that implements a Pseudodistributed Hadoop cluster.
 */
public class PseudoDistributedCluster extends HadoopCluster {

	/** The base pseudodistributed configuration. */
	protected PseudoDistributedConfiguration conf;

	/**
	 * Initializes the pseudodistributed cluster and sets up a new pseudo
	 * distributed configuration.  Writes the configuration to disk for 
	 * initializting the cluster.
	 * 
	 * @throws Exception if the cluster configuration can not be initialized,
	 *         if the configuration can not be written to disk, or if the 
	 *         cluster nodes can not be initialized.
	 */
	public PseudoDistributedCluster() 
			throws Exception {
		this.conf = new PseudoDistributedConfiguration();
		this.conf.write();
		super.initNodes();
	}

	/**
	 * Initializes the fully distributed cluster and sets up a pseudo
	 * distributed configuration using the passed-in configuration.
	 * 
	 * @param conf the configuration to use for the cluster.
	 * 
	 * @throws Exception if the cluster configuration can not be initialized,
	 *         if the configuration can not be written to disk, or if the 
	 *         cluster nodes can not be initialized.
	 */
	public PseudoDistributedCluster(PseudoDistributedConfiguration conf)
			throws Exception {
		this.conf = conf;
		super.initNodes();
	}
	
	/**
	 * Starts the pseudodistributed cluster instance by starting:
	 *   - NameNode
	 *   - SecondaryNameNode
	 *   - DataNode
	 *   - ResourceManager
	 *   - JobHistoryServer
	 *   
	 * Also verifies that the daemons have started by using jps.
	 * 
	 * (non-Javadoc)
	 * @see hadooptest.cluster.Cluster#start()
	 * 
	 * @param waitForSafemodeOff option to wait for safemode off after start.
	 * Default value is true. 
	 * 
	 * @return boolean true for success, false for failure.
	 * 
	 * @throws Exception if there is a fatal error running a process that starts a daemon node.
	 */
	public boolean start(boolean waitForSafemodeOff) 
			throws Exception {		
		String[] start_dfs = {
				this.getConf().getHadoopProp("HADOOP_INSTALL") + "/sbin/start-dfs.sh", 
				"--config", TestSession.cluster.getConf().getHadoopConfDir() };
		String[] start_yarn = {
				this.getConf().getHadoopProp("HADOOP_INSTALL") + "/sbin/start-yarn.sh", 
				"--config", TestSession.cluster.getConf().getHadoopConfDir() };
		String[] start_historyserver = {
				this.getConf().getHadoopProp("HADOOP_INSTALL") + "/sbin/mr-jobhistory-daemon.sh",
				"start", "historyserver", 
				"--config", TestSession.cluster.getConf().getHadoopConfDir() };
		String[] start_datanode = {
				this.getConf().getHadoopProp("HADOOP_INSTALL") + "/sbin/hadoop-daemon.sh",
				"--config", TestSession.cluster.getConf().getHadoopConfDir(),
				"start", "datanode" };
		
		TestSession.logger.info("STARTING DFS...");
		runProcess(start_dfs);
		
		TestSession.logger.info("STARTING DATANODE...");
		runProcess(start_datanode);
		
		TestSession.logger.info("STARTING YARN");
		runProcess(start_yarn);

		TestSession.logger.info("STARTING JOB HISTORY SERVER...");
		runProcess(start_historyserver);
		
		boolean isFullyUp = this.isFullyUp();
		TestSession.logger.info("isFullyUp=" + isFullyUp);

		boolean isSafeModeOff =false;
		if (waitForSafemodeOff) {
			TestSession.logger.info("Wait for HDFS to get out of safe mode.");
			isSafeModeOff = this.waitForSafemodeOff();
			TestSession.logger.info("waitForSafemodeOff=" + isSafeModeOff);
		}
		
		return (waitForSafemodeOff) ? (isSafeModeOff && isFullyUp) : isFullyUp;
	}

	/**
	 * Stops all daemons associated with the pseudodistributed cluster instance, and
	 * verifies they have stopped with jps.
	 * 
	 * @throws Exceptoin if there is a fatal error running a process that stops a daemon node.
	 * 
	 * (non-Javadoc)
	 * @see hadooptest.cluster.Cluster#stop()
	 */
	public boolean stop() 
			throws Exception {
		String[] stop_dfs = { this.getConf().getHadoopProp("HADOOP_INSTALL") + "/sbin/stop-dfs.sh" };
		String[] stop_yarn = { this.getConf().getHadoopProp("HADOOP_INSTALL") + "/sbin/stop-yarn.sh" };
		String[] stop_historyserver = { this.getConf().getHadoopProp("HADOOP_INSTALL") + "/sbin/mr-jobhistory-daemon.sh", "stop", "historyserver" };
		String[] stop_datanode = { this.getConf().getHadoopProp("HADOOP_INSTALL") + "/sbin/hadoop-daemon.sh", "stop", "datanode" };

		runProcess(stop_dfs);
		runProcess(stop_yarn);
		runProcess(stop_historyserver);
		runProcess(stop_datanode);

		// Wait for 10 seconds to ensure that the daemons have had time to stop.
		Util.sleep(10);

		return this.isFullyDown();
	}

	/**
	 * Set a custom configuration for the pseudodistributed cluster instance.
	 * 
	 * @param conf The custom PseudoDistributedConfiguration
	 */
	public void setConf(HadoopConfiguration conf) {
		this.conf = (PseudoDistributedConfiguration)conf;
	}

	/**
	 * Gets the configuration for this pseudodistributed cluster instance.
	 * 
	 * @return PseudoDistributedConfiguration the configuration for the cluster instance.
	 */
	public PseudoDistributedConfiguration getConf() {
		return this.conf;
	}

	/**
	 * Returns the state of the pseudodistributed cluster instance.
	 * 
	 * @return ClusterState the state of the cluster.
	 * 
	 * @throws IOException if there is a fatal error checking the state of the cluster.
	 * 
	 * (non-Javadoc)
	 * @see hadooptest.cluster.Cluster#getState()
	 */
	public ClusterState getState() throws IOException {
		ClusterState clusterState = ClusterState.UNKNOWN;
		if (this.isFullyUp()) {
			clusterState = ClusterState.UP;
		}
		else if (this.isFullyDown()) {
			clusterState = ClusterState.DOWN;
		}
		return clusterState;
	}

	/**
	 * Gets the version of the Hadoop cluster instance.
	 * 
	 * @return String the version of the Hadoop cluster.
	 */
	public String getVersion() {
    	return this.conf.getHadoopProp("HADOOP_VERSION");
	}	
	
	/**
	 * Check to see if all of the cluster daemons are running.
	 * 
	 * @return boolean true if all cluster daemons are running.
	 * 
	 * @throws IOException if there is a fatal error checking the
	 *         state of the cluster.
	 */
	public boolean isFullyUp() throws IOException {
		String[] components = {
                "NameNode",
                "SecondaryNameNode",
                "DataNode",
                "ResourceManager",
                "JobHistoryServer" };
		boolean isFullyUp = true;
		for (String component : components) {			
			isFullyUp = (isFullyUp && verifyJpsProcRunning(component));
		}
		return isFullyUp;
	}
	
	/**
	 * Check to see if all of the cluster daemons are stopped.
	 * 
	 * @return boolean true if all cluster daemons are stopped.
	 * 
	 * @throws IOException if there is a fatal error checking the
	 *         state of the cluster.
	 */
	public boolean isFullyDown() throws IOException {
		String[] components = {
                "NameNode",
                "SecondaryNameNode",
                "DataNode",
                "ResourceManager",
                "JobHistoryServer" };
		boolean isFullyDown = true;
		for (String component : components) {			
			isFullyDown = (isFullyDown && !verifyJpsProcRunning(component));
		}
		return isFullyDown;
	}
	
	/**
	 * Runs a process through the Executor ProcessBuilder, and applies a
	 * BufferedReader to the output, to put the output in the logs.
	 * 
	 * @param cmd The string array of the command to process, where each element of the
	 *                array is a whitespace-delimited element of the command string.
	 *                
	 * @throws Exception if there is a fatal error running the process, or if the 
	 *         input stream can not be read.
	 */
	private void runProcess(String[] cmd) 
			throws Exception {
		Process proc = null;

		try {
			proc = TestSession.exec.runProcBuilderSecurityGetProc(cmd, TestSession.conf.getProperty("USER", System.getProperty("user.name")));
			BufferedReader reader=new BufferedReader(new InputStreamReader(proc.getInputStream())); 

			String line=reader.readLine(); 
			while(line!=null) 
			{ 
				TestSession.logger.debug(line);				
				line=reader.readLine();
			}
		}
		finally {
			if (proc != null) {
				proc.destroy();
			}
		}
	}

	/**
	 * Verifies, with jps, that a given process name is running.
	 * 
	 * @param process The String representing the name of the process to verify.
	 * 
	 * @throws IOException if there is a fatal error running the process to check
	 *         the jps processes, or if the input stream from the process can not
	 *         be read.
	 */
	private static boolean verifyJpsProcRunning(String process) 
			throws IOException {

		Process jpsProc = null;

		String jpsCmd = "jps";

		TestSession.logger.debug(jpsCmd);

		String jpsPatternStr = "(.*)(" + process + ")(.*)";
		Pattern jpsPattern = Pattern.compile(jpsPatternStr);

		try {
			jpsProc = Runtime.getRuntime().exec(jpsCmd);
			BufferedReader reader=new BufferedReader(new InputStreamReader(jpsProc.getInputStream())); 
			String line=reader.readLine(); 
			while(line!=null) 
			{  
				TestSession.logger.debug(line);

				Matcher jpsMatcher = jpsPattern.matcher(line);

				if (jpsMatcher.find()) {
					TestSession.logger.debug("FOUND PROCESS: " + process);
					return true;
				}

				line=reader.readLine();
			}
		}
		finally {
			if (jpsProc != null) {
				jpsProc.destroy();
			}
		}

		TestSession.logger.debug("PROCESS IS NO LONGER RUNNING: " + process);
		return false;
	}

}
