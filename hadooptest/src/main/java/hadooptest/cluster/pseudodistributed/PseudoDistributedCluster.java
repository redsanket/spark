/*
 * YAHOO!
 */

package hadooptest.cluster.pseudodistributed;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import hadooptest.cluster.Cluster;
import hadooptest.cluster.ClusterState;
import hadooptest.config.testconfig.PseudoDistributedConfiguration;
import hadooptest.Util;
import hadooptest.TestSession;
import hadooptest.config.TestConfiguration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A Cluster subclass that implements a Pseudodistributed Hadoop cluster.
 */
public class PseudoDistributedCluster implements Cluster {

	/** The base pseudodistributed configuration. */
	protected PseudoDistributedConfiguration conf;
	
	/** The state of the pseudodistributed cluster. */
	protected ClusterState cluster_state;
	
	/**
	 * Initializes the pseudodistributed cluster and sets up a new pseudo
	 * distributed configuration.  Writes the configuration to disk for 
	 * initializting the cluster.
	 */
	public PseudoDistributedCluster() throws IOException
	{
		this.conf = new PseudoDistributedConfiguration();
		
		this.conf.write();
	}

	/**
	 * Initializes the fully distributed cluster and sets up a pseudo
	 * distributed configuration using the passed-in configuration.
	 * 
	 * @param conf the configuration to use for the cluster.
	 */
	public PseudoDistributedCluster(PseudoDistributedConfiguration conf)
	{
		this.conf = conf;
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
	 */
	public boolean start() {
		
		String[] start_dfs = { this.getConf().getHadoopProp("HADOOP_INSTALL") + "/sbin/start-dfs.sh", "--config", TestSession.cluster.getConf().getHadoopConfDirPath() };
		String[] start_yarn = { this.getConf().getHadoopProp("HADOOP_INSTALL") + "/sbin/start-yarn.sh", "--config", TestSession.cluster.getConf().getHadoopConfDirPath() };
		String[] start_historyserver = { this.getConf().getHadoopProp("HADOOP_INSTALL") + "/sbin/mr-jobhistory-daemon.sh", "start", "historyserver", "--config", TestSession.cluster.getConf().getHadoopConfDirPath() };
		String[] start_datanode = { this.getConf().getHadoopProp("HADOOP_INSTALL") + "/sbin/hadoop-daemon.sh", "--config", TestSession.cluster.getConf().getHadoopConfDirPath(), "start", "datanode" };
		
		TestSession.logger.info("STARTING DFS...");
		runProcess(start_dfs);
		
		TestSession.logger.info("STARTING DATANODE...");
		runProcess(start_datanode);
		
		TestSession.logger.info("STARTING YARN");
		runProcess(start_yarn);

		TestSession.logger.info("STARTING JOB HISTORY SERVER...");
		runProcess(start_historyserver);
		
		TestSession.logger.info("Sleeping for 30s to wait for HDFS to get out of safe mode.");
		Util.sleep(30);

		return this.isClusterFullyUp();
	}

	/**
	 * Stops all daemons associated with the pseudodistributed cluster instance, and
	 * verifies they have stopped with jps.
	 * 
	 * (non-Javadoc)
	 * @see hadooptest.cluster.Cluster#stop()
	 */
	public boolean stop() {
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

		return this.isClusterFullyDown();
	}

	/**
	 * Currently unimplemented for PseudoDistributedCluster.
	 * 
	 * (non-Javadoc)
	 * @see hadooptest.cluster.Cluster#die()
	 */
	public void die() {

	}

	/**
	 * Currently unimplemented for PseudoDistributedCluster.
	 * 
	 * (non-Javadoc)
	 * @see hadooptest.cluster.Cluster#reset()
	 */
	public boolean reset() {
		boolean stopped = stop();
		boolean started = start();
		return (stopped && started);
	}

	/**
	 * Set a custom configuration for the pseudodistributed cluster instance.
	 * 
	 * @param conf The custom PseudoDistributedConfiguration
	 */
	public void setConf(TestConfiguration conf) {
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
	 * (non-Javadoc)
	 * @see hadooptest.cluster.Cluster#getState()
	 */
	public ClusterState getState() {
		if (this.isClusterFullyUp()) {
			this.cluster_state = ClusterState.UP;
		}
		else if (this.isClusterFullyDown()) {
			this.cluster_state = ClusterState.DOWN;
		}
		else {
			this.cluster_state = ClusterState.UNKNOWN;
		}
		
		return this.cluster_state;
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
	 */
	private boolean isClusterFullyUp() {
		boolean isNameNodeRunning = verifyJpsProcRunning("NameNode");
		boolean isSecondaryNameNodeRunning = verifyJpsProcRunning("SecondaryNameNode");
		boolean isDataNodeRunning = verifyJpsProcRunning("DataNode");
		boolean isResourceManagerRunning = verifyJpsProcRunning("ResourceManager");
		boolean isJobHistoryServerRunning = verifyJpsProcRunning("JobHistoryServer");

		assertTrue("The NameNode was not started.", isNameNodeRunning);
		assertTrue("The SecondaryNameNode was not started.", isSecondaryNameNodeRunning);
		assertTrue("The DataNode was not started.", isDataNodeRunning);
		assertTrue("The ResourceManager was not started.", isResourceManagerRunning);
		assertTrue("The JobHistoryServer was not started.", isJobHistoryServerRunning);		

		return (isNameNodeRunning && isSecondaryNameNodeRunning && 
				isDataNodeRunning && isResourceManagerRunning &&
				isJobHistoryServerRunning);
	}
	
	/**
	 * Check to see if all of the cluster daemons are stopped.
	 * 
	 * @return boolean true if all cluster daemons are stopped.
	 */
	private boolean isClusterFullyDown() {
		boolean isNameNodeRunning = verifyJpsProcRunning("NameNode");
		boolean isSecondaryNameNodeRunning = verifyJpsProcRunning("SecondaryNameNode");
		boolean isDataNodeRunning = verifyJpsProcRunning("DataNode");
		boolean isResourceManagerRunning = verifyJpsProcRunning("ResourceManager");
		boolean isJobHistoryServerRunning = verifyJpsProcRunning("JobHistoryServer");

		assertFalse("The NameNode was not stopped.", isNameNodeRunning);
		assertFalse("The SecondaryNameNode was not stopped.", isSecondaryNameNodeRunning);
		assertFalse("The DataNode was not stopped.", isDataNodeRunning);
		assertFalse("The ResourceManager was not stopped.", isResourceManagerRunning);
		assertFalse("The JobHistoryServer was not stopped.", isJobHistoryServerRunning);		

		return !(isNameNodeRunning || isSecondaryNameNodeRunning || 
				isDataNodeRunning || isResourceManagerRunning ||
				isJobHistoryServerRunning);
	}
	
	/**
	 * Runs a process through the Executor ProcessBuilder, and applies a
	 * BufferedReader to the output, to put the output in the logs.
	 * 
	 * @param cmd The string array of the command to process, where each element of the
	 *                array is a whitespace-delimited element of the command string.
	 */
	private void runProcess(String[] cmd) {
		Process proc = null;
		
		try {
			proc = TestSession.exec.runHadoopProcBuilderGetProc(cmd, TestSession.conf.getProperty("USER", System.getProperty("user.name")));
			BufferedReader reader=new BufferedReader(new InputStreamReader(proc.getInputStream())); 
			String line=reader.readLine(); 
			while(line!=null) 
			{ 
				TestSession.logger.debug(line);				
				line=reader.readLine();
			} 
		}
		catch (IOException ioe) {
			if (proc != null) {
				proc.destroy();
			}
			ioe.printStackTrace();
		}
	}

	/**
	 * Verifies, with jps, that a given process name is running.
	 * 
	 * @param process The String representing the name of the process to verify.
	 */
	private static boolean verifyJpsProcRunning(String process) {

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
		catch (Exception e) {
			if (jpsProc != null) {
				jpsProc.destroy();
			}
			e.printStackTrace();
		}

		TestSession.logger.debug("PROCESS IS NO LONGER RUNNING: " + process);
		return false;
	}

}