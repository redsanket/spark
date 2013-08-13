/*
 * YAHOO!
 */

package hadooptest.cluster.hadoop.pseudodistributed;

import hadooptest.TestSession;
import hadooptest.cluster.hadoop.HadoopCluster;
import hadooptest.config.hadoop.HadoopConfiguration;
import hadooptest.config.hadoop.pseudodistributed.PseudoDistributedConfiguration;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

import coretest.Util;
import coretest.cluster.ClusterState;

/**
 * A Cluster subclass that implements a Pseudodistributed Hadoop cluster.
 */
public class PseudoDistributedCluster extends HadoopCluster {

	/**
	 * Initializes the pseudodistributed cluster and sets up a new pseudo
	 * distributed configuration.  Writes the configuration to disk for 
	 * initializting the cluster.
	 * 
	 * @throws Exception if the cluster configuration can not be initialized,
	 *         if the configuration can not be written to disk, or if the 
	 *         cluster nodes can not be initialized.
	 */
	public PseudoDistributedCluster() throws Exception {
	    this.initDefault();
		this.initNodes();
	}

    /**
     * Initialize the cluster nodes hostnames for the namenode,
     * resource manager, datanode, and nodemanager. 
     * 
     * @throws Exception if the datanode can not be initialized.
     */
    protected void initDefault() throws Exception {
        // Initialize the Hadoop install conf directory for PDCluster.
        TestSession.conf.setProperty("HADOOP_INSTALL_CONF_DIR",
                TestSession.conf.getProperty("HADOOP_INSTALL", "") +
                "/conf/hadoop/");
        TestSession.conf.setProperty("HADOOP_COMMON_HOME",
                TestSession.conf.getProperty("HADOOP_INSTALL"));

        // Initialized temp dir
        String defaultTmpDir =
                "/Users/" + System.getProperty("user.name") + "/hadooptest/tmp";
        TestSession.conf.setProperty("TMP_DIR", 
                TestSession.conf.getProperty("TMP_DIR", defaultTmpDir));
        DateFormat df = new SimpleDateFormat("yyyy-MMdd-hhmmss");  
        df.setTimeZone(TimeZone.getTimeZone("CST"));  
        String tmpDir = TestSession.conf.getProperty("TMP_DIR") +
                "/hadooptest-" + df.format(new Date());
        new File(tmpDir).mkdirs();
        TestSession.conf.setProperty("TMP_DIR", tmpDir);

    }

    /**
     * Initialize the cluster nodes hostnames for the namenode,
     * resource manager, datanode, and nodemanager. 
     * 
     * @throws Exception if the datanode can not be initialized.
     */
    public void initNodes() throws Exception {        
        String[] gwHosts = new String[] {"localhost"};
        String[] nnHosts = new String[] {"localhost"};
        String[] rmHosts = new String[] {"localhost"};
        String[] dnHosts = new String[] {"localhost"};       
        super.initNodes(gwHosts, nnHosts, rmHosts, dnHosts);
    }
	
    /**
     * Gets the gateway client node configuration for this pseudo distributed 
     * cluster instance.
     * 
     * @return PseudoDistributedConfiguration the gateway client component node
     * configuration for the cluster instance.
     */
    public PseudoDistributedConfiguration getConf() {
        return (PseudoDistributedConfiguration) getNode().getConf();
    }

    /**
     * Gets the component node configuration for this pseudo distributed cluster
     * instance.
     * 
     * @return PseudoDistributedConfiguration the component node configuration
     * for the cluster instance.
     */
    public PseudoDistributedConfiguration getConf(String component) {
        return (PseudoDistributedConfiguration) getNode(component).getConf();
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
		        TestSession.conf.getProperty("HADOOP_INSTALL") +
		        "/sbin/start-dfs.sh", "--config",
		        TestSession.cluster.getConf().getHadoopConfDir() };
		String[] start_yarn = {
		        TestSession.conf.getProperty("HADOOP_INSTALL") +
		        "/sbin/start-yarn.sh", "--config",
		        TestSession.cluster.getConf().getHadoopConfDir() };
		String[] start_historyserver = {
		        TestSession.conf.getProperty("HADOOP_INSTALL") +
		        "/sbin/mr-jobhistory-daemon.sh", "start", "historyserver", 
				"--config", TestSession.cluster.getConf().getHadoopConfDir() };
		String[] start_datanode = {
		        TestSession.conf.getProperty("HADOOP_INSTALL") +
		        "/sbin/hadoop-daemon.sh", "--config",
		        TestSession.cluster.getConf().getHadoopConfDir(), "start",
		        "datanode" };
		
		TestSession.logger.info("STARTING DFS..." +
		        StringUtils.join(start_dfs, ","));
		runProcess(start_dfs);
		
		TestSession.logger.info(
		        "STARTING DATANODE: " + StringUtils.join(start_datanode, ","));
		runProcess(start_datanode);
		
		TestSession.logger.info(
		        "STARTING YARN: " + StringUtils.join(start_yarn, ","));
		runProcess(start_yarn);

		TestSession.logger.info(
		        "STARTING JOB HISTORY SERVER: " +
		                StringUtils.join(start_historyserver, ","));
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
		String[] stop_dfs = {
		        TestSession.conf.getProperty("HADOOP_INSTALL") +
		        "/sbin/stop-dfs.sh"
		        };
		String[] stop_yarn = {
		        TestSession.conf.getProperty("HADOOP_INSTALL") +
		        "/sbin/stop-yarn.sh"
		        };
		String[] stop_historyserver = {
		        TestSession.conf.getProperty("HADOOP_INSTALL") +
		        "/sbin/mr-jobhistory-daemon.sh", "stop", "historyserver"
		        };
		String[] stop_datanode = {
		        TestSession.conf.getProperty("HADOOP_INSTALL") +
		        "/sbin/hadoop-daemon.sh", "stop", "datanode"
		        };

		runProcess(stop_dfs);
		runProcess(stop_yarn);
		runProcess(stop_historyserver);
		runProcess(stop_datanode);

		// Wait for 10 seconds to ensure that the daemons have had time to stop.
		Util.sleep(10);

		return this.isFullyDown();
	}

    /**
     * Start or stop the Hadoop daemon processes.
     *
     * @param action The action to perform on the Hadoop daemon
     * {"start", "stop"}
     * @param component The cluster component to perform the action on. 
     * 
     * @return 0 for success or 1 for failure.
     * 
     * @throws Exception if there is a fatal error starting or stopping
     *         a daemon node.
     */
    public int hadoopDaemon(Action action, String component) 
            throws Exception {
        throw new Exception("Method not implemented!!!");
    }
        
    /**
     * Start or stop the Hadoop daemon processes.
     *
     * @param action The action to perform on the Hadoop daemon
     * {"start", "stop"}
     * @param component The cluster component to perform the action on. 
     * @param daemonHost The hostnames to perform the action on. 
     * 
     * @return 0 for success or 1 for failure.
     * 
     * @throws Exception if there is a fatal error starting or stopping
     *         a daemon node.
     */
    public int hadoopDaemon(Action action, String component,
            String[] daemonHost) throws Exception {
        throw new Exception("Method not implemented!!!");
    }
    
    /**
     * Start or stop the Hadoop daemon processes. The method will also wait for
     * the daemons to fully start or stop depending on the expected state. 
     * It will also reinitialize the hadooptest configuration object with the
     * configuration directory if the action is start, and the configuration
     * directory is not the default one (which cannot be modified due to root
     * permission).  
     *
     * @param action The action to perform on the Hadoop daemon
     * {"start", "stop"}
     * @param component The cluster component to perform the action on. 
     * @param daemonHost The hostnacomponent The cluster component to perform the action on. 
     * @param confDir The configuration directory to perform the action with. 
     * 
     * @return 0 for success or 1 for failure.
     * 
     * @throws Exception if there is a fatal error starting or stopping
     *         a daemon node.
     */
    public int hadoopDaemon(Action action, String component,
            String[] daemonHost, String confDir) throws Exception {
        throw new Exception("Method not implemented!!!");
    }
    

	/**
	 * Set a custom configuration for the pseudodistributed cluster instance.
	 * 
	 * @param conf The custom PseudoDistributedConfiguration
	 */
	public void setConf(HadoopConfiguration conf) {
	    getNode().setConf(conf);
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
        PseudoDistributedConfiguration conf =
                (PseudoDistributedConfiguration) getNode().getConf();
        return conf.getHadoopProp("HADOOP_VERSION");
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
     * Check if the cluster component is fully up.
     * 
     * @param component cluster component such as gateway, namenode,
     * 
     * @return true if the cluster is fully up, false if the cluster is not
     * fully up.
     * 
     * @throws Exception if there is a fatal error checking if a component is
     * up.
     */
    public boolean isComponentFullyUp(String component) 
            throws Exception {
        return isComponentFullyUp(component, null);
    }
    
    /**
     * Check if the cluster component is fully up for a given String Array of
     * host names. 
     * 
     * @param component cluster component such as gateway, namenode,
     * @param daemonHost host names String Array of daemon host names,
     * 
     * @return boolean true if the cluster is fully up, false if the cluster is 
     * not fully up.
     * 
     * @throws Exception if there is a fatal error checking the state of a 
     * component.
     */
    public boolean isComponentFullyUp(String component, String[] daemonHost) 
            throws Exception {
        return isComponentFullyInExpectedState(Action.START,
                component, daemonHost);
    }
    

    /**
     * Check if the cluster component is fully down.
     * 
     * @param component cluster component such as gateway, namenode,
     * 
     * @return boolean true if the cluster is fully down, false if the cluster 
     * is not fully down.
     * 
     * @throws Exception if there is a failure checking if a component is down.
     */
    public boolean isComponentFullyDown(String component) 
            throws Exception {
        throw new Exception("Method not implemented!!!");
    }   

    /**
     * Check if the cluster component is fully down for a given String Array of
     * host names. 
     * 
     * @param component cluster component such as gateway, namenode,
     * @param daemonHost host names String Array of daemon host names,
     * 
     * @return boolean true if the cluster is fully down, false if the cluster
     * is not fully down.
     * 
     * @throws Exception if there is a failure checking if a component is down.
     */
    public boolean isComponentFullyDown(String component, String[] daemonHost) 
            throws Exception {      
        return isComponentFullyInExpectedState(Action.STOP,
                component, daemonHost);
    }
        
    /**
     * Check if the cluster component is fully in a specified state associated
     * with start or stop.
     * 
     * @param action the action associated with the expected state
     * {"start", "stop"}
     * @param component cluster component such as gateway, namenode,
     * 
     * @return boolean true if the cluster is fully in the expected state,
     * false if the cluster is not fully in the expected state.
     * 
     * @throws Exception if there is a failure checking if a component is in 
     * the expected state.
     */
    public boolean isComponentFullyInExpectedState(Action action,
            String component) 
                    throws Exception {
        return isComponentFullyInExpectedState(action, component, null);
    }
    
    /**
     * Check if the cluster component is fully in a specified state associated
     * with start or stop.
     * 
     * @param action the action associated with the expected state 
     * {"start", "stop"}
     * @param component cluster component such as gateway, namenode,
     * @param daemonHosts host names String Array of daemon host names,
     * 
     * @return boolean true if the cluster is fully in the expected state,
     * false if the cluster is not fully in the expected state.
     * 
     * @throws Exception if there is a failure checking if a component is in 
     * the expected state.
     */
    public boolean isComponentFullyInExpectedState(Action action,
            String component, String[] daemonHosts) throws Exception {
        throw new Exception("Method not implemented!!!");
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
