/*
 * YAHOO!
 */

package hadooptest.cluster.hadoop.pseudodistributed;

import hadooptest.cluster.ClusterState;
import hadooptest.cluster.hadoop.HadoopCluster;
import hadooptest.config.hadoop.HadoopConfiguration;
import hadooptest.config.hadoop.pseudodistributed.PseudoDistributedConfiguration;
import hadooptest.node.hadoop.HadoopNode;
import hadooptest.Util;
import hadooptest.TestSession;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Enumeration;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

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
        // Gateway
        TestSession.logger.info("Initialize the gateway client node:");
        initComponentNodes(HadoopCluster.GATEWAY, new String[] {"localhost"});
        
        // Namenode
        TestSession.logger.info("Initialize the namenode node(s):");
        initComponentNodes(HadoopCluster.NAMENODE, new String[] {"localhost"});
        
        // Resource Manager
        TestSession.logger.info("Initialize the resource manager node(s):");
        initComponentNodes(HadoopCluster.RESOURCE_MANAGER,
                new String[] {"localhost"});

        // Datanode
        initComponentNodes(HadoopCluster.DATANODE, new String[] {"localhost"});
        
        // Nodemanager
        TestSession.logger.info("Initialize the nodemanager node(s):");
        initComponentNodes(HadoopCluster.NODEMANAGER,
                new String[] {"localhost"});
       
        // Show all balances in hash table. 
        TestSession.logger.debug("-- listing cluster nodes --");
        Enumeration<String> components = hadoopNodes.keys(); 
        while (components.hasMoreElements()) { 
            String component = (String) components.nextElement(); 
            Enumeration<HadoopNode> iterator = hadoopNodes.get(component).elements();
            while(iterator.hasMoreElements()) {
              HadoopNode node = (HadoopNode)iterator.nextElement();
              TestSession.logger.info("component '" + component + "' node='" +
                      node.getHostname() + "'.");
            }
        }   

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
