/*
 * YAHOO!
 */

package hadooptest.cluster.hadoop;

import hadooptest.TestSession;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.config.hadoop.HadoopConfiguration;
import hadooptest.node.hadoop.HadoopNode;
import hadooptest.node.hadoop.fullydistributed.FullyDistributedNode;
import hadooptest.node.hadoop.pseudodistributed.PseudoDistributedNode;

import java.io.IOException;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.security.SecurityUtil;
//import org.apache.hadoop.yarn.client.YarnClientImpl; // H0.23
import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl; // H2.x

import coretest.Util;
import coretest.cluster.ClusterState;

/**
 * An interface which should represent the base capability of any cluster
 * type in the framework.  This interface is also what should be commonly
 * used to reference the common cluster instance maintained by the
 * TestSession.  Subclasses of Cluster should implement a specific cluster
 * type.
 */
public abstract class HadoopCluster {

    /** String representing the cluster components. */
    public static final String NAMENODE = "namenode";
    public static final String RESOURCE_MANAGER = "resourcemanager";
    public static final String DATANODE = "datanode";
    public static final String NODEMANAGER = "nodemanager";
    public static final String HISTORYSERVER = "historyserver";
    public static final String GATEWAY = "gateway";
    
    /** String array for the cluster components */
    public static final String[] components = {
        HadoopCluster.NAMENODE,
        HadoopCluster.RESOURCE_MANAGER,
        HadoopCluster.HISTORYSERVER,
        HadoopCluster.DATANODE,
        HadoopCluster.NODEMANAGER,
        HadoopCluster.GATEWAY
        };

    // Admin hosts
    public static final String ADMIN = "adm102.blue.ygrid.yahoo.com";

    /** String representing the cluster type. */
    public static final String FD_CLUSTER_TYPE =
            "hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster";
    public static final String PD_CLUSTER_TYPE =
            "hadooptest.cluster.hadoop.pseudodistributed.PseudoDistributedCluster";
    
    public static enum State { UP, DOWN, UNKNOWN }
    public static enum Action { START, STOP, RESET, STATUS }
    public static final String START = "start";
    public static final String STOP = "stop";
    
    /**
     * Container for storing the Hadoop cluster node objects by components
     * Each component contains a hash table of key hostname and
     * value HadoopNode */
    protected Hashtable<String, HadoopComponent> hadoopComponents =
            new Hashtable<String, HadoopComponent>();
    
    /* TODO: Consider maintaining a cluster level properties for tracking
     * cluster level paths and settings. 
     */

    /* Get the custom default settings filename. If the file exists, the content
     * is the full path name of the custom default hadoop config directory. 
     */
    public static String getDefaultConfSettingsFile(String component,
            String hostname) {
        return TestSession.conf.getProperty("HADOOP_CUSTOM_DEFAULT_CONF_DIR") +
                "/" + component + "-" + hostname;        
    }

	/**
	 * Start the cluster from a stopped state.
	 *
	 * @param waitForSafemodeOff to wait for safemode off after start.
	 * Default is true. 
	 * 
	 * @return boolean true for success and false for failure.
	 * 
	 * @throws Exception if there is a fatal error in starting the cluster.
	 */
	public abstract boolean start(boolean waitForSafemodeOff) 
			throws Exception;

	/**
	 * Stop the cluster, shut it down cleanly to a state from which
	 * it can be restarted.
	 * 
	 * @return boolean true for success and false for failure.
	 * 
	 * @throws Exception if there is a fatal error in starting the cluster.
	 */
	public abstract boolean stop() 
			throws Exception;

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
    public abstract int hadoopDaemon(Action action, String component) 
            throws Exception;
        
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
    public abstract int hadoopDaemon(Action action, String component,
            String[] daemonHost) throws Exception;
    
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
    public abstract int hadoopDaemon(Action action, String component,
            String[] daemonHost, String confDir) throws Exception;
    
	/**
	 * Get the current state of the cluster.
	 * 
	 * @return ClusterState the state of the cluster.
	 * 
	 * @throws Exception if there is a fatal error getting
	 * 	       the cluster state.
	 */
	public abstract ClusterState getState()
			throws Exception;

	/**
	 * Check to see if all of the cluster daemons are running.
	 * 
	 * @return boolean true if all cluster daemons are running.
	 * 
	 * @throws Exception if there is a fatal error checking 
	 *         cluster state.
	 */
	public abstract boolean isFullyUp()
			throws Exception;

	/**
	 * Check to see if all of the cluster daemons are stopped.
	 * 
	 * @return boolean true if all cluster daemons are stopped.
	 * 
	 * @throws Exception if there is a fatal error checking
	 *         cluster state.
	 */
	public abstract boolean isFullyDown()
			throws Exception;

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
    public abstract boolean isComponentFullyUp(String component) 
            throws Exception;
    
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
    public abstract boolean isComponentFullyUp(String component,
            String[] daemonHost) throws Exception;
    
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
    public abstract boolean isComponentFullyDown(String component) 
            throws Exception; 

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
    public abstract boolean isComponentFullyDown(String component,
            String[] daemonHost) throws Exception;
        
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
    public abstract boolean isComponentFullyInExpectedState(Action action,
            String component) throws Exception;
    
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
    public abstract boolean isComponentFullyInExpectedState(Action action,
            String component, String[] daemonHosts) throws Exception;
    	
	/**
	 * Get the current version of the cluster.
	 * 
	 * @return ClusterState the state of the cluster.
	 */
	public abstract String getVersion();

	/**
	 * Get the cluster Hadoop configuration.
	 * 
	 * @return TestConfiguration the Hadoop cluster configuration.
	 */
	public abstract HadoopConfiguration getConf();

	/**
	 * Set the cluster Hadoop configuration.
	 * 
	 * @param conf the Hadoop cluster configuration to set for the cluster.
	 */
	public abstract void setConf(HadoopConfiguration conf);

    /**
	 * Sets the keytab and user and initializes Hadoop security through
	 * the Hadoop API.
	 * 
	 * @param keytab the keytab (like "keytab-hadoopqa")
	 * @param user the user (like "user-hadoopqa")
	 * 
	 * @throws IOException if cluster security can't be initialized.
	 */
	public void setSecurityAPI(String keytab, String user) throws IOException {
		TestSession.logger.info("Initializing Hadoop security");
		TestSession.logger.debug("Keytab = " + keytab);
		TestSession.logger.debug("User = " + user);
		SecurityUtil.login(TestSession.cluster.getConf(), keytab, user);
	}

    public void initNodes () throws Exception {
        initNodes(null, null, null, null);
    }
    
    public void initNodes (String[] gwHosts, String[] nnHosts, String[] rmHosts,
            String[] dnHosts) throws Exception {
        // Gateway
        TestSession.logger.info("Initialize the gateway client node:");
        if (gwHosts == null || gwHosts.length == 0) {
            gwHosts = new String[] {"localhost"};
        }
        initComponentNodes(HadoopCluster.GATEWAY, gwHosts);
        
        // Namenode
        TestSession.logger.info("Initialize the namenode node(s):");
        if (nnHosts == null || nnHosts.length == 0) {
            String namenode_addr =
                    this.getConf().get("dfs.namenode.https-address");
            String namenode = namenode_addr.split(":")[0];
            nnHosts = new String[] {namenode};
        }
        initComponentNodes(HadoopCluster.NAMENODE, nnHosts);
        
        // Resource Manager
        TestSession.logger.info("Initialize the resource manager node(s):");
        if (rmHosts == null || rmHosts.length == 0) {
            String rm_addr = this.getConf().get(
                    "yarn.resourcemanager.resource-tracker.address");
            String rm = rm_addr.split(":")[0];
            rmHosts = new String[] {rm};
        }
        initComponentNodes(HadoopCluster.RESOURCE_MANAGER, rmHosts);

        // History Server
        TestSession.logger.info("Initialize the history server node:");
        initComponentNodes(HadoopCluster.HISTORYSERVER, rmHosts);

        // Datanode
        TestSession.logger.info("Initialize the datanode node(s):");
        if (dnHosts == null || dnHosts.length == 0) {
            // The slave file must come from the namenode. They have different
            // values on other nodes. This will require that the namenode node and 
            // configuration class be initialized beforehand.         
            FullyDistributedCluster fdcluster = (FullyDistributedCluster) this;
            dnHosts = this.getHostsFromList(
                    nnHosts[0],
                    fdcluster.getConf(HadoopCluster.NAMENODE).getHadoopConfDir() +
                    "/slaves");            
        }
        initComponentNodes(HadoopCluster.DATANODE, dnHosts);
        
        // Nodemanager
        TestSession.logger.info("Initialize the nodemanager node(s):");
        initComponentNodes(HadoopCluster.NODEMANAGER, dnHosts);

        printNodes();
    }

    
    public void printNodes() {
        TestSession.logger.debug("-- listing cluster nodes --");
        Enumeration<String> components = hadoopComponents.keys();
        while(components.hasMoreElements()) {
            String component = (String) components.nextElement();
            hadoopComponents.get(component).printNodes();
        }        
    }
    
    /**
     * Initialize the Hadoop component nodes for a give component type.
     * 
     * @param component String.
     * @param hosts String Array.
     * 
     * @throws IOException if nodes can't be initialized.
     */
	protected void initComponentNodes(String component, String[] hosts)
	        throws Exception {
        String clusterType = TestSession.conf.getProperty("CLUSTER_TYPE");
	    TestSession.logger.debug("Initialize cluster nodes for component '" +
	        component + "', hosts '" + StringUtils.join(hosts, ",") + "', " +
	            "cluster type '" +
	            clusterType.substring(clusterType.lastIndexOf(".")+1) + "'.");

	    Hashtable<String, HadoopNode> cNodes =
	            new Hashtable<String, HadoopNode>();
	    String compHostsSize = Integer.toString(hosts.length);
	    int index=1;
	    for (String host : hosts) {
	        TestSession.logger.debug("Initialize '" + component + "' " +
	                "component host '" + host + "' [" + index++ + "/" +
	                compHostsSize + "].");
	        
	        // Initialize the component node.
	        if (clusterType.equals(HadoopCluster.FD_CLUSTER_TYPE)) {
	            cNodes.put(host, new FullyDistributedNode(host, component));
	        } else {
	            cNodes.put(host, new PseudoDistributedNode(host, component));
	        }
	            
	        // Verify the instantiated node.
	        if (TestSession.logger.isTraceEnabled()) {
	            TestSession.logger.trace("Verify instantiated cluster nodes:");
	            HadoopNode node = cNodes.get(host);         
	            TestSession.logger.trace("Instantiated node name='" +
	                    node.getHostname() + "': " + "default conf='" +
	                    node.getDefaultConfDir() + "', " + "conf='" +
	                    node.getConfDir() + "'.");           
	            HadoopConfiguration conf = node.getConf();
	            if (conf == null) {
	                TestSession.logger.error("Node conf object is null!!!");
	            }
	            TestSession.logger.trace("Instantiated node conf object: " + 
	                    "default conf dir='" + conf.getDefaultHadoopConfDir() +
	                    "', conf dir='" + conf.getHadoopConfDir() + "'");
	        }
	    }
	    	    
	    this.hadoopComponents.put(
	            component,
	            new HadoopComponent(component, cNodes));
	}
	
	/**
	 * Parse the host names from a host name list on the namenode.
	 * 
	 * @param namenode the namenode hostname. 
	 * @param file the file name. 
	 * 
	 * @return String Array of host names.
	 * 
	 * @throws Exception if the ssh process to cat the namenode hostname
	 *         file fails in a fatal manner.
	 */
	protected String[] getHostsFromList(String namenode, String file) 
			throws Exception {
		String[] output = TestSession.exec.runProcBuilder(
				new String[] {
				        "ssh",
	                    "-o", "StrictHostKeyChecking=no",
	                    "-o", "UserKnownHostsFile=/dev/null",
				        namenode,
				        "/bin/cat",
				        file});
		String[] nodes = output[1].replaceAll("\\s+", " ").trim().split(" ");
		TestSession.logger.trace("Hosts in file are: " + Arrays.toString(nodes));		
		return nodes;
	}

	/**
	 * Returns the Hadoop cluster hostnames hashtable.
	 * 
	 * @return Hashtable of String Arrays hostnames for each of the cluster
	 * components.
	 */
    public Hashtable<String, HadoopComponent> getComponents() {
        return this.hadoopComponents;
    }

	/**
	 * Returns the cluster nodes hostnames for the given component.
	 * 
	 * @param component The hadoop component such as gateway, namenode,
	 * resourcemaanger, etc.
	 * 
	 * @return String Arrays for the cluster nodes hostnames.
	 */
	public Hashtable<String, HadoopNode> getNodes(String component) {
	    return this.hadoopComponents.get(component).getNodes();
	}

    /**
     * Returns the first cluster node instance for the gateway component.
     * 
     * @return HadoopNode object.
     */
    public HadoopNode getNode() {
        return this.getNode(null);
    }    
    
    /**
     * Returns the first cluster node instance for the given component.
     * 
     * @param component The hadoop component such as gateway, namenode,
     * resourcemaanger, etc.
     * 
     * @return HadoopNode object.
     */
    public HadoopNode getNode(String component) {
        if ((component == null) || component.isEmpty()) {
            component = HadoopCluster.GATEWAY;
        }        
        HadoopNode node = this.getNodes(component).elements().nextElement();
        return node;
    }    
	
    /**
     * Returns the cluster nodes hostnames for the given component.
     * 
     * @param component The hadoop component such as gateway, namenode,
     * resourcemaanger, etc.
     * 
     * @return String Arrays for the cluster nodes hostnames.
     */
    public String[] getNodeNames(String component) {
        Hashtable<String, HadoopNode> nodes = this.getNodes(component);
        Set<String> keys = nodes.keySet();
        return keys.toArray(new String[0]);
    }
	    
	/**
	 * Start the cluster from a stopped state.
	 *
	 * @return boolean true for success and false for failure.
	 * 
	 * @throws Exception if there is a fatal error starting the cluster.
	 */
	public boolean start() 
			throws Exception {
		return start(true);
	}
	
	/**
	 * Restart the cluster.
	 * 
	 * @return boolean true for success and false for failure.
	 * 
	 * @throws Exception if there is a fatal error stopping or
	 *         starting the cluster.
	 */
	public boolean reset() 
			throws Exception {	
		boolean stopped = this.stop();
		if (!stopped) {
			TestSession.logger.error("cluster did not stop!!!");
		}
		boolean started = this.start();
		if (!started) {
			TestSession.logger.error("cluster did not start!!!");
		}
		return (stopped && started);
	}
	
	/**
	 * Get the Hadoop Cluster object containing the cluster info.
	 * 
	 * @return Cluster for the cluster instance.
	 * 
	 * @throws IOException if we can not get the Hadoop FS.
	 */
	public Cluster getClusterInfo() throws IOException {
		return new Cluster(TestSession.cluster.getConf());
	}

	/**
	 * Get the Hadoop Cluster object containing the cluster info.
	 * 
	 * @return Cluster for the cluster instance.
	 * 
	 * @throws IOException if we can not get the Hadoop FS.
	 */
	public YarnClientImpl getYarnClient() throws IOException {
		YarnClientImpl yarnClient = new YarnClientImpl();
		yarnClient.init(TestSession.getCluster().getConf());
		yarnClient.start();
		return yarnClient;
	}
	
	/**
	 * Get the cluster Hadoop file system.
	 * 
	 * @return FileSystem for the cluster instance.
	 * 
	 * @throws IOException if we can not get the Hadoop FS.
	 */
	public FileSystem getFS() throws IOException {
		return FileSystem.get(this.getConf());
	}

	/**
	 * Get the cluster Hadoop file system shell.
	 * 
	 * @return FS Shell for the cluster instance.
	 * 
	 * @throws IOException if we can not get the Hadoop FS shell.
	 */
	public FsShell getFsShell() throws IOException {
		return new FsShell(this.getConf());
	}

	/**
	 * Wait for the safemode on the namenode to be OFF. 
	 * 
	 * @return boolean true if safemode is OFF, or false if safemode is ON.
	 * 
	 * @throws Exception if there is a fatal error when waiting to turn
	 *         safe mode off.
	 */
	public boolean waitForSafemodeOff() 
			throws Exception {
		return waitForSafemodeOff(-1, null);
	}

	/**
	 * Wait for the safemode on the namenode to be OFF. 
	 *
	 * @param timeout time to wait for safe mode to be off.
	 * @param fs file system under test
	 * 
	 * @return boolean true if safemode is OFF, or false if safemode is ON.
	 * 
	 * @throws Exception if there is a fatal error when waiting to turn
	 *         safe mode off.
	 */
	public boolean waitForSafemodeOff(int timeout, String fs) 
			throws Exception {
		return waitForSafemodeOff(timeout, fs, false);
	}

	/**
	 * Wait for the safemode on the namenode to be OFF. 
	 *
	 * @param timeout time to wait for safe mode to be off.
	 * @param fs file system under test
	 * @param verbose true for on, false for off.
	 * 
	 * @return boolean true if safemode is OFF, or false if safemode is ON.
	 * 
	 * @throws Exception if there is a fatal error in the process to 
	 *         check safe mode state, or the thread is not able to sleep.
	 */
	public boolean waitForSafemodeOff(int timeout, String fs, boolean verbose) 
			throws Exception {

		if (timeout < 0) {
			int defaultTimeout = 300;
			timeout = defaultTimeout;
		}

		if ((fs == null) || fs.isEmpty()) {
			fs = this.getConf().get(
					"fs.defaultFS", HadoopConfiguration.HADOOP_CONF_CORE);
		}

        String namenode = this.getNodeNames(HadoopCluster.NAMENODE)[0];
		String[] safemodeGetCmd = { this.getConf().getHadoopProp("HDFS_BIN"),
				"--config", this.getConf().getHadoopConfDir(),
				"dfsadmin", "-fs", fs, "-safemode", "get" };

		String[] output = TestSession.exec.runProcBuilderSecurity(
		        safemodeGetCmd, verbose);

		boolean isSafemodeOff = 
				(output[1].trim().equals("Safe mode is OFF")) ? true : false;

		/* for the time out duration wait and see if the namenode comes out of
		 * safemode
		 */
		int waitTime=5;
		int i=1;
		while ((timeout > 0) && (!isSafemodeOff)) {
			TestSession.logger.info("Wait for safemode to be OFF: TRY #" + i +
			        ": WAIT " + waitTime + "s:" );
			Util.sleep(waitTime);

			output = TestSession.exec.runProcBuilderSecurity(safemodeGetCmd, verbose);

			isSafemodeOff = 
					(output[1].trim().contains("Safe mode is OFF")) ? true : false;
			timeout = timeout - waitTime;
			i++;
		}

		if (!isSafemodeOff) {
			TestSession.logger.info("ALERT: NAMENODE '" + namenode +
			        "' IS STILL IN SAFEMODE");
		}

		return isSafemodeOff;
	}

}
