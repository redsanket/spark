/*
 * YAHOO!
 */

package hadooptest.cluster.hadoop;

import java.io.IOException;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Hashtable;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.yarn.client.YarnClientImpl;

import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.cluster.ClusterState;
import hadooptest.config.hadoop.HadoopConfiguration;

/**
 * An interface which should represent the base capability of any cluster
 * type in the framework.  This interface is also what should be commonly
 * used to reference the common cluster instance maintained by the
 * TestSession.  Subclasses of Cluster should implment a specific cluster
 * type.
 */
public abstract class HadoopCluster {

	/** Contains the nodes on the cluster */
	private Hashtable<String, String[]> nodes = new Hashtable<String, String[]>();

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
	 * Get the current state of the cluster.
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
	
	/**
	 * Initialize the cluster nodes hostnames for the namenode,
	 * resource manager, datanode, and nodemanager. 
	 */
	public void initNonFDCNodes() {
		String host = "localhost";        	      	  
		String[] components = { 
				"namenode", 
				"datanode", 
				"resourcemanager",
		"nodemanager" };
		for (String component : components) {
			nodes.put(component, new String[] {host});
		}
		return;
	}

	/**
	 * Initializes FDC nodes.
	 * 
	 * @throws Exception if the datanode can not be initialized.
	 */
	public void initFDCNodes() 
			throws Exception {
		nodes.put("admin", new String[] {
				"adm102.blue.ygrid.yahoo.com",
		"adm103.blue.ygrid.yahoo.com"});

		// Namenode
		String namenode_addr = this.getConf().get("dfs.namenode.https-address");
		String namenode = namenode_addr.split(":")[0];
		nodes.put("namenode", new String[] {namenode});		

		// Resource Manager
		String rm_addr =
				this.getConf().get("yarn.resourcemanager.resource-tracker.address");
		String rm = rm_addr.split(":")[0];
		nodes.put("resourcemanager", new String[] {rm});		

		// Datanode
		nodes.put("datanode", this.getHostsFromList(namenode,
				this.getConf().getHadoopConfDir() + "/slaves"));		

		// Nodemanager
		nodes.put("nodemanager", nodes.get("datanode"));		
	}

	/**
	 * Initialize the cluster nodes hostnames for the namenode,
	 * resource manager, datanode, and nodemanager. 
	 * 
	 * @throws Exception if the datanode can not be initialized.
	 */
	public void initNodes() 
			throws Exception {
		// Retrieve the cluster type from the framework configuration file.
		// This should be in the format of package.package.class
		String strClusterType = TestSession.conf.getProperty("CLUSTER_TYPE");

		// Initialize the nodes with the correct cluster type.
		if (!strClusterType.equals("hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster")) {
			initNonFDCNodes();
		}
		else {
			initFDCNodes();
		}

		// Show all balances in hash table. 
		TestSession.logger.debug("-- listing cluster nodes --");
		Enumeration<String> components = nodes.keys(); 
		while (components.hasMoreElements()) { 
			String component = (String) components.nextElement(); 
			TestSession.logger.debug(component + ": " +
			        Arrays.toString(nodes.get(component))); 
		} 	
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
	private String[] getHostsFromList(String namenode, String file) 
			throws Exception {
		String[] output = TestSession.exec.runProcBuilder(
				new String[] {"ssh", namenode, "/bin/cat", file});
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
	public Hashtable<String, String[]> getNodes() {
		return nodes;
	}

	/**
	 * Returns the cluster nodes hostnames for the given component.
	 * 
	 * @param component The hadoop component such as gateway, namenode,
	 * resourcemaanger, etc.
	 * 
	 * @return String Arrays for the cluster nodes hostnames.
	 */
	public String[] getNodes(String component) {
		return nodes.get(component);
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
		boolean started = this.start();
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

		String namenode = nodes.get("namenode")[0];
		String[] safemodeGetCmd = { this.getConf().getHadoopProp("HDFS_BIN"),
				"--config", this.getConf().getHadoopConfDir(),
				"dfsadmin", "-fs", fs, "-safemode", "get" };

		String[] output =
		        TestSession.exec.runHadoopProcBuilder(safemodeGetCmd, verbose);
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
			output = TestSession.exec.runHadoopProcBuilder(safemodeGetCmd,
			        verbose);
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
