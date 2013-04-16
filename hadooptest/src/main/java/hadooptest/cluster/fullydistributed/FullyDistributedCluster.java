package hadooptest.cluster.fullydistributed;

import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.cluster.TestCluster;
import hadooptest.cluster.ClusterState;
import hadooptest.config.testconfig.FullyDistributedConfiguration;
import hadooptest.config.TestConfiguration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;

/**
 * A Cluster subclass that implements a Fully Distributed Hadoop cluster.
 */
public class FullyDistributedCluster extends TestCluster {

	/** The base fully distributed configuration. */
	protected FullyDistributedConfiguration conf;

    /** The Hadoop version on the fully distributed cluster. */
    protected String clusterVersion = "";
	
    /** The name of the cluster */
	private String CLUSTER_NAME;
	
	/**
	 * Initializes the fully distributed cluster and sets up a new fully
	 * distributed configuration.
	 * 
	 * @throws Exception if the cluster configuration or cluster nodes
	 *         can not be initialized.
	 */
	public FullyDistributedCluster() 
			throws Exception {
		this.initTestSessionConf();
		this.conf = new FullyDistributedConfiguration();
		super.initNodes();
	}

	/**
	 * Initializes the fully distributed cluster and sets up the fully
	 * distributed configuration using a passed-in FullyDistributedConfiguration.
	 * 
	 * @param conf a configuration to initialize the cluster with.
	 * 
	 * @throws Exception if the cluster nodes can not be initialized.
	 */
	public FullyDistributedCluster(FullyDistributedConfiguration conf)
			throws Exception {
		this.conf = conf;
		this.initTestSessionConf();
		super.initNodes();
	}
	
    /**
     * Start the cluster.
     * 
	 * @param waitForSafemodeOff option to wait for safemode off after start.
	 * Default value is true. 
	 * 
     * @return boolean true for success, false for failure.
     * 
     * @throws Exception if a daemon node can not be started, if there
     *         if a fatal error checking the state of the cluster, or if
     *         there is a fatal error checking the dfs safe mode state.
     */
	public boolean start(boolean waitForSafemodeOff) 
			throws Exception {	
		TestSession.logger.info("------------------ START CLUSTER " + 
				conf.getHadoopProp("CLUSTER_NAME") + 
				" ---------------------------------");
		int returnValue = 0;
		String action = "start";		  
		String[] components = { 
				"namenode", 
				"datanode", 
				"resourcemanager",
				"nodemanager" };
		for (String component : components) {
			returnValue += this.hadoopDaemon(action, component);
		}

		if (returnValue > 0) {
			TestSession.logger.error("Stop Cluster returned error exit code!!!");
		}

		boolean foundNoErrors = (returnValue == 0) ? true : false;
		TestSession.logger.info("return value has error=" + foundNoErrors);
		
		boolean isFullyUp = this.isFullyUp();
		TestSession.logger.info("isFullyUp=" + isFullyUp);

		boolean isSafeModeOff =false;
		if (waitForSafemodeOff) {
			TestSession.logger.info("Wait for HDFS to get out of safe mode.");
			isSafeModeOff = this.waitForSafemodeOff();
			TestSession.logger.info("waitForSafemodeOff=" + isSafeModeOff);
		}
		
		return (waitForSafemodeOff) ?
				(foundNoErrors && isSafeModeOff && isFullyUp) :
				(foundNoErrors && isFullyUp);		
	}

    /**
     * Stop the cluster.
     * 
     * @return boolean true for success, false for failure.
     * 
     * @throws Exception if there is a fatal error stopping a daemon node.
     */	
	public boolean stop() 
			throws Exception {
		TestSession.logger.info("------------------ STOP CLUSTER " + 
				conf.getHadoopProp("CLUSTER_NAME") + 
				" ---------------------------------");
		int returnValue = 0;
		String action = "stop";

		String[] components = { 
				"nodemanager", 
				"resourcemanager", 
				"datanode",
				"namenode" };
		for (String component : components) {
			returnValue += this.hadoopDaemon(action, component);	
		}
		  
		if (returnValue > 0) {
			TestSession.logger.error("Stop Cluster returned error exit code!!!");
		}
		return (returnValue == 0) ? true : false;
	}

	/**
	 * Set a custom configuration for the fully distributed cluster instance.
	 * 
	 * @param conf The custom configuration to set.
	 */
	public void setConf(TestConfiguration conf) {
		this.conf = (FullyDistributedConfiguration)conf;
	}

	/**
	 * Gets the configuration for this fully distributed cluster instance.
	 * 
	 * @return FullyDistributedConfiguration the configuration for the cluster instance.
	 */
	public FullyDistributedConfiguration getConf() {
		return this.conf;
	}

	/**
	 * Returns the state of the fully distributed cluster instance.
	 * 
	 * @return ClusterState the state of the cluster.
	 * 
	 * @throws Exception if there is a fatal error checking cluster state.
	 * 
	 * (non-Javadoc)
	 * @see hadooptest.cluster.Cluster#getState()
	 */
	public ClusterState getState() 
			throws Exception {
		return (this.isFullyUp()) ? ClusterState.UP : ClusterState.DOWN;
	}
	
	/**
	 * Gets the name of the cluster instance.
	 * 
	 * @return String the name of the cluster
	 */
	public String getClusterName() {
		return CLUSTER_NAME;
	}
	
    /**
     * Returns the version of the fully distributed hadoop cluster being used.
     * 
     * @return String the Hadoop version for the fully distributed cluster.
     * 
     * (non-Javadoc)
     * @see hadooptest.cluster.Cluster#getVersion()
     */
    public String getVersion() {
    	return this.conf.getHadoopProp("HADOOP_VERSION");
    }

    /**
	 * Initialize the test session configuration properties necessary to use the 
	 * fully distributed cluster instance.
	 */
	private void initTestSessionConf() {
		CLUSTER_NAME = TestSession.conf.getProperty("CLUSTER_NAME", "");
	}

    /**
     * Get the sudoers for a given component.
     * 
     * @return String of the sudoer.
     */
	private String getSudoer(String component) {
		String sudoer = "";
	    if (component.equals("namenode")) {
	        sudoer = "hdfs";
	    } else if (component.equals("datanode")) {
	        sudoer = "root";
	    } else if ((component.equals("jobtracker")) || (component.equals("tasktracker"))) {
	        sudoer = "mapred";
	    } else if (component.equals("resourcemanager")) {
	        sudoer = "mapredqa";
	    }
		return sudoer;
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
	public int hadoopDaemon(String action, String component) 
			throws Exception {
		return hadoopDaemon(action, component, null, null);
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
	public int hadoopDaemon(String action, String component, String[] daemonHost) 
			throws Exception {
		return hadoopDaemon(action, component, daemonHost, null);
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
	public int hadoopDaemon(String action, String component, String[] daemonHost, String confDir) 
			throws Exception {
		if (daemonHost == null) {				
			daemonHost = this.getNodes(component);	
		}
		if (action.equals("start")) {
			if ((confDir == null) || confDir.isEmpty()) {
				/*
				 * Check if the configuration property for the component has
				 * been set. If so, use it otherwise use the default Hadoop
				 * configuration directory.
				 */
				Properties hadoopConfDir = this.conf.getHadoopConfDirPaths();
				confDir = hadoopConfDir.getProperty(component,
						this.conf.getHadoopProp("HADOOP_DEFAULT_CONF_DIR"));
			}
		}
		else {
			// Configuration directory is not needed for stopping the daemon.  
			confDir = "";			
		}
		
		String[] cmd1 = { "/home/y/bin/pdsh", "-w", StringUtils.join(daemonHost, ",") };
		String[] cmd2 = { "/usr/bin/sudo", "/usr/local/bin/yinst", "set", "-root",
				this.conf.getHadoopProp("HADOOP_INSTALL"),
				"hadoop_qa_restart_config.HADOOP_CONF_DIR="+confDir, ";" };
		String[] cmd3 = { "/usr/bin/sudo", "/usr/local/bin/yinst", action, "-root",
				this.conf.getHadoopProp("HADOOP_INSTALL"), component };
		ArrayList<String> temp = new ArrayList<String>();
		temp.addAll(Arrays.asList(cmd1));
		temp.addAll(Arrays.asList(cmd2));
		temp.addAll(Arrays.asList(cmd3));
		String [] cmd = temp.toArray(new String[cmd1.length+cmd2.length+cmd3.length]);
		String output[] = TestSession.exec.runProcBuilder(cmd);

		TestSession.logger.trace(Arrays.toString(output));
		
		int returnCode = Integer.parseInt(output[0]);
		if (returnCode != 0) {
			TestSession.logger.error("Operation '" + action + " " + component + "' failed!!!");
			return returnCode;
		}
		else {
			/* When running as hadoopqa and using the yinst stop command to stop the
			 * jobtracker instead of calling hadoop-daemon.sh directly, there can be a
			 * delay before the job tracker is actually stopped. This is not ideal as it
			 * poses potential timing issue. Should investigate why yinst stop is exiting
			 * before the job PID goes away.
			 */
			waitForComponentState(action, component);
					
			/* Reinitialize the hadooptest configuration object with the
			 * configuration directory if the action is start, and the
			 * configuration directory is not the default one (which cannot be
			 * modified due to root permission).
			 *   
			 * NOTE: this could still produce unnecessary re-initialization if
			 * the configurations are not changed. In the future, would be 
			 * better if we are able to determine this. 
			 */
			if (action.equals("start")) {
				Properties hadoopConfDir = this.conf.getHadoopConfDirPaths();
				confDir = hadoopConfDir.getProperty(component);
				if ((confDir != null) && !confDir.isEmpty()) {
					this.conf.loadResourceForComponent(confDir, component);
				}
			}
		}
		return returnCode;
	}
	
    /**
     * Wait for the component state for a given action and a given component.
     *
     * @param action The action correlating to the expected state on the Hadoop daemon
     * {"start", "stop"}
     * @param component The cluster component to perform the action on. 
     * 
     * @return boolean true for success or false for failure.
     * 
     * @throws Exception if there is a fatal error waiting for the component state.
     */
	public boolean waitForComponentState(String action, String component) 
			throws Exception {
		int waitInterval = 3;
		int maxWait = 10;
		return waitForComponentState(action, component, waitInterval, maxWait);
	}
	
    /**
     * Wait for the component state for a given action and a given component.
     *
     * @param action The action correlating to the expected state on the Hadoop daemon
     * {"start", "stop"}
     * @param component The cluster component to perform the action on. 
     * @param waitInterval the wait interval in seconds.
     * @param maxWait the maximum iteration to wait for
     * 
     * @return boolean true for success or false for failure.
     * 
     * @throws Exception if there is a fatal error waiting for the component state.
     */
	public boolean waitForComponentState(String action, String component,
			int waitInterval, int maxWait) 
					throws Exception {
		return waitForComponentState(action, component,
				waitInterval, maxWait, this.getNodes(component));
	}
		
    /**
     * Wait for the component state for a given action and a given component.
     *
     * @param action The action correlating to the expected state on the Hadoop daemon
     * {"start", "stop"}
     * @param component The cluster component to perform the action on. 
     * @param waitInterval the wait interval in seconds.
     * @param maxWait the maximum iteration to wait for
     * @param daemonHost String Array of daemon host names
     * 
     * @return boolean true for success or false for failure.
     * 
     * @throws Exception if there is a fatal error checking the component state.
     */
	public boolean waitForComponentState(String action, String component,
			int waitInterval, int maxWait, String[] daemonHost) 
					throws Exception {
		
	    String expStateStr = action.equals("start") ? "started" : "stopped";

		if (waitInterval == 0) {
			waitInterval = 3;
		}

		if (maxWait == 0) {
			maxWait = 10;
		}
		
		if (daemonHost == null) {
			daemonHost = this.getNodes(component);	
		}

		int count = 1;
	    while (count <= maxWait) {
	    	
	    	if (isComponentFullyInExpectedState(action, component, daemonHost)) {
	            TestSession.logger.debug("Daemon process for " + component + " is " +
	            		expStateStr + ".");
	            break;	    			    		
	    	}

	    	TestSession.logger.debug("Wait #" + count + " of " + maxWait + " for " +
	    			component + "daemon on " + Arrays.toString(daemonHost) + 
	    			" hosts to be " + expStateStr + " in " + waitInterval +
	    			"(s): total wait time = " + (count-1)*waitInterval +
	    			"(s): ");
	    		  

	    	Util.sleep(waitInterval);
	    		
	        count++;
	    }	
	    
	    if (count > maxWait) {
	    	TestSession.logger.warn("Wait time expired before daemon can be " +
	        		expStateStr + "." );
	        return false;
	    }
	    return true;
	}
	
    /**
     * Check if the cluster is fully up.
     * 
     * @return true if the cluster is fully up, false if the cluster is not
     * fully up.
     * 
     * @throws Exception if there is a fatal error checking if a component
     *         is fully up.
     */
	public boolean isFullyUp() 
			throws Exception {
		String[] components = {
	                       "namenode",
	                       "resourcemanager",
	                       "nodemanager",
	                       "datanode" };
		boolean overallStatus = true;
		boolean componentStatus = true;
		for (String component : components) {
			  componentStatus = this.isComponentFullyUp(component);
			  TestSession.logger.info("Get Cluster Status: " + component + " status is " +
					  ((componentStatus == true) ? "up" : "down"));
			  if (componentStatus == false) {
				  overallStatus = false;
			  }
		}
	    return overallStatus;
	}
	
    /**
     * Check if the cluster is fully down.
     * 
     * @return true if the cluster is fully up, false if the cluster is not
     * fully up.
     * 
     * @throws Exception if there is a fatal error checking if a component is fully down.
     */
	public boolean isFullyDown() 
			throws Exception {
		String[] components = {
	                       "namenode",
	                       "resourcemanager",
	                       "nodemanager",
	                       "datanode" };
		boolean overallStatus = true;
		boolean componentStatus = true;
		for (String component : components) {
			  componentStatus = this.isComponentFullyDown(component);
			  TestSession.logger.info("Get Cluster Status: " + component + " status is " +
					  ((componentStatus == true) ? "up" : "down"));
			  if (componentStatus == false) {
				  overallStatus = false;
			  }
		}
	    return overallStatus;
	}
	
    /**
     * Check if the cluster component is fully up.
     * 
     * @param component cluster component such as gateway, namenode,
     * 
     * @return true if the cluster is fully up, false if the cluster is not
     * fully up.
     * 
     * @throws Exception if there is a fatal error checking if a component is up.
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
     * @return boolean true if the cluster is fully up, false if the cluster is not
     * fully up.
     * 
     * @throws Exception if there is a fatal error checking the state of a component.
     */
	public boolean isComponentFullyUp(String component, String[] daemonHost) 
			throws Exception {
		return isComponentFullyInExpectedState("start", component, daemonHost);
	}
	
    /**
     * Check if the cluster component is up for a single given host name. 
     * 
     * @param component cluster component such as gateway, namenode,
     * @param daemonHost host name String of a single daemon host name,
     * 
     * @return boolean true if the cluster is fully up, false if the cluster is not
     * fully up.
     * 
     * @throws Exception if there is a fatal error checking if a component is up.
     */
	public boolean isComponentUpOnSingleHost(String component,
			String daemonHost) 
					throws Exception {
		return isComponentFullyUp(component, new String[] {daemonHost});		
	}

    /**
     * Check if the cluster component is fully down.
     * 
     * @param component cluster component such as gateway, namenode,
     * 
     * @return boolean true if the cluster is fully down, false if the cluster is not
     * fully down.
     * 
     * @throws Exception if there is a failure checking if a component is down.
     */
	public boolean isComponentFullyDown(String component) 
			throws Exception {
		return isComponentFullyDown(component, null);
	}	

    /**
     * Check if the cluster component is fully down for a given String Array of
     * host names. 
     * 
     * @param component cluster component such as gateway, namenode,
     * @param daemonHost host names String Array of daemon host names,
     * 
     * @return boolean true if the cluster is fully down, false if the cluster is not
     * fully down.
     * 
     * @throws Exception if there is a failure checking if a component is down.
     */
	public boolean isComponentFullyDown(String component, String[] daemonHost) 
			throws Exception {		
		return isComponentFullyInExpectedState("stop", component, daemonHost);
	}
		
    /**
     * Check if the cluster component is fully in a specified state associated
     * with start or stop.
     * 
     * @param action the action associated with the expected state {"start", "stop"}
     * @param component cluster component such as gateway, namenode,
     * 
     * @return boolean true if the cluster is fully in the expected state, false if the
     * cluster is not fully in the expected state.
     * 
     * @throws Exception if there is a failure checking if a component is in the expected state.
     */
	public boolean isComponentFullyInExpectedState(String action,
			String component) 
					throws Exception {
		return isComponentFullyInExpectedState(action, component, null);
	}
	
    /**
     * Check if the cluster component is fully in a specified state associated
     * with start or stop.
     * 
     * @param action the action associated with the expected state {"start", "stop"}
     * @param component cluster component such as gateway, namenode,
     * @param daemonHosts host names String Array of daemon host names,
     * 
     * @return boolean true if the cluster is fully in the expected state, false if the
     * cluster is not fully in the expected state.
     * 
     * @throws Exception if there is a failure checking if a component is in the expected state.
     */
	public boolean isComponentFullyInExpectedState(String action,
			String component, String[] daemonHosts) 
					throws Exception {
		String adminHost = this.getNodes("admin")[0];
		if (daemonHosts == null) {
			daemonHosts = this.getNodes(component);	
		}
		TestSession.logger.trace("Daemon hosts for " + component + ": " +
				Arrays.toString(daemonHosts));		
		
		// Get the number of running process(es) for a given component
		String prog = (component.equals("datanode")) ? "jsvc.exec" : "java";		    
		String[] cmd = {"ssh", adminHost, "pdsh", "-w",
				StringUtils.join(daemonHosts, ","), 
				"ps auxww", "|", "grep \"" + prog + " -Dproc_" + component +
		        "\"", "|", "/usr/bin/cut", "-d':'", "-f1" };		        
		String output[] = TestSession.exec.runProcBuilder(cmd);

		String[] daemonProcesses = StringUtils.split(output[1].trim(), "\n");
		TestSession.logger.trace("Daemon processes found = " +
				Arrays.toString(daemonProcesses) + ", length=" +
				daemonProcesses.length);
		String domain = daemonHosts[0].substring(daemonHosts[0].indexOf("."));		

		// Get the number of processes reported per host
		HashMap<String, Integer> processCounter = new HashMap<String, Integer>();
		
        for (String daemonHost : daemonProcesses) {
        	daemonHost = daemonHost + domain;
        	TestSession.logger.trace("update counter for: " + daemonHost);
            if (!processCounter.containsKey(daemonHost)) {
                processCounter.put(daemonHost, 1);
            } else {
                processCounter.put(daemonHost, processCounter.get(daemonHost)+1);
            }
        }		

        // Figure out expected versus actual number of processes
		String expectedState = (action.equals("start")) ? "up" : "down";
		int numCapableDaemonProcessesPerHost = (component.equals("datanode")) ?
				2 : 1;
		int numExpectedDaemonProcessesPerHost = (action.equals("start")) ?
				numCapableDaemonProcessesPerHost : 0;
	    for(String host : daemonHosts) {
	    	// Check for the expected number of processes for each host
	    	TestSession.logger.trace("Check against process counter for host " + host);
	    	
	        int numActualDaemonProcessesPerHost = (processCounter.containsKey(host)) ? processCounter.get(host) : 0;	        				
			TestSession.logger.trace(
					"actual daemon process per host=" + numActualDaemonProcessesPerHost +
					", expected daemon process per host=" +  numExpectedDaemonProcessesPerHost);
	        if (numActualDaemonProcessesPerHost != numExpectedDaemonProcessesPerHost) {
	        	String log = "/home/gs/var/log/hdfsqa/hadoop-hdfsqa-datanode-"+host+".log";
	            TestSession.logger.debug("Daemon on host " + host +
	            		" is not in expected state of '" + expectedState + "'.");
	            TestSession.logger.debug("See log: " + host + ":"+ log);
	    	}
	    }

		int numCapableDaemonProcesses = numCapableDaemonProcessesPerHost * daemonHosts.length;
		int numExpectedDaemonProcesses = (action.equals("start")) ? numCapableDaemonProcesses : 0;		
		int numActualDaemonProcesses = daemonProcesses.length;
		TestSession.logger.trace(
				"actual daemon process=" + numActualDaemonProcesses +
				", expected daemon process=" +  numExpectedDaemonProcesses);
		boolean isComponentFullyInExpectedState =
				(numActualDaemonProcesses == numExpectedDaemonProcesses) ? true : false;
		
		if (action.equals("start")) {
			TestSession.logger.debug("Number of " + component +
					" process up: " + numActualDaemonProcesses + "/" +
					numCapableDaemonProcesses);
		}
		else {
			TestSession.logger.debug("Number of " + component +
					" process down: " +
					(numCapableDaemonProcesses-numActualDaemonProcesses) + "/" +
					numCapableDaemonProcesses);
		}
		return isComponentFullyInExpectedState;
	}	
}
