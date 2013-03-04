package hadooptest.cluster.fullydistributed;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import hadooptest.TestSession;
import hadooptest.cluster.Cluster;
import hadooptest.cluster.ClusterState;
import hadooptest.config.testconfig.FullyDistributedConfiguration;
import hadooptest.config.TestConfiguration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;

/**
 * A Cluster subclass that implements a Fully Distributed Hadoop cluster.
 */
public class FullyDistributedCluster implements Cluster {

	/** The base fully distributed configuration. */
	protected FullyDistributedConfiguration conf;

    /** The Hadoop version on the fully distributed cluster. */
    protected String clusterVersion = "";
	
    /** The name of the cluster */
	private String CLUSTER_NAME;
	
	/**
	 * Initializes the fully distributed cluster and sets up a new fully
	 * distributed configuration.
	 */
	public FullyDistributedCluster() throws IOException
	{
		this.initTestSessionConf();
		this.conf = new FullyDistributedConfiguration();
	}

	/**
	 * Initializes the fully distributed cluster and sets up the fully
	 * distributed configuration using a passed-in FullyDistributedConfiguration.
	 * 
	 * @param conf a configuration to initialize the cluster with.
	 */
	public FullyDistributedCluster(FullyDistributedConfiguration conf)
	{
		this.conf = conf;
		this.initTestSessionConf();
	}
	
    /**
     * Start the cluster.
     * 
     * @return boolean true for success, false for failure.
     */
	public boolean start() {
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
		return (returnValue == 0) ? true : false;
	}

    /**
     * Stop the cluster.
     * 
     * @return boolean true for success, false for failure.
     */	
	public boolean stop() {
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
	 * Currently unimplemented for FullyDistributedCluster.
	 */
	public void die() {

	}

	/**
	 * Restarts the fully distributed cluster.
	 * 
	 * @return boolean true for success and false for failure.
	 */
	public boolean reset() {
		
		boolean stopped = this.stop();
		boolean started = this.start();
		return (stopped && started);
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
	 * Gets the FileSystem object for this cluster instance.
	 * 
	 * @return FileSystem for the cluster instance.
	 */
	public FileSystem getFS() throws IOException {
		return FileSystem.get(this.conf);
	}
	
	/**
	 * Returns the state of the fully distributed cluster instance.
	 * 
	 * @return ClusterState the state of the cluster.
	 * 
	 * (non-Javadoc)
	 * @see hadooptest.cluster.Cluster#getState()
	 */
	public ClusterState getState() {
		return (this.isClusterFullyUp()) ? ClusterState.UP : ClusterState.DOWN;
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
     */
	public int hadoopDaemon(String action, String component) {
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
     */
	public int hadoopDaemon(String action, String component, String[] daemonHost) {
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
     */
	public int hadoopDaemon(String action, String component, String[] daemonHost, String confDir) {
		if (daemonHost == null) {				
			daemonHost = this.conf.getClusterNodes(component);	
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
					this.conf.initComponentConfFiles(confDir, component);
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
     */
	public boolean waitForComponentState(String action, String component) {
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
     */
	public boolean waitForComponentState(String action, String component,
			int waitInterval, int maxWait) {
		return waitForComponentState(action, component,
				waitInterval, maxWait, this.conf.getClusterNodes(component));
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
     */
	public boolean waitForComponentState(String action, String component,
			int waitInterval, int maxWait, String[] daemonHost) {
		
	    String expStateStr = action.equals("start") ? "started" : "stopped";

		if (waitInterval == 0) {
			waitInterval = 3;
		}

		if (maxWait == 0) {
			maxWait = 10;
		}
		
		if (daemonHost == null) {
			daemonHost = this.conf.getClusterNodes(component);	
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
	    		  
	    	try {
	    		Thread.sleep(waitInterval*1000);
	    	} catch  (InterruptedException e) {
	    		TestSession.logger.error("Encountered Interrupted Exception: " +
	    				e.toString());
	    	}
	        count++;
	    }	
	    
	    if (count > maxWait) {
	    	TestSession.logger.warn("Wait time expired before daemon can be " +
	        		expStateStr + ":" );
	        return false;
	    }
	    return true;
	}
	
    /**
     * Check if the cluster is fully up.
     * 
     * @return true if the cluster is fully up, false if the cluster is not
     * fully up.
     */
	public boolean isClusterFullyUp() {
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
     */
	public boolean isClusterFullyDown() {
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
     */
	public boolean isComponentFullyUp(String component) {
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
     */
	public boolean isComponentFullyUp(String component, String[] daemonHost) {
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
     */
	public boolean isComponentUpOnSingleHost(String component,
			String daemonHost) {
		return isComponentFullyUp(component, new String[] {daemonHost});		
	}

    /**
     * Check if the cluster component is fully down.
     * 
     * @param component cluster component such as gateway, namenode,
     * 
     * @return boolean true if the cluster is fully down, false if the cluster is not
     * fully down.
     */
	public boolean isComponentFullyDown(String component) {
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
     */
	public boolean isComponentFullyDown(String component, String[] daemonHost) {		
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
     */
	public boolean isComponentFullyInExpectedState(String action,
			String component) {
		return isComponentFullyInExpectedState(action, component, null);
	}
	
    /**
     * Check if the cluster component is fully in a specified state associated
     * with start or stop.
     * 
     * @param action the action associated with the expected state {"start", "stop"}
     * @param component cluster component such as gateway, namenode,
     * @param daemonHost host names String Array of daemon host names,
     * 
     * @return boolean true if the cluster is fully in the expected state, false if the
     * cluster is not fully in the expected state.
     */
	public boolean isComponentFullyInExpectedState(String action,
			String component, String[] daemonHost) {
		String adminHost = this.conf.getClusterNodes("admin")[0];
		if (daemonHost == null) {
			daemonHost = this.conf.getClusterNodes(component);	
		}

		// Get the number of running process(es) for a given component
		String prog = (component.equals("datanode")) ? "jsvc.exec" : "java";		    
		String[] cmd = {"ssh", adminHost, "pdsh", "-w",
				StringUtils.join(daemonHost, ","), 
				"ps auxww", "|", "grep \"" + prog + " -Dproc_" + component +
		        "\"", "|","grep -v grep -c" };		        
		String output[] = TestSession.exec.runProcBuilder(cmd);
		
		int numExpectedProcessPerHost = (component.equals("datanode")) ? 2 : 1;
		TestSession.logger.trace("Daemon hosts for " + component + ": " +
				Arrays.toString(daemonHost));
		int numDaemonProcess = numExpectedProcessPerHost * daemonHost.length;

		int numExpectedProcess =
				(action.equals("start")) ? numDaemonProcess : 0;

		int numActualProcess = Integer.parseInt(output[1].replace("\n",""));
		
		boolean isComponentFullyInExpectedState =
				(numActualProcess == numExpectedProcess) ? true : false;
			if (action.equals("start")) {
				TestSession.logger.debug("Number of " + component +
						" process up: " + numActualProcess + "/" +
						numDaemonProcess);
			}
			else {
				TestSession.logger.debug("Number of " + component +
						" process down: " +
						(numDaemonProcess-numActualProcess) + "/" +
						numDaemonProcess);
			}
		return isComponentFullyInExpectedState;
	}
	
    /**
     * Wait for the safemode on the namenode to be OFF. 
     * 
     * @return boolean true if safemode is OFF, or false if safemode is ON.
     */
	public boolean waitForSafemodeOff() {
		return waitForSafemodeOff(-1, null);
	}
		
    /**
     * Wait for the safemode on the namenode to be OFF. 
     *
     * @param timeout time to wait for safe mode to be off.
     * @param fs file system under test
     * 
     * @return boolean true if safemode is OFF, or false if safemode is ON.
     */
	public boolean waitForSafemodeOff(int timeout, String fs) {

		if (timeout < 0) {
			int defaultTimeout = 300;
	    	timeout = defaultTimeout;
	    }

	    if ((fs == null) || fs.isEmpty()) {
	        fs = this.conf.getHadoopConfFileProp(
	        		"fs.defaultFS",
	        		FullyDistributedConfiguration.HADOOP_CONF_CORE);
		}

		String namenode = this.conf.getClusterNodes("namenode")[0];	
		String[] safemodeGetCmd = { this.conf.getHadoopProp("HDFS_BIN"),
				"--config", this.conf.getHadoopProp("HADOOP_CONF_DIR"),
				"dfsadmin", "-fs", fs, "-safemode", "get" };
		String[] output = TestSession.exec.runHadoopProcBuilder(safemodeGetCmd);
		boolean isSafemodeOff = 
				(output[1].trim().equals("Safe mode is OFF")) ? true : false;
		 
	    // for the time out duration wait and see if the namenode comes out of safemode
	    int waitTime=30;
	    int i=1;
	    while ((timeout > 0) && (!isSafemodeOff)) {
	    	TestSession.logger.info("Wait for safemode to be OFF: TRY #" + i + ": WAIT " + waitTime + "s:" );
	    	try {
	    		Thread.sleep(waitTime*1000);
	    	} catch  (InterruptedException e) {
	    		TestSession.logger.error("Encountered Interrupted Exception: " +
	    				e.toString());
	    	}
	    	output = TestSession.exec.runHadoopProcBuilder(safemodeGetCmd);
	    	isSafemodeOff = 
	    			(output[1].trim().equals("Safe mode is OFF")) ? true : false;
	        timeout = timeout - waitTime;
	        i++;
	    }
	    
	    if (!isSafemodeOff) {
	    	TestSession.logger.info("ALERT: NAMENODE " + namenode + " IS STILL IN SAFEMODE");
	    }
	    
	    return isSafemodeOff;
	}
	

	/**
	 * Verifies, with jps, that a given process name is running.
	 * 
	 * @param process The String representing the name of the process to verify.
	 * 
	 * @return boolean whether the java process is running or not.
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
