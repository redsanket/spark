package hadooptest.cluster.fullydistributed;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import hadooptest.TestSession;
import hadooptest.cluster.Cluster;
import hadooptest.cluster.Executor;
import hadooptest.cluster.ClusterState;
import hadooptest.config.testconfig.FullyDistributedConfiguration;
import hadooptest.config.TestConfiguration;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.lang.StringUtils;

public class FullyDistributedCluster implements Cluster {

	// The base fully distributed configuration.
	protected FullyDistributedConfiguration conf;

	// The state of the fully distributed cluster.
	protected ClusterState clusterState;

    // The Hadoop version on the fully distributed cluster.
    protected String clusterVersion = "";
	
	private String HADOOP_INSTALL;
	private String CONFIG_BASE_DIR;
	private String CLUSTER_NAME;
	
	/*
	 * Class constructor.
	 * 
	 * Creates a brand new default FullyDistributedConfiguration, and writes out the configuration to disk.
	 */
	public FullyDistributedCluster() throws IOException
	{
		this.initTestSessionConf();
		this.conf = new FullyDistributedConfiguration();
		
		// stopCluster();
		// startCluster();
		
		// this.conf.write();
	}

	/*
	 * Class constructor.
	 * 
	 * Accepts a custom configuration, and assumed you will write it to disk.
	 */
	public FullyDistributedCluster(FullyDistributedConfiguration conf)
	{
		this.conf = conf;
		this.initTestSessionConf();
	}
	
	/*
	 * Starts the fully distributed cluster instance by starting:
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
	public void start() {
/*
		//String format_dfs = HADOOP_INSTALL + "/bin/hadoop --config " + CONFIG_BASE_DIR + " namenode -format";
		String start_dfs = HADOOP_INSTALL + "/sbin/start-dfs.sh --config " + CONFIG_BASE_DIR;
		String start_yarn = HADOOP_INSTALL + "/sbin/start-yarn.sh --config " + CONFIG_BASE_DIR;
		String start_historyserver = HADOOP_INSTALL + "/sbin/mr-jobhistory-daemon.sh start historyserver --config " + CONFIG_BASE_DIR;
		String start_datanode = HADOOP_INSTALL + "/sbin/hadoop-daemon.sh --config " + CONFIG_BASE_DIR + " start datanode";

		//TSM.logger.info("FORMATTING DFS...");
		//runProc(format_dfs);
		
		TSM.logger.info("STARTING DFS...");
		runProc(start_dfs);
		assertTrue("The NameNode was not started.", verifyJpsProcRunning("NameNode"));
		assertTrue("The SecondaryNameNode was not started.", verifyJpsProcRunning("SecondaryNameNode"));

		TSM.logger.info("STARTING DATANODE...");
		runProc(start_datanode);
		assertTrue("The DataNode was not started.", verifyJpsProcRunning("DataNode"));
		
		TSM.logger.info("STARTING YARN");
		runProc(start_yarn);
		assertTrue("The ResourceManager was not started.", verifyJpsProcRunning("ResourceManager"));

		TSM.logger.info("STARTING JOB HISTORY SERVER...");
		runProc(start_historyserver);
		assertTrue("The JobHistoryServer was not started.", verifyJpsProcRunning("JobHistoryServer"));
		
		TSM.logger.info("Sleeping for 30s to wait for HDFS to get out of safe mode.");
		Util.sleep(30);
*/
	}

	/* 
	 * Stops all daemons associated with the fully distributed cluster instance, and
	 * verifies they have stopped with jps.
	 * 
	 * (non-Javadoc)
	 * @see hadooptest.cluster.Cluster#stop()
	 */
	public void stop() {
/*
		String stop_dfs = HADOOP_INSTALL + "/sbin/stop-dfs.sh";
		String stop_yarn = HADOOP_INSTALL + "/sbin/stop-yarn.sh";
		String stop_historyserver = HADOOP_INSTALL + "/sbin/mr-jobhistory-daemon.sh stop historyserver";
		String stop_datanode = HADOOP_INSTALL + "/sbin/hadoop-daemon.sh stop datanode";

		runProc(stop_dfs);
		runProc(stop_yarn);
		runProc(stop_historyserver);
		runProc(stop_datanode);

		// Wait for 10 seconds to ensure that the daemons have had time to stop.
		Util.sleep(10);

		assertFalse("The NameNode was not stopped.", verifyJpsProcRunning("NameNode"));
		assertFalse("The SecondaryNameNode was not stopped.", verifyJpsProcRunning("SecondaryNameNode"));
		assertFalse("The DataNode was not stopped.", verifyJpsProcRunning("DataNode"));
		assertFalse("The ResourceManager was not stopped.", verifyJpsProcRunning("ResourceManager"));
		assertFalse("The JobHistoryServer was not stopped.", verifyJpsProcRunning("JobHistoryServer"));
*/
	}

	/*
	 * (non-Javadoc)
	 * @see hadooptest.cluster.Cluster#die()
	 */
	public void die() {

	}

	/*
	 * (non-Javadoc)
	 * @see hadooptest.cluster.Cluster#reset()
	 */
	public void reset() {
		
		restartCluster();

	}

	/*
	 * Set a custom configuration for the fully distributed cluster instance.
	 * 
	 * @param conf The custom FullyDistributedConfiguration
	 */
	public void setConf(TestConfiguration conf) {
		this.conf = (FullyDistributedConfiguration)conf;
	}

	/*
	 * Gets the configuration for this fully distributed cluster instance.
	 * 
	 * @return FullyDistributedConfiguration the configuration for the cluster instance.
	 */
	public FullyDistributedConfiguration getConf() {
		return this.conf;
	}

	/*
	 * Returns the state of the fully distributed cluster instance.
	 * 
	 * @return ClusterState the state of the cluster.
	 * 
	 * (non-Javadoc)
	 * @see hadooptest.cluster.Cluster#getState()
	 */
	public ClusterState getState() {
		return this.clusterState;
	}
	
	public String getClusterName() {
		return CLUSTER_NAME;
	}
	
    /*
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

    /*
	 * Initialize the test session configuration properties necessary to use the 
	 * fully distributed cluster instance.
	 */
	private void initTestSessionConf() {
		HADOOP_INSTALL = TestSession.conf.getProperty("HADOOP_INSTALL", "");
		CONFIG_BASE_DIR = TestSession.conf.getProperty("CONFIG_BASE_DIR", "");
		CLUSTER_NAME = TestSession.conf.getProperty("CLUSTER_NAME", "");
	}

	public int restartCluster() {
		int returnValue = 0;
		returnValue += stopCluster();
		returnValue += startCluster();
		return returnValue;
	}
	
	public int startCluster() {
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
			  if (returnValue == 0 ) {
				  waitForComponentState(action, component);
			  }
		  }
		  if (returnValue > 0) {
			  TestSession.logger.error("Stop Cluster returned error exit code!!!");
		  }
		  return returnValue;
	}
		  
	public int stopCluster() {
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
		  if (returnValue == 0 ) {
			  waitForComponentState(action, component);
		  }
	  }
	  
	  if (returnValue > 0) {
		  TestSession.logger.error("Stop Cluster returned error exit code!!!");
	  }
	  return returnValue;
	}

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
	
	public int hadoopDaemon(String action, String component) {
		return hadoopDaemon(action, component, null, null);
	}
	
	public int hadoopDaemon(String action, String component, String hosts, String confDir) {
		String[] daemonHost = this.conf.getClusterNodes(component);	
		if (!action.equals("stop")) {
			if ((confDir == null) || confDir.isEmpty()) {
				confDir = this.conf.getHadoopProp("HADOOP_CONF_DIR");
			}
		}
		else {
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
		TestSession.logger.info(Arrays.toString(output));
		
		// When running as hadoopqa and using the yinst stop command to stop the
		// jobtracker instead of calling hadoop-daemon.sh directly, there can be a
		// delay before the job tracker is actually stopped. This is not ideal as it
		// poses potential timing issue. Should investigate why yinst stop is existing
		// before the job pid goes away.

		// $return += $self->wait_for_daemon_state($operation, $component, $daemon_hosts);
		
		
		int returnCode = Integer.parseInt(output[0]);
		if (returnCode != 0) {
			TestSession.logger.error("Operation '" + action + " " + component + "' failed!!!");
		}
		return returnCode;
	}
	
	public boolean waitForComponentState(String action, String component) {
		int waitInterval = 3;
		int maxWait = 10;
		String[] daemonHost = this.conf.getClusterNodes(component); 
		return waitForComponentState(action, component,
				daemonHost, waitInterval, maxWait);
	}
	
	public boolean waitForComponentState(String action, String component,
			int waitInterval, int maxWait) {
		return waitForComponentState(action, component,
				this.conf.getClusterNodes(component), waitInterval, maxWait);
	}
	
	public boolean waitForComponentState(String action, String component,
			String[] daemonHost, int waitInterval, int maxWait) {
	    boolean expectedToBeUp = action.equals("start") ? true : false;
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
	    	if (isComponentUp(component) == expectedToBeUp) {
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
	
	public boolean getClusterStatus() {
		String[] components = {
	                       "namenode",
	                       "resourcemanager",
	                       "nodemanager",
	                       "datanode" };
		boolean overallStatus = true;
		boolean componentStatus = true;
		for (String component : components) {
			  componentStatus = this.isComponentUp(component);
			  // $status->{uc($component)} = $hosts_status;
			  TestSession.logger.info("Get Cluster Status: " + component + " status is " +
					  ((componentStatus == true) ? "up" : "down"));
			  if (componentStatus == false) {
				  overallStatus = false;
			  }
		}
	    return overallStatus;
	}
	
	public boolean isComponentUp(String component) {
		return isComponentUp(component, null);
	}
	
	public boolean isComponentUp(String component, String[] daemonHost) {
		String adminHost = this.conf.getClusterNodes("ADMIN_HOST")[0];
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
		
		// For data node, there should be two jobs per host.
		// One is started by root, and the other by hdfs.	
		int numExpectedProcessPerHost = (component.equals("datanode")) ? 2 : 1;
		TestSession.logger.debug("Daemon hosts for " + component + ": " + Arrays.toString(daemonHost));
		int numExpectedProcess = numExpectedProcessPerHost * daemonHost.length;
		int numActualProcess = Integer.parseInt(output[1].replace("\n",""));
		TestSession.logger.debug("Number of expected process: " + numExpectedProcess +
				", Number of actual process: " + numActualProcess);
		return (numActualProcess == numExpectedProcess) ? true : false;
	}
	
	public boolean isComponentUpOnSingleHost(String component) {
		return this.isComponentUpOnSingleHost(component, null);
	}
	
	public boolean isComponentUpOnSingleHost(String component,
			String daemonHost) {
		return isComponentUp(component, new String[] {daemonHost});		
	}
	
	public boolean waitForSafemodeOff() {
		int timeout = 300;
		String fs = this.conf.getHadoopConfFileProp(
				"HADOOP_CONF_CORE", "fs.DefaultFS");
		return waitForSafemodeOff(timeout, fs);
	}
		
	public boolean waitForSafemodeOff(int timeout, String fs) {
	    if (timeout == 0) {
	    	timeout = 300;
	    }
		if ((fs == null) || fs.isEmpty()) {
	        fs = this.conf.getHadoopConfFileProp(
	        		"HADOOP_CONF_CORE", "fs.defaultFS");
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
	    	TestSession.logger.debug("TRY #" + i);	        
	    	TestSession.logger.info("WAIT " + waitTime + "s:" );
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
	

	/*
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
