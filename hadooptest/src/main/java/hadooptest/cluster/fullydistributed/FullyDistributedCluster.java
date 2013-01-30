package hadooptest.cluster.fullydistributed;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

import hadooptest.TestSession;
import hadooptest.cluster.Cluster;
import hadooptest.cluster.ClusterState;
import hadooptest.config.testconfig.FullyDistributedConfiguration;

public class FullyDistributedCluster implements Cluster {

	// The base fully distributed configuration.
	public FullyDistributedConfiguration conf;

	// The state of the fully distributed cluster.
	protected ClusterState clusterState;

    // The Hadoop version on the fully distributed cluster.
    protected String clusterVersion = "";
    
	private static TestSession TSM;
	
	private String HADOOP_INSTALL;
	private String CONFIG_BASE_DIR;
	private String CLUSTER_NAME;
	
	/*
	 * Class constructor.
	 * 
	 * Creates a brand new default PseudoDistributedConfiguration, and writes out the configuration to disk.
	 */
	public FullyDistributedCluster(TestSession testSession) throws IOException
	{
		TSM = testSession;
		this.initTestSessionConf();
		this.conf = new FullyDistributedConfiguration(testSession);		
		
		// stopCluster();
		// startCluster();

		// this.conf.write();
	}

	/*
	 * Class constructor.
	 * 
	 * Accepts a custom configuration, and assumed you will write it to disk.
	 */
	public FullyDistributedCluster(TestSession testSession,
			FullyDistributedConfiguration conf)
	{
		TSM = testSession;
		this.conf = conf;
		this.initTestSessionConf();
	}
	
	/*
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
	public void start() throws IOException {
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
	 * Stops all daemons associated with the pseudodistributed cluster instance, and
	 * verifies they have stopped with jps.
	 * 
	 * (non-Javadoc)
	 * @see hadooptest.cluster.Cluster#stop()
	 */
	public void stop() throws IOException {
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
	public void die() throws IOException {

	}

	/*
	 * (non-Javadoc)
	 * @see hadooptest.cluster.Cluster#reset()
	 */
	public void reset() {

	}

	/*
	 * Set a custom configuration for the pseudodistributed cluster instance.
	 * 
	 * @param conf The custom PseudoDistributedConfiguration
	 */
	public void setConf(FullyDistributedConfiguration conf) {
		this.conf = conf;
	}

	/*
	 * Gets the configuration for this pseudodistributed cluster instance.
	 * 
	 * @return PseudoDistributedConfiguration the configuration for the cluster instance.
	 */
	public FullyDistributedConfiguration getConf() {
		return this.conf;
	}
	
	/*
	 * Returns the state of the pseudodistributed cluster instance.
	 * 
	 * @return ClusterState the state of the cluster.
	 * 
	 * (non-Javadoc)
	 * @see hadooptest.cluster.Cluster#getState()
	 */
	/*
	public FullyDistributedConfiguration getConfig() {
		return this.conf;
	}
	*/

	/*
	 * Returns the state of the pseudodistributed cluster instance.
	 * 
	 * @return ClusterState the state of the cluster.
	 * 
	 * (non-Javadoc)
	 * @see hadooptest.cluster.Cluster#getState()
	 */
	public ClusterState getState() {
		return this.clusterState;
	}
	
	/*
	 * Returns the state of the pseudodistributed cluster instance.
	 * 
	 * @return ClusterState the state of the cluster.
	 * 
	 * (non-Javadoc)
	 * @see hadooptest.cluster.Cluster#getState()
	 */
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
	 * pseudo distributed cluster instance.
	 */
	private void initTestSessionConf() {
		HADOOP_INSTALL = TSM.conf.getProperty("HADOOP_INSTALL", "");
		CONFIG_BASE_DIR = TSM.conf.getProperty("CONFIG_BASE_DIR", "");
		CLUSTER_NAME = TSM.conf.getProperty("CLUSTER_NAME", "");
	}

	public void startCluster() {
		  TSM.logger.info("------------------ STOP CLUSTER $CLUSTER ---------------------------------");
		  // this.hadoopDaemon("start", "namenode", null, null);	  
		  //this.hadoopDaemon("start", "datanodes", null, null);
		  //this.hadoopDaemon("start", "resourcemanager", null, null);
		  //this.hadoopDaemon("stop", "nodemanager", null, null);

	}
		  
	public void stopCluster() {
	  TSM.logger.info("------------------ STOP CLUSTER $CLUSTER ---------------------------------");
	  //this.hadoopDaemon("stop", "nodemanager", null, null);
	  //this.hadoopDaemon("stop", "resourcemanager", null, null);
	  //this.hadoopDaemon("stop", "datanodes", null, null);
	  this.hadoopDaemon("stop", "namenode", null, null);	  
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
	

	public void hadoopDaemon(String action, String component, String hosts, String confDir) {
		String adminHost = this.conf.getClusterNodes("ADMIN_HOST")[0];
		String sudoer = getSudoer(component);
		String[] daemonHost = this.conf.getClusterNodes(component);	
		if (!action.equals("stop")) {
			if (confDir.isEmpty()) {
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
		String output[] = TSM.hadoop.runProcBuilder(cmd);
		TSM.logger.info(Arrays.toString(output));
		
		// When running as hadoopqa and using the yinst stop command to stop the
		// jobtracker instead of calling hadoop-daemon.sh directly, there can be a
		// delay before the job tracker is actually stopped. This is not ideal as it
		// poses potential timing issue. Should investigate why yinst stop is existing
		// before the job pid goes away.
	}
	

	
	/*
	 * Verifies, with jps, that a given process name is running.
	 * 
	 * @param process The String representing the name of the process to verify.
	 */
	private static boolean verifyJpsProcRunning(String process) {

		Process jpsProc = null;

		String jpsCmd = "jps";

		TSM.logger.debug(jpsCmd);

		String jpsPatternStr = "(.*)(" + process + ")(.*)";
		Pattern jpsPattern = Pattern.compile(jpsPatternStr);

		try {
			jpsProc = Runtime.getRuntime().exec(jpsCmd);
			BufferedReader reader=new BufferedReader(new InputStreamReader(jpsProc.getInputStream())); 
			String line=reader.readLine(); 
			while(line!=null) 
			{  
				TSM.logger.debug(line);

				Matcher jpsMatcher = jpsPattern.matcher(line);

				if (jpsMatcher.find()) {
					TSM.logger.debug("FOUND PROCESS: " + process);
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

		TSM.logger.debug("PROCESS IS NO LONGER RUNNING: " + process);
		return false;
	}
	
}
