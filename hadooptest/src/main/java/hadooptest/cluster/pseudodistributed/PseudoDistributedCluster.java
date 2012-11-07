/*
 * YAHOO!
 */

package hadooptest.cluster.pseudodistributed;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import hadooptest.cluster.Cluster;
import hadooptest.cluster.ClusterState;
import hadooptest.config.testconfig.PseudoDistributedConfiguration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/* 
 * A class that is the base representation of any pseudodistributed cluster.
 */
public class PseudoDistributedCluster implements Cluster {

	// The base pseudodistributed configuration.
	protected PseudoDistributedConfiguration conf;
	
	// The state of the pseudodistributed cluster.
	protected ClusterState cluster_state;

	private final String HADOOP_INSTALL = "/Users/rbernota/workspace/eclipse/branch-0.23.4/hadoop-dist/target/hadoop-0.23.4";
	private final String CONFIG_BASE_DIR = "/Users/rbernota/workspace/hadoop/test/pseudodistributed_configs/test/";
	
	/*
	 * Class constructor.
	 * 
	 * Creates a brand new default PseudoDistributedConfiguration, and writes out the configuration to disk.
	 */
	public PseudoDistributedCluster() throws IOException
	{
		this.conf = new PseudoDistributedConfiguration();
		this.conf.write();
	}

	/*
	 * Class constructor.
	 * 
	 * Accepts a custom configuration, and assumed you will write it to disk.
	 */
	public PseudoDistributedCluster(PseudoDistributedConfiguration conf)
	{
		this.conf = conf;
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

		String start_dfs = HADOOP_INSTALL + "/sbin/start-dfs.sh --config " + CONFIG_BASE_DIR;
		String start_yarn = HADOOP_INSTALL + "/sbin/start-yarn.sh --config " + CONFIG_BASE_DIR;
		String start_historyserver = HADOOP_INSTALL + "/sbin/mr-jobhistory-daemon.sh start historyserver --config " + CONFIG_BASE_DIR;
		//String start_datanode = HADOOP_INSTALL + "/sbin/hadoop-daemon.sh --config " + CONFIG_BASE_DIR + " start datanode";

		System.out.println("STARTING DFS...");
		runProc(start_dfs);
		assertTrue("The NameNode was not started.", verifyJpsProcRunning("NameNode"));
		assertTrue("The SecondaryNameNode was not started.", verifyJpsProcRunning("SecondaryNameNode"));

		//System.out.println("STARTING DATANODE...");
		//runProc(start_datanode);
		assertTrue("The DataNode was not started.", verifyJpsProcRunning("DataNode"));
		
		System.out.println("STARTING YARN...");
		runProc(start_yarn);
		assertTrue("The ResourceManager was not started.", verifyJpsProcRunning("ResourceManager"));

		System.out.println("STARTING JOB HISTORY SERVER...");
		runProc(start_historyserver);
		assertTrue("The JobHistoryServer was not started.", verifyJpsProcRunning("JobHistoryServer"));
		
		System.out.println("Sleeping for 30s to wait for HDFS to get out of safe mode.");
		try {
			Thread.currentThread().sleep(30000);
		}
		catch (InterruptedException ie) {
			System.out.println("Couldn't sleep the current Thread.");
		}
	}

	/* 
	 * Stops all daemons associated with the pseudodistributed cluster instance, and
	 * verifies they have stopped with jps.
	 * 
	 * (non-Javadoc)
	 * @see hadooptest.cluster.Cluster#stop()
	 */
	public void stop() throws IOException {
		String stop_dfs = HADOOP_INSTALL + "/sbin/stop-dfs.sh";
		String stop_yarn = HADOOP_INSTALL + "/sbin/stop-yarn.sh";
		String stop_historyserver = HADOOP_INSTALL + "/sbin/mr-jobhistory-daemon.sh stop historyserver";

		runProc(stop_dfs);
		runProc(stop_yarn);
		runProc(stop_historyserver);

		// Wait for 10 seconds to ensure that the daemons have had time to stop.
		try {
			Thread.currentThread().sleep(10000);
		}
		catch (InterruptedException ie) {
			System.out.println("Couldn't sleep the current Thread.");
		}

		assertFalse("The NameNode was not stopped.", verifyJpsProcRunning("NameNode"));
		assertFalse("The SecondaryNameNode was not stopped.", verifyJpsProcRunning("SecondaryNameNode"));
		assertFalse("The DataNode was not stopped.", verifyJpsProcRunning("DataNode"));
		assertFalse("The ResourceManager was not stopped.", verifyJpsProcRunning("ResourceManager"));
		assertFalse("The JobHistoryServer was not stopped.", verifyJpsProcRunning("JobHistoryServer"));
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
	public void setConf(PseudoDistributedConfiguration conf) {
		this.conf = conf;
	}

	/*
	 * Gets the configuration for this pseudodistributed cluster instance.
	 * 
	 * @return PseudoDistributedConfiguration the configuration for the cluster instance.
	 */
	public PseudoDistributedConfiguration getConf() {
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
	public ClusterState getState() {
		return this.cluster_state;
	}

	private static void runProc(String command) {
		Process proc = null;

		System.out.println(command);

		try {
			proc = Runtime.getRuntime().exec(command);
			BufferedReader reader=new BufferedReader(new InputStreamReader(proc.getInputStream())); 
			String line=reader.readLine(); 
			while(line!=null) 
			{ 
				System.out.println(line); 				
				line=reader.readLine();
			} 
		}
		catch (Exception e) {
			if (proc != null) {
				proc.destroy();
			}
			e.printStackTrace();
		}
	}

	/*
	 * Verifies, with jps, that a given process name is running.
	 * 
	 * @param process The String representing the name of the process to verify.
	 */
	private static boolean verifyJpsProcRunning(String process) {

		Process jpsProc = null;

		String jpsCmd = "jps";

		System.out.println(jpsCmd);

		String jpsPatternStr = "(.*)(" + process + ")(.*)";
		Pattern jpsPattern = Pattern.compile(jpsPatternStr);

		try {
			jpsProc = Runtime.getRuntime().exec(jpsCmd);
			BufferedReader reader=new BufferedReader(new InputStreamReader(jpsProc.getInputStream())); 
			String line=reader.readLine(); 
			while(line!=null) 
			{ 
				System.out.println(line); 

				Matcher jpsMatcher = jpsPattern.matcher(line);

				if (jpsMatcher.find()) {
					System.out.println("FOUND PROCESS: " + process);
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

		System.out.println("PROCESS IS NO LONGER RUNNING: " + process);
		return false;
	}


}