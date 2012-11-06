/*
 * YAHOO!
 * 
 * An class that is the base representation of any pseudodistributed cluster.
 * 
 * 2012.11.02 - Rick Bernotas - Initial version.
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

public class PseudoDistributedCluster implements Cluster {

	protected PseudoDistributedConfiguration conf;
	protected ClusterState cluster_state;

	private final String HADOOP_INSTALL = "/Users/rbernota/workspace/eclipse/branch-0.23.4/hadoop-dist/target/hadoop-0.23.4";
	private final String CONFIG_BASE_DIR = "/Users/rbernota/workspace/hadoop/test/pseudodistributed_configs/test/";
	
	public PseudoDistributedCluster() throws IOException
	{
		this.conf = new PseudoDistributedConfiguration();
		this.conf.write();
	}

	public PseudoDistributedCluster(PseudoDistributedConfiguration conf)
	{
		this.conf = conf;
	}

	public void start() throws IOException {

		String start_dfs = HADOOP_INSTALL + "/sbin/start-dfs.sh --config " + CONFIG_BASE_DIR;
		String start_yarn = HADOOP_INSTALL + "/sbin/start-yarn.sh --config " + CONFIG_BASE_DIR;
		//String start_datanode = HADOOP_INSTALL + "/sbin/hadoop-daemon.sh --config " + CONFIG_BASE_DIR + " start datanode";

		System.out.println("STARTING DFS...");
		runProc(start_dfs);

		// verify with jps
		assertTrue("The NameNode was not started.", verifyJpsProcRunning("NameNode"));
		assertTrue("The SecondaryNameNode was not started.", verifyJpsProcRunning("SecondaryNameNode"));

		System.out.println("STARTING YARN...");
		runProc(start_yarn);

		// verify with jps
		assertTrue("The ResourceManager was not started.", verifyJpsProcRunning("ResourceManager"));

		//System.out.println("STARTING DATANODE...");
		//runProc(start_datanode);

		// verify with jps
		assertTrue("The DataNode was not started.", verifyJpsProcRunning("DataNode"));

		System.out.println("Sleeping for 30s to wait for HDFS to get out of safe mode.");
		try {
			Thread.currentThread().sleep(30000);
		}
		catch (InterruptedException ie) {
			System.out.println("Couldn't sleep the current Thread.");
		}
	}

	public void stop() throws IOException {
		String stop_dfs = HADOOP_INSTALL + "/sbin/stop-dfs.sh";
		String stop_yarn = HADOOP_INSTALL + "/sbin/stop-yarn.sh";

		runProc(stop_dfs);

		runProc(stop_yarn);

		try {
			Thread.currentThread().sleep(10000);
		}
		catch (InterruptedException ie) {
			System.out.println("Couldn't sleep the current Thread.");
		}

		// verify with jps
		assertFalse("The NameNode was not stopped.", verifyJpsProcRunning("NameNode"));
		assertFalse("The SecondaryNameNode was not stopped.", verifyJpsProcRunning("SecondaryNameNode"));
		assertFalse("The ResourceManager was not stopped.", verifyJpsProcRunning("ResourceManager"));
		assertFalse("The DataNode was not stopped.", verifyJpsProcRunning("DataNode"));
	}

	public void die() throws IOException {

	}

	public void reset() {

	}

	public void setConf(PseudoDistributedConfiguration conf) {
		this.conf = conf;
	}

	public PseudoDistributedConfiguration getConf() {
		return this.conf;
	}

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