package hadooptest.cluster.fullydistributed;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.cluster.Cluster;
import hadooptest.cluster.ClusterState;
import hadooptest.config.testconfig.FullyDistributedConfiguration;

public class FullyDistributedCluster implements Cluster {

	// The base pseudodistributed configuration.
	protected FullyDistributedConfiguration conf;
	
	// The state of the pseudodistributed cluster.
	protected ClusterState cluster_state;

    // The Hadoop version on the fully distributed cluster.
    protected String cluster_version = "";

	private static TestSession TSM;
	
	private String HADOOP_INSTALL;
	private String CONFIG_BASE_DIR;
	
	/*
	 * Class constructor.
	 * 
	 * Creates a brand new default PseudoDistributedConfiguration, and writes out the configuration to disk.
	 */
	public FullyDistributedCluster(TestSession testSession) throws IOException
	{
		TSM = testSession;
		
		this.conf = new FullyDistributedConfiguration(testSession);
		
		this.initTestSessionConf();
		
		// this.conf.write();
	}

	/*
	 * Class constructor.
	 * 
	 * Accepts a custom configuration, and assumed you will write it to disk.
	 */
	public FullyDistributedCluster(TestSession testSession, FullyDistributedConfiguration conf)
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
	public ClusterState getState() {
		return this.cluster_state;
	}

    /*
     * Returns the Hadoop version for the fully distributed cluster.
     * 
     * @return String the Hadoop version for the fully distributed cluster.
     * 
     * (non-Javadoc)
     * @see hadooptest.cluster.Cluster#getVersion()
     */
    public String getVersion() {
        // Get Cluster Version if undefined
        if (cluster_version.equals("")) {
        	// Call hadoop version to fetch the version
        	String[] cmd = { HADOOP_INSTALL+"/share/hadoop/bin/hadoop",
        			"--config", CONFIG_BASE_DIR, "version" };
        	this.cluster_version = runProcBuilder(cmd);
        }	
        return this.cluster_version;
    }
    
    // Putting this here temporary
    public String runSleepJob() {
    	String version = "0.23.6.0.1301071353";
    	String sleepJobJar = HADOOP_INSTALL +
    			"/share/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-"+
    			version+"-tests.jar";
    	String[] cmd = { HADOOP_INSTALL+"/share/hadoop/bin/hadoop",
    			"--config", CONFIG_BASE_DIR, "jar", sleepJobJar, "sleep", 
    			"-m", "1", "-r", "1", "-mt", "1", "-rt", "1" };
    	return runProcBuilder(cmd);
    }
    
	/*
	 * Initialize the test session configuration properties necessary to use the 
	 * pseudo distributed cluster instance.
	 */
	private void initTestSessionConf() {
		HADOOP_INSTALL = TSM.conf.getProperty("HADOOP_INSTALL", "");
		CONFIG_BASE_DIR = TSM.conf.getProperty("CONFIG_BASE_DIR", "");
	}
	
	/*
	 * Run a local system command.
	 * 
	 * @param command The system command to run.
	 */
	private static String runProcBuilder(String[] commandArray) {
		Process proc = null;
		TSM.logger.debug(Arrays.toString(commandArray));
		System.out.println("run command: "+Arrays.toString(commandArray));
		String output = null;
		String error = null;
		try {
			ProcessBuilder pb = new ProcessBuilder(commandArray);
	        proc = pb.start();
	        output = loadStream(proc.getInputStream());
	        error = loadStream(proc.getErrorStream());
	        int rc = proc.waitFor();
	        TSM.logger.debug("Process ended with rc=" + rc);
	        TSM.logger.debug("Standard Output:" + output);
	        TSM.logger.debug("Standard Error:" + error);
		}
		catch (Exception e) {
			if (proc != null) {
				proc.destroy();
			}
			e.printStackTrace();
		}
		return output+error;
	}
	
    private static String loadStream(InputStream is) throws Exception {
		BufferedReader br = new BufferedReader(new InputStreamReader(is)); 
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = br.readLine()) != null) {
 			TSM.logger.debug(line);
            sb.append(line).append("\n");
        }
        return sb.toString();
    }
    
	/*
	 * Run a local system command.
	 * 
	 * @param command The system command to run.
	 */
	private static String runProc(String command) {
		Process proc = null;
		TSM.logger.debug(command);
		System.out.println("run command="+command);
		String output = null;
		try {
			proc = Runtime.getRuntime().exec(command);
	        output = loadStream(proc.getInputStream());
		}
		catch (Exception e) {
			if (proc != null) {
				proc.destroy();
			}
			e.printStackTrace();
		}
		return output;
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
