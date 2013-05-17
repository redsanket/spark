package hadooptest.cluster.hadoop.standalone;

import hadooptest.cluster.ClusterState;
import hadooptest.cluster.hadoop.HadoopCluster;
import hadooptest.config.hadoop.HadoopConfiguration;
import hadooptest.config.hadoop.standalone.StandaloneConfiguration;

import java.io.IOException;
import java.util.Hashtable;

import org.apache.hadoop.fs.FileSystem;

/**
 * Cluster sublass representation of a Standalone Hadoop cluster.
 * 
 * This is an unfinished class and should not be used.
 */
public class StandaloneCluster extends HadoopCluster {

	// The base pseudodistributed configuration.
	protected StandaloneConfiguration conf;

	// The state of the pseudodistributed cluster.
	protected ClusterState cluster_state;
	
	// The version of the cluster.
	protected String cluster_version = "";

    protected Hashtable<String, String> paths = new Hashtable<String, String>();
	
	/**
	 * Class constructor.
	 * 
	 * Creates a brand new default PseudoDistributedConfiguration, and writes out the configuration to disk.
	 */
	public StandaloneCluster() 
			throws InterruptedException, IOException, Exception {
		this.conf = new StandaloneConfiguration();
		// TODO: implement initNodes()
		// super.initNodes();
	}

	/**
	 * Class constructor.
	 * 
	 * Accepts a custom configuration, and assumed you will write it to disk.
	 */
	public StandaloneCluster(StandaloneConfiguration conf)
			throws InterruptedException, IOException, Exception {
		this.conf = conf;
        // TODO: implement initNodes()
		// super.initNodes();
	}

	/**
	 * Starts the cluster instance:
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
	 */
	public boolean start(boolean waitForSafemodeOff) {
		return false;
	}

	/**
	 * Stop the cluster, shut it down cleanly to a state from which
	 * it can be restarted.
	 * 
	 * @return boolean true for success and false for failure.
	 **/
	public boolean stop() {
		return false;
	}

	/**
	 * Kill the cluster irrespective of the state it is left in.
	 **/
	public void die() {

	}

	/**
	 * Set a custom configuration for the pseudodistributed cluster instance.
	 * 
	 * @param conf The custom PseudoDistributedConfiguration
	 */
	public void setConf(HadoopConfiguration conf) {
		this.conf = (StandaloneConfiguration)conf;
	}

	/**
	 * Gets the configuration for this pseudodistributed cluster instance.
	 * 
	 * @return PseudoDistributedConfiguration the configuration for the cluster instance.
	 */
	public StandaloneConfiguration getConf() {
		return this.conf;
	}
	/**
	 * Get the current state of the cluster.
	 * 
	 * @return ClusterState the state of the cluster.
	 **/
	public ClusterState getState() {
		return this.cluster_state;
	}

	public String getVersion() {
		return this.cluster_version;
	}
	

    public Hashtable<String, String> getPaths() {
    	return paths;
    }

    public String getPaths(String key) {
    	return paths.get(key).toString();
    }
    
	/**
	 * Check to see if all of the cluster daemons are running.
	 * 
	 * @return boolean true if all cluster daemons are running.
	 */
	public boolean isFullyUp() {
		return false;
	}
	
	/**
	 * Check to see if all of the cluster daemons are stopped.
	 * 
	 * @return boolean true if all cluster daemons are stopped.
	 */
	public boolean isFullyDown() {
		return false;
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
		return false;
	}

}
