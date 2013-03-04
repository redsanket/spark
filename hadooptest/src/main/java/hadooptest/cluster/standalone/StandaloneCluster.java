package hadooptest.cluster.standalone;

import hadooptest.cluster.ClusterState;
import hadooptest.cluster.Cluster;
import hadooptest.config.testconfig.StandaloneConfiguration;
import hadooptest.config.TestConfiguration;

import java.io.IOException;
import java.util.Hashtable;

import org.apache.hadoop.fs.FileSystem;

/**
 * Cluster sublass representation of a Standalone Hadoop cluster.
 * 
 * This is an unfinished class and should not be used.
 */
public class StandaloneCluster implements Cluster {

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
	public StandaloneCluster() {
		this.conf = new StandaloneConfiguration();
	}

	/**
	 * Class constructor.
	 * 
	 * Accepts a custom configuration, and assumed you will write it to disk.
	 */
	public StandaloneCluster(StandaloneConfiguration conf)
	{
		this.conf = conf;
	}

	/**
	 * Start the cluster from a stopped state.
	 * 
	 * @return boolean true for success and false for failure.
	 **/
	public boolean start() {
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
	 * Reset the cluster to a default state with the current 
	 * configuration, without stopping or killing it.
	 * 
	 * @return boolean true for success and false for failure.
	 **/ 
	public boolean reset() {
		return false;
	}

	/**
	 * Set a custom configuration for the pseudodistributed cluster instance.
	 * 
	 * @param conf The custom PseudoDistributedConfiguration
	 */
	public void setConf(TestConfiguration conf) {
		this.conf = (StandaloneConfiguration)conf;
	}

	/**
	 * Gets the file system for this cluster instance.
	 * 
	 * @return FileSystem for the cluster instance.
	 */
	public FileSystem getFS() throws IOException {
		return FileSystem.get(this.conf);
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

}
