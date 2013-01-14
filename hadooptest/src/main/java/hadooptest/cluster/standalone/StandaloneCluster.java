package hadooptest.cluster.standalone;

import hadooptest.cluster.ClusterState;
import hadooptest.cluster.Cluster;
import hadooptest.config.testconfig.PseudoDistributedConfiguration;
import hadooptest.config.testconfig.StandaloneConfiguration;

import java.io.IOException;

public class StandaloneCluster implements Cluster {

	// The base pseudodistributed configuration.
	protected StandaloneConfiguration conf;

	// The state of the pseudodistributed cluster.
	protected ClusterState cluster_state;
	
	// The version of the cluster.
	protected String cluster_version = "";
	
	/*
	 * Class constructor.
	 * 
	 * Creates a brand new default PseudoDistributedConfiguration, and writes out the configuration to disk.
	 */
	public StandaloneCluster() {
		this.conf = new StandaloneConfiguration();
	}

	/*
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
	 **/
	public void start() throws IOException {

	}

	/**
	 * Stop the cluster, shut it down cleanly to a state from which
	 * it can be restarted.
	 **/
	public void stop() throws IOException {

	}

	/**
	 * Kill the cluster irrespective of the state it is left in.
	 **/
	public void die() throws IOException {

	}

	/**
	 * Reset the cluster to a default state with the current 
	 * configuration, without stopping or killing it.
	 **/ 
	public void reset() {

	}

	/*
	 * Set a custom configuration for the pseudodistributed cluster instance.
	 * 
	 * @param conf The custom PseudoDistributedConfiguration
	 */
	public void setConf(StandaloneConfiguration conf) {
		this.conf = conf;
	}

	/*
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

}
