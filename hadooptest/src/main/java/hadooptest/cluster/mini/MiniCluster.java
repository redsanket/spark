/*
 * YAHOO!
 */

package hadooptest.cluster.mini;

import hadooptest.cluster.Cluster;
import hadooptest.cluster.ClusterState;
import hadooptest.config.testconfig.MiniclusterConfiguration;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;

/**
 * Cluster instance to represent a base mini cluster.
 * 
 * This is an unfinished class and should not yet be used.
 */
public abstract class MiniCluster extends Cluster {

	protected MiniclusterConfiguration conf;
	protected ClusterState clusterState;
	protected String clusterVersion = "";

   public boolean start() {
      this.conf = new MiniclusterConfiguration();
      return startMiniClusterService(this.conf);

   }

   public boolean start(MiniclusterConfiguration conf) throws IOException {
	   this.conf = conf;
	   return startMiniClusterService(conf);
   }

   public boolean stop() {
      return stopMiniClusterService();
   }

   public void die() {

   }

   public void setConf(MiniclusterConfiguration conf) {
      this.conf = conf;
   }

   public MiniclusterConfiguration getConf() {
      return this.conf;
   }

   public ClusterState getState() {
      return this.clusterState;
   }
   
   public String getVersion() {
	   	return this.clusterVersion;
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

   protected abstract boolean startMiniClusterService(MiniclusterConfiguration conf);

   protected abstract boolean stopMiniClusterService();

}
