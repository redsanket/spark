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

   protected abstract boolean startMiniClusterService(MiniclusterConfiguration conf);

   protected abstract boolean stopMiniClusterService();

}
