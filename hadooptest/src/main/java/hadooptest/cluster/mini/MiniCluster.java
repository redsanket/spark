/*
 * YAHOO!
 */

package hadooptest.cluster.mini;

import hadooptest.cluster.Cluster;
import hadooptest.cluster.ClusterState;
import hadooptest.config.testconfig.MiniclusterConfiguration;

import java.io.IOException;

/**
 * Cluster instance to represent a base mini cluster.
 * 
 * This is an unfinished class and should not yet be used.
 */
public abstract class MiniCluster implements Cluster {

   protected MiniclusterConfiguration conf;
   protected ClusterState cluster_state;
   protected String cluster_version = "";

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

   public boolean reset() {
	   return false;
   }

   public void setConf(MiniclusterConfiguration conf) {
      this.conf = conf;
   }

   public MiniclusterConfiguration getConf() {
      return this.conf;
   }

   public ClusterState getState() {
      return this.cluster_state;
   }
   
   public String getVersion() {
	   	return this.cluster_version;
   }

   protected abstract boolean startMiniClusterService(MiniclusterConfiguration conf);

   protected abstract boolean stopMiniClusterService();

}
