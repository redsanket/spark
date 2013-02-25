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

   public void start() {

      this.conf = new MiniclusterConfiguration();
      startMiniClusterService(this.conf);

   }

   public void start(MiniclusterConfiguration conf) throws IOException {

	      this.conf = conf;
	      startMiniClusterService(conf);

   }

   public void stop() {
      stopMiniClusterService();
   }

   public void die() {

   }

   public void reset() {

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

   protected abstract void startMiniClusterService(MiniclusterConfiguration conf);

   protected abstract void stopMiniClusterService();

}
