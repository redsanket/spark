/*
 * YAHOO!
 * 
 * A class that represents a MapReduce minicluster.
 * 
 * 2012.10.08 - Rick Bernotas - Initial version.
 */

package hadooptest.cluster.mini.mapreduce;

import hadooptest.cluster.mini.MiniCluster;
import hadooptest.config.testconfig.MiniclusterConfiguration;

import org.apache.hadoop.mapred.MiniMRClientClusterFactory;
import org.apache.hadoop.mapred.MiniMRClientCluster;
import org.apache.hadoop.yarn.server.MiniYARNCluster;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

public class MRMiniCluster extends MiniCluster {
	
   private MiniMRClientCluster cluster;

   public MRMiniCluster()
   {
      this.conf = new MiniclusterConfiguration(); 
   }

   public MRMiniCluster(MiniclusterConfiguration conf)
   {
      this.conf = conf;
   }
   
   public void startMiniClusterService(MiniclusterConfiguration conf) throws IOException
   {
      this.conf = conf;
      startMiniClusterService();
   }

   public void stopMiniClusterService() throws IOException
   { 
	   this.cluster.stop();
   }

   
   public MiniMRClientCluster getCluster()
   {
	   return this.cluster;
   }
   
   protected void startMiniClusterService() throws IOException
   {
	   int numNodeManagers = 1;
	   
	   this.cluster = MiniMRClientClusterFactory.create(MRMiniCluster.class, numNodeManagers, this.conf);
	   
	   //this.cluster.start();

   }
   

   
}
