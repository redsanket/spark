/*
 * YAHOO!
 * 
 * A class that represents a YARN minicluster.
 * 
 * 2012.10.08 - Rick Bernotas - Initial version.
 */

package hadooptest.cluster.mini.yarn;

import hadooptest.cluster.mini.MiniCluster;
import hadooptest.config.testconfig.MiniclusterConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public class YARNMiniCluster extends MiniCluster {

   private MiniYARNCluster cluster;

   public YARNMiniCluster()
   {
      this.conf = new MiniclusterConfiguration();
   }

   public YARNMiniCluster(MiniclusterConfiguration conf)
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
   
   public MiniYARNCluster getCluster()
   {
	   return this.cluster;
   }

   protected void startMiniClusterService()
   {
	   String test_name = "hadooptest_YARN_minicluster";
	   int numNodeManagers = 2;
	   int numLocalDirs = 1;
	   int numLogDirs = 1;
	   
	   this.cluster = new MiniYARNCluster(test_name, numNodeManagers, numLocalDirs, numLogDirs);
	  
	   this.cluster.init(this.conf);
	   this.cluster.start();
   }

}
