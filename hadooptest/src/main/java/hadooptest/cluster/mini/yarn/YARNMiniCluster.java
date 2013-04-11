/*
 * YAHOO!
 * 
 * A class that represents a YARN minicluster.
 * 
 * 2012.10.08 - Rick Bernotas - Initial version.
 */

package hadooptest.cluster.mini.yarn;

import hadooptest.cluster.mini.MiniCluster;
import hadooptest.config.TestConfiguration;
import hadooptest.config.testconfig.MiniclusterConfiguration;

import java.net.UnknownHostException;
import java.util.Hashtable;

import org.apache.hadoop.yarn.server.MiniYARNCluster;

/**
 * MiniCluster instance to represent a base YARN mini cluster.
 * 
 * This is an unfinished class and should not yet be used.
 */
public class YARNMiniCluster extends MiniCluster {

   private MiniYARNCluster cluster;

   protected Hashtable<String, String> paths = new Hashtable<String, String>();
   
   public YARNMiniCluster() throws Exception {
      this.conf = new MiniclusterConfiguration();
   }

   public YARNMiniCluster(MiniclusterConfiguration conf) {
      this.conf = conf;
   }

   public boolean startMiniClusterService(MiniclusterConfiguration conf) {
      this.conf = conf;
      return startMiniClusterService();
   }

   public boolean stopMiniClusterService() {
	   this.cluster.stop();
	   return true;
   }
   
   public MiniYARNCluster getCluster() {
	   return this.cluster;
   }

   protected boolean startMiniClusterService() {
	   String test_name = "hadooptest_YARN_minicluster";
	   int numNodeManagers = 2;
	   int numLocalDirs = 1;
	   int numLogDirs = 1;
	   
	   this.cluster = new MiniYARNCluster(test_name, numNodeManagers, numLocalDirs, numLogDirs);
	  
	   this.cluster.init(this.conf);
	   this.cluster.start();
	   return true;
   }
   
   public Hashtable<String, String> getPaths() {
   	return paths;
   }

   public String getPaths(String key) {
   	return paths.get(key).toString();
   }

	/**
	 * Set a custom configuration for the pseudodistributed cluster instance.
	 * 
	 * @param conf The custom PseudoDistributedConfiguration
	 */
	public void setConf(TestConfiguration conf) {
		this.conf = (MiniclusterConfiguration)conf;
	}

}
