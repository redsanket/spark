/*
 * YAHOO!
 * 
 * A class that represents a MapReduce minicluster.
 * 
 * 2012.10.08 - Rick Bernotas - Initial version.
 */

package hadooptest.cluster.hadoop.mini.mapreduce;

import hadooptest.TestSession;
import hadooptest.cluster.hadoop.mini.MiniCluster;
import hadooptest.config.hadoop.HadoopConfiguration;
import hadooptest.config.hadoop.minicluster.MiniclusterConfiguration;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Hashtable;

import org.apache.hadoop.mapred.MiniMRClientCluster;
import org.apache.hadoop.mapred.MiniMRClientClusterFactory;

/**
 * MiniCluster instance to represent a base MapReduce mini cluster.
 * 
 * This is an unfinished class and should not yet be used.
 */
public class MRMiniCluster extends MiniCluster {
	
   private MiniMRClientCluster cluster;

   protected Hashtable<String, String> paths = new Hashtable<String, String>();
   
   public MRMiniCluster()
		   throws Exception {
      this.conf = new MiniclusterConfiguration(); 
   }

   public MRMiniCluster(MiniclusterConfiguration conf) {
      this.conf = conf;
   }
   
   public boolean startMiniClusterService(MiniclusterConfiguration conf) {
      this.conf = conf;
      return startMiniClusterService();
   }

   public boolean stopMiniClusterService() { 
	   try {
		   this.cluster.stop();
		   return true;
	   }
	   catch (IOException ioe) {
		   ioe.printStackTrace();
		   TestSession.logger.error("There was a problem stopping the mini cluster service.");
		   return false;
	   }
   }

   
   public MiniMRClientCluster getCluster()
   {
	   return this.cluster;
   }
   
   protected boolean startMiniClusterService() {
	   int numNodeManagers = 1;
	   
	   try {
		   this.cluster = MiniMRClientClusterFactory.create(MRMiniCluster.class, numNodeManagers, this.conf);
		   return true;
	   }
	   catch (IOException ioe) {
		   ioe.printStackTrace();
		   TestSession.logger.error("There was a problem starting the mini cluster service.");
		   return false;
	   }
	   
	   //this.cluster.start();

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
	public void setConf(HadoopConfiguration conf) {
		this.conf = (MiniclusterConfiguration)conf;
	}
   
}
