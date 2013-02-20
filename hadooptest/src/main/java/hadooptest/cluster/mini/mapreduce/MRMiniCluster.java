/*
 * YAHOO!
 * 
 * A class that represents a MapReduce minicluster.
 * 
 * 2012.10.08 - Rick Bernotas - Initial version.
 */

package hadooptest.cluster.mini.mapreduce;

import hadooptest.TestSession;
import hadooptest.cluster.mini.MiniCluster;
import hadooptest.config.TestConfiguration;
import hadooptest.config.testconfig.MiniclusterConfiguration;

import org.apache.hadoop.mapred.MiniMRClientClusterFactory;
import org.apache.hadoop.mapred.MiniMRClientCluster;

import java.io.IOException;
import java.util.Hashtable;

public class MRMiniCluster extends MiniCluster {
	
   private MiniMRClientCluster cluster;

   protected Hashtable<String, String> paths = new Hashtable<String, String>();
   
   public MRMiniCluster()
   {
      this.conf = new MiniclusterConfiguration(); 
   }

   public MRMiniCluster(MiniclusterConfiguration conf) {
      this.conf = conf;
   }
   
   public void startMiniClusterService(MiniclusterConfiguration conf) {
      this.conf = conf;
      startMiniClusterService();
   }

   public void stopMiniClusterService() { 
	   try {
		   this.cluster.stop();
	   }
	   catch (IOException ioe) {
		   ioe.printStackTrace();
		   TestSession.logger.error("There was a problem stopping the mini cluster service.");
	   }
   }

   
   public MiniMRClientCluster getCluster()
   {
	   return this.cluster;
   }
   
   protected void startMiniClusterService() {
	   int numNodeManagers = 1;
	   
	   try {
		   this.cluster = MiniMRClientClusterFactory.create(MRMiniCluster.class, numNodeManagers, this.conf);
	   }
	   catch (IOException ioe) {
		   ioe.printStackTrace();
		   TestSession.logger.error("There was a problem starting the mini cluster service.");
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
	public void setConf(TestConfiguration conf) {
		this.conf = (MiniclusterConfiguration)conf;
	}
   
}
