/*
 * YAHOO!
 * 
 * A class that represents a HDFS minicluster.
 * 
 * 2012.10.08 - Rick Bernotas - Initial version.
 */

package hadooptest.cluster.mini.hdfs;

import hadooptest.TestSession;
import hadooptest.cluster.mini.MiniCluster;
import hadooptest.config.TestConfiguration;
import hadooptest.config.testconfig.MiniclusterConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Hashtable;

/**
 * MiniCluster instance to represent a base HDFS mini cluster.
 * 
 * This is an unfinished class and should not yet be used.
 */
public class HDFSMiniCluster extends MiniCluster {

   private MiniDFSCluster cluster;

   protected Hashtable<String, String> paths = new Hashtable<String, String>();
   
   public HDFSMiniCluster() throws UnknownHostException {
      this.conf = new MiniclusterConfiguration(); 
   }

   public HDFSMiniCluster(MiniclusterConfiguration conf)
   {
      this.conf = conf;
   }

   public MiniDFSCluster getCluster()
   {
	   return this.cluster;
   }
   
   public boolean startMiniClusterService(MiniclusterConfiguration conf) {
      this.conf = conf;
      return startMiniClusterService();
   }

   public boolean stopMiniClusterService() {
      this.cluster.shutdown();
      return true;
   }

   protected boolean startMiniClusterService() {

	   try {
		   cluster = new MiniDFSCluster.Builder(this.conf).build();
		   return true;
	   }
	   catch (IOException ioe) {
		   ioe.printStackTrace();
		   TestSession.logger.error("There was a problem starting the mini cluster service.");
		   return false;
	   }
		   
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
