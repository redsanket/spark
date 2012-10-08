/*
 * YAHOO!
 * 
 * A class that represents a HDFS minicluster.
 * 
 * 2012.10.08 - Rick Bernotas - Initial version.
 */

package hadooptest.cluster.mini.hdfs;

import hadooptest.cluster.mini.MiniCluster;
import hadooptest.config.testconfig.MiniclusterConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.MiniMRClientCluster;

import java.io.IOException;

public class HDFSMiniCluster extends MiniCluster {

   private MiniDFSCluster cluster;

   public HDFSMiniCluster()
   {
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
   
   public void startMiniClusterService(MiniclusterConfiguration conf) throws IOException
   {
      this.conf = conf;
      startMiniClusterService();
   }

   public void stopMiniClusterService() throws IOException
   {
      this.cluster.shutdown(); 
   }

   protected void startMiniClusterService() throws IOException
   {

	   cluster = new MiniDFSCluster.Builder(this.conf).build();

   }

}
