/*
 * YAHOO!
 * 
 * A set of JUnit tests that load different types of miniclusters.
 * 
 * 2012.10.08 - Rick Bernotas - Initial version.
 */

package hadooptest;

import hadooptest.cluster.mini.yarn.YARNMiniCluster;
import hadooptest.cluster.mini.hdfs.HDFSMiniCluster;
import hadooptest.cluster.mini.mapreduce.MRMiniCluster;


import org.junit.Test;
import org.junit.Assert;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import org.apache.hadoop.yarn.service.Service;


import java.io.IOException;

public class MiniClusterLoad {

   @Test
   public void loadTestYARNMiniCluster() throws IOException {
      //MRMiniCluster mapreduce_cluster = new MRMiniCluster();
      //HDFSMiniCluster hdfs_cluster = new HDFSMiniCluster();
      YARNMiniCluster yarn_cluster = new YARNMiniCluster();

      //mapreduce_cluster.start();
      //hdfs_cluster.start();
      yarn_cluster.start();
      
      //Assert.assertNotNull(mapreduce_cluster);
      //Assert.assertNotNull(hdfs_cluster);
      Assert.assertNotNull(yarn_cluster);
      
      String tmp = yarn_cluster.getCluster().getResourceManager().getName();
      System.out.println(tmp);
      Service.STATE yarn_nm_state = yarn_cluster.getCluster().getNodeManager(1).getServiceState();
      Assert.assertEquals(Service.STATE.STARTED, yarn_nm_state);
      
      //mapreduce_cluster.stop();
      //hdfs_cluster.stop();
      yarn_cluster.stop();
      
      yarn_nm_state = yarn_cluster.getCluster().getNodeManager(1).getServiceState();
      
      Assert.assertEquals(Service.STATE.STOPPED, yarn_nm_state);
   }
   
   @Test
   public void loadTestMapReduceMiniCluster() throws IOException {
      MRMiniCluster mapreduce_cluster = new MRMiniCluster();

      mapreduce_cluster.start();
      
      Assert.assertNotNull(mapreduce_cluster);
      
      mapreduce_cluster.stop();

   }
   
   @Test
   public void loadTestHDFSMiniCluster() throws IOException {
	   HDFSMiniCluster hdfs_cluster = new HDFSMiniCluster();
	   
	   hdfs_cluster.start();
	   
	   Assert.assertNotNull(hdfs_cluster);
	   
	   System.out.println(hdfs_cluster.getCluster().getNameNode().getNameNodeAddress().toString());
	   Assert.assertEquals(hdfs_cluster.getCluster().getDataNodes().get(0).isDatanodeFullyStarted(), true);
	   
	   hdfs_cluster.stop();
   }

   
}

