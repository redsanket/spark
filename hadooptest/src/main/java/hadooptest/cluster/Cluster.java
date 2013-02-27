/*
 * YAHOO!
 */

package hadooptest.cluster;

import hadooptest.config.TestConfiguration;

/**
 * An interface which should represent the base capability of any cluster
 * type in the framework.  This interface is also what should be commonly
 * used to reference the common cluster instance maintained by the
 * TestSession.  Subclasses of Cluster should implment a specific cluster
 * type.
 */
public interface Cluster {

   /**
    * Start the cluster from a stopped state.
    *
    * @return boolean true for success and false for failure.
    */
   public boolean start();
   
   /**
    * Stop the cluster, shut it down cleanly to a state from which
    * it can be restarted.
    * 
    * @return boolean true for success and false for failure.
    */
   public boolean stop();

   /**
    * Kill the cluster irrespective of the state it is left in.
    */
   public void die();

   /**
    * Reset the cluster to a default state with the current 
    * configuration, without stopping or killing it.
    * 
    * @return boolean true for success and false for failure.
    */ 
   public boolean reset();
   
   /**
    * Get the current state of the cluster.
    * 
    * @return ClusterState the state of the cluster.
    */
   public ClusterState getState();

   /**
    * Get the current state of the cluster.
    * 
    * @return ClusterState the state of the cluster.
    */
   public String getVersion();
   
   /**
    * Get the cluster Hadoop configuration.
    * 
    * @return TestConfiguration the Hadoop cluster configuration.
    */
   public TestConfiguration getConf();
   
   /**
    * Set the cluster Hadoop configuration.
    * 
    * @param conf the Hadoop cluster configuration to set for the cluster.
    */
   public void setConf(TestConfiguration conf);

}
