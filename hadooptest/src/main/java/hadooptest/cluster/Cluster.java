/*
 * YAHOO!
 */

package hadooptest.cluster;

import hadooptest.config.TestConfiguration;

import java.io.IOException;

/*
 * An interface which should represent the base capability of any cluster
 * type in the framework.
 */
public interface Cluster {

   /**
    * Start the cluster from a stopped state.
    **/
   public void start() throws IOException;
   
   /**
    * Stop the cluster, shut it down cleanly to a state from which
    * it can be restarted.
    **/
   public void stop() throws IOException;

   /**
    * Kill the cluster irrespective of the state it is left in.
    **/
   public void die() throws IOException;

   /**
    * Reset the cluster to a default state with the current 
    * configuration, without stopping or killing it.
    **/ 
   public void reset();
  
   /**
    * Get the current state of the cluster.
    * 
    * @return ClusterState the state of the cluster.
    **/
   public ClusterState getState();
}
