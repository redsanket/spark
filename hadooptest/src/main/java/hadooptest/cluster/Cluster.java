/*
 * YAHOO!
 */

package hadooptest.cluster;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;

import hadooptest.TestSession;
import hadooptest.config.TestConfiguration;

/**
 * An interface which should represent the base capability of any cluster
 * type in the framework.  This interface is also what should be commonly
 * used to reference the common cluster instance maintained by the
 * TestSession.  Subclasses of Cluster should implment a specific cluster
 * type.
 */
public abstract class Cluster {

   /**
    * Start the cluster from a stopped state.
    *
    * @return boolean true for success and false for failure.
    */
   public abstract boolean start();
   
   /**
    * Stop the cluster, shut it down cleanly to a state from which
    * it can be restarted.
    * 
    * @return boolean true for success and false for failure.
    */
   public abstract boolean stop();

   /**
    * Kill the cluster irrespective of the state it is left in.
    */
   public abstract void die();

   /**
    * Reset the cluster to a default state with the current 
    * configuration, without stopping or killing it.
    * 
    * @return boolean true for success and false for failure.
    */ 
   
   /**
    * Restart the cluster.
    * 
    * @return boolean true for success and false for failure.
    */
	public boolean reset() {	
		boolean stopped = this.stop();
		boolean started = this.start();
		return (stopped && started);
	}

	/**
    * Get the current state of the cluster.
    * 
    * @return ClusterState the state of the cluster.
    */
   public abstract ClusterState getState();

   /**
    * Get the current state of the cluster.
    * 
    * @return ClusterState the state of the cluster.
    */
   public abstract String getVersion();
   
   /**
    * Get the cluster Hadoop configuration.
    * 
    * @return TestConfiguration the Hadoop cluster configuration.
    */
   public abstract TestConfiguration getConf();
   
   /**
    * Set the cluster Hadoop configuration.
    * 
    * @param conf the Hadoop cluster configuration to set for the cluster.
    */
   public abstract void setConf(TestConfiguration conf);

   
   /**
    * Get the cluster Hadoop file system.
    * 
    * @return FileSystem for the cluster instance.
    */
   public FileSystem getFS() throws IOException {
		return FileSystem.get(this.getConf());
   }

   /**
    * Get the cluster Hadoop file system shell.
    * 
    * @return FS Shell for the cluster instance.
    */
   public FsShell getFsShell() throws IOException {
		return new FsShell(this.getConf());
   }
}
