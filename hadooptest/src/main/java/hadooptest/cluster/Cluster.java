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
   public boolean start() {
	   return start(true);
   }

   /**
    * Start the cluster from a stopped state.
    *
    * @param waitForSafemodeOff to wait for safemode off after start.
    * Default is true. 
    * 
    * @return boolean true for success and false for failure.
    */
   public abstract boolean start(boolean waitForSafemodeOff);
   
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
	 * Check to see if all of the cluster daemons are running.
	 * 
	 * @return boolean true if all cluster daemons are running.
	 */
	public abstract boolean isFullyUp();

	/**
	 * Check to see if all of the cluster daemons are stopped.
	 * 
	 * @return boolean true if all cluster daemons are stopped.
	 */
	public abstract boolean isFullyDown();

    /**
     * Wait for the safemode on the namenode to be OFF. 
     * 
     * @return boolean true if safemode is OFF, or false if safemode is ON.
     */
	public boolean waitForSafemodeOff() {
		return waitForSafemodeOff(-1, null);
	}
		
    /**
     * Wait for the safemode on the namenode to be OFF. 
     *
     * @param timeout time to wait for safe mode to be off.
     * @param fs file system under test
     * 
     * @return boolean true if safemode is OFF, or false if safemode is ON.
     */
	public boolean waitForSafemodeOff(int timeout, String fs) {

		if (timeout < 0) {
			int defaultTimeout = 300;
	    	timeout = defaultTimeout;
	    }

	    if ((fs == null) || fs.isEmpty()) {
		    // TODO: check if this will still work if custom conf dir is used.
	    	// fs = this.getConf().getHadoopConfFileProp(
	        fs = this.getConf().get(
	        		"fs.defaultFS",
	        		TestConfiguration.HADOOP_CONF_CORE);
		}

	    // TODO: get the namenode regardless of cluster type
	    // String namenode = this.getConf().getClusterNodes("namenode")[0];	
		String[] safemodeGetCmd = { this.getConf().getHadoopProp("HDFS_BIN"),
				"--config", this.getConf().getHadoopProp("HADOOP_CONF_DIR"),
				"dfsadmin", "-fs", fs, "-safemode", "get" };
			
		String[] output = TestSession.exec.runHadoopProcBuilder(safemodeGetCmd);
		boolean isSafemodeOff = 
				(output[1].trim().equals("Safe mode is OFF")) ? true : false;
		 
	    // for the time out duration wait and see if the namenode comes out of safemode
	    int waitTime=5;
	    int i=1;
	    while ((timeout > 0) && (!isSafemodeOff)) {
	    	TestSession.logger.info("Wait for safemode to be OFF: TRY #" + i + ": WAIT " + waitTime + "s:" );
	    	try {
	    		Thread.sleep(waitTime*1000);
	    	} catch  (InterruptedException e) {
	    		TestSession.logger.error("Encountered Interrupted Exception: " +
	    				e.toString());
	    	}
	    	output = TestSession.exec.runHadoopProcBuilder(safemodeGetCmd);
	    	isSafemodeOff = 
	    			(output[1].trim().equals("Safe mode is OFF")) ? true : false;
	        timeout = timeout - waitTime;
	        i++;
	    }
	    
	    if (!isSafemodeOff) {
	    	// TestSession.logger.info("ALERT: NAMENODE " + namenode + " IS STILL IN SAFEMODE");
	    	TestSession.logger.info("ALERT: NAMENODE IS STILL IN SAFEMODE");
	    }
	    
	    return isSafemodeOff;
	}
	
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
