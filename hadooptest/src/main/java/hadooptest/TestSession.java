package hadooptest;

import hadooptest.cluster.hadoop.HadoopCluster;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedExecutor;
import hadooptest.cluster.hadoop.pseudodistributed.PseudoDistributedCluster;
import hadooptest.cluster.hadoop.pseudodistributed.PseudoDistributedExecutor;

import java.io.IOException;
import java.lang.reflect.Constructor;

import coretest.TestSessionCore;
import coretest.cluster.ClusterState;

/**
 * TestSession is the main driver for the automation framework.  It
 * maintains a central logging framework, and central configuration
 * for the framework.  Additionally, the TestSession maintains a
 * common instance of the Hadoop cluster type specified in the 
 * framework configuration file, as well as a process executor to match.
 * 
 * For each test based on the framework, TestSession should be the 
 * superclass (a test class must extend TestSession).  TestSession will
 * then provide that class with a logger, cluster instance, framework
 * configuration reference, and an executor for system processes.
 * 
 * Additionally, for each test based on the framework, the test will need
 * to call TestSession.start() exactly once for each instance of the test
 * class.  TestSession.start() initializes all of the items that 
 * TestSession provides.
 */
public abstract class TestSession extends TestSessionCore {

	/** The Hadoop Cluster to use for the test session */
	public static HadoopCluster cluster;
	
	/**
	 * Initializes the test session in the following order:
	 * initilizes framework configuration, initializes the
	 * centralized logger, initializes the cluster reference.
	 * 
	 * This method should be called once from every subclass
	 * of TestSession, in order to initialize the 
	 * TestSession for a test class.
	 */
	public static void start() {

		// Initialize the framework name
		initFrameworkName();
		
		// Initialize the framework configuration
		initConfiguration();
		
		// Intitialize the framework logger
		initLogging();
		
		// Log Java Properties
		initLogJavaProperties();
		
		// Initialize the cluster to be used in the framework
		initCluster();

		initSecurity();
		
		initMultiCluster();
	}
	
	/**
	 * Get the Hadoop cluster instance for the test session.
	 * 
	 * @return HadoopCluster the Hadoop cluster instance for the test session.
	 */
	public static HadoopCluster getCluster() {
		return cluster;
	}
	
	/**
	 * Initialize the framework name.
	 */
	private static void initFrameworkName() {
		frameworkName = "hadooptest";
	}
	
	/**
	 * Initialize the cluster instance for the framework.
	 */
	private static void initCluster() {
		// The unknown class type for the cluster
		Class<?> clusterClass = null;
		
		// The unknown constructor for the cluster class
		Constructor<?> clusterClassConstructor = null;
		
		// The unknown class type object instance for the cluster
		Object clusterObject = null;
		
		// Retrieve the cluster type from the framework configuration file.
		// This should be in the format of package.package.class
		String strClusterType = conf.getProperty("CLUSTER_TYPE");
		
		// Initialize the test session executor instance with the correct cluster type.
		if (strClusterType.equals("hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster")) {
			exec = new FullyDistributedExecutor();
		}
		else if (strClusterType.equals("hadooptest.cluster.hadoop.pseudodistributed.PseudoDistributedCluster")) {
			exec = new PseudoDistributedExecutor();
		}
		else {
			logger.error("The cluster type is not yet fully supported: " + strClusterType);
		}

		// Create a new instance of the cluster class specified in the 
		// framework configuration file.
		try {
			clusterClass = Class.forName(strClusterType);
			clusterClassConstructor = clusterClass.getConstructor();
			clusterObject = clusterClassConstructor.newInstance();
		}
		catch (ClassNotFoundException cnf) {
			logger.error("The cluster type is not supported: " + strClusterType);
			cnf.printStackTrace();
		}
		catch (NoSuchMethodException nsm) {
			logger.error("The cluster type was found, but there is a problem locating the constructor for the class: " + strClusterType);
			nsm.printStackTrace();
		}
		catch (Exception e) {
			logger.error("The test session wasn't able to instantiate the class of type: " + strClusterType);
			e.printStackTrace();
		}
		
		// Initialize the test session cluster instance with the correct cluster type.
		if (clusterObject instanceof FullyDistributedCluster) {
			cluster = (FullyDistributedCluster)clusterObject;
		}
		else if (clusterObject instanceof PseudoDistributedCluster) {
			cluster = (PseudoDistributedCluster)clusterObject;
		}
		else {
			logger.error("The cluster type is not yet fully supported: " + strClusterType);
		}

		if (cluster != null) {
			ClusterState clusterState = null;
			try {
				clusterState = cluster.getState();
			}
			catch (Exception e) {
				logger.error("Failed to get the cluster state.", e);
			}

			if (clusterState != ClusterState.UP) {
				logger.warn("Cluster is not fully up: cluster state='" +
						clusterState.toString() + "'.'");
				/*
			TODO: optionally restart the cluster. This could impact how the
			tests are being run in parallel classes.

			try {
				cluster.reset();				
			} catch (Exception e) {
				logger.error("Failed to restart the cluster:", e);				
			}
				 */
			}
		}
	}
	
	/**
	 * Initialize Hadoop API security for the test session.
	 */
	private static void initSecurity() {
		if (cluster != null) {
			try {
				// Initialize API security for the FullyDistributedCluster type only.
				if (cluster instanceof FullyDistributedCluster) {
					cluster.setSecurityAPI("keytab-hadoopqa", "user-hadoopqa");
				}
			}
			catch (IOException ioe) {
				logger.error("Failed to set the Hadoop API security.", ioe);
			}
		}
	}
	
}
