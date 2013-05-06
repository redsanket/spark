package hadooptest;

import java.io.File;
import java.io.IOException;

import java.lang.reflect.Constructor;
import java.util.Enumeration;

import org.apache.hadoop.security.SecurityUtil;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import hadooptest.ConfigProperties;
import hadooptest.cluster.ClusterState;
import hadooptest.cluster.Executor;
import hadooptest.cluster.MultiClusterServer;
import hadooptest.cluster.hadoop.HadoopCluster;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedExecutor;
import hadooptest.cluster.hadoop.pseudodistributed.PseudoDistributedCluster;
import hadooptest.cluster.hadoop.pseudodistributed.PseudoDistributedExecutor;
import hadooptest.cluster.hadoop.standalone.StandaloneCluster;
import hadooptest.cluster.hadoop.standalone.StandaloneExecutor;

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
public abstract class TestSession {

	/** The Logger for the test session */
	public static Logger logger;

	/** The Hadoop Cluster to use for the test session */
	public static HadoopCluster cluster;
	
	/** The test session configuration properties */
	public static ConfigProperties conf;
	
	/** The process executor for the test session */
	public static Executor exec;
	
	/** The multi-cluster server host thread **/
	private static MultiClusterServer multiClusterServer;

	private static int MULTI_CLUSTER_PORT = 4444;
	
	/**
	 * In the JUnit architecture,
	 * this constructor will be called before every test
	 * (per JUnit).  Therefore, it is better to leave the
	 * constructor here empty and use start() to initialize
	 * the test session instead.
	 * 
	 * We can also use the TestSession constructor to execute
	 * instructions before each test, so the user doesn't have
	 * to code these instructions into every test.  However,
	 * this can be dangerous with JUnit as exceptions that occur
	 * in the TestSession constructor won't be caught by JUnit
	 * (and thus it won't run the @After for the test which
	 * triggers the exception).
	 */
	public TestSession() {
	}
	
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
		// Initialize the framework configuration
		initConfiguration();
		
		// Intitialize the framework logger
		initLogging();
		
		// Log Java Properties
		logger.debug("JAVA home ='" + System.getProperty("java.home") + "'");
		logger.debug("JAVA version ='" + System.getProperty("java.version") + "'");
		logger.debug("JVM bit size ='" + System.getProperty("sun.arch.data.model") + "'");
		logger.debug("JAVA native library path='" + System.getProperty("java.library.path") + "'");
		logger.debug("JAVA CLASSPATH='" + System.getProperty("java.class.path") + "'");
		
		// Initialize the cluster to be used in the framework
		initCluster();

		initSecurity();
		
		//initMultiClusterServer();
	}
	
	// stop() being implemented for multi-cluster support, so we can stop the
	// multi-cluster server thread at the end of the test session.
	//public static void stop() {
	//	multiClusterServer.stopServer();
	//}
	
	/**
	 * Get the Hadoop cluster instance for the test session.
	 * 
	 * @return HadoopCluster the Hadoop cluster instance for the test session.
	 */
	public static HadoopCluster getCluster() {
		return cluster;
	}
	
	/**
	 * Initialize the framework configuration.
	 */
	private static void initConfiguration() {
		File conf_location = null;
		conf = new ConfigProperties();
		
		String osName = System.getProperty("os.name");
		System.out.println("Operating System: " + osName);
		
		String userHome = System.getProperty("user.home");
		System.out.println("User home: " + userHome);
		
		String userName = System.getProperty("user.name");
		System.out.println("User name: " + userName);
		
		if (osName.contains("Mac OS X") || osName.contains("Linux")) {
			System.out.println("Detected OS is supported by framework.");
		}
		else {
			System.out.println("OS is not supported by framework: "  + osName);
		}
		
		String strFrameworkConfig = System.getProperty("hadooptest.config", userHome + "/hadooptest.conf");
		System.out.println("Framework configuration file location: " + strFrameworkConfig);
		System.out.println("To specify a different location, use -Dhadooptest.config=");
		
		conf_location = new File(strFrameworkConfig);

		try {
			conf.load(conf_location);

			Enumeration<Object> keys = conf.keys();
			while (keys.hasMoreElements()) {
			  String key = (String)keys.nextElement();
			  String value = (String)conf.get(key);
			  System.out.println(key + ": " + value);
			}
		} catch (IOException ioe) {
			System.out.println("Could not load the framework configuration file.");
		}

		/*
		 * Check for system properties that if specified should override the
		 * configuration file setting. For instance, For instance, user may use
		 * a common configuration file but with a different CLUSTER_NAME and/or
		 * WORKSPACE.
		 */
		String workspace = System.getProperty("WORKSPACE");
		if (workspace != null && !workspace.isEmpty()) {
			System.out.println("WORKSPACE: " + workspace);
			conf.setProperty("WORKSPACE", workspace);
		}
		String clusterName = System.getProperty("CLUSTER_NAME");
		if (clusterName != null && !clusterName.isEmpty()) {
			System.out.println("CLUSTER_NAME: " + clusterName);
			conf.setProperty("CLUSTER_NAME", clusterName);
		}
	}
	
	/**
	 * Initialize the framework logging.
	 */
	private static void initLogging() {
		logger = Logger.getLogger(TestSession.class);
		Level logLevel = Level.ALL;  // All logging is turned on by default	
		
		String strLogLevel = conf.getProperty("LOG_LEVEL");		
		
		if (strLogLevel.equals("OFF")) {
			logLevel = Level.OFF;
		}
		else if (strLogLevel.equals("ALL")) {
			logLevel = Level.ALL;
		}
		else if (strLogLevel.equals("DEBUG")) {
			logLevel = Level.DEBUG;
		}
		else if (strLogLevel.equals("ERROR")) {
			logLevel = Level.ERROR;
		}
		else if (strLogLevel.equals("FATAL")) {
			logLevel = Level.FATAL;
		}
		else if (strLogLevel.equals("INFO")) {
			logLevel = Level.INFO;
		}
		else if (strLogLevel.equals("TRACE")) {
			logLevel = Level.TRACE;
		}
		else if (strLogLevel.equals("WARN")) {
			logLevel = Level.WARN;
		}
		else {
			System.out.println("set to all ");
			logLevel = Level.ALL;
		}
		logger.setLevel(logLevel);
		logger.trace("Set log level to " + logLevel);
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
		else if (strClusterType.equals("hadooptest.cluster.hadoop.standalone.StandaloneCluster")) {
			exec = new StandaloneExecutor();
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
		else if (clusterObject instanceof StandaloneCluster) {
			cluster = (StandaloneCluster)clusterObject;
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
	
	/**
	 * Start listening for framework clients on other cluster gateways.
	 */
	private static void initMultiClusterServer() {
		logger.info("Starting MultiClusterServer on port: " + MULTI_CLUSTER_PORT);
		multiClusterServer = (new MultiClusterServer(MULTI_CLUSTER_PORT));
		multiClusterServer.start();
	}
	
}
