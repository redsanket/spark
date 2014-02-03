package hadooptest;

import java.lang.reflect.Constructor;

import hadooptest.cluster.storm.StormCluster;
import hadooptest.cluster.storm.StormExecutor;
import hadooptest.config.storm.StormTestConfiguration;
import hadooptest.TestSessionCore;
import hadooptest.ConfigProperties;

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
public abstract class TestSessionStorm extends TestSessionCore {

    /** The Storm Cluster to use for the test session */
    public static StormCluster cluster;
    private static int count = 0;    

    /**
     * Initializes the test session in the following order:
     * initilizes framework configuration, initializes the
     * centralized logger, initializes the cluster reference.
     * 
     * This method should be called once from every subclass
     * of TestSession, in order to initialize the 
     * TestSession for a test class.
     */
    public static synchronized void start() throws Exception {
        if (cluster == null) {
            // Initialize the framework name
            initFrameworkName();
       
            // Initialize the framework configuration
            initConfiguration();
            if (conf.get("STORM_TEST_HOME") == null) {
                String sth = System.getProperty("stormtest.home");
                if (sth != null) {
                    conf.put("STORM_TEST_HOME", sth);
                } else {
                    throw new IllegalArgumentException("Could not find STORM_TEST_HOME");
                }
            }
        
            // Intitialize the framework logger
            initLogging();

            // Log Java Properties
            initLogJavaProperties();
        
            // Initialize the cluster to be used in the framework
            initCluster();
        } else if (count == 0) {
            initCluster();
        }
        count++;
    }
   
    public static synchronized void stop() throws Exception {
        count--;
        if (count == 0) {
            cleanupCluster();
        }
    }
 
    /**
     * Get the Storm cluster instance for the test session.
     * 
     * @return StormCluster the Storm cluster instance for the test session.
     */
    public static StormCluster getCluster() {
        return cluster;
    }
    
    /**
     * Initialize the framework name.
     */
    private static void initFrameworkName() {
        frameworkName = "stormtest";
    }
   
    private static void reinitCluster() throws Exception {
        cluster.init(conf);
    }
 
    private static void cleanupCluster() throws Exception {
        cluster.cleanup();
    }

    /**
     * Initialize the cluster instance for the framework.
     */
    private static void initCluster() throws Exception {
        // The unknown class type for the cluster
        Class<?> clusterClass = null;
        
        // The unknown constructor for the cluster class
        Constructor<?> clusterClassConstructor = null;
        
        // The unknown class type object instance for the cluster
        Object clusterObject = null;
        
        // Retrieve the cluster type from the framework configuration file.
        // This should be in the format of package.package.class
        String strClusterType = conf.getProperty("CLUSTER_TYPE", "stormtest.cluster.storm.LocalModeStormCluster");
        logger.info("Running with StormCluster "+strClusterType);
        exec = new StormExecutor();

        // Create a new instance of the cluster class specified in the 
        // framework configuration file.
        clusterClass = Class.forName(strClusterType);
        clusterClassConstructor = clusterClass.getConstructor();
        clusterObject = clusterClassConstructor.newInstance();
        
        // Initialize the test session cluster instance with the correct cluster type.
        if (clusterObject instanceof StormCluster) {
            cluster = (StormCluster)clusterObject;
            cluster.init(conf);
        }
        else {
            throw new IllegalArgumentException("The cluster type is not a StormCluster: " + strClusterType);
        }
    } 
}
