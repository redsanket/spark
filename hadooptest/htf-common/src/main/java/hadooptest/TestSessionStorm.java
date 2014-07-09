package hadooptest;

import backtype.storm.generated.TopologySummary;
import backtype.storm.generated.KillOptions;

import hadooptest.cluster.storm.StormCluster;
import hadooptest.cluster.storm.StormExecutor;

import java.io.File;
import java.lang.reflect.Constructor;
import java.nio.file.Paths;

import org.junit.BeforeClass;

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

    public static void killAll() throws Exception {
        boolean killedOne = false;
        if (cluster != null) {
            KillOptions killOpts = new KillOptions();
            killOpts.setFieldValue(KillOptions._Fields.WAIT_SECS, 0);
            for (TopologySummary ts: cluster.getClusterInfo().get_topologies()) {
                System.out.println("Killing " + ts.get_name());
                cluster.killTopology(ts.get_name(), killOpts);
                killedOne = true;
            }
        } else {
                System.out.println(" killAll : cluster is null ");
        }
        if (killedOne) {
            Util.sleep(10);
        }
    }

    /*
     * Run before the start of each test class.
     */
    @BeforeClass
    public static void startTestSession() throws Exception {
        System.out.println("--------- @BeforeClass: TestSession: startTestSession ---------------------------");
        start();
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
    public static synchronized void start() throws Exception {
        // Pass the caller class name
        printBanner(Thread.currentThread().getStackTrace()[2].getClassName());
        
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

        // Kill any running topologies
        killAll();
    }
   
    public static synchronized void stop() throws Exception {
        cleanupCluster();
    }
 
    /**
     * Get the Storm cluster instance for the test session.
     * 
     * @return StormCluster the Storm cluster instance for the test session.
     */
    public static StormCluster getCluster() {
        return cluster;
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
        String strClusterType = conf.getProperty("CLUSTER_TYPE", "hadooptest.cluster.storm.LocalModeStormCluster");
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

    public File getTopologiesJarFile() {
        return Paths.get(conf.getProperty("WORKSPACE"), "topologies",
                "target","topologies-1.0-SNAPSHOT-jar-with-dependencies.jar")
                .toFile();
    }

    protected TopologySummary getTS(String name) throws Exception {
        for (TopologySummary ts: cluster.getClusterInfo().get_topologies()) {
            if (name.equals(ts.get_name())) {
                return ts;
            }
        }
        throw new IllegalArgumentException("Topology "+name+""
                + "does not appear to be up yet");
    }

    protected String getFirstTopoIdForName(final String topoName)
            throws Exception {
        return getTS(topoName).get_id();
    }

    protected void waitForTopoUptimeSeconds(final String topoId,
            int waitSeconds) throws Exception {
        int uptime = 0;
        while ((uptime = cluster.getTopologyInfo(topoId).get_uptime_secs())
                < waitSeconds) {
            Util.sleep(waitSeconds - uptime);
        }
    }
}
