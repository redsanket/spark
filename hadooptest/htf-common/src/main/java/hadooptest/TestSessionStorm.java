package hadooptest;

import backtype.storm.generated.TopologySummary;
import backtype.storm.generated.KillOptions;
import backtype.storm.generated.TopologyInfo;
import org.json.simple.JSONValue;
import hadooptest.automation.utils.http.HTTPHandle;
import hadooptest.automation.utils.http.Response;
import hadooptest.cluster.storm.StormCluster;
import hadooptest.cluster.storm.StormExecutor;
import hadooptest.cluster.storm.ModifiableStormCluster;

import java.io.File;
import java.io.FileInputStream;
import java.io.DataInputStream;
import java.io.FileOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;
import java.lang.reflect.Constructor;
import java.nio.file.Paths;
import java.net.URLEncoder;
import org.apache.commons.httpclient.HttpMethod;
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

    public static void makeRandomTempFile(String filename, int numMb) throws Exception {
        makeRandomTempFile(filename, numMb, false);
    }

    public static void makeRandomTempFile(String filename, int numMb, Boolean forceCreate ) throws Exception {
        if (!forceCreate) {
            try {
                File theFile = new File(filename);
                if ( theFile != null) {
                    // See if it is all there or not.
                    Long sizeInBytes = new Long(numMb) * 1024 * 1024;
                    logger.info("Testing file size >="+sizeInBytes);
                    Long fileLength = theFile.length();
                    logger.info("File is "+ fileLength + " bytes");
                    if ( fileLength >= sizeInBytes ) {
                        return;
                    }
                }
            } catch (Exception ignore) {
                // Ignoring exceptions, will just recreate file
            }
        }
        String[] ddReturnValue = exec.runProcBuilder(new String[] {
                "dd", "if=/dev/urandom", "of=/tmp/OneMbRand", "bs=1M", "count=1" }, true);
        if (!ddReturnValue[0].equals("0")) {
            throw new IOException("Could not create random one megabyte temp file at " +
                    "/tmp/OneMbRand" + " of size " + "1 Mb" );
        }

        FileInputStream fin;
        DataInputStream din;
        FileOutputStream fout;
        DataOutputStream dout;
        byte[] b = new byte [1024*1024];
        try {
            fin = new FileInputStream ("/tmp/OneMbRand");
            din = new DataInputStream(fin);
            din.read(b, 0, 1024*1024);
            fin.close();
            fout = new FileOutputStream (filename);
            dout = new DataOutputStream(fout);
            for (int reps=0; reps < numMb; reps++) {
                dout.write(b);
            }
            fout.close();
            File fileIn = new File("/tmp/OneMbRand");
            fileIn.delete();
        } catch (Exception e)
        {
            throw (new RuntimeException(e));
        }
    }

    //Turn on/off the supervisor running on the same node as Nimbus
    public static Boolean turnOffNimbusSupervisor() {
        ModifiableStormCluster mc;
        if (cluster instanceof ModifiableStormCluster) {
            mc = (ModifiableStormCluster) cluster;            //Only do this for modifiable cluster
            if (mc != null) {
                logger.info("In TestSessionStorm, about to call mc.turnOffNimbusSupervisor()");
                return mc.turnOffNimbusSupervisor();
            }
        }
        return false;
    }

    //Turn on/off the supervisor running on the same node as Nimbus
    public static void turnOnNimbusSupervisor() {
        ModifiableStormCluster mc;
        if (cluster instanceof ModifiableStormCluster) {
            mc = (ModifiableStormCluster) cluster;            //Only do this for modifiable cluster
            if (mc != null) {
                mc.turnOnNimbusSupervisor();
            }
        }
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

    protected String getId(String name) throws Exception {
        TopologySummary ts = getTS( name );

        return ts.get_id();
    }

    protected int getUptime(String name) throws Exception {
        return getTS(name).get_uptime_secs();
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

    protected String getLogForTopology(String topoName, Integer executor) throws Exception {
        final String topoId = getFirstTopoIdForName(topoName);

        // Worker Host
        TopologyInfo ti = cluster.getTopologyInfo(topoId);
        String host = ti.get_executors().get(executor).get_host();
        int workerPort = ti.get_executors().get(executor).get_port();

        // Logviewer port on worker host
        String jsonStormConf = cluster.getNimbusConf();
        @SuppressWarnings("unchecked")
        Map<String, Object> stormConf =
                (Map<String, Object>) JSONValue.parse(jsonStormConf);
        Integer logviewerPort = backtype.storm.utils.Utils.getInt(stormConf
                .get(backtype.storm.Config.LOGVIEWER_PORT));

        ModifiableStormCluster mc;
        mc = (ModifiableStormCluster)cluster;

        backtype.storm.Config theconf = new backtype.storm.Config();
        theconf.putAll(backtype.storm.utils.Utils.readStormConfig());

        String filter = (String)theconf.get("ui.filter");
        String pw = null;
        String user = null;

        // Only get bouncer auth on secure cluster.
        if ( filter != null ) {
            if (mc != null) {
                user = mc.getBouncerUser();
                pw = mc.getBouncerPassword();
            }
        }

        HTTPHandle client = new HTTPHandle();
        if (filter != null) {
            client.logonToBouncer(user,pw);
        }
        logger.info("Cookie = " + client.YBYCookie);

        String getURL = "http://" + host + ":" + logviewerPort +
                "/download/" + URLEncoder.encode(topoId + "/" + workerPort + "/worker.log", "UTF-8");
        logger.info("URL to get is: " + getURL);
        HttpMethod getMethod = client.makeGET(getURL, new String(""), null);
        Response response = new Response(getMethod, false);
        return response.getResponseBodyAsString();
    }

    protected String getLogForTopology(String topoName) throws Exception {
        return getLogForTopology( topoName, 0 );
    }

    protected void kinit(String keytab, String principal) throws Exception {
        logger.debug("About to kinit to " + principal + " with keytab " + keytab );
        String[] kinitReturnValue = exec.runProcBuilder(new String[] { "kinit", "-kt", keytab, principal }, true);
        if (!kinitReturnValue[0].equals("0")) {
            throw new IllegalArgumentException("Could not kinit to " + principal + " from keytab " + keytab );
        }

        // We need to sleep to make sure that the ticket cache file is written before we use it
        Util.sleep(10);

        // Debug.  Let's do a klist and see what's there
        String[] klistReturnValue = exec.runProcBuilder(new String[] { "klist" }, true);
        logger.info("Principal is now " + klistReturnValue[1]);
    }
    
    protected void kinit() throws Exception {
        kinit(conf.getProperty("DEFAULT_KEYTAB"), conf.getProperty("DEFAULT_PRINCIPAL") );
    }
}
