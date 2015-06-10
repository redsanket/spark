package hadooptest.cluster.storm;

import hadooptest.ConfigProperties;
import hadooptest.TestSessionStorm;
import hadooptest.automation.utils.http.JSONUtil;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.thrift7.TException;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.KillOptions;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.RebalanceOptions;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.SubmitOptions;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.utils.DRPCClient;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

/**
 * The current storm cluster, assumed to be installed through yinst.
 */
public class YahooStormCluster extends ModifiableStormCluster {
    private NimbusClient cluster;
    private ClusterUtil ystormConf = new ClusterUtil("ystorm");
    private ClusterUtil registryConf = new ClusterUtil("ystorm_registry");
    private String registryURI;
    private DRPCClient drpc;
    private HashMap<StormDaemon, ArrayList<String>> dnsNames = new HashMap<StormDaemon, ArrayList<String>>();

    private void initDnsNames() throws Exception {
        for (StormDaemon d : StormDaemon.values()) {
            if (ystormConf.clusterRoleConfExists()) {
                dnsNames.put(d, StormDaemon.lookupClusterRoles(d));
            } else {
                // Use Igor if a cluster configuration wasn't passed to the framework
                dnsNames.put(d, StormDaemon.lookupIgorRoles(d,
                        TestSessionStorm.conf.getProperty("CLUSTER_NAME")));
            }
        }
    }

    public void init(ConfigProperties conf) throws Exception {
        TestSessionStorm.logger.info("INIT CLUSTER");
        setupClient();
        ystormConf.init("ystorm");
        initDnsNames();
    }

    private void setupClient() throws Exception {
        TestSessionStorm.logger.info("SETUP CLIENT");
        backtype.storm.Config stormConf = new backtype.storm.Config();
        stormConf.putAll(Utils.readStormConfig());
        cluster = NimbusClient.getConfiguredClient(stormConf);
        List<String> servers = (List<String>) stormConf.get(backtype.storm.Config.DRPC_SERVERS);
        int port = Integer.parseInt(stormConf.get(backtype.storm.Config.DRPC_PORT).toString());
        drpc = new DRPCClient(stormConf, servers.get(0), port);
    }

    public void cleanup() {
        TestSessionStorm.logger.info("CLEANUP CLIENT");
        cluster.close();
        cluster = null;
        drpc.close();
        drpc = null;
    }

    public void submitTopology(File jar, String name, Map stormConf, StormTopology topology)
            throws AlreadyAliveException, InvalidTopologyException,
            AuthorizationException {

        synchronized (YahooStormCluster.class) {
            System.setProperty("storm.jar", jar.getPath());
            StormSubmitter.submitTopology(name, stormConf, topology);
        }
    }

    /*
        public void submitTopology(File jar, String name, Map stormConf,
                StormTopology topology, SubmitOptions opts)
                        throws AlreadyAliveException, InvalidTopologyException,
                        AuthorizationException {
    	
            synchronized (YahooStormCluster.class) {
                System.setProperty("storm.jar", jar.getPath());
                StormSubmitter.submitTopology(name, stormConf, topology, opts, null);
            }
        }
    */
    public void pushCredentials(String name, Map stormConf,
                                Map<String, String> credentials)
            throws NotAliveException, InvalidTopologyException,
            AuthorizationException {

        StormSubmitter.pushCredentials(name, stormConf, credentials);
    }

    public void killTopology(String name)
            throws NotAliveException, AuthorizationException, TException {

        cluster.getClient().killTopology(name);
    }

    public void killTopology(String name, KillOptions opts)
            throws NotAliveException, AuthorizationException, TException {

        cluster.getClient().killTopologyWithOpts(name, opts);
    }

    public void activate(String name)
            throws NotAliveException, AuthorizationException, TException {

        cluster.getClient().activate(name);
    }

    public void deactivate(String name)
            throws NotAliveException, AuthorizationException, TException {

        cluster.getClient().deactivate(name);
    }

    public void rebalance(String name, RebalanceOptions options)
            throws NotAliveException, InvalidTopologyException,
            AuthorizationException, TException {

        cluster.getClient().rebalance(name, options);
    }

    public String getNimbusConf() throws AuthorizationException, TException {
        return cluster.getClient().getNimbusConf();
    }

    public ClusterSummary getClusterInfo()
            throws AuthorizationException, TException {

        return cluster.getClient().getClusterInfo();
    }

    public TopologyInfo getTopologyInfo(String topologyId)
            throws NotAliveException, AuthorizationException, TException {

        return cluster.getClient().getTopologyInfo(topologyId);
    }

    public String getTopologyConf(String topologyId)
            throws NotAliveException, AuthorizationException, TException {

        return cluster.getClient().getTopologyConf(topologyId);
    }

    public StormTopology getTopology(String topologyId)
            throws NotAliveException, AuthorizationException, TException {
        return cluster.getClient().getTopology(topologyId);
    }

    public StormTopology getUserTopology(String topologyId)
            throws NotAliveException, AuthorizationException, TException {

        return cluster.getClient().getUserTopology(topologyId);
    }

    public void resetConfigs() throws Exception {
        TestSessionStorm.logger.info("RESET CONFIGS");
        if (!ystormConf.changed() && !registryConf.changed()) {
            return;
        }

        if (ystormConf.changed()) {
            ystormConf.resetConfigs();
        }

        if (registryConf.changed()) {
            registryConf.resetConfigs();
        }
    }

    public void resetConfigsAndRestart() throws Exception {
        TestSessionStorm.logger.info("RESET CONFIGS AND RESTART");
        resetConfigs();
        restartCluster();
    }

    public void restartCluster() throws Exception {
        TestSessionStorm.logger.info("*** RESTARTING CLUSTER ***");

        restartDaemon(StormDaemon.NIMBUS);
        restartDaemon(StormDaemon.UI);
        restartDaemon(StormDaemon.SUPERVISOR);
        restartDaemon(StormDaemon.LOGVIEWER);
        restartDaemon(StormDaemon.DRPC);

        Thread.sleep(120000);//TODO replace this with something to detect the cluster is up.
        cleanup();
        setupClient();
    }

    public void restartDaemon(StormDaemon daemon) throws Exception {

        TestSessionStorm.logger.info(
                "*** RESTARTING DAEMON ON ALL MEMBER NODES FOR DAEMON:  " +
                        daemon + " ***");

        // restart each node specified for that daemon in Igor config
        for (String nodeDNSName : dnsNames.get(daemon)) {
            restartDaemonNode(daemon, nodeDNSName);
        }
    }

    /**
     * Restart a daemon on a given node.
     *
     * @param hostname The hostname running the worker.
     * @param port     The port number the worker process is bound to.
     * @throws RuntimeException
     */
    public String getWorkerPid(String host, String port) throws Exception {
        String returnValue = null;

        String[] output = TestSessionStorm.exec.runProcBuilder(
                new String[]{"ssh", host, "ps", "uaxww"});

        if (!output[0].equals("0")) {
            TestSessionStorm.logger.info("Got unexpected non-zero exit code: " +
                    output[0]);
            TestSessionStorm.logger.info("stdout" + output[1]);
            TestSessionStorm.logger.info("stderr" + output[2]);
            throw new RuntimeException(
                    "ssh and ps returned an error code.");
        }

        String[] lines = output[1].split("\\r?\\n");
        String search = "-Dworker.port=" + port;
        for (int i = 0; i < lines.length; i++) {
            if (lines[i].contains(search)) {
                String[] args = lines[i].split(" +");
                return args[1];
            }
        }
        TestSessionStorm.logger.warn("Returning null");
        return returnValue;
    }

    /**
     * Deactivate yinst package for a daemon on a given node.
     *
     * @param daemon      The daemon to deactivate.
     * @param nodeDNSName The DNS name of the node to deactivate the daemon on.
     * @throws IOException
     * @throws InterruptedException
     */
    public String deactivateYinstPackageOnNode(StormDaemon daemon, String nodeDNSName)
            throws Exception {

        TestSessionStorm.logger.info("*** DEACTIVATING DAEMON:  " + daemon +
                " ON NODE:  " + nodeDNSName + " ***");

        String[] output = TestSessionStorm.exec.runProcBuilder(
                new String[]{"ssh", nodeDNSName, "yinst", "deactivate",
                        StormDaemon.getDaemonYinstString(daemon)});

        if (!output[0].equals("0")) {
            TestSessionStorm.logger.info("Got unexpected non-zero exit code: " +
                    output[0]);
            TestSessionStorm.logger.info("stdout" + output[1]);
            TestSessionStorm.logger.info("stderr" + output[2]);
            throw new RuntimeException(
                    "ssh and yinst returned an error code.");
        }

        // Parse the stdout for the full package name, with version and return it.
        Pattern p = Pattern.compile("(ystorm_.*-.*):.deact");
        Matcher regexMatcher = p.matcher(output[1]);

        String returnValue = null;
        if (regexMatcher.find()) {
            returnValue = regexMatcher.group(1);
            TestSessionStorm.logger.info("Package deactivated was " + returnValue);
        }

        return returnValue;
    }

    /**
     * Activate yinst package for a daemon on a given node.
     *
     * @param daemon      The daemon to activate.
     * @param nodeDNSName The DNS name of the node to activate the daemon on.
     * @throws IOException
     * @throws InterruptedException
     */
    public void activateYinstPackageOnNode(String myPackage, String nodeDNSName)
            throws Exception {

        TestSessionStorm.logger.info("*** ACTIVATING DAEMON:  " + myPackage  +
                " ON NODE:  " + nodeDNSName + " ***");

        String[] output = TestSessionStorm.exec.runProcBuilder(
                new String[]{"ssh", nodeDNSName, "yinst", "activate", myPackage});

        if (!output[0].equals("0")) {
            TestSessionStorm.logger.info("Got unexpected non-zero exit code: " +
                    output[0]);
            TestSessionStorm.logger.info("stdout" + output[1]);
            TestSessionStorm.logger.info("stderr" + output[2]);
            throw new RuntimeException(
                    "ssh and yinst returned an error code.");
        }
    }

    /**
     * Restart a daemon on a given node.
     *
     * @param daemon      The daemon to restart.
     * @param nodeDNSName The DNS name of the node to restart the daemon on.
     * @throws IOException
     * @throws InterruptedException
     */
    public void restartDaemonNode(StormDaemon daemon, String nodeDNSName)
            throws Exception {

        TestSessionStorm.logger.info("*** RESTARTING DAEMON:  " + daemon +
                " ON NODE:  " + nodeDNSName + " ***");

        String[] output = TestSessionStorm.exec.runProcBuilder(
                new String[]{"ssh", nodeDNSName, "yinst", "restart",
                        StormDaemon.getDaemonYinstString(daemon)});

        if (!output[0].equals("0")) {
            TestSessionStorm.logger.info("Got unexpected non-zero exit code: " +
                    output[0]);
            TestSessionStorm.logger.info("stdout" + output[1]);
            TestSessionStorm.logger.info("stderr" + output[2]);
            throw new RuntimeException(
                    "ssh and yinst returned an error code.");
        }
    }

    /**
     * Stop daemons on all nodes the daemon is acting on.
     *
     * @param daemon The daemon to stop.
     */
    public void stopDaemon(StormDaemon daemon) throws Exception {

        TestSessionStorm.logger.info(
                "*** STOPPING DAEMON ON ALL MEMBER NODES FOR DAEMON:  " +
                        daemon + " ***");

        // restart each node specified for that daemon in Igor config
        for (String nodeDNSName : dnsNames.get(daemon)) {
            stopDaemonNode(daemon, nodeDNSName);
        }
    }

    /**
     * Stop a daemon on a given node.
     *
     * @param daemon      The daemon to stop.
     * @param nodeDNSName The DNS name of the node to restart the daemon on.
     * @throws IOException
     * @throws InterruptedException
     */
    public void stopDaemonNode(StormDaemon daemon, String nodeDNSName)
            throws Exception {

        TestSessionStorm.logger.info("*** STOPPING DAEMON:  " + daemon +
                " ON NODE:  " + nodeDNSName + " ***");

        String[] output = TestSessionStorm.exec.runProcBuilder(
                new String[]{"ssh", nodeDNSName, "yinst", "stop",
                        StormDaemon.getDaemonYinstString(daemon)});

        if (!output[0].equals("0")) {
            TestSessionStorm.logger.info("Got unexpected non-zero exit code: " +
                    output[0]);
            TestSessionStorm.logger.info("stdout" + output[1]);
            TestSessionStorm.logger.info("stderr" + output[2]);
            throw new RuntimeException(
                    "ssh and yinst returned an error code.");
        }
    }

    /**
     * Start daemons on all nodes the daemon is acting on.
     *
     * @param daemon The daemon to start.
     */
    public void startDaemon(StormDaemon daemon) throws Exception {

        TestSessionStorm.logger.info(
                "*** STARTING DAEMON ON ALL MEMBER NODES FOR DAEMON:  " +
                        daemon + " ***");

        // restart each node specified for that daemon in Igor config
        for (String nodeDNSName : dnsNames.get(daemon)) {
            startDaemonNode(daemon, nodeDNSName);
        }
    }

    /**
     * Stop a daemon on a given node.
     *
     * @param daemon      The daemon to stop.
     * @param nodeDNSName The DNS name of the node to restart the daemon on.
     * @throws IOException
     * @throws InterruptedException
     */
    public void startDaemonNode(StormDaemon daemon, String nodeDNSName)
            throws Exception {

        TestSessionStorm.logger.info("*** STARTING DAEMON:  " + daemon +
                " ON NODE:  " + nodeDNSName + " ***");

        String[] output = TestSessionStorm.exec.runProcBuilder(
                new String[]{"ssh", nodeDNSName, "yinst", "start",
                        StormDaemon.getDaemonYinstString(daemon)});

        if (!output[0].equals("0")) {
            TestSessionStorm.logger.info("Got unexpected non-zero exit code: " +
                    output[0]);
            TestSessionStorm.logger.info("stdout" + output[1]);
            TestSessionStorm.logger.info("stderr" + output[2]);
            throw new RuntimeException(
                    "ssh and yinst returned an error code.");
        }
    }

    public Boolean turnOffNimbusSupervisor() {
        try {
            ArrayList<String> nimbusRoles = lookupRole(StormDaemon.NIMBUS);
            String nimbusMachine = nimbusRoles.get(0);
            ArrayList<String> supervisorRoles = lookupRole(StormDaemon.SUPERVISOR);
            if (supervisorRoles.contains(nimbusMachine)) {
                TestSessionStorm.logger.info("Nimbus node has a Supervisor running. Turning it off.");
                stopDaemonNode(StormDaemon.SUPERVISOR, nimbusMachine);
                return true;
            } else {
                TestSessionStorm.logger.info("Nimbus node did NOT have a Supervisor running. Will not try to turn it off.");
            }
        } catch (Exception ignore) {

        }
        return false;
    }

    public void turnOnNimbusSupervisor() {
        try {
            ArrayList<String> nimbusRoles = lookupRole(StormDaemon.NIMBUS);
            String nimbusMachine = nimbusRoles.get(0);
            TestSessionStorm.logger.info("Starting a Supervisor on the Nimbus node.");
            startDaemonNode(StormDaemon.SUPERVISOR, nimbusMachine);
        } catch (Exception ignore) {

        }
    }

    public void unsetConf(String key) throws Exception {
        ystormConf.unsetConf(key);
    }

    public void unsetConf(String key, StormDaemon daemon) throws Exception {
        ystormConf.unsetConf(key, daemon);
    }

    public Object getConf(String key, StormDaemon daemon) throws Exception {
        return ystormConf.getConf(key, daemon);
    }

    public void setConf(String key, Object value, StormDaemon daemon) throws Exception {
        ystormConf.setConf(key, value, daemon);
    }

    public void setConf(String key, Object value) throws Exception {
        ystormConf.setConf(key, value);
    }

    public void stopCluster() throws Exception {
        TestSessionStorm.logger.info("*** STOPPING CLUSTER ***");

        stopDaemon(StormDaemon.NIMBUS);
        stopDaemon(StormDaemon.UI);
        stopDaemon(StormDaemon.SUPERVISOR);
        stopDaemon(StormDaemon.LOGVIEWER);
        stopDaemon(StormDaemon.DRPC);

        cleanup();
    }

    public void startCluster() throws Exception {
        TestSessionStorm.logger.info("*** STARTING CLUSTER ***");

        startDaemon(StormDaemon.NIMBUS);
        startDaemon(StormDaemon.UI);
        startDaemon(StormDaemon.SUPERVISOR);
        startDaemon(StormDaemon.LOGVIEWER);
        startDaemon(StormDaemon.DRPC);

        Thread.sleep(30000);//TODO replace this with something to detect the cluster is up.
        cleanup();
        setupClient();
    }

    public void unsetRegistryConf(String key) throws Exception {
        registryConf.unsetConf(key);
    }

    public void setRegistryConf(String key, Object value) throws Exception {
        registryConf.setConf(key, value);
    }

    public void stopRegistryServer() throws Exception {
        TestSessionStorm.logger.info("*** STOPPING REGISTRY SERVER ***");
        stopDaemon(StormDaemon.REGISTRY);
    }

    /**
     * Performs an http get to an endpoing in the Registry Server REST API
     *
     * @param endpoint The endpoint to hit to stop.  URL will be VH + endpoint
     * @return String containing the data returned from the server
     */
    public String getFromRegistryServer(String endpoint) throws Exception {
        HttpClient client = new HttpClient();
        try {
            client.start();
        } catch (Exception e) {
            throw new IOException("Could not start Http Client", e);
        }
        String theURL = registryURI.split(",")[0] + endpoint;

        Request req = client.newRequest(theURL);
        ContentResponse resp = null;
        TestSessionStorm.logger.warn("Trying to get from " + theURL);
        resp = req.send();

        if (resp == null) {
            throw new IOException("Response was null");
        }

        if (resp != null && resp.getStatus() != 200) {
            throw new Exception("Response code " + Integer.toString(resp.getStatus()) + " was not 200.");
        }

        // Stop client
        try {
            client.stop();
        } catch (Exception e) {
            throw new IOException("Could not stop http client", e);
        }

        // Return the data returned from the get.
        if (resp == null) {
            return null;
        }
        return resp.getContentAsString();
    }

    /**
     * See if the virtual host exists on the server
     *
     * @param vhName The virutal host fqdn to test
     * @return boolean true if host is present in registry
     */
    public boolean isVirtualHostDefined(String vhName) {
        String endpoint = "virtualHost/" + vhName;
        try {
            String host = getFromRegistryServer(endpoint);
            TestSessionStorm.logger.info("Host Definition Get returned " + host);
            JSONUtil json = new JSONUtil();

            json.setContent(host);
            String fromJsonVH = json.getElement("virtualHost/name").toString();
            return fromJsonVH.equals(vhName);
        } catch (Exception e) {
            return false;
        }
    }

    public void startRegistryServer() throws Exception {

        TestSessionStorm.logger.info("*** STARTING REGISTRY SERVER ***");
        startDaemon(StormDaemon.REGISTRY);

        Thread.sleep(30000);//TODO replace this with something to detect the registry server is up.
        // Let's periodically poll the server's status api until it comes back clean or until we think it's been too long.

        // Configure the Jetty client to talk to the RS.  TODO:  Add API to the registry stub to do all this for us.....
        // Create and start client
        HttpClient client = new HttpClient();
        try {
            client.start();
        } catch (Exception e) {
            throw new IOException("Could not start Http Client", e);
        }

        // Get the URI to ping
        String statusURL = registryURI.split(",")[0] + "status/";

        // Let's try for 3 minutes, or until we get a 200 back.
        boolean done = false;
        int tryCount = 200;
        while (!done && tryCount > 0) {
            Thread.sleep(1000);
            Request req = client.newRequest(statusURL);
            ContentResponse resp = null;
            TestSessionStorm.logger.warn("Trying to get status at " + statusURL);
            try {
                resp = req.send();
            } catch (Exception e) {
                tryCount -= 1;
            }

            if (resp != null && resp.getStatus() == 200) {
                done = true;
            }
        }

        // Stop client
        try {
            client.stop();
        } catch (Exception e) {
            throw new IOException("Could not stop http client", e);
        }

        // Did we fail?
        if (!done) {
            throw new IOException(
                    "Timed out trying to get Registry Server Status\n");
        }
    }

    public void setRegistryServerURI(String uri) throws Exception {
        registryURI = uri;
        setRegistryConf("yarn_registry_uri", uri);
    }

    public Stream newDRPCStream(TridentTopology topology, String function) {
        return topology.newDRPCStream(function, null);
    }

    public String DRPCExecute(String func, String args) throws TException, DRPCExecutionException, AuthorizationException {
        return drpc.execute(func, args);
    }

    /**
     * Set the yinst configuration for DRPC authorization for running a
     * function securely.
     *
     * @param function The name of the function
     * @param user     The invocation user running the function
     * @throws Exception if there is a problem setting the yinst configuration
     */
    public void setDrpcInvocationAuthAclForFunction(String function, String user)
            throws Exception {

        setConf("drpc_auth_acl_" + function + "_invocation_user", user, StormDaemon.DRPC);
    }

    /**
     * Set the yinst configuration for DRPC authorization for running a
     * function securely.
     *
     * @param function The name of the function
     * @param user     The comma separated list of thrift clients or yca roles
     *                 allowed to access the function
     * @throws Exception if there is a problem setting the yinst configuration
     */
    public void setDrpcClientAuthAclForFunction(String function, String user)
            throws Exception {

        setConf("drpc_auth_acl_" + function + "_client_users", user, StormDaemon.DRPC);
    }

    /**
     * Set the yinst configuration for DRPC authorization for running a
     * function securely.
     *
     * @param function The name of the function
     * @param user     The comma separated list of thrift clients or yca roles
     *                 allowed to access the function
     * @param v1Role   The yca v1 role allowed to access the function
     * @throws Exception if there is a problem setting the yinst configuration
     */
    public void setDrpcAclForFunction(String function, String user, String v1Role)
            throws Exception {
        setDrpcInvocationAuthAclForFunction(function, user);
        setDrpcClientAuthAclForFunction(function, user + "," + v1Role);
    }

    /**
     * Set the yinst configuration for DRPC authorization for running a
     * function securely.
     *
     * @param function The name of the function
     * @param user     The comma separated list of thrift clients or yca roles
     *                 allowed to access the function
     *                 Default the v1 role to be based on the cluster name
     * @throws Exception if there is a problem setting the yinst configuration
     */
    public void setDrpcAclForFunction(String function, String user)
            throws Exception {
        String v1Role = "yahoo.grid_re.storm." + TestSessionStorm.conf.getProperty("CLUSTER_NAME");
        setDrpcAclForFunction(function, user, v1Role);
    }

    /**
     * Set the yinst configuration for DRPC authorization for running a
     * function securely.
     *
     * @param function The name of the function
     * @throws Exception if there is a problem setting the yinst configuration
     *                   <p/>
     *                   Make the default user hadoopqa and the default role based off of cluster name
     */
    public void setDrpcAclForFunction(String function)
            throws Exception {
        setDrpcAclForFunction(function, TestSessionStorm.conf.getProperty("USER"));
    }

    /**
     * Get the bouncer user used for testing.
     *
     * @throws Exception if there is a problem getting user name
     */
    public String getBouncerUser() throws Exception {
        String user = TestSessionStorm.conf.getProperty("DEFAULT_BOUNCER_USER");
        if ( user == null) {
            String[] output = TestSessionStorm.exec.runProcBuilder(
                    new String[]{"keydbgetkey", "hadoopqa_re_bouncer.user"});
            if (!output[0].equals("0")) {
                throw new IllegalStateException("keydbgetkey failed for user");
            }
            user = output[1].trim();
        }
        return user;
    }

    /**
     * Get the bouncer pw used for testing.
     *
     * @throws Exception if there is a problem getting user name
     */
    public String getBouncerPassword() throws Exception {
        String password = TestSessionStorm.conf.getProperty("DEFAULT_BOUNCER_PASSWORD");
        if (password == null) {
            String[] output = TestSessionStorm.exec.runProcBuilder(
                    new String[]{"keydbgetkey", "hadoopqa_re_bouncer.passwd"});
            if (!output[0].equals("0")) {
                throw new IllegalStateException("keydbgetkey failed for password");
            }
            password = output[1].trim();
        }
        return password;
    }

    /**
     * Get the list of machines that correspond to a role
     * 
     * @throws Exception if there is a problem getting role
     */
    public ArrayList<String> lookupRole(StormDaemon roleName) throws Exception {
        return dnsNames.get(roleName);
    }
}
