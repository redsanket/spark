package hadooptest.cluster.storm;

import hadooptest.ConfigProperties;
import hadooptest.TestSessionStorm;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.thrift7.TException;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;

import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.KillOptions;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.RebalanceOptions;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.SubmitOptions;
import backtype.storm.generated.TopologyInfo;
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

    public void init(ConfigProperties conf) throws Exception {
    	TestSessionStorm.logger.info("INIT CLUSTER");
        setupClient();
        ystormConf.init("ystorm");
    }

    private void setupClient() throws Exception {
        TestSessionStorm.logger.info("SETUP CLIENT");
        Map stormConf = Utils.readStormConfig();
        cluster = NimbusClient.getConfiguredClient(stormConf);
    }

    public void cleanup() {
    	TestSessionStorm.logger.info("CLEANUP CLIENT");
        cluster.close();
        cluster = null;
    }

    public void submitTopology(File jar, String name, Map stormConf, StormTopology topology) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
        synchronized (YahooStormCluster.class) {
            System.setProperty("storm.jar", jar.getPath());
            StormSubmitter.submitTopology(name, stormConf, topology);
        }
    }

    public void submitTopology(File jar, String name, Map stormConf, StormTopology topology, SubmitOptions opts) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
        synchronized (YahooStormCluster.class) {
            System.setProperty("storm.jar", jar.getPath());
            StormSubmitter.submitTopology(name, stormConf, topology, opts);
        }
    }

    public void pushCredentials(String name, Map stormConf, Map<String,String> credentials) throws NotAliveException, InvalidTopologyException, AuthorizationException {
        StormSubmitter.pushCredentials(name, stormConf, credentials);
    }

    public void killTopology(String name) throws NotAliveException, AuthorizationException, TException {
        cluster.getClient().killTopology(name);
    }

    public void killTopology(String name, KillOptions opts) throws NotAliveException, AuthorizationException, TException {
        cluster.getClient().killTopologyWithOpts(name, opts);
    }

    public void activate(String name) throws NotAliveException, AuthorizationException, TException {
        cluster.getClient().activate(name);
    }

    public void deactivate(String name) throws NotAliveException, AuthorizationException, TException {
        cluster.getClient().deactivate(name);
    }

    public void rebalance(String name, RebalanceOptions options) throws NotAliveException, InvalidTopologyException, AuthorizationException, TException {
        cluster.getClient().rebalance(name, options);
    }

    public String getNimbusConf() throws AuthorizationException, TException {
        return cluster.getClient().getNimbusConf();
    }

    public ClusterSummary getClusterInfo() throws AuthorizationException, TException {
        return cluster.getClient().getClusterInfo();
    }

    public TopologyInfo getTopologyInfo(String topologyId) throws NotAliveException, AuthorizationException, TException {
        return cluster.getClient().getTopologyInfo(topologyId);
    }

    public String getTopologyConf(String topologyId) throws NotAliveException, AuthorizationException, TException {
        return cluster.getClient().getTopologyConf(topologyId);
    }

    public StormTopology getTopology(String topologyId) throws NotAliveException, AuthorizationException, TException {
        return cluster.getClient().getTopology(topologyId);
    }

    public StormTopology getUserTopology(String topologyId) throws NotAliveException, AuthorizationException, TException {
        return cluster.getClient().getUserTopology(topologyId);
    }

    public static Map<String,String> getYinstConf() throws Exception {
        //TODO save stats for multiple nodes
        Map<String,String> ret = new HashMap<String,String>();
        ProcessBuilder pb = new ProcessBuilder("yinst", "set", "ystorm");
        pb.redirectError(ProcessBuilder.Redirect.INHERIT);
        Process p = pb.start();
        BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
        String line;
        while ((line = reader.readLine()) != null) {
            String[] parts = line.split(": ", 2);
            if (parts.length != 2) {
                throw new IllegalArgumentException("Error parsing yinst output "+line);
            }
            ret.put(parts[0], parts[1]);
        }
        if (p.waitFor() != 0) {
            throw new RuntimeException("yinst returned an error code "+p.exitValue());
        }
        return ret;
    }

    public void resetConfigsAndRestart() throws Exception {
    	TestSessionStorm.logger.info("RESET CONFIGS AND RESTART");
        if (!ystormConf.changed()) {
            return;
        }

        ystormConf.resetConfigs();

        restartCluster();
    }

    public void restartCluster() throws Exception {
    	restartDaemon(StormDaemon.NIMBUS);
    	restartDaemon(StormDaemon.UI);
    	restartDaemon(StormDaemon.SUPERVISOR);
    	restartDaemon(StormDaemon.LOGVIEWER);
        restartDaemon(StormDaemon.DRPC);

        Thread.sleep(120000);//TODO replace this with something to detect the cluster is up.
        cleanup();
        setupClient();
    }
    
    /**
     * Restart daemons on all nodes the daemon is acting on.
     * 
     * @param daemon The daemon to restart.
     */
    public void restartDaemon(StormDaemon daemon) throws Exception {
    	
    	ArrayList<String> dnsNames = 
    			StormDaemon.lookupIgorRoles(daemon, 
    					TestSessionStorm.conf.getProperty("CLUSTER_NAME"));
    	
    	// restart each node specified for that daemon in Igor config
    	for (String nodeDNSName: dnsNames) {
    		restartDaemonNode(daemon, nodeDNSName);
    	}
    }
    
    /**
     * Restart a daemon on a given node.
     * 
     * @param daemon The daemon to restart.
     * @param nodeDNSName The DNS name of the node to restart the daemon on.
     * 
     * @throws IOException
     * @throws InterruptedException
     */
    public void restartDaemonNode(StormDaemon daemon, String nodeDNSName) 
    		throws Exception {
    	
    	String[] output = TestSessionStorm.exec.runProcBuilder(new String[] {"ssh", nodeDNSName, "yinst", 
    			"restart", StormDaemon.getDaemonYinstString(daemon) } );
    	
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

    	ArrayList<String> dnsNames = 
    			StormDaemon.lookupIgorRoles(daemon, 
    					TestSessionStorm.conf.getProperty("CLUSTER_NAME"));
    	
    	// restart each node specified for that daemon in Igor config
    	for (String nodeDNSName: dnsNames) {
    		stopDaemonNode(daemon, nodeDNSName);
    	}
    }
    
    /**
     * Stop a daemon on a given node.
     * 
     * @param daemon The daemon to stop.
     * @param nodeDNSName The DNS name of the node to restart the daemon on.
     * @throws IOException
     * @throws InterruptedException
     */
    public void stopDaemonNode(StormDaemon daemon, String nodeDNSName) 
    		throws Exception {
        
    	String[] output = TestSessionStorm.exec.runProcBuilder(new String[] {"ssh", nodeDNSName, "yinst",
    			"stop", StormDaemon.getDaemonYinstString(daemon) } );
    	
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

    	ArrayList<String> dnsNames = 
    			StormDaemon.lookupIgorRoles(daemon, 
    					TestSessionStorm.conf.getProperty("CLUSTER_NAME"));
    	
    	// restart each node specified for that daemon in Igor config
    	for (String nodeDNSName: dnsNames) {
    		startDaemonNode(daemon, nodeDNSName);
    	}
    }
    
    /**
     * Stop a daemon on a given node.
     * 
     * @param daemon The daemon to stop.
     * @param nodeDNSName The DNS name of the node to restart the daemon on.
     * @throws IOException
     * @throws InterruptedException
     */
    public void startDaemonNode(StormDaemon daemon, String nodeDNSName) 
    		throws Exception {
        
    	String[] output = TestSessionStorm.exec.runProcBuilder(new String[] {"ssh", nodeDNSName, "yinst",
    			"start", StormDaemon.getDaemonYinstString(daemon) } );
    	
		if (!output[0].equals("0")) {
			TestSessionStorm.logger.info("Got unexpected non-zero exit code: " + 
					output[0]);
			TestSessionStorm.logger.info("stdout" + output[1]);
			TestSessionStorm.logger.info("stderr" + output[2]);	
            throw new RuntimeException(
            		"ssh and yinst returned an error code.");		
		}
    }
    
    public void unsetConf(String key) throws Exception {
	ystormConf.unsetConf(key);
    }

    public void setConf(String key, Object value) throws Exception {
	ystormConf.setConf(key, value);
    }

    public void stopCluster() throws Exception {
    	TestSessionStorm.logger.info("STOPPING CLUSTER YINST");
        
        stopDaemon(StormDaemon.NIMBUS);
        stopDaemon(StormDaemon.UI);
        stopDaemon(StormDaemon.SUPERVISOR);
        stopDaemon(StormDaemon.LOGVIEWER);
        stopDaemon(StormDaemon.DRPC);

        cleanup();
    }

    public void startCluster() throws Exception {
    	TestSessionStorm.logger.info("STARTING CLUSTER YINST"); 

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
        TestSessionStorm.logger.info("STOPPING REGISTRY SERVER");
    	stopDaemon(StormDaemon.REGISTRY);
    }

    public void startRegistryServer() throws Exception {

        TestSessionStorm.logger.info("STARTING REGISTRY SERVER");
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
    	String statusURL = registryURI + "status/";

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
    		throw new IOException("Timed out trying to get Registry Server Status\n");
    	}
    }

    public void setRegistryServerURI(String uri) throws Exception {
    	registryURI = uri;
    	setRegistryConf("yarn_registry_uri", uri);
    }
}
