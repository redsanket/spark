package hadooptest.cluster.storm;

import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.io.File;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

import hadooptest.ConfigProperties;
import hadooptest.cluster.storm.ClusterUtil;
import hadooptest.TestSessionStorm;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpRequest;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;


import org.apache.thrift7.TException;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.*;

import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The current storm cluster, assumed to be installed through yinst.
 */
public class YahooStormCluster extends ModifiableStormCluster {
    private NimbusClient cluster;
    private ClusterUtil ystormConf = new ClusterUtil("ystorm");
    private ClusterUtil registryConf = new ClusterUtil("ystorm_registry");
    private String registryURI;

    public void init(ConfigProperties conf) throws Exception {
        System.err.println("INIT CLUSTER");
        setupClient();
	ystormConf.init("ystorm");
    }

    private void setupClient() throws Exception {
        System.err.println("SETUP CLIENT");
        Map stormConf = Utils.readStormConfig();
        cluster = NimbusClient.getConfiguredClient(stormConf);
    }

    public void cleanup() {
        System.err.println("CLEANUP CLIENT");
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
        System.err.println("RESET CONFIGS AND RESTART");
        if (!ystormConf.changed()) {
            return;
        }

	ystormConf.resetConfigs();

        restartCluster();
    }

    public void restartCluster() throws Exception {
        //TODO allow this to run on multiple nodes
        System.err.println("RESTARTING...");
        ProcessBuilder pb = new ProcessBuilder("yinst","restart","ystorm_nimbus","ystorm_ui","ystorm_supervisor","ystorm_logviewer");
        pb.inheritIO();
        Process p = pb.start();
        if (p.waitFor() != 0) {
            throw new RuntimeException("yinst returned an error code "+p.exitValue());
        }
        Thread.sleep(120000);//TODO replace this with something to detect the cluster is up.
        cleanup();
        setupClient();
    }

    public void unsetConf(String key) throws Exception {
	ystormConf.unsetConf(key);
    }

    public void setConf(String key, Object value) throws Exception {
	ystormConf.setConf(key, value);
    }

    public void stopCluster() throws Exception {
        System.err.println("STOPPING CLUSTER YINST"); 
        //TODO allow this to run on multiple nodes
        ProcessBuilder pb = new ProcessBuilder("yinst","stop","ystorm_nimbus","ystorm_ui","ystorm_supervisor","ystorm_logviewer");
        pb.inheritIO();
        Process p = pb.start();
        if (p.waitFor() != 0) {
            throw new RuntimeException("yinst returned an error code "+p.exitValue());
        }
        cleanup();
    }

    public void startCluster() throws Exception {
        System.err.println("STARTING CLUSTERi YINST"); 
        //TODO allow this to run on multiple nodes
        ProcessBuilder pb = new ProcessBuilder("yinst","start","ystorm_nimbus","ystorm_ui","ystorm_supervisor","ystorm_logviewer");
        pb.inheritIO();
        Process p = pb.start();
        if (p.waitFor() != 0) {
            throw new RuntimeException("yinst returned an error code "+p.exitValue());
        }
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
        System.err.println("STOPPING REGISTRY SERVER"); 
        //TODO allow this to run on multiple nodes
        ProcessBuilder pb = new ProcessBuilder("yinst","stop","ystorm_registry");
        pb.inheritIO();
        Process p = pb.start();
        if (p.waitFor() != 0) {
            throw new RuntimeException("yinst returned an error code "+p.exitValue());
        }
    }

    public void startRegistryServer() throws Exception {
        System.err.println("STARTING REGISTRY SERVER"); 
        //TODO allow this to run on multiple nodes
        ProcessBuilder pb = new ProcessBuilder("yinst","start","ystorm_registry");
        pb.inheritIO();
        Process p = pb.start();
        if (p.waitFor() != 0) {
            throw new RuntimeException("yinst returned an error code "+p.exitValue());
        }
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
