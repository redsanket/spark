package hadooptest.cluster.storm;

import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.io.File;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import hadooptest.ConfigProperties;

import org.apache.thrift7.TException;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.*;

import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

/**
 * The current storm cluster, assumed to be installed through yinst.
 */
public class YahooStormCluster implements ModifiableStormCluster {
    private NimbusClient cluster;
    private Map<String,String> origConf;
    private boolean confChanged;

    public void init(ConfigProperties conf) throws Exception {
        System.err.println("INIT CLUSTER");
        setupClient();
        origConf = getYinstConf();
        confChanged = false;
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
        if (!confChanged) {
            return;
        }
        //TODO allow this to run on multiple nodes
        Map<String,String> current = getYinstConf();
        HashSet<String> toRemove = new HashSet<String>(current.keySet());
        toRemove.removeAll(origConf.keySet());

        if (!toRemove.isEmpty()) {
            List<String> command = new ArrayList<String>();
            command.add("yinst");
            command.add("unset");
            command.addAll(toRemove);
            System.err.println("Running "+command);
            ProcessBuilder pb = new ProcessBuilder(command);
            pb.inheritIO();
            Process p = pb.start();
            if (p.waitFor() != 0) {
                throw new RuntimeException("yinst returned an error code "+p.exitValue());
            }
        }

        List<String> command = new ArrayList<String>();
        command.add("yinst");
        command.add("set");
        for (Map.Entry<String,String> entry: origConf.entrySet()) {
            command.add(entry.getKey()+"="+entry.getValue());
        }
        System.err.println("Running "+command);
        ProcessBuilder pb = new ProcessBuilder(command);
        pb.inheritIO();
        Process p = pb.start();
        if (p.waitFor() != 0) {
            throw new RuntimeException("yinst returned an error code "+p.exitValue());
        }
        confChanged = false;
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
        System.err.println("Unsetting "+key);
        confChanged = true;
        String strKey = "ystorm."+key.replace('.','_');
        //TODO for all of the nodes (May want something that allows for a specific class of node to be set)
        ProcessBuilder pb = new ProcessBuilder("yinst","unset",strKey);
        pb.inheritIO();
        Process p = pb.start();
        if (p.waitFor() != 0) {
            throw new RuntimeException("yinst returned an error code "+p.exitValue());
        }
    }

    public void setConf(String key, Object value) throws Exception {
        System.err.println("setting "+key+"="+value);
        confChanged = true;
        String strVal = value.toString();
        if (value instanceof Map) {
            StringBuffer b = new StringBuffer();
            for (Map.Entry<?,?> entry: ((Map<?,?>)value).entrySet()) {
                if (b.length() != 0) {
                    b.append(",");
                }
                b.append(entry.getKey().toString());
                b.append(",");
                b.append(entry.getValue().toString());
            }
            strVal = b.toString();
        } else if (value instanceof Iterable) {
            StringBuffer b = new StringBuffer();
            for (Object o: ((Iterable<?>)value)) {
                if (b.length() != 0) {
                    b.append(",");
                }
                b.append(o.toString());
            }
            strVal = b.toString();
        }

        String strKey = "ystorm."+key.replace('.','_');
        //TODO for all of the nodes (May want something that allows for a specific class of node to be set)
        ProcessBuilder pb = new ProcessBuilder("yinst","set",strKey+"="+strVal);
        pb.inheritIO();
        Process p = pb.start();
        if (p.waitFor() != 0) {
            throw new RuntimeException("yinst returned an error code "+p.exitValue());
        }
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

}
