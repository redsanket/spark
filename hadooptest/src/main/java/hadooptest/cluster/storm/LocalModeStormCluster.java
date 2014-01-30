package hadooptest.cluster.storm;

import java.util.Map;
import java.io.File;

import hadooptest.ConfigProperties;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.*;

public class LocalModeStormCluster implements StormCluster {
    private LocalCluster cluster;

    public void init(ConfigProperties conf) {
        cluster = new LocalCluster();
        StormSubmitter.setLocalNimbus(cluster);
    }

    public void cleanup() {
        cluster.shutdown();
        cluster = null;
    }

    public void submitTopology(File jar, String name, Map stormConf, StormTopology topology) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
        synchronized (LocalModeStormCluster.class) {
            System.setProperty("storm.jar", jar.getPath());
            StormSubmitter.submitTopology(name, stormConf, topology);
        }
    }

    public void submitTopology(File jar, String name, Map stormConf, StormTopology topology, SubmitOptions opts) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
        synchronized (LocalModeStormCluster.class) {
            System.setProperty("storm.jar", jar.getPath());
            StormSubmitter.submitTopology(name, stormConf, topology, opts);
        }
    }

    public void pushCredentials(String name, Map stormConf, Map<String,String> credentials) throws NotAliveException, InvalidTopologyException, AuthorizationException {
        StormSubmitter.pushCredentials(name, stormConf, credentials);
    }

    public void killTopology(String name) throws NotAliveException, AuthorizationException {
        cluster.killTopology(name);
    }

    public void killTopology(String name, KillOptions opts) throws NotAliveException, AuthorizationException {
        cluster.killTopologyWithOpts(name, opts);
    }

    public void activate(String name) throws NotAliveException, AuthorizationException {
        cluster.activate(name);
    }

    public void deactivate(String name) throws NotAliveException, AuthorizationException {
        cluster.deactivate(name);
    }

    public void rebalance(String name, RebalanceOptions options) throws NotAliveException, InvalidTopologyException, AuthorizationException {
        cluster.rebalance(name, options);
    }

    public String getNimbusConf() throws AuthorizationException {
        //TODO
        throw new IllegalStateException("NOT IMPLEMENTED YET");
    }

    public ClusterSummary getClusterInfo() throws AuthorizationException {
        return cluster.getClusterInfo();
    }

    public TopologyInfo getTopologyInfo(String topologyId) throws NotAliveException, AuthorizationException {
        return cluster.getTopologyInfo(topologyId);
    }

    public String getTopologyConf(String topologyId) throws NotAliveException, AuthorizationException {
        return cluster.getTopologyConf(topologyId);
    }

    public StormTopology getTopology(String topologyId) throws NotAliveException, AuthorizationException {
        return cluster.getTopology(topologyId);
    }

    public StormTopology getUserTopology(String topologyId) throws NotAliveException, AuthorizationException {
        //TODO
        throw new IllegalStateException("NOT IMPLEMENTED YET");
    }
}
