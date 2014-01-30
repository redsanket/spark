package hadooptest.cluster.storm;

import java.util.Map;
import java.io.File;

import hadooptest.ConfigProperties;

import backtype.storm.Config;
import backtype.storm.generated.*;
import org.apache.thrift7.TException;

public interface StormCluster {
    public void init(ConfigProperties conf) throws Exception;
    public void cleanup() throws Exception;

    public void submitTopology(File jar, String name, Map stormConf, StormTopology topology) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException, TException;
    public void submitTopology(File jar, String name, Map stormConf, StormTopology topology, SubmitOptions opts) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException, TException; 
    public void pushCredentials(String name, Map stormConf, Map<String,String> credentials) throws NotAliveException, InvalidTopologyException, AuthorizationException, TException;

    public void killTopology(String name) throws NotAliveException, AuthorizationException, TException;
    public void killTopology(String name, KillOptions opts) throws NotAliveException, AuthorizationException, TException;
    public void activate(String name) throws NotAliveException, AuthorizationException, TException;
    public void deactivate(String name) throws NotAliveException, AuthorizationException, TException;
    public void rebalance(String name, RebalanceOptions options) throws NotAliveException, InvalidTopologyException, AuthorizationException, TException;

    public String getNimbusConf() throws AuthorizationException, TException;
    public ClusterSummary getClusterInfo() throws AuthorizationException, TException;
    public TopologyInfo getTopologyInfo(String topologyId) throws NotAliveException, AuthorizationException, TException;
    public String getTopologyConf(String topologyId) throws NotAliveException, AuthorizationException, TException;
    public StormTopology getTopology(String topologyId) throws NotAliveException, AuthorizationException, TException;
    public StormTopology getUserTopology(String topologyId) throws NotAliveException, AuthorizationException, TException;
}
