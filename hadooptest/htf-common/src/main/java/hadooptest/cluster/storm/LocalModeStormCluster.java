package hadooptest.cluster.storm;

import hadooptest.TestSessionStorm;

import hadooptest.ConfigProperties;

import java.io.File;
import java.util.ArrayList;
import java.util.Map;

import org.apache.thrift7.TException;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
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

public class LocalModeStormCluster extends StormCluster {
    private LocalCluster cluster;
    private LocalDRPC drpcServer=null;
    private String localHostname = null;

    public void init(ConfigProperties conf) {
        drpcServer = new LocalDRPC();
        cluster = new LocalCluster();
        StormSubmitter.setLocalNimbus(cluster);
        try {
            String[] returnValue = TestSessionStorm.exec.runProcBuilder(new String[] { "hostname" }, true);
            localHostname = returnValue[1];
        } catch (Exception e ) {
            localHostname = new String("localhost");
        }
    }

    public ArrayList<String> lookupRole(StormDaemon roleName) throws Exception {
        ArrayList<String> returnValue = new ArrayList<String>();
        returnValue.add( new String(localHostname) );
        return returnValue;
    }

    public void cleanup() {
        cluster.shutdown();
        drpcServer.shutdown();
        drpcServer = null;
        cluster = null;
    }

    public void submitTopology(File jar, String name, Map stormConf, StormTopology topology) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
        synchronized (LocalModeStormCluster.class) {
            System.setProperty("storm.jar", jar.getPath());
            StormSubmitter.submitTopology(name, stormConf, topology);
        }
    }

    /*
    public void submitTopology(File jar, String name, Map stormConf, StormTopology topology, SubmitOptions opts) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
        synchronized (LocalModeStormCluster.class) {
            System.setProperty("storm.jar", jar.getPath());
            StormSubmitter.submitTopology(name, stormConf, topology, opts, null);
        }
    }
    */

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

    public Stream newDRPCStream(TridentTopology topology, String function) {
        return topology.newDRPCStream(function, drpcServer);
    }

    public String DRPCExecute(String func, String args) throws TException, DRPCExecutionException, AuthorizationException {
        return drpcServer.execute(func, args);
    }
    
    public void setDrpcClientAuthAclForFunction(String function, String user) 
        throws Exception {} 
    
    /**
     * Does nothing for LocalModeStormCluster.  Only fully implemented for 
     * YahooStormCluster.
     * 
     * @param function The name of the function
     * @param user The user running the function
     * 
     * @throws Exception if there is a problem setting the yinst configuration
     */
    public void setDrpcInvocationAuthAclForFunction(String function, String user) 
            throws Exception {}
}
