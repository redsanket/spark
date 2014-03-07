package hadooptest.cluster.storm;

import java.util.ArrayList;
import java.util.Map;
import java.io.File;

import hadooptest.ConfigProperties;
import hadooptest.TestSessionStorm;

import backtype.storm.Config;
import backtype.storm.generated.*;
import org.apache.thrift7.TException;

public abstract class StormCluster {
    public abstract void init(ConfigProperties conf) throws Exception;
    public abstract void cleanup() throws Exception;

    public abstract void submitTopology(File jar, String name, Map stormConf, StormTopology topology) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException, TException;
    public abstract void submitTopology(File jar, String name, Map stormConf, StormTopology topology, SubmitOptions opts) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException, TException; 
    public abstract void pushCredentials(String name, Map stormConf, Map<String,String> credentials) throws NotAliveException, InvalidTopologyException, AuthorizationException, TException;

    public abstract void killTopology(String name) throws NotAliveException, AuthorizationException, TException;
    public abstract void killTopology(String name, KillOptions opts) throws NotAliveException, AuthorizationException, TException;
    public abstract void activate(String name) throws NotAliveException, AuthorizationException, TException;
    public abstract void deactivate(String name) throws NotAliveException, AuthorizationException, TException;
    public abstract void rebalance(String name, RebalanceOptions options) throws NotAliveException, InvalidTopologyException, AuthorizationException, TException;

    public abstract String getNimbusConf() throws AuthorizationException, TException;
    public abstract ClusterSummary getClusterInfo() throws AuthorizationException, TException;
    public abstract TopologyInfo getTopologyInfo(String topologyId) throws NotAliveException, AuthorizationException, TException;
    public abstract String getTopologyConf(String topologyId) throws NotAliveException, AuthorizationException, TException;
    public abstract StormTopology getTopology(String topologyId) throws NotAliveException, AuthorizationException, TException;
    public abstract StormTopology getUserTopology(String topologyId) throws NotAliveException, AuthorizationException, TException;
    
    /**
     * Lookup an Igor role and get the role members.
     * 
     * @param roleName The name of the role.
     * 
     * @return ArrayList<String> a list of all of the role members.
     * 
     * @throws Exception
     */
	public static ArrayList<String> lookupIgorRole(String roleName) throws Exception {
		ArrayList<String> roleMembers = new ArrayList<String>();
		
		TestSessionStorm.logger.debug("Fetching role members from Igor for role: " + roleName);
		String[] members = TestSessionStorm.exec.runProcBuilder(new String[] {"yinst", "range", "-ir", "@" + roleName});
		TestSessionStorm.logger.debug(members);
		
		for(String s: members) {
			if (s.contains("ygrid.yahoo.com")) {
				roleMembers.add(s.trim());
			}
		}
		
		return roleMembers;
	}
	
	/**
	 * Lookup the Igor role members for a cluster.
	 * 
	 * @param clusterName The name of the cluster.
	 * 
	 * @return ArrayList<String> the nodes in the cluster.
     *
	 * @throws Exception
	 */
	public static ArrayList<String> lookupIgorRoleClusterAllNodes(String clusterName) 
			throws Exception {
		return lookupIgorRole("grid_re.storm." + clusterName);
	}
	
	/**
	 * Lookup the Igor-defined contrib node for a cluster.
	 * 
	 * @param clusterName The name of the cluster.
	 * 
	 * @return ArrayList<String> the contrib nodes.
	 * 
	 * @throws Exception
	 */
	public static ArrayList<String> lookupIgorRoleClusterContrib(String clusterName) 
			throws Exception {
		return lookupIgorRole("grid_re.storm." + clusterName + ".contrib");
	}
	
	/**
	 * Lookup the Igor-defined DRPC node for a cluster.
	 * 
	 * @param clusterName The name of the cluster.
	 * 
	 * @return ArrayList<String> the DRPC nodes.
	 * 
	 * @throws Exception
	 */
	public static ArrayList<String> lookupIgorRoleClusterDrpc(String clusterName) 
			throws Exception {
		return lookupIgorRole("grid_re.storm." + clusterName + ".drpc");
	}
	
	/**
	 * Lookup the Igor-defined Nimbus node for a cluster.
	 * 
	 * @param clusterName The name of the cluster.
	 * 
	 * @return ArrayList<String> the Nimbus nodes.
	 * 
	 * @throws Exception
	 */
	public static ArrayList<String> lookupIgorRoleClusterNimbus(String clusterName) 
			throws Exception {
		return lookupIgorRole("grid_re.storm." + clusterName + ".nimbus");
	}
	
	/**
	 * Lookup the Igor-defined Registry node for a cluster.
	 * 
	 * @param clusterName The name of the cluster.
	 * 
	 * @return ArrayList<String> the Registry nodes.
	 * 
	 * @throws Exception
	 */
	public static ArrayList<String> lookupIgorRoleClusterRegistry(String clusterName) 
			throws Exception {
		return lookupIgorRole("grid_re.storm." + clusterName + ".registry");
	}
	
	/**
	 * Lookup the Igor-defined Supervisor node for a cluster.
	 * 
	 * @param clusterName The name of the cluster.
	 * 
	 * @return ArrayList<String> the Supervisor nodes.
	 * 
	 * @throws Exception
	 */
	public static ArrayList<String> lookupIgorRoleClusterSupervisor(String clusterName) 
			throws Exception {
		return lookupIgorRole("grid_re.storm." + clusterName + ".supervisor");
	}
	
	/**
	 * Lookup the Igor-defined UI node for a cluster.
	 * 
	 * @param clusterName The name of the cluster.
	 * 
	 * @return ArrayList<String> the UI nodes.
	 * 
	 * @throws Exception
	 */
	public static ArrayList<String> lookupIgorRoleClusterUI(String clusterName) 
			throws Exception {
		return lookupIgorRole("grid_re.storm." + clusterName + ".ui");
	}
}
