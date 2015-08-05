package hadooptest.cluster.storm;

import hadooptest.ConfigProperties;
import hadooptest.TestSessionStorm;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.thrift7.TException;
import org.yaml.snakeyaml.Yaml;

import storm.trident.Stream;
import storm.trident.TridentTopology;
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

public abstract class StormCluster {
    public abstract void init(ConfigProperties conf) throws Exception;
    public abstract void cleanup() throws Exception;

    public abstract void submitTopology(File jar, String name, Map stormConf, StormTopology topology) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException, TException;
    //public abstract void submitTopology(File jar, String name, Map stormConf, StormTopology topology, SubmitOptions opts) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException, TException; 
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
    public abstract Stream newDRPCStream(TridentTopology topology, String function);
    public abstract String DRPCExecute(String func, String args) throws TException, DRPCExecutionException, AuthorizationException;
    public abstract void setDrpcClientAuthAclForFunction(String function, String user) throws Exception;
    public abstract void setDrpcInvocationAuthAclForFunction(String function, String user) throws Exception; 
    public abstract void setDrpcAclForFunction(String function, String user, String v1role) throws Exception;
    public abstract void setDrpcAclForFunction(String function, String user) throws Exception;
    public abstract void setDrpcAclForFunction(String function) throws Exception;
    public abstract ArrayList<String> lookupRole(StormDaemon roleName) throws Exception; 
    
    /**
     * Lookup a cluster role and get the role members.
     * 
     * @param roleName The name of the role.
     * 
     * @return ArrayList<String> a list of all of the role members.
     * 
     * @throws Exception
     */
    public static ArrayList<String> lookupYamlRole(StormDaemon role) throws Exception {
        
        if (role == StormDaemon.ALL) {
            return lookupYamlClusterRoleAllNodes();
        }
        
        ArrayList<String> roleMembers = new ArrayList<String>();
        
        TestSessionStorm.logger.debug(
                "*** Fetching cluster for role: " + 
                        role.toString() + " ***");
        
        String configFile = null;
        configFile = System.getProperty("STORM_CLUSTER_CONF");
        if (configFile == null) {
            configFile = TestSessionStorm.conf.getProperty("CLUSTER_CONF");
        }
        
        TestSessionStorm.logger.debug(
                "*** Fetching cluster configuration from: " + 
                        configFile + " ***");
        
        Yaml yaml = new Yaml(); 
        InputStream mainConfigStream;
        mainConfigStream = new FileInputStream(new File(configFile));
        
        HashMap<String, Object> config = null;
        if(mainConfigStream!=null){
            config = (HashMap<String, Object>) yaml.load(mainConfigStream);
        }
        
        ArrayList<String> members = (ArrayList<String>) config.get(role.toString());
        
        TestSessionStorm.logger.debug(members);
        
        for(String s: members) {
            s = s.replaceAll("(\n|\r|\t)", " ");
            String[] splitString = s.split("\\s+");
            
            for(String t: splitString) {
                if (t.contains("yahoo.com")) {
                    roleMembers.add(t.trim());
                }
            }
        }
        
        TestSessionStorm.logger.info("*********************");
        TestSessionStorm.logger.info("Role members are:");
        for(String m: roleMembers) {
            TestSessionStorm.logger.info(m);
        }
        TestSessionStorm.logger.info("*********************");
        
        return roleMembers;
    }
    
    /**
     * Lookup the role members for a cluster.
     * 
     * @return ArrayList<String> the nodes in the cluster.
     *
     * @throws Exception
     */
    public static ArrayList<String> lookupYamlClusterRoleAllNodes() 
            throws Exception {
        ArrayList<String> tmp;
        ArrayList<String> ret = lookupYamlClusterRoleGateway();
        
        tmp = lookupYamlClusterRoleContrib();
        ret.removeAll(tmp);
        ret.addAll(tmp);
        
        tmp = lookupYamlClusterRoleDrpc();
        ret.removeAll(tmp);
        ret.addAll(tmp);
        
        tmp = lookupYamlClusterRoleNimbus();
        ret.removeAll(tmp);
        ret.addAll(tmp);
        
        tmp = lookupYamlClusterRoleRegistry();
        ret.removeAll(tmp);
        ret.addAll(tmp);
        
        tmp = lookupYamlClusterRoleSupervisor();
        ret.removeAll(tmp);
        ret.addAll(tmp);
        
        tmp = lookupYamlClusterRoleUI();
        ret.removeAll(tmp);
        ret.addAll(tmp);

        TestSessionStorm.logger.debug("*** CLUSTER NODES ARE: ***");
        TestSessionStorm.logger.debug(ret);
        
        return ret;
    }

    /**
     * Lookup the gateway node for a cluster.
     * 
     * @return ArrayList<String> the contrib nodes.
     * 
     * @throws Exception
     */
    public static ArrayList<String> lookupYamlClusterRoleGateway() 
            throws Exception {
        return lookupYamlRole(StormDaemon.GATEWAY);
    }
    
    /**
     * Lookup the contrib node for a cluster.
     * 
     * @return ArrayList<String> the contrib nodes.
     * 
     * @throws Exception
     */
    public static ArrayList<String> lookupYamlClusterRoleContrib() 
            throws Exception {
        return lookupYamlRole(StormDaemon.CONTRIB);
    }
    
    /**
     * Lookup the DRPC node for a cluster.
     * 
     * @return ArrayList<String> the DRPC nodes.
     * 
     * @throws Exception
     */
    public static ArrayList<String> lookupYamlClusterRoleDrpc() 
            throws Exception {
        return lookupYamlRole(StormDaemon.DRPC);
    }
    
    /**
     * Lookup the Nimbus node for a cluster.
     * 
     * @return ArrayList<String> the Nimbus nodes.
     * 
     * @throws Exception
     */
    public static ArrayList<String> lookupYamlClusterRoleNimbus() 
            throws Exception {
        return lookupYamlRole(StormDaemon.NIMBUS);
    }
    
    /**
     * Lookup the Registry node for a cluster.
     * 
     * @return ArrayList<String> the Registry nodes.
     * 
     * @throws Exception
     */
    public static ArrayList<String> lookupYamlClusterRoleRegistry() 
            throws Exception {
        return lookupYamlRole(StormDaemon.REGISTRY);
    }

    /**
     * Lookup the Pacemaker node for a cluster.
     * 
     * @return ArrayList<String> the Pacemaker nodes.
     * 
     * @throws Exception
     */
    public static ArrayList<String> lookupYamlClusterRolePacemaker() 
            throws Exception {
        return lookupYamlRole(StormDaemon.PACEMAKER);
    }
    
    
    /**
     * Lookup the Supervisor node for a cluster.
     * 
     * @return ArrayList<String> the Supervisor nodes.
     * 
     * @throws Exception
     */
    public static ArrayList<String> lookupYamlClusterRoleSupervisor() 
            throws Exception {
        return lookupYamlRole(StormDaemon.SUPERVISOR);
    }
    
    /**
     * Lookup the UI node for a cluster.
     * 
     * @return ArrayList<String> the UI nodes.
     * 
     * @throws Exception
     */
    public static ArrayList<String> lookupYamlClusterRoleUI() 
            throws Exception {
        return lookupYamlRole(StormDaemon.UI);
    }
    
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
		
		TestSessionStorm.logger.debug(
				"*** Fetching role members from Igor for role: " + 
						roleName + " ***");
		String[] members = TestSessionStorm.exec.runProcBuilder(
				new String[] {"yinst", "range", "-ir", "@" + roleName});
		TestSessionStorm.logger.debug(members);
		
		for(String s: members) {
			s = s.replaceAll("(\n|\r|\t)", " ");
			String[] splitString = s.split("\\s+");
			
			for(String t: splitString) {
				if (t.contains("yahoo.com")) {
					roleMembers.add(t.trim());
				}
			}
		}
		
		TestSessionStorm.logger.info("*********************");
		TestSessionStorm.logger.info("Igor role members are:");
		for(String m: roleMembers) {
			TestSessionStorm.logger.info(m);
		}
		TestSessionStorm.logger.info("*********************");
		
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
