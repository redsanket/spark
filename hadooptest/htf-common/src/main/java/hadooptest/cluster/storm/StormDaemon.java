package hadooptest.cluster.storm;

import java.util.ArrayList;

/**
 * An enumerated type that can represent the daemon type of each node in a
 * Storm cluster
 */
public enum StormDaemon {
	
	ALL, NIMBUS, UI, SUPERVISOR, LOGVIEWER, REGISTRY, CONTRIB, DRPC;

	/**
	 * Get the yinst package name for a particular Storm daemon.
	 * 
	 * @param daemon The daemon type.
	 * 
	 * @return String the yinst package name corresponding to the daemon type.
	 */
	public static String getDaemonYinstString(StormDaemon daemon) {
    	String daemonString = "";
    	
    	if (daemon == StormDaemon.NIMBUS) {
    		daemonString = "ystorm_nimbus";
    	}
    	else if (daemon == StormDaemon.UI) {
    		daemonString = "ystorm_ui";
    	}
    	else if (daemon == StormDaemon.SUPERVISOR) {
    		daemonString = "ystorm_supervisor";
    	}
    	else if (daemon == StormDaemon.LOGVIEWER) {
    		daemonString = "ystorm_logviewer";
    	}
    	else if (daemon == StormDaemon.REGISTRY) {
    		daemonString = "ystorm_registry";
    	}
    	else if (daemon == StormDaemon.CONTRIB) {
    		daemonString = "ystorm_contrib";
    	}
    	else if (daemon == StormDaemon.DRPC) {
    		daemonString = "ystorm_drpc";
    	}
    	
    	return daemonString;
	}
	
	/**
	 * Get the list of DNS names stored in Igor, that correspond to a given
	 * Storm daemon type.
	 * 
	 * @param daemon The daemon type to lookup.
	 * @param clusterName The name of the Storm cluster in Igor.
	 * 
	 * @return A list of DNS names that correspond to the daemon type.
	 * 
	 * @throws Exception
	 */
	public static ArrayList<String> lookupIgorRoles(StormDaemon daemon, 
			String clusterName) throws Exception {
		
    	ArrayList<String> dnsNames = null;
    	
    	// lookup dns names of all nodes for specified daemon
    	if (daemon.equals(StormDaemon.NIMBUS)) {
    		dnsNames = StormCluster.lookupIgorRoleClusterNimbus(clusterName);
    	}
    	else if (daemon.equals(StormDaemon.UI)) {
    		dnsNames = StormCluster.lookupIgorRoleClusterUI(clusterName);
    	}
    	else if (daemon.equals(StormDaemon.SUPERVISOR)) {
    		dnsNames = StormCluster.lookupIgorRoleClusterSupervisor(clusterName);
    	}
    	else if (daemon.equals(StormDaemon.LOGVIEWER)) {
    		dnsNames = StormCluster.lookupIgorRoleClusterSupervisor(clusterName);
    	}
    	else if (daemon.equals(StormDaemon.REGISTRY)) {
    		dnsNames = StormCluster.lookupIgorRoleClusterRegistry(clusterName);
    	}
    	else if (daemon.equals(StormDaemon.CONTRIB)) {
    		dnsNames = StormCluster.lookupIgorRoleClusterContrib(clusterName);
    	}
    	else if (daemon.equals(StormDaemon.DRPC)) {
    		dnsNames = StormCluster.lookupIgorRoleClusterDrpc(clusterName);
    	}
    	else if (daemon.equals(StormDaemon.ALL)) {
    		dnsNames = StormCluster.lookupIgorRoleClusterAllNodes(clusterName);
    	}
    	
    	return dnsNames;
	}
} 
