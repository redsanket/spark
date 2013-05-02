package hadooptest.cluster.storm;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.thrift7.TException;

import hadooptest.TestSession;
import hadooptest.config.storm.StormTestConfiguration;

import backtype.storm.Config;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.ExecutorSummary;
import backtype.storm.generated.KillOptions;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.generated.TopologySummary;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

/**
 * @author bachbui
 *
 */
public class StormCluster {
	private static final String String = null;
//	Nimbus.Client client;
	Map<String, Object> config;
	Map<String, Object> nodeList;
	@SuppressWarnings("unchecked")
	public StormCluster(StormTestConfiguration testConfig){
		String stormConfigPath = testConfig.getStormConfigPath();
		String stormConfigFile = testConfig.getStormConfigFile();
		String classPath = System.getProperty("java.class.path");
		System.setProperty("java.class.path", classPath + ":" + stormConfigPath);
		System.setProperty("storm.conf.file", stormConfigFile);
		nodeList = testConfig.getNodeList();
    	config = Utils.readStormConfig();
    	
    	//TODO: the new API required to set task timeout, this bug will be removed later
    	config.put(Config.NIMBUS_TASK_TIMEOUT_SECS, 3600);
	}
	
	public Nimbus.Client getClient(){
		return NimbusClient.getConfiguredClient(config).getClient();
	}
	
	public Map<String, Object> getConfig(){
		return config;
	}
	
	public Map<String, Object> getNodeList() {
		return nodeList;
	}
	/**
	 * 
	 * @param topoName
	 * @return TopologyInfo
	 */
	public TopologyInfo getTopology(String topoName) throws AuthorizationException{
		try {
			ClusterSummary cluster = getClient().getClusterInfo();
			List<TopologySummary> topos = cluster.get_topologies();
			
			for(TopologySummary topo: topos){
				if (topo.get_name().equals(topoName)){
					return getClient().getTopologyInfo(topo.get_id());
				}
			}						
		} catch (NotAliveException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public void killIfExist(String topoName) throws AuthorizationException{
		try {
			ClusterSummary cluster = getClient().getClusterInfo();
			List<TopologySummary> topos = cluster.get_topologies();
			
			for(TopologySummary topo: topos){
				if (topo.get_name().equals(topoName)){
					KillOptions opts = new KillOptions();
					opts.set_wait_secs(0);
					getClient().killTopologyWithOpts(topoName, opts);
				}
			}						
		} catch (NotAliveException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	
	public ExecutorSummary getExecutor(TopologyInfo topo, String executorId) {
		ArrayList<ExecutorSummary> execs = getExecutors(topo, executorId);
		if (execs.size()>0)
			return execs.get(0);
		
		return null;
	}
	
	public ArrayList<ExecutorSummary> getExecutors(TopologyInfo topo, String executorId) {
		List<ExecutorSummary> executors = topo.get_executors();
		ArrayList<ExecutorSummary> result  = new ArrayList<ExecutorSummary>();
		for (ExecutorSummary exec: executors){
			if (exec.get_component_id().equals(executorId)){
				result.add(exec);
			}				
		}		
		return result;
	}

	public String getNimbus(){
		return (String) config.get("nimbus.host");
	}
	
	public Boolean setAll(String key, String value) throws Exception {
		return set_nimbus(key, value) && set_supervisors(key, value) && set_ui(key, value);
	}

	public Boolean set(String host, String key, String value) throws Exception {
		String command = java.lang.String.format("yinst set %s=%s", key, value);
		
		TestSession.exec.runProcBuilder(new String[] { "ssh", host, command });
		
		return true;
	}
    
	public String get(String host, String key) {
		return null;		 		
	}
	
	public Boolean set_nimbus(String key, String value) throws Exception{
		String host = (String) nodeList.get("nimbus");
		String yinst_key = "ystorm_nimbus."+key.replace(".", "_");
		return set(host, yinst_key, value);
	}
	
	public Boolean set_supervisors(String key, String value) throws Exception{
		List<String> hosts = (List<String>) nodeList.get("supervisors");
		String yinst_key = "ystorm_supervisor."+key.replace(".", "_");
		Boolean res = true;
		
		for (String host: hosts){
			res &= set(host, yinst_key, value);
		}
		return res;
	}
	
	public Boolean set_ui(String key, String value) throws Exception {
		String host = (String) nodeList.get("ui");
		String yinst_key = "ystorm_ui."+key.replace(".", "_");
		return set(host, yinst_key, value);
	}
	
	public Boolean stop_nimbus() throws Exception {
		String host = (String) nodeList.get("nimbus");
		String command = java.lang.String.format("yinst stop ystorm_nimbus");
		
		TestSession.exec.runProcBuilder(new String[] { "ssh", host, command });
		
		return true;	
	}
	
	public Boolean start_nimbus() throws Exception{
		String host = (String) nodeList.get("nimbus");
		String command = java.lang.String.format("yinst start ystorm_nimbus");
		
		TestSession.exec.runProcBuilder(new String[] { "ssh", host, command });
		
		return true;	
	}
	
	public Boolean stop_ui() throws Exception {
		String nimbus_host = (String) config.get("nimbus.host");
		String command = java.lang.String.format("yinst stop ystorm_ui", nimbus_host);
		
		TestSession.exec.runProcBuilder(new String[] { "ssh", nimbus_host, command });
		
		return true;	
	}
	
	public Boolean start_ui() throws Exception {
		String nimbus_host = (String) config.get("nimbus.host");
		String command = java.lang.String.format("yinst start ystorm_ui", nimbus_host);

		TestSession.exec.runProcBuilder(new String[] { "ssh", nimbus_host, command });
		
		return true;	
	}
	
	public Boolean restart_ui() throws Exception {
		return stop_ui() && start_ui();
	}
	
	public Boolean restart_nimbus() throws Exception {
		Boolean res = stop_nimbus();
		res &= start_nimbus();
		return res;
	}
	
	public Boolean stop_supervisors() throws Exception {
		List<String> hosts = (List<String>) nodeList.get("supervisors");
		
		for (String host: hosts){
			String command = String.format("yinst stop ystorm_supervisor", host);
			TestSession.exec.runProcBuilder(new String[] { "ssh", host, command });
		}
		return true;		
	}
	
	public Boolean start_supervisors() throws Exception{
		List<String> hosts = (List<String>) nodeList.get("supervisors");
		
		for (String host: hosts){
			String command = String.format("yinst start ystorm_supervisor", host);
			TestSession.exec.runProcBuilder(new String[] { "ssh", host, command });
		}
		return true;		
	}
	
	public Boolean restart_supervisors() throws Exception {
		Boolean res = stop_supervisors() && start_supervisors();
		return res;
	}
	
	public Boolean start_all() throws Exception {
		return start_nimbus() && start_supervisors() && start_ui();
	}
	
	public Boolean stop_all() throws Exception {
		return stop_supervisors() && stop_nimbus() && stop_ui();
	}
}
