package hadooptest.hadoop.stress.ClusterUsage;

import hadooptest.TestSession;
import hadooptest.node.hadoop.HadoopNode;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.StringTokenizer;

import org.junit.BeforeClass;
import org.junit.Test;

public class TestClusterCpuUsage extends TestSession {
	
	@BeforeClass
	public static void startTestSession() {
		TestSession.start();
	}
	
	@Test
	public void ClusterCpuUsage() throws Exception {
		
		Hashtable<String, HadoopNode> allDNs = TestSession.getCluster().getNodes("datanode");
		if(allDNs.isEmpty()||allDNs.size() == 0){
			TestSession.logger.info("No datanodes for cluster.");
			return;
		}
		
		HashMap<String,String> dnsDomainMap = new HashMap<String,String>();
		
		StringBuffer dnsbuffer = new StringBuffer();
		for(String str:allDNs.keySet()){
			int dotIndex = str.indexOf(".");
			dnsDomainMap.put(str.substring(0,dotIndex), str.substring(dotIndex));
			dnsbuffer.append(str);
			dnsbuffer.append(",");
		}
		
		String dnsInStr = dnsbuffer.toString();
		dnsInStr = dnsInStr.substring(0,dnsInStr.length()-1);
		TestSession.logger.info("datanodes in string = "+dnsInStr);
		
		int refreshRate = Integer.parseInt(System.getProperty("TestClusterCpuUsage.refreshRate"));
		
		while(true){
			long start = System.currentTimeMillis();
			
			/*
			 * The top command calculates %CPU by looking at the change in CPU time values between samples.
			 * When you first run it, it has no previous sample to compare to,
			 * so these initial values are the percentages since boot.
			 * So we need to run it twice to get the instantaneous CPU usage
			 */
			String[] cpuCmd  = {"bash", "-c", "pdsh -u "+Integer.parseInt(System.getProperty("TestClusterCpuUsage.timeOutSec"))
					+" -w "+dnsInStr+" top -b -n2 -d"+Double.parseDouble(System.getProperty("TestClusterCpuUsage.topInterval"))};

			/*
			 * Doing pdsh on hosts separately would greatly increase running time
			 */
			HashMap<String,Double> dnsCpuUsage = GetCpuUsage(cpuCmd);

			int livednNum = dnsCpuUsage.keySet().size();
			double idleSum = 0;
			
			HashMap<String,String> dnsDomainMapCopy = new HashMap<String,String>(dnsDomainMap);
			for(String dnsBotLevelDomainName : dnsCpuUsage.keySet()){
				idleSum += dnsCpuUsage.get(dnsBotLevelDomainName);
				TestSession.logger.debug(dnsBotLevelDomainName+dnsDomainMapCopy.get(dnsBotLevelDomainName)
						+" CPU usage is "+ new DecimalFormat("##.##").format(100-dnsCpuUsage.get(dnsBotLevelDomainName))+"%.");

				if(dnsDomainMapCopy.containsKey(dnsBotLevelDomainName))
					dnsDomainMapCopy.remove(dnsBotLevelDomainName);
			}
			
			TestSession.logger.info("Cluster has "+livednNum+" live node(s).");
			TestSession.logger.debug("Cluster has "+dnsDomainMapCopy.keySet().size()+" busy or dead node(s).");
			for(String deaddn : dnsDomainMapCopy.keySet())
				TestSession.logger.debug(deaddn+dnsDomainMapCopy.get(deaddn)+" is busy or dead.");
				
			if(livednNum != 0)
				TestSession.logger.info("Average cpu usage is "+ new DecimalFormat("##.##").format(100-idleSum/(double)livednNum)+"%.");
			
			Thread.sleep(refreshRate*1000-(System.currentTimeMillis()-start) > 0 ? refreshRate*1000-(System.currentTimeMillis()-start):0);
			TestSession.logger.info("============= One Loop use "+(System.currentTimeMillis()-start)/1000F+" secs. =============");
		}
	}
	
	public HashMap<String,Double> GetCpuUsage(String[] cmd) throws IOException {
		
		Process p = Runtime.getRuntime().exec(cmd);
		HashMap<String,Double> cpus = new HashMap<String,Double>();
		HashSet<String> dns = new HashSet<String>();
        BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()));
		String line;
		String cpu = "";
		while ((line = r.readLine()) != null){
			if(!line.contains("%CPU")&&(line.contains("cpu")||line.contains("Cpu")||line.contains("CPU"))){
				StringTokenizer st = new StringTokenizer(line);
				String hostname = st.nextToken();				
				// if run into one host first time, ignore this iteration
				if(!dns.contains(hostname))
					dns.add(hostname);
				else{
					while (st.hasMoreTokens()){
						String cur = st.nextToken();
						if(cur.contains("idle")||cur.contains("id")){
							StringTokenizer tmpst = new StringTokenizer(cur,"%");	
							cpu = tmpst.nextToken();
							cpus.put(hostname.substring(0,hostname.length()-1),Double.parseDouble(cpu));
						}
					}
				}
			}
		}
		return cpus;
	}
}
