package hadooptest.hadoop.stress.ClusterUsage;

import hadooptest.TestSession;
import hadooptest.node.hadoop.HadoopNode;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.StringTokenizer;

import org.junit.BeforeClass;
import org.junit.Test;

/*
 * This test is about the datanode memory usage.
 * Actually this is not that useful since not all memory is allocate to hadoop container.
 * Thus looking at webUI api Memory usage is more accurate
 */
public class TestClusterMemUsageCLI extends TestSession {
	
	final long kilo = 1024;
	final long mega = kilo*1024;
	final long giga = mega*1024;
	final long tera = giga*1024;
	
	@BeforeClass
	public static void startTestSession() {
		TestSession.start();
	}
	
	@Test
	public void ClusterMemUsage() throws Exception {
		
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
		
		int refreshRate = System.getProperty("TestClusterCpuUsage.refreshRate") == null? 0 : Integer.parseInt(System.getProperty("TestClusterMemUsage.refreshRate"));
		int timeOut = System.getProperty("TestClusterCpuUsage.timeOutSec") == null? 10 : Integer.parseInt(System.getProperty("TestClusterMemUsage.timeOutSec"));

		while(true){
			long start = System.currentTimeMillis();
			
			/*
			 * The -/+ buffers/cache line shows how much memory is used and free from the perspective of the applications. 
			 * http://linuxaria.com/howto/linux-memory-management?lang=en
			 * used free
			 */
			String[] memCmd  = {"bash", "-c", "pdsh -u "+ timeOut +" -w "+dnsInStr+" free"};
			HashMap<String,ArrayList<Long>> dnsMemUsage = GetMemUsage(memCmd);
			
			int livednNum = dnsMemUsage.keySet().size();
			HashMap<String,String> dnsDomainMapCopy = new HashMap<String,String>(dnsDomainMap);
			
			long free = 0;
			long used = 0;
			for(String dnsBotLevelDomainName : dnsMemUsage.keySet()){
				if(dnsDomainMapCopy.containsKey(dnsBotLevelDomainName))
					dnsDomainMapCopy.remove(dnsBotLevelDomainName);
				
				used += dnsMemUsage.get(dnsBotLevelDomainName).get(0);
				free += dnsMemUsage.get(dnsBotLevelDomainName).get(1);

				TestSession.logger.info(dnsBotLevelDomainName + " used "+convertUnit(dnsMemUsage.get(dnsBotLevelDomainName).get(0)));
				TestSession.logger.info(dnsBotLevelDomainName + " free "+convertUnit(dnsMemUsage.get(dnsBotLevelDomainName).get(1)));
			}

			
			TestSession.logger.info("Cluster has "+livednNum+" live node(s).");
			TestSession.logger.debug("Cluster has "+dnsDomainMapCopy.keySet().size()+" busy or dead node(s).");
			for(String deaddn : dnsDomainMapCopy.keySet())
				TestSession.logger.debug(deaddn+dnsDomainMapCopy.get(deaddn)+" is busy or dead.");
				
			if(livednNum != 0){
				TestSession.logger.info("Total Used Mem: "+convertUnit(used));
				TestSession.logger.info("Total Free Mem: "+convertUnit(free));
			}
			Thread.sleep(refreshRate*1000-(System.currentTimeMillis()-start) > 0 ? refreshRate*1000-(System.currentTimeMillis()-start):0);
			TestSession.logger.info("============= One Loop use "+(System.currentTimeMillis()-start)/1000F+" secs. =============");
		}
	}
	
	public String convertUnit(long input){
		String mem = Long.toString(input);
		if(input > tera)
			mem = Float.toString((float)input/(float)tera) + 'T';
		else if (input > giga)
			mem = Float.toString((float)input/(float)giga) + 'G';
		else if (input > mega)
			mem = Float.toString((float)input/(float)mega) + 'M';
		else if (input > kilo)
			mem = Float.toString((float)input/(float)kilo) + 'K';
		return mem;
	}
	
	public HashMap<String,ArrayList<Long>> GetMemUsage(String[] cmd) throws IOException {
		
		Process p = Runtime.getRuntime().exec(cmd);
		HashMap<String,ArrayList<Long>> mems = new HashMap<String,ArrayList<Long>>();
        BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()));
		String line;
		while ((line = r.readLine()) != null){
			if(line.contains("buffers/cache")){
				System.out.println(line);
				
				StringTokenizer st = new StringTokenizer(line);
				String hostname = st.nextToken();
				hostname = hostname.substring(0,hostname.length()-1);
				
				System.out.println(st.nextToken());
				System.out.println(st.nextToken());
				
				ArrayList<Long> tmp = new ArrayList<Long>();
				// since it is default to kilo bytes
				tmp.add(1024*Long.parseLong(st.nextToken()));
				tmp.add(1024*Long.parseLong(st.nextToken()));
				mems.put(hostname, tmp);
			}
		}
		return mems;
	}
}
