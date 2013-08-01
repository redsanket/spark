package hadooptest.hadoop.stress.ClusterUsage;

import hadooptest.TestSession;
import hadooptest.node.hadoop.HadoopNode;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.StringTokenizer;

import org.junit.BeforeClass;
import org.junit.Test;

/*
 * refreshRate	: Info print frequency
 * NodeMode		: Print out Single node info
 * timeOut		: Time limit to print info
 */
public class TestClusterNetUsage extends TestSession {
	
	@BeforeClass
	public static void startTestSession() {
		TestSession.start();
	}
	
	@Test
	public void ClusterNetUsage() throws Exception {
		
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
		
		int refreshRate = System.getProperty("TestClusterNetUsage.refreshRate") == null? 0 : Integer.parseInt(System.getProperty("TestClusterNetUsage.refreshRate"));
		int timeOut = System.getProperty("TestClusterNetUsage.timeOutSec") == null? 10 : Integer.parseInt(System.getProperty("TestClusterNetUsage.timeOutSec"));
		boolean NodeMode = Boolean.parseBoolean(System.getProperty("TestClusterNetUsage.NodeMode"));
		
		while(true){
			long start = System.currentTimeMillis();
			
			HashMap<String, Float> Recv = new HashMap<String, Float>();
			HashMap<String, Float> Trans = new HashMap<String,Float>();
			String[] cpuCmd  = {"bash", "-c", "pdsh -u "+timeOut+" -w "+dnsInStr+" sar -n DEV 1 1"};

			GetNetUsage(cpuCmd,Recv,Trans);
			
			HashMap<String,String> dnsDomainMapCopy = new HashMap<String,String>(dnsDomainMap);
			float RecvSum = 0,TranSum = 0;
			for(String hostname : Recv.keySet()){
				RecvSum += Recv.get(hostname);
				TranSum += Trans.get(hostname);
				
				if(NodeMode){
					TestSession.logger.info(hostname.substring(0,hostname.length()-1)+" receive rate "+ConvertUnit.convertUnit(Recv.get(hostname))+"/s");
					TestSession.logger.info(hostname.substring(0,hostname.length()-1)+" transmit rate "+ConvertUnit.convertUnit(Trans.get(hostname))+"/s");
				}
				
				if(dnsDomainMapCopy.containsKey(hostname.substring(0,hostname.length()-1)))
					dnsDomainMapCopy.remove(hostname.substring(0,hostname.length()-1));
			}
			
			TestSession.logger.info("Cluster has "+Recv.size()+" live node(s).");
			TestSession.logger.debug("Cluster has "+dnsDomainMapCopy.keySet().size()+" busy or dead node(s).");
			if(NodeMode)
				for(String deaddn : dnsDomainMapCopy.keySet())
					TestSession.logger.debug(deaddn+dnsDomainMapCopy.get(deaddn)+" is busy or dead.");
			
			TestSession.logger.info("Total "+ConvertUnit.convertUnit(RecvSum)+" bytes received per second.");
			TestSession.logger.info("Total "+ConvertUnit.convertUnit(TranSum)+" bytes transmitted per second.");
			Thread.sleep(refreshRate*1000-(System.currentTimeMillis()-start) > 0 ? refreshRate*1000-(System.currentTimeMillis()-start):0);
			TestSession.logger.info("============= One Loop use "+(System.currentTimeMillis()-start)/1000F+" secs. =============");
		}
	}
	
	public void GetNetUsage(String[] cmd,HashMap<String,Float> Recv,HashMap<String,Float> Trans) throws IOException {
		
		Process p = Runtime.getRuntime().exec(cmd);
        BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()));
		String line;
		int rxbyt = 0;
		int txbyt = 0;
		while ((line = r.readLine()) != null){
			StringTokenizer st = new StringTokenizer(line);
			String hostname = st.nextToken();
			if(line.contains("Average"))	continue;
			else if(line.contains("rxbyt/s")&&line.contains("txbyt/s")){
				int index = 0;
				while (st.hasMoreTokens()){
					index++;
					String cur = st.nextToken();
					rxbyt = cur.contains("rxbyt/s")?index:rxbyt;
					txbyt = cur.contains("txbyt/s")?index:txbyt;
				}
			}else{
				int index = 0;
				while (st.hasMoreTokens()){
					index++;
					String cur = st.nextToken();
					if(index == rxbyt){
						if(!Recv.containsKey(hostname))
							Recv.put(hostname, Float.parseFloat(cur));
						else
							Recv.put(hostname, Recv.get(hostname)+Float.parseFloat(cur));
					}
					else if(index == txbyt){
						if(!Trans.containsKey(hostname))
							Trans.put(hostname,Float.parseFloat(cur));
						else
							Trans.put(hostname, Trans.get(hostname)+Float.parseFloat(cur));
					}
				}
			}
		}
	}
}
