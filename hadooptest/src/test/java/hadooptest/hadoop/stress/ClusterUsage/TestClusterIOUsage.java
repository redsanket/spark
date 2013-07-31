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
 * TestClusterIOUsage.timeOutSec:	Integer. Time limit for waiting for reply
 * TestClusterIOUsage.refreshRate: 	Integer. The longest refresh rate to get IO status. 
 * 	if it is shorter than minimum time consuming, it just do it as fast as possible
 */
public class TestClusterIOUsage extends TestSession {
	
	@BeforeClass
	public static void startTestSession() {
		TestSession.start();
	}
	
	@Test
	public void ClusterIOUsage() throws Exception {
		
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
		
		int refreshRate = System.getProperty("TestClusterIOUsage.refreshRate") == null? 0 : Integer.parseInt(System.getProperty("TestClusterIOUsage.refreshRate"));
		int timeOut = System.getProperty("TestClusterIOUsage.timeOutSec") == null? 10 : Integer.parseInt(System.getProperty("TestClusterIOUsage.timeOutSec"));
		
		
		while(true){
			long start = System.currentTimeMillis();
			
			/*
			 * The iostat command calculates cpu and IO by looking at the change in time values between samples.
			 * When you first run it, it has no previous sample to compare to,
			 * so these initial values are the percentages since boot.
			 * So we need to run it twice to get the instantaneous device IO usage
			 */
			String[] cpuCmd  = {"bash", "-c", "pdsh -u "+timeOut+" -w "+dnsInStr+" iostat -d 1 2"};

			HashMap<String, Double> Read = new HashMap<String, Double>();
			HashMap<String, Double> Write = new HashMap<String,Double>();
			/*
			 * Doing pdsh on hosts separately would greatly increase running time
			 */
			GetIOUsage(cpuCmd,Read,Write);
			
			HashMap<String,String> dnsDomainMapCopy = new HashMap<String,String>(dnsDomainMap);
			Double ReadSum = 0.0,WriteSum = 0.0;
			for(String hostname : Read.keySet()){
				
				ReadSum += Read.get(hostname);
				WriteSum += Write.get(hostname);
				TestSession.logger.info(hostname.substring(0,hostname.length()-1)+" read rate "+Read.get(hostname)+"/s");
				TestSession.logger.info(hostname.substring(0,hostname.length()-1)+" write rate "+Write.get(hostname)+"/s");
				
				if(dnsDomainMapCopy.containsKey(hostname.substring(0,hostname.length()-1)))
					dnsDomainMapCopy.remove(hostname.substring(0,hostname.length()-1));
			}
			
			TestSession.logger.info("Cluster has "+Read.size()+" live node(s).");
			TestSession.logger.debug("Cluster has "+dnsDomainMapCopy.keySet().size()+" busy or dead node(s).");
			for(String deaddn : dnsDomainMapCopy.keySet())
				TestSession.logger.debug(deaddn+dnsDomainMapCopy.get(deaddn)+" is busy or dead.");
			
			TestSession.logger.info("Total Read rate "+ReadSum+"/s.");
			TestSession.logger.info("Total Write rate "+WriteSum+"/s.");
			Thread.sleep(refreshRate*1000-(System.currentTimeMillis()-start) > 0 ? refreshRate*1000-(System.currentTimeMillis()-start):0);
			TestSession.logger.info("============= One Loop use "+(System.currentTimeMillis()-start)/1000F+" secs. =============");
		}
	}
	
	public void GetIOUsage(String[] cmd,HashMap<String, Double> Read,HashMap<String, Double> Write) throws IOException {
		
		Process p = Runtime.getRuntime().exec(cmd);
		HashMap<String,Boolean> dnsMap = new HashMap<String,Boolean>();
        BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()));
		String line;
		int Blk_read = 0;
		int Blk_wrtn = 0;
		int Device = 0;
		while ((line = r.readLine()) != null){
			
			StringTokenizer st = new StringTokenizer(line);
			String hostname = st.nextToken();

			if(line.contains("Device")&&!dnsMap.containsKey(hostname)){// deveice line first time
				dnsMap.put(hostname, false);
				// record label position
				int index = 0;
				while (st.hasMoreTokens()){
					index++;
					String cur = st.nextToken();
					Blk_read = cur.contains("Blk_read/s")?index:Blk_read;
					Blk_wrtn = cur.contains("Blk_wrtn/s")?index:Blk_wrtn;
					Device   = cur.contains("Device:")?index:Device;
				}
			}else if(line.contains("Device")&&dnsMap.containsKey(hostname)){// device line second time
				dnsMap.put(hostname,true);
			}else if(dnsMap.containsKey(hostname)&&(dnsMap.get(hostname) == true)){
				int index = 0;
				while (st.hasMoreTokens()){
					index++;
					String cur = st.nextToken();
					if(index == Device){
						// if it is the subdivision of the previous disk, ignore it
						if((cur.charAt(cur.length()-1) >='0') && (cur.charAt(cur.length()-1) <= '9'))	break;
					}else if(index == Blk_read){
						if(!Read.containsKey(hostname))
							Read.put(hostname, Double.parseDouble(cur));
						else
							Read.put(hostname, Read.get(hostname)+Double.parseDouble(cur));
					}
					else if(index == Blk_wrtn){
						if(!Write.containsKey(hostname))
							Write.put(hostname,Double.parseDouble(cur));
						else
							Write.put(hostname, Write.get(hostname)+Double.parseDouble(cur));
					}
				}
			}
		}
	}
}
