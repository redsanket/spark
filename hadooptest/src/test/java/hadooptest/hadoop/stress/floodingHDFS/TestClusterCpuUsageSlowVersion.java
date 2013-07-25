package hadooptest.hadoop.stress.floodingHDFS;

import hadooptest.TestSession;
import hadooptest.node.hadoop.HadoopNode;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Set;
import java.util.StringTokenizer;

import org.junit.BeforeClass;
import org.junit.Test;

public class TestClusterCpuUsageSlowVersion extends TestSession {
	
	@BeforeClass
	public static void startTestSession() {
		TestSession.start();
	}

	
	@Test
	public void ClusterUsage() throws Exception {
		Hashtable<String, HadoopNode> ret = TestSession.getCluster().getNodes("datanode");
		int freq = Integer.parseInt(System.getProperty("TestClusterUsage.refreshFreq"));
		while(true){
			long start = System.currentTimeMillis();
			
			/*
			 * The top command calculates %CPU by looking at the change
			 * in CPU time values between samples. When you first run it, it has no previous
			 * sample to compare to, so these initial values are the percentages since boot.
			 * So we need to run it twice to get the instantaneous CPU usage
			 */
			int dnNum = ret.keySet().size();
			double sum = 0;
			for(String dn:ret.keySet()){
				String[] cpuCmd  = {"bash", "-c", "pdsh -w "+dn+" top -b n2"};
				double idle = GetCpuUsagePerHost(cpuCmd);
				if(idle == -1){
					TestSession.logger.debug(dn+" is down, not have CPU usage.");
					dnNum--;
					continue;
				}
				sum += idle;
			}
			TestSession.logger.info("There are "+dnNum+" datanodes.");
			TestSession.logger.info("Average cpu idle is "+sum/(double)dnNum+"%");
			TestSession.logger.info("Average cpu usage is "+(100-sum/(double)dnNum)+"%");
			Thread.sleep(freq*1000);
			long elapsedTimeMillis = System.currentTimeMillis()-start;
			float elapsedTimeSec = elapsedTimeMillis/1000F;
			TestSession.logger.info("One Loop use "+elapsedTimeSec+" secs.");
		}
	}
	
	public double GetCpuUsagePerHost(String[] cmd) throws IOException {
		
		Process p = Runtime.getRuntime().exec(cmd);
        BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()));
		String line;
		String cpu= "";
		int count = 0;
		while ((line = r.readLine()) != null){
			if(!line.contains("%CPU")&&(line.contains("cpu")||line.contains("Cpu")||line.contains("CPU"))){
				count++;
				if(count != 2)	continue;
				System.out.println(line);
				StringTokenizer st = new StringTokenizer(line);
				while (st.hasMoreTokens()){
					String cur = st.nextToken();
					if(cur.contains("idle")||cur.contains("id")){
						StringTokenizer tmpst = new StringTokenizer(cur,"%");	
						cpu = tmpst.nextToken();
						return Double.parseDouble(cpu);
					}
				}
			}
		}
		return -1;
	}
}
