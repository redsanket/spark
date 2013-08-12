package hadooptest.hadoop.stress.ClusterUsage;

import hadooptest.TestSession;
import hadooptest.node.hadoop.HadoopNode;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Hashtable;
import java.util.StringTokenizer;

import org.junit.BeforeClass;
import org.junit.Test;

public class TestClusterMemUsage extends TestSession {
	
	
	@BeforeClass
	public static void startTestSession() {
		TestSession.start();
	}
	
	@Test
	public void ClusterMemUsage() throws Exception {
		
		String rmaddr = "";
		Hashtable<String,Hashtable<String,HadoopNode>> nodes = TestSession.getCluster().getNodes();
		for(String str : nodes.keySet())
			if(str.equals("resourcemanager"))
				for(String s : nodes.get(str).keySet())
					rmaddr = s;
		
		int refreshRate = System.getProperty("TestClusterMemUsage.refreshRate") == null? 0 : Integer.parseInt(System.getProperty("TestClusterMemUsage.refreshRate"));
		boolean NodeMode = Boolean.parseBoolean(System.getProperty("TestClusterMemUsage.NodeMode"));

		while(true){
			long start = System.currentTimeMillis();
		
			if(NodeMode)
				GetClusterNodesMem(rmaddr);
			
			GetClusterMem(rmaddr);
			
			Thread.sleep(refreshRate*1000-(System.currentTimeMillis()-start) > 0 ? refreshRate*1000-(System.currentTimeMillis()-start):0);
			TestSession.logger.info("============= One Loop use "+(System.currentTimeMillis()-start)/1000F+" secs. =============");
		}
	}
	
	public void GetClusterNodesMem(String rmaddr) throws IOException {
		
		URL url = new URL("http://"+rmaddr+":8088/ws/v1/cluster/nodes");
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();

		if (conn.getResponseCode() != 200)
		    throw new IOException(conn.getResponseMessage());
		
		BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
		String line;
		while ((line = rd.readLine()) != null) {
			StringTokenizer st = new StringTokenizer(line,",");
			while (st.hasMoreTokens()){
				String cur = st.nextToken();
				if((cur.contains("nodeHostName"))||(cur.contains("usedMemoryMB"))||(cur.contains("availMemoryMB")))
					TestSession.logger.info(cur);
			}
		}
		rd.close();
		conn.disconnect();
	}

	public void GetClusterMem(String rmaddr) throws IOException {
		
		URL url = new URL("http://"+rmaddr+":8088/ws/v1/cluster/metrics");
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();

		if (conn.getResponseCode() != 200)
		    throw new IOException(conn.getResponseMessage());
		
		long availableMem = 0, allocatedMem = 0,totalMem = 0;
		BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
		String line;
		while ((line = rd.readLine()) != null) {
			StringTokenizer st = new StringTokenizer(line,",");
			while (st.hasMoreTokens()){
				String cur = st.nextToken();
				int index = cur.indexOf(':');

				if(cur.contains("availableMB"))
					availableMem = Long.parseLong(cur.substring(index+1))*ConvertUnit.mega;
				if(cur.contains("allocatedMB"))
					allocatedMem = Long.parseLong(cur.substring(index+1))*ConvertUnit.mega;
				if(cur.contains("totalMB"))
					totalMem = Long.parseLong(cur.substring(index+1))*ConvertUnit.mega;
			}
		}
		TestSession.logger.info("Cluster Summary : availableMem = "+ConvertUnit.convertUnit(availableMem)
				+",allocatedMem = "+ConvertUnit.convertUnit(allocatedMem)+",totalMem = "+ConvertUnit.convertUnit(totalMem));
		rd.close();
		conn.disconnect();
	}
}
