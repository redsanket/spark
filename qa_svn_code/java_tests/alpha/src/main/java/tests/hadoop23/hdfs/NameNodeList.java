package tests.hadoop23.hdfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class NameNodeList {
	ArrayList<NameNode> namenodeList = new ArrayList<NameNode>();
	Configuration conf;
	private Log LOG = LogFactory.getLog(NameNodeList.class);
	private int curPos = 0;
	
	public NameNodeList(Configuration conf) throws IOException{
		this.conf = conf;
		
		String[] nameServices = getNameServiceFromConfig();
								
		for(int i=0; i< nameServices.length; i++){
			String namenodeAddress = conf.get("dfs.namenode.rpc-address"+"."+nameServices[i]);
			NameNode nameNode = new NameNode(namenodeAddress);
			namenodeList.add(nameNode);			
		}						
	}
	
	public class NameNode {
		private String address;
		private String port;

		public NameNode(String uri){
			LOG.info("Get name node: "+uri);
			String[] uriSplit = uri.split(":");
			address = uriSplit[0];
			port = uriSplit[1];
		}
		
		public String getAddress(){
			return address;
		}
		
		public String getPort(){
			return port;
		}
		
		public String getFullAddress(){
			return address+":"+port;
		}
	}
	
	private String[] getNameServiceFromConfig() throws IOException{		
		return conf.get("dfs.federation.nameservices").split(",");
	}
	
	public Iterator iterator(){
		return namenodeList.iterator();
	}
	
}
