package tests.hadoop23.libs.hdfs;

import java.io.IOException;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tests.hadoop23.libs.Cluster;
import tests.hadoop23.libs.HadoopNode;
import tests.hadoop23.libs.Shell;

public class NameNode extends HadoopNode {	
	private Log LOG = LogFactory.getLog(NameNode.class);
	
	public NameNode(Cluster cluster, String uri){
		super(cluster, uri, "namenode");
		LOG.info("Get name node: "+uri);		
	}
	
	public void start() throws Exception{
		remoteCommand(new String[] {"starting namenode", "namenode running as process"}, "start");
	}
	
	public void stop() throws Exception{
		remoteCommand(new String[] {"stopping namenode", "no namenode to stop"}, "stop");
	}
}
