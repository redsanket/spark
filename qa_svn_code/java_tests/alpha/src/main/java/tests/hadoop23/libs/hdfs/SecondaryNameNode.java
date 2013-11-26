package tests.hadoop23.libs.hdfs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tests.hadoop23.libs.Cluster;
import tests.hadoop23.libs.HadoopNode;

public class SecondaryNameNode extends HadoopNode {	
	private Log LOG = LogFactory.getLog(NameNode.class);

	public SecondaryNameNode(Cluster cluster, String uri){
		super(cluster, uri, "secondarynamenode");
		LOG.info("Get name node: "+uri);		
	}
	
	@Override
	public void start() throws Exception{
		remoteCommand(new String[] {"starting secondarynamenode", "secondarynamenode running as process"}, "start");
	}
	
	@Override
	public void stop() throws Exception{
		remoteCommand(new String[] {"stopping secondarynamenode", "no secondarynamenode to stop"}, "stop");
	}
}