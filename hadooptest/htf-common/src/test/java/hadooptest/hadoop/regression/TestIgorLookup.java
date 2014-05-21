package hadooptest.hadoop.regression;

import hadooptest.TestSession;
import org.junit.Test;
import org.junit.BeforeClass;
import java.util.ArrayList;
import static org.junit.Assert.fail;

public class TestIgorLookup extends TestSession {

	private static String CLUSTER_NAME;
	
	@BeforeClass
	public static void getCurrentCluster() {
		CLUSTER_NAME = TestSession.conf.getProperty("CLUSTER_NAME");
		logger.info("Got cluster name = " + CLUSTER_NAME);
	}
	
	@Test
	public void igorLookupAllForCluster() throws Exception {		
		ArrayList<String> list = 
				cluster.lookupIgorRoleClusterAllNodes(CLUSTER_NAME);
		
		if (list.size() < 1) { 
			fail("Did not get any Igor role members for the cluster."); 
		}
		
		for(String s: list) {
			logger.info("All cluster nodes: " + s);
		}
	}
	
	@Test
	public void igorLookupClusterGateway() throws Exception {
		ArrayList<String> list = 
				cluster.lookupIgorRoleClusterGateway(CLUSTER_NAME);
		
		if (list.size() < 1) { 
			fail("Did not get any Igor role members for the cluster gateway."); 
		}
		
		for(String s: list) {
			logger.info("Gateway nodes: " + s);
		}
	}
	
	@Test
	public void igorLookupClusterNameNode() throws Exception {
		ArrayList<String> list = 
				cluster.lookupIgorRoleClusterNamenode(CLUSTER_NAME);
		
		if (list.size() < 1) { 
			fail("Did not get any Igor role members for the cluster namenode."); 
		}
		
		for(String s: list) {
			logger.info("Name nodes: " + s);
		}
	}
	
	@Test
	public void igorLookupClusterSecondaryNameNode() throws Exception {
		ArrayList<String> list = 
				cluster.lookupIgorRoleClusterSecondaryNamenode(CLUSTER_NAME);
		
		if (list.size() < 1) { 
			fail("Did not get any Igor role members for the cluster 2NN."); 
		}
		
		for(String s: list) {
			logger.info("Secondary name nodes: " + s);
		}
	}
	
	@Test
	public void igorLookupClusterJobtracker() throws Exception {
		ArrayList<String> list = 
				cluster.lookupIgorRoleClusterJobtracker(CLUSTER_NAME);
		
		if (list.size() < 1) { 
			fail("Did not get any Igor role members for the cluster jobtracker."); 
		}
		
		for(String s: list) {
			logger.info("Jobtracker nodes: " + s);
		}
	}
}
