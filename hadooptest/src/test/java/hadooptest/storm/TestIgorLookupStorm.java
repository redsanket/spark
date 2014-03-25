package hadooptest.storm;

import static org.junit.Assert.fail;
import hadooptest.SerialTests;
import hadooptest.TestSessionStorm;

import java.util.ArrayList;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SerialTests.class)
public class TestIgorLookupStorm extends TestSessionStorm {

	private static String CLUSTER_NAME;
	
    @BeforeClass
    public static void setup() throws Exception {
        start();
		CLUSTER_NAME = "ystormQE_CI";
    }
	
    @AfterClass
    public static void cleanup() throws Exception {
        stop();
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
	public void igorLookupClusterContrib() throws Exception {
		ArrayList<String> list = 
				cluster.lookupIgorRoleClusterContrib(CLUSTER_NAME);
		
		if (list.size() < 1) { 
			fail("Did not get any Igor role members for the cluster contrib."); 
		}
		
		for(String s: list) {
			logger.info("Contrib nodes: " + s);
		}
	}
	
	@Test
	public void igorLookupClusterDrpc() throws Exception {
		ArrayList<String> list = 
				cluster.lookupIgorRoleClusterDrpc(CLUSTER_NAME);
		
		if (list.size() < 1) { 
			fail("Did not get any Igor role members for the cluster DRPC."); 
		}
		
		for(String s: list) {
			logger.info("DRPC nodes: " + s);
		}
	}
	
	@Test
	public void igorLookupClusterNimbus() throws Exception {
		ArrayList<String> list = 
				cluster.lookupIgorRoleClusterNimbus(CLUSTER_NAME);
		
		if (list.size() < 1) { 
			fail("Did not get any Igor role members for the cluster Nimbus."); 
		}
		
		for(String s: list) {
			logger.info("Nimbus nodes: " + s);
		}
	}
	
	@Test
	public void igorLookupClusterRegistry() throws Exception {
		ArrayList<String> list = 
				cluster.lookupIgorRoleClusterRegistry(CLUSTER_NAME);
		
		if (list.size() < 1) { 
			fail("Did not get any Igor role members for the cluster registry."); 
		}
		
		for(String s: list) {
			logger.info("Registry nodes: " + s);
		}
	}
	
	@Test
	public void igorLookupClusterSupervisor() throws Exception {
		ArrayList<String> list = 
				cluster.lookupIgorRoleClusterSupervisor(CLUSTER_NAME);
		
		if (list.size() < 1) { 
			fail("Did not get any Igor role members for the cluster supervisor."); 
		}
		
		for(String s: list) {
			logger.info("Supervisor nodes: " + s);
		}
	}
	
	
	@Test
	public void igorLookupClusterUI() throws Exception {
		ArrayList<String> list = 
				cluster.lookupIgorRoleClusterUI(CLUSTER_NAME);
		
		if (list.size() < 1) { 
			fail("Did not get any Igor role members for the cluster UI."); 
		}
		
		for(String s: list) {
			logger.info("UI nodes: " + s);
		}
	}
}
