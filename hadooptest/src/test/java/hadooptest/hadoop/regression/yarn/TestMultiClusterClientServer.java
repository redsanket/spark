
package hadooptest.hadoop.regression.yarn;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import hadooptest.TestSession;
import hadooptest.cluster.hadoop.DFS;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import coretest.SerialTests;
import coretest.Util;

/**
 * Tests to exercise the multi-cluster functionality in the framework.
 */
@Category(SerialTests.class)
public class TestMultiClusterClientServer extends TestSession {
	
	@BeforeClass
	public static void startTestSession() throws Exception {
		TestSession.start();
	}

	/**
	 * The test stages a copy from DFS to DFS when the initiating cluster
	 * is on another gateway.
	 */
	@Test
	public void copyWithinClusterHDFS() throws Exception {
		DFS localDfs = new DFS();
		
		// Stage the client instance of HTF on another gateway for another cluster
		TestSession.multiClusterServer.remoteStartMultiClusterClient(
				"gwbl2005.blue.ygrid.yahoo.com", "omegab", "hadoopqa", 
				"/homes/hadoopqa/hadooptest_multicluster_configs/hadooptest_client.conf");
		
		// Wait for the client to connect to the server.
		String strTimeout = TestSession.conf.getProperty(
				"MULTI_CLUSTER_SERVER_SESSION_TIMEOUT");
		int timeout = Integer.parseInt(strTimeout);
		while (!TestSession.multiClusterServer.isClientConnected()) {
			logger.info("Waiting for client to connect to server...");
			Util.sleep(5);
			timeout = timeout - 5;
			if (timeout <= 0) {
				logger.error("The multi cluster client did not connect to the server within " + strTimeout + " seconds.");
				fail();
			}
		}
			
		// Trigger getting the DFS name of the client instance of HTF
		String clientDfsName = TestSession.multiClusterServer.getClientDFSName(30);
		logger.info("Client DFS name is: " + clientDfsName);
		
		// Trigger the local copy of a file into the HDFS on the client instance of HTF
		TestSession.multiClusterServer.requestClientDfsRemoteLocalCopy("/homes/hadoopqa/hadooptest.conf", clientDfsName + "/user/hadoopqa/hadooptest.conf");
		
		// Perform a fs ls on the client instance of HTF to verify
		assertTrue("Local file on client was not successfully copied to client DFS.", 
				TestSession.multiClusterServer.requestClientDfsLs(clientDfsName + "/user/hadoopqa/hadooptest.conf", 30));
		
		// Trigger the cluster-to-cluster copy of the file from the client instance of HTF
		// DFS, to this instance of HTF DFS.
		TestSession.multiClusterServer.requestClientDfsRemoteDfsCopy(clientDfsName, "/user/hadoopqa/hadooptest.conf", getFSDefaultName(), "/user/hadoopqa/hadooptest.conf.test.1");
		
		// Wait about 15s for the file to copy
		Util.sleep(15);

		// Stop the client.
		logger.info("Stopping multi cluster client...");
		TestSession.multiClusterServer.requestClientStop();
		
		// Stop the server.
		logger.info("Stopping multi cluster server...");
		TestSession.multiClusterServer.stopServer();
		
		// Perform a fs ls on this instance of HTF DFS to verify that the file was
		// copied to this DFS.
		logger.info("Checking to see if the file was copied from DFS to DFS.");
		assertTrue("File was not successfully copied between DFS systems.", localDfs.fileExists(getFSDefaultName() + "/user/hadoopqa/hadooptest.conf.test.1"));
		
	}

	/**
	 * Get the default FS name of the host cluster.
	 * 
	 * @return String the name of the cluster.
	 * @throws Exception if there is a fatal error getting the cluster name.
	 */
	private String getFSDefaultName() throws Exception {
		String fsDefaultName = null;
		
		fsDefaultName = cluster.getConf().get("fs.defaultFS");
		
		return fsDefaultName;
	}
	
}
