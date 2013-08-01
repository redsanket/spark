
package hadooptest.hadoop.regression.yarn;

import static org.junit.Assert.assertTrue;
import hadooptest.TestSession;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import coretest.SerialTests;
import coretest.Util;

import hadooptest.cluster.hadoop.DFS;

@Category(SerialTests.class)
public class TestMultiClusterClientServer extends TestSession {
	
	@BeforeClass
	public static void startTestSession() throws Exception {
		TestSession.start();
	}

	@Test
	public void copyWithinClusterHDFS() throws Exception {
		DFS localDfs = new DFS();
		
		// stage the client instance of HTF on another gateway for another cluster
		
		// Wait for the client to connect to the server.
		logger.info("Waiting 30s for client to connect to server.");
		Util.sleep(30);
		
		// Perform a fs ls on this instance of HTF DFS
		//logger.info("Listing the contents of the local DFS.");
		//localDfs.printFsLs(getFSDefaultName() + "/user/hadoopqa/", true);
		
		// Trigger getting the DFS name of the client instance of HTF
		String clientDfsName = TestSession.multiClusterServer.getClientDFSName(30);
		logger.info("Client DFS name is: " + clientDfsName);
		
		// Trigger the local copy of a file into the HDFS on the client instance of HTF
		TestSession.multiClusterServer.requestClientDfsRemoteLocalCopy("/homes/hadoopqa/hadooptest.conf", clientDfsName + "/user/hadoopqa/hadooptest.conf");
		
		// Perform a fs ls on the client instance of HTF to verify
		// 1. verify we recieved the local copy result of true
		// 2. check for the file in the fs ls
		TestSession.multiClusterServer.requestClientDfsLs(clientDfsName + "/user/hadoopqa/hadooptest.conf", 30);
		
		// Trigger the cluster-to-cluster copy of the file from the client instance of HTF
		// DFS, to this instance of HTF DFS.
		TestSession.multiClusterServer.requestClientDfsRemoteDfsCopy(clientDfsName, "/user/hadoopqa/hadooptest.conf", getFSDefaultName(), "/user/hadoopqa/hadooptest.conf.test.1");
		
		// Wait about 15s for the file to copy
		Util.sleep(15);
		
		// Perform a fs ls on this instance of HTF DFS to verify that the file was
		// copied to this DFS.
		logger.info("Checking to see if the file was copied from DFS to DFS.");
		//localDfs.printFsLs(getFSDefaultName() + "/user/hadoopqa/", true);
		assertTrue("File was not successfully copied between DFS systems.", localDfs.fileExists(getFSDefaultName() + "/user/hadoopqa/hadooptest.conf.test.1"));
		
		// Disconnect the client from the server.
	}

	private String getFSDefaultName() throws Exception {
		String fsDefaultName = null;
		
		fsDefaultName = cluster.getConf().get("fs.defaultFS");
		
		return fsDefaultName;
	}
	
}
