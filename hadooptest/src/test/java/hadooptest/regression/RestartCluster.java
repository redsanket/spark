package hadooptest.regression;

import static org.junit.Assert.assertTrue;
import hadooptest.TestSession;
import hadooptest.cluster.fullydistributed.FullyDistributedCluster;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class RestartCluster extends TestSession {

	@BeforeClass
	public static void startTestSession() throws IOException {
		TestSession.start();
	}
	
	@Test
	public void restartCluster() {
		assertTrue("Cluster reset failed", TestSession.cluster.reset());
		
		FullyDistributedCluster cluster = (FullyDistributedCluster) TestSession.cluster;
		assertTrue("Cluster is not off of safemode after cluster reset", cluster.waitForSafemodeOff());
		assertTrue("Cluster is not fully up after cluster reset", cluster.isClusterFullyUp());		
	}	
	
}