package hadooptest.hadoop.regression;

import hadooptest.TestSession;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;

import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import coretest.TearDownTests;

@Category(TearDownTests.class)
public class TestTearDownJacoco extends TestSession {

    @BeforeClass
    public static void startTestSession() throws IOException {
        TestSession.start(    );
    }
    
	@Test
	public void resetHadoopConf() throws Exception {
	    // Reset Hadoop configuration directory to the installed default
	    FullyDistributedCluster cluster =
	            (FullyDistributedCluster) TestSession.cluster;
	    cluster.getConf().resetHadoopDefaultConfDir();
	    cluster.getConf().resetHadoopConfDir();

	    // Restart the cluster
	    TestSession.cluster.reset();
	    cluster.waitForSafemodeOff();
	    cluster.isFullyUp();
	}

}