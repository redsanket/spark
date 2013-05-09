package hadooptest.hadoop.regression;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import hadooptest.TearDownTests;
import hadooptest.TestSession;
import hadooptest.SerialTests;
import hadooptest.cluster.hadoop.HadoopCluster;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.config.hadoop.HadoopConfiguration;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.SleepJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;

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