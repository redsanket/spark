package hadooptest.hdfsproxy;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import hadooptest.TestSession;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import hadooptest.SerialTests;

/**
 * Starts a cluster.
 */
@Category(SerialTests.class)
public class TestHdfsProxyPerf extends TestSession {

	@Test
	public void runHdfsProxyPerf() throws Exception {
	    String DEFAULT_PAYLOAD_SIZE = "500";
        String DEFAULT_PAYLOAD_UNIT = "G";
        String DEFAULT_NUM_THREADS  = "16";
        String scriptDir            = 
                TestSession.conf.getProperty("WORKSPACE") +
                "htf-common/src/test/java/hadooptest/hdfsproxy/bin";
        String script = scriptDir + "/run_hproxy_perf";
	    String output[] = TestSession.exec.runProcBuilder(
	            new String[] {
	                    script,
	                    "-cluster",
	                    TestSession.cluster.getClusterName(),
	                    "-payload_size",
	                    System.getProperty("PAYLOAD_SIZE", DEFAULT_PAYLOAD_SIZE),
	                    "-payload_unit",
                        System.getProperty("PAYLOAD_UNIT", DEFAULT_PAYLOAD_UNIT),
	                    "-threads_per_host",
                        System.getProperty("THREADS_PER_HOST", DEFAULT_NUM_THREADS),
	                    });
	    TestSession.logger.trace(Arrays.toString(output));
	    assertTrue( "Could not run hdfsproxy perf!!!", output[0].equals("0") );
	}

    @After
    public void logTaskReportSummary() throws Exception  {
    }

}
