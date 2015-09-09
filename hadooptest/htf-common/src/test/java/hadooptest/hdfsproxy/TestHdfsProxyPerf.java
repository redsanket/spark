package hadooptest.hdfsproxy;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.ArrayList;

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
                "/htf-common/src/test/java/hadooptest/hdfsproxy/bin";
        String script = scriptDir + "/run_hproxy_perf";

        /*
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
	    */

        ArrayList<String> cmd = new ArrayList<String>();
        cmd.add(script);
        cmd.add("-cluster");
        cmd.add(TestSession.cluster.getClusterName());
        cmd.add("-payload_size");
        cmd.add(System.getProperty("PAYLOAD_SIZE", DEFAULT_PAYLOAD_SIZE));
        cmd.add("-payload_unit");
        cmd.add(System.getProperty("PAYLOAD_UNIT", DEFAULT_PAYLOAD_UNIT));
        cmd.add("-threads_per_host");
        cmd.add(System.getProperty("THREADS_PER_HOST", DEFAULT_NUM_THREADS));
        String proxyHost=System.getProperty("PROXY_HOST");
        if ((proxyHost != null) && (!proxyHost.isEmpty()) &&
                (!proxyHost.equals("default"))) {
            cmd.add("-proxy");
            cmd.add(proxyHost);
        }
        String clientHosts=System.getProperty("CLIENT_HOSTS");
        if ((clientHosts!= null) && (!clientHosts.isEmpty()) &&
                (!clientHosts.equals("default"))) {
            cmd.add("-client_hosts");
            cmd.add(clientHosts);
        }
        String testHttp=System.getProperty("TEST_HTTP");
        if ((testHttp!= null) && (!testHttp.isEmpty()) &&
                (!testHttp.equals("default")) && (!testHttp.equals("false"))) {
            cmd.add("-hproxy_http");
            cmd.add(testHttp);
        }
        else {
            cmd.add("-hproxy_http");
            cmd.add("false");
        } 
        String[] command = cmd.toArray(new String[0]);

	    Process process = null;
	    process = TestSession.exec.runProcBuilderGetProc(command);
	    String response = TestSession.exec.getProcessInputStream(process);
	}

    @After
    public void logTaskReportSummary() throws Exception  {
    }

}
