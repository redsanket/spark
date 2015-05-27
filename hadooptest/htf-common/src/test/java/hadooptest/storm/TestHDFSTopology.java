package hadooptest.storm;

import static org.junit.Assert.assertEquals;
import hadooptest.SerialTests;
import hadooptest.Util;
import hadooptest.automation.utils.http.Response;
import hadooptest.cluster.storm.ModifiableStormCluster;
import hadooptest.automation.utils.http.HTTPHandle;
import org.apache.commons.httpclient.HttpMethod;
import hadooptest.TestSessionStorm;
import hadooptest.Util;
import static org.junit.Assume.assumeTrue;
import org.json.simple.JSONValue;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import static org.junit.Assume.assumeTrue;
import static org.junit.Assert.*;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.TopologySummary;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.utils.Utils;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataInputStream;

@Category(SerialTests.class)
public class TestHDFSTopology extends TestSessionStorm {

    static ModifiableStormCluster mc = null;
    static final String PATH_TO_JAR = "/home/y/lib64/jars/yahoo_examples.jar";
    static final String TOPO_NAME = "run-hdfs";
    static final String DRPC = "hdfs";

    @BeforeClass
    public static void setup() throws Exception {
        cluster.setDrpcAclForFunction(DRPC);
        mc = (ModifiableStormCluster) cluster;
        assumeTrue(mc != null);
        String outloc = conf.getProperty("TMP_HDFS_OUTPUT");
        logger.info("output location: "+outloc);
        assumeTrue("TMP_HDFS_OUTPUT is not set, cannot run this test", outloc != null);
        assumeTrue(PATH_TO_JAR + " not found", new File(PATH_TO_JAR).exists());
    }

    @AfterClass
    public static void cleanup() throws Exception {
        if (mc != null) {
            mc.resetConfigs();
        }
        stop();
    }

    FileSystem cached = null;
    //I know this does not really work if outloc changes locations
    public synchronized FileSystem getFSFor(Path outloc) throws Exception {
        if (cached == null) {
            Configuration hdfsConf = new Configuration();
            //Have to override this so we can talk to test cluster
            hdfsConf.set("dfs.namenode.kerberos.principal.pattern", "hdfs/*.{blue,red,tan}.ygrid.yahoo.com@DEV.YGRID.YAHOO.COM");
            cached = outloc.getFileSystem(hdfsConf);
        }
        return cached;
    }

    public void launchHDFSTopology(String outloc, String topoName, String drpc) throws Exception {
        Path p = new Path(outloc);
        getFSFor(p).delete(p, true);

        String[] returnValue = exec.runProcBuilder(new String[] { "storm", "jar", PATH_TO_JAR, "com.yahoo.storm.examples.hdfs.HDFSTopology",  "-name", topoName, outloc,
            "-drpc", drpc, "-rotateSize", "0.0001",
            //Have to override this so we can talk to test cluster
            "-hdfsConf", "dfs.namenode.kerberos.principal.pattern=hdfs/*.{blue,red,tan}.ygrid.yahoo.com@DEV.YGRID.YAHOO.COM"}, true);
        assertTrue( "Could not launch topology "+java.util.Arrays.toString(returnValue), returnValue[0].equals("0") );
    }

    public void checkOutputFile(String outloc, String expected) throws Exception {
        Path p = new Path(outloc);
        FileSystem fs = getFSFor(p);
        FileStatus dir = fs.getFileStatus(p);
        assertTrue(dir.isDirectory());
        FileStatus [] subs = fs.listStatus(p);
        assertEquals(2, subs.length);
        boolean checked = false;
        for (FileStatus subStat: subs) {
           if (subStat.getLen() > 0) {
               //The first column is the expected data, then a tab, then some DRPC routing information.
               assertTrue(expected.length() <= subStat.getLen());
               byte[] buffer = new byte[expected.length() + 1];
               FSDataInputStream in = fs.open(subStat.getPath());
               in.read(buffer);
               in.close();
               assertEquals(expected+"\t", new String(buffer));
               fs.delete(subStat.getPath(), false);
               checked = true;
           }
        }
        assertTrue(checked);
    }

    @Test(timeout=600000)
    public void HDFSTest() throws Exception {
        String outloc = conf.getProperty("TMP_HDFS_OUTPUT");
        launchHDFSTopology(outloc, TOPO_NAME, DRPC);

        // Wait for it to come up
        Util.sleep(30);

        // Hit it with drpc function
        char[] chars = new char[1025];
        java.util.Arrays.fill(chars, 'a');
        String test = new String(chars);

        String drpcResult = cluster.DRPCExecute(DRPC, test);
        logger.debug("drpc result = " + drpcResult);
        assertTrue("Did not get expected result back from hdfs topology", drpcResult.equals(test));
        Util.sleep(2);
        checkOutputFile(outloc, test);
        
        java.util.Arrays.fill(chars, 'b');
        String testB = new String(chars);
     
        // Hit it again
        drpcResult = cluster.DRPCExecute(DRPC, testB);
        logger.debug("drpc result = " + drpcResult);
        assertTrue("Did not get expected result back from hdfs topology", drpcResult.equals(testB));
        Util.sleep(2);
        checkOutputFile(outloc, testB);
       
        cluster.killTopology(TOPO_NAME);
    }
}
