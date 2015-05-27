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

import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Category(SerialTests.class)
public class TestHBaseTopology extends TestSessionStorm {

    static ModifiableStormCluster mc = null;

    @BeforeClass
    public static void setup() throws Exception {
        cluster.setDrpcAclForFunction("hbase");
        mc = (ModifiableStormCluster) cluster;
        assumeTrue(mc != null);
    }

    @AfterClass
    public static void cleanup() throws Exception {
        if (mc != null) {
            mc.resetConfigs();
        }
        stop();
    }

    public void launchHBaseTopology() throws Exception {
        String pathToJar = conf.getProperty("WORKSPACE") + "/topologies/target/topologies-1.0-SNAPSHOT-jar-with-dependencies.jar";
        String pathToConf = conf.getProperty("WORKSPACE") + "/htf-common/resources/storm/testinputoutput/TestHBaseTopology/hbase-site.xml";
        String byUser = mc.getBouncerUser();
        String[] returnValue = exec.runProcBuilder(new String[] { "storm", "jar", pathToJar, "hadooptest.topologies.HBaseTopology",  "run", pathToConf, "test", "-c",
            "topology.worker.childopts=\"-Dsun.security.krb5.debug=true -Dhadoop.home.dir=/tmp\"", "-c", "ui.users=[\""+byUser+"\"]", "-c", "logs.users=[\""+byUser+"\"]" }, true);
        assertTrue( "Could not launch topology", returnValue[0].equals("0") );
    }

    public void checkLogviewerLog(String topoName) throws Exception {
        String output = getLogForTopology(topoName);
        logger.info("******* OUTPUT = " + output);
        
        final String expectedRegex =
                "Found ticket for hadoopqa@DEV.YGRID.YAHOO.COM to go to hbaseqa/.*yahoo.com@DEV.YGRID.YAHOO.COM expiring on (.*)";
        Pattern p = Pattern.compile(expectedRegex);
        Matcher regexMatcher = p.matcher(output);
        
        assertTrue("Couldn't find first ticket.",
                regexMatcher.find());
        String date1String = regexMatcher.group(1);
        logger.info("Date 1 = " + date1String);
        assertTrue("Couldn't find second ticket.",
                regexMatcher.find());
        String date2String = regexMatcher.group(1);
        logger.info("Date 2 = " + date2String);

        SimpleDateFormat sdfParser = new SimpleDateFormat("EEE MMM d HH:mm:ss zzz yyyy");
        Date date1 = sdfParser.parse(date1String);
        Date date2 = sdfParser.parse(date2String);
        assertTrue("Date didn't change.", date1.before(date2));
    }

    @Test(timeout=600000)
    public void HBaseTest() throws Exception {
        launchHBaseTopology();

        // Wait for it to come up
        Util.sleep(30);

        // Hit it with drpc function
        String drpcResult = cluster.DRPCExecute( "hbase", "mike" );
        logger.debug("drpc result = " + drpcResult);
        assertTrue("Did not get expected result back from hbase topology", drpcResult.equals("mike"));
        
        // Update creds
        kinit();
        String[] uploadReturnValue = exec.runProcBuilder(new String[] { "storm", "upload-credentials", "run" }, true);
        assertTrue( "Could not push credentials", uploadReturnValue[0].equals("0") );
        Util.sleep(30);
     
        // Hit it again
        drpcResult = cluster.DRPCExecute( "hbase", "nathan" );
        logger.debug("drpc result = " + drpcResult);
        assertTrue("Did not get expected result back from hbase topology", drpcResult.equals("nathan"));
        
        // Look through logviewer and try to find, after "nathan" the timestamp of the ticket.  It should match the new one
        checkLogviewerLog("run");
        
        cluster.killTopology("run");
    }
}
