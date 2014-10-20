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

    @BeforeClass
    public static void setup() throws Exception {
        cluster.setDrpcInvocationAuthAclForFunction("hbase", "hadoopqa");
        String v1Role = "yahoo.grid_re.storm." + conf.getProperty("CLUSTER_NAME");
        cluster.setDrpcClientAuthAclForFunction("hbase", "hadoopqa," +v1Role );
    }

    @AfterClass
    public static void cleanup() throws Exception {
        stop();
    }

    public TopologySummary getTS(String name) throws Exception {
        for (TopologySummary ts: cluster.getClusterInfo().get_topologies()) {
            if (name.equals(ts.get_name())) {
                return ts;
            }
        }
        throw new IllegalArgumentException("Topology "+name+" does not appear to be up yet");
    }

    public String getId(String name) throws Exception {
        TopologySummary ts = getTS( name );

        return ts.get_id();
    }
 
    public int getUptime(String name) throws Exception {
        return getTS(name).get_uptime_secs();
    }

    public void launchHBaseTopology() throws Exception {
        String pathToJar = conf.getProperty("WORKSPACE") + "/topologies/target/topologies-1.0-SNAPSHOT-jar-with-dependencies.jar";
        String pathToConf = conf.getProperty("WORKSPACE") + "/htf-common/resources/storm/testinputoutput/TestHBaseTopology/hbase-site.xml";
        String[] returnValue = exec.runProcBuilder(new String[] { "storm", "jar", pathToJar, "hadooptest.topologies.HBaseTopology",  "run", pathToConf, "test", "-c",
            "topology.worker.childopts=\"-Dsun.security.krb5.debug=true -Dhadoop.home.dir=/tmp\"", "-c", "ui.users=[\"hadoop_re\"]", "-c", "logs.users=[\"hadoop_re\"]" }, true);
        assertTrue( "Could not launch topology", returnValue[0].equals("0") );
    }

    public void checkLogviewerLog(String topoName) throws Exception {
        final String topoId = getFirstTopoIdForName(topoName);

        // Worker Host
        TopologyInfo ti = cluster.getTopologyInfo(topoId);
        String host = ti.get_executors().get(0).get_host();
        int workerPort = ti.get_executors().get(0).get_port();

        // Logviewer port on worker host
        String jsonStormConf = cluster.getNimbusConf();
        @SuppressWarnings("unchecked")
        Map<String, Object> stormConf =
                (Map<String, Object>) JSONValue.parse(jsonStormConf);
        Integer logviewerPort = Utils.getInt(stormConf
                .get(Config.LOGVIEWER_PORT));
        
        ModifiableStormCluster mc;
        mc = (ModifiableStormCluster)cluster;
        
        backtype.storm.Config theconf = new backtype.storm.Config();
        theconf.putAll(backtype.storm.utils.Utils.readStormConfig());

        String filter = (String)theconf.get("ui.filter");
        String pw = null;
        String user = null;

        // Only get bouncer auth on secure cluster.
        if ( filter != null ) {
            if (mc != null) {
                user = mc.getBouncerUser();
                pw = mc.getBouncerPassword();
            }
        }
        
        HTTPHandle client = new HTTPHandle();
        if (filter != null) {
            client.logonToBouncer(user,pw);
        }
        logger.info("Cookie = " + client.YBYCookie);
        
        String getURL = "http://" + host + ":" + logviewerPort + 
                "/download/" + topoId + "-worker-" + workerPort + ".log";
        logger.info("URL to get is: " + getURL);
        HttpMethod getMethod = client.makeGET(getURL, new String(""), null);
        Response response = new Response(getMethod, false);
        String output = response.getResponseBodyAsString();
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
        //
        // ToDo:  Make this configurable.  The orininal kinit is hardcoded from run_hadooptest.  It should be in some sort of a generic method.
        String[] kinitReturnValue = exec.runProcBuilder(new String[] { "kinit", "-kt", "/homes/hadoopqa/hadoopqa.dev.headless.keytab", "hadoopqa@DEV.YGRID.YAHOO.COM" }, true);
        assertTrue( "Could not kinit", kinitReturnValue[0].equals("0") );
        Util.sleep(2);
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
