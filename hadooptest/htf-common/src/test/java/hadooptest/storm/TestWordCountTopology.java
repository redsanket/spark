package hadooptest.storm;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import hadooptest.SerialTests;
import hadooptest.TestSessionStorm;
import hadooptest.Util;
import hadooptest.automation.utils.http.HTTPHandle;
import hadooptest.automation.utils.http.Response;
import hadooptest.cluster.storm.ModifiableStormCluster;
import hadooptest.workflow.storm.topology.bolt.Split;
import hadooptest.workflow.storm.topology.spout.FixedBatchSpout;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.net.CookieManager;
import java.net.CookieStore;
import java.net.HttpCookie;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.lang3.StringEscapeUtils;
import org.json.simple.JSONValue;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.w3c.dom.Document;
import org.w3c.tidy.Tidy;

import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.MemoryMapState;
import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

@Category(SerialTests.class)
public class TestWordCountTopology extends TestSessionStorm {
    static int numSpouts = 2; //This must match the topology
    public String decodedCookie = null;
    public HttpCookie theCookie = null;
    public CookieManager manager = null;
    public CookieStore cookieJar = null;

    @BeforeClass
    public static void setup() throws Exception {
        cluster.setDrpcInvocationAuthAclForFunction("words", "hadoopqa");
        cluster.setDrpcClientAuthAclForFunction("words", "hadoopqa"); 
    }
    
    @AfterClass
    public static void cleanup() throws Exception {
        stop();
    }

    private boolean isRedirect(int status) {
        if (status == HttpURLConnection.HTTP_MOVED_TEMP ||
            status == HttpURLConnection.HTTP_MOVED_PERM ||
            status == HttpURLConnection.HTTP_SEE_OTHER) {
            return true;
        } else {
            return false;
        }
    }

    @Test(timeout=300000)
    public void LogviewerPagingTest() throws Exception {
        assumeTrue(cluster instanceof ModifiableStormCluster);
        StormTopology topology = buildTopology();

        String topoName = "logviewer-paging-test";
        String outputLoc = new File("/tmp", topoName).getCanonicalPath();

        Config config = new Config();
        config.put("test.output.location", outputLoc);
        config.setDebug(true);
        config.setNumWorkers(1);
        config.put(Config.NIMBUS_TASK_TIMEOUT_SECS, 200);
        config.put(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD, "123:456");
        List<String> whiteList = Arrays.asList("hadoop_re");
        config.put(Config.UI_USERS, whiteList);
        config.put(Config.LOGS_USERS, whiteList);
        cluster.submitTopology(getTopologiesJarFile(), topoName, config, topology);
        try {
            final String topoId = getFirstTopoIdForName(topoName);
            waitForTopoUptimeSeconds(topoId, 20);

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
                    ".*TRANSFERR?ING.*an apple a day keeps the doctor away.*";
            Pattern p = Pattern.compile(expectedRegex, Pattern.DOTALL);
            Matcher regexMatcher = p.matcher(output);
            
            assertTrue("Topology appears to be up-and-running.",
                    regexMatcher.find());

            final int startByteNum = 42;
            final int byteLength = 1947;
            assertTrue("Log file is long enough for testing.",
                    output.length() >= byteLength);
            
            File tmpFile = File.createTempFile(this.getClass().getName(), null);
            
            // Write out the original logfile
            String logPath = tmpFile.getCanonicalPath();
            PrintWriter out = new PrintWriter(logPath);
            out.println(output);
            out.close();
            
            String logBytesString = readBytesFromFile(logPath, startByteNum,
                    byteLength);
            String expected = StringEscapeUtils.escapeXml(logBytesString);
            String actual = getLogviewerPageContent(client, topoId, host,
                    logviewerPort, workerPort, startByteNum, byteLength);
            assertEquals("Log page returns correct bytes", expected, actual);
        } finally {
            cluster.killTopology(topoName);
        }
    }

    private String getLogviewerPageContent(HTTPHandle client, String topoId, String host,
            int logviewerPort, int workerPort, final int startByteNum,
            final int byteLength) throws MalformedURLException, IOException,
            FileNotFoundException, XPathExpressionException, URISyntaxException {
        URL logPageUrl = new URL("http", host, logviewerPort, "/log?file="
                + topoId + "-worker-" + workerPort + ".log&" + "start="
                + startByteNum + "&length=" + byteLength);

        HttpMethod getMethod = client.makeGET(logPageUrl.toString(), new String(""), null);
        Response response = new Response(getMethod, false);
        String output = response.getResponseBodyAsString();
        
        File tmpFile = File.createTempFile("logPage" + this.getClass().getName(), null);
        
        // Write out the original logfile
        String logPagePath = tmpFile.getCanonicalPath();
        PrintWriter out = new PrintWriter(logPagePath);
        out.println(output);
        out.close();
        
        //String logPagePath = downloadFile(logPageUrl);
        Tidy tidy = new Tidy();
        tidy.setQuiet(true);
        Document doc = tidy.parseDOM(new FileInputStream(logPagePath),
                new NullOutputStream());
        XPath xpath = XPathFactory.newInstance().newXPath();
        String actual = (String) xpath.evaluate(
                "//html/body/pre[contains(@id, 'logContent')]/text()", doc,
                XPathConstants.STRING);
        return actual;
    }

    private String readBytesFromFile(String filePath, final int startByteNum,
            final int byteLength) throws FileNotFoundException, IOException {
        File logFile = new File(filePath);
        assertTrue("Log file is long enough for testing.",
                logFile.length() >= byteLength);
        byte[] buf = new byte[byteLength];
        RandomAccessFile logPageFile = null;
        try {
            logPageFile = new RandomAccessFile(filePath, "r");
            logPageFile.seek(startByteNum);
            logPageFile.readFully(buf, 0, byteLength);
        } finally {
            logPageFile.close();
        }
        return new String(buf);
    }

    @Test(timeout=300000)
    public void WordCountTopologyTest() throws Exception{
        StormTopology topology = buildTopology();

        String topoName = "wc-topology-test";
        String outputLoc = "/tmp/wordcount"; //TODO change this to use a shared directory or soemthing, so we can get to it simply
                           
        Config config = new Config();
        config.put("test.output.location",outputLoc);
        config.setDebug(true);
        config.setNumWorkers(3);
        config.put(Config.NIMBUS_TASK_TIMEOUT_SECS, 200);
        config.put(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD, "123:456");
        //TODO turn this into a utility that has a conf setting
        File jar = new File(conf.getProperty("WORKSPACE") + "/topologies/target/topologies-1.0-SNAPSHOT-jar-with-dependencies.jar");
        cluster.submitTopology(jar, topoName, config, topology);
        try {
            final String topoId = getFirstTopoIdForName(topoName);
            waitForTopoUptimeSeconds(topoId, 20);

            //get expected results
            String file = conf.getProperty("WORKSPACE") + "/htf-common/resources/storm/testinputoutput/WordCountFromFile/expected_results";
        
            HashMap<String, Integer> expectedWordCount = Util.readMapFromFile(file);

            // TODO FIX ME assertEquals (expectedWordCount.size(), resultWordCount.size());
        
            //int numSpouts = cluster.getExecutors(topo, "sentence_spout").size();
            logger.info("Number of spouts: " + numSpouts);
            Pattern pattern = Pattern.compile("(\\d+)", Pattern.CASE_INSENSITIVE);
            for (String key: expectedWordCount.keySet()){
                //for WordCount from file the output depends on how many spouts are created
                //todo.  Abstarcted drpc execute call.  Need to add cluster interface so that local mode and non-local mode are the same
                String drpcResult = cluster.DRPCExecute( "words", key );
                logger.info("The value for " + key + " is " + drpcResult );
                Matcher matcher = pattern.matcher(drpcResult);
                int thisCount = -1;
                if (matcher.find()) {
                    logger.info("Matched " + matcher.group(0));
                    thisCount = Integer.parseInt(matcher.group(1));
                } else {
                    logger.warn("Did not match.");
                }
                assertEquals(key, thisCount, (int)expectedWordCount.get(key)*numSpouts);
            }
        } finally {
            cluster.killTopology(topoName);
        }
    }
        
    public static StormTopology buildTopology() {
        @SuppressWarnings("unchecked")
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3,
                new Values ("the cow jumped over the moon"),
                new Values ("an apple a day keeps the doctor away"),
                new Values ("four score and seven years ago"),
                new Values ("snow white and the seven dwarfs"),
                new Values ("i am at two"));

        spout.setCycle(numSpouts);

        TridentTopology topology = new TridentTopology();
        TridentState wordCounts = topology.newStream("spout1", spout)
                .parallelismHint(1)
                .each(new Fields("sentence"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .persistentAggregate(new MemoryMapState.Factory(),
                        new Count(), new Fields("count"))         
                        .parallelismHint(16);

        cluster.newDRPCStream(topology, "words")
            .each(new Fields("args"), new Split(), new Fields("word"))
            .groupBy(new Fields("word"))
            .stateQuery(wordCounts, new Fields("word"), new MapGet(), new Fields("count"))
            .each(new Fields("count"), new FilterNull())
            .aggregate(new Fields("count"), new Sum(), new Fields("sum"))
            ;
        return topology.build();
    }    
}
