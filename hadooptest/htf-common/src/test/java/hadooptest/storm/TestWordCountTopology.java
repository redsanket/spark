package hadooptest.storm;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import hadooptest.SerialTests;
import hadooptest.TestSessionStorm;
import hadooptest.Util;
import hadooptest.cluster.storm.ModifiableStormCluster;
import hadooptest.automation.utils.http.HTTPHandle;
import hadooptest.workflow.storm.topology.bolt.Split;
import hadooptest.workflow.storm.topology.spout.FixedBatchSpout;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.CookieManager;
import java.net.CookieHandler;
import java.net.CookieStore;
import java.net.HttpCookie;
import java.net.URL;
import java.net.URISyntaxException;
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

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
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
import backtype.storm.generated.TopologySummary;
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
        cluster.setDrpcAuthAclForFunction("words", "mapredqa");
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
 
    public int getUptime(String name) throws Exception {
        return getTS(name).get_uptime_secs();
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
    private String downloadFile(URL url) throws IOException, URISyntaxException {
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        URL useUrl = url;
        if (theCookie != null && isRedirect(conn.getResponseCode())) {
            // Handle redirect if Bouncer Auth is on.
            // get redirect url from "location" header field
            String newUrl = conn.getHeaderField("Location");
            logger.info("Redirect to URL : " + newUrl);
            useUrl = new URL(newUrl);
            addToCookieJar( useUrl );
     
            // get the cookie if need, for login
            String cookies = conn.getHeaderField("Set-Cookie");
     
            // open the new connnection again
            conn = (HttpURLConnection) useUrl.openConnection();
            conn.setRequestProperty("Cookie", cookies);
            //conn.addRequestProperty("Accept-Language", "en-US,en;q=0.8");
            //conn.addRequestProperty("User-Agent", "Mozilla");
            //conn.addRequestProperty("Referer", "google.com");
     

            assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());

        }

        assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
        InputStream in = null;
        FileOutputStream out = null;
        try {
            //in = (InputStream) useUrl.openConnection().getInputStream();
            in = (InputStream) conn.getInputStream();
            byte[] buf = new byte[1024];
            int read = -1;
            File tmpFile = File.createTempFile(this.getClass().getName(), null);
            //tmpFile.deleteOnExit();
            out = new FileOutputStream(tmpFile);
            try {
                while ((read = in.read(buf)) != -1) {
                    out.write(buf, 0, read);
                }
            } catch (IOException e) {
                if (!"Premature EOF".equals(e.getMessage())) {
                    throw e;
                }
                // Do nothing.
                // We can remove this when the logviewer properly closes
                // connections.
            }
            return tmpFile.getCanonicalPath();
        } finally {
            if (in != null) {
                in.close();
            }
            if (out != null) {
                out.close();
            }
        }
    }

    public void SetUpCookie() throws Exception {
        backtype.storm.Config theconf = new backtype.storm.Config();
        theconf.putAll(backtype.storm.utils.Utils.readStormConfig());

        String filter = theconf.get("ui.filter").toString();
        String pw = null;
        String user = null;

        // Only get bouncer auth on secure cluster.
        if ( filter != null ) {
            ModifiableStormCluster mc;
            mc = (ModifiableStormCluster)cluster;
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
        // Now let's break this cookie up.
        Pattern pattern = Pattern.compile("YBY=(.+)", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(client.YBYCookie);
        String cookie = null;
        if (matcher.find()) {
            cookie = matcher.group(1);
            logger.info("Decomposed cookie = " + cookie);
            decodedCookie = java.net.URLDecoder.decode(cookie, "UTF-8");
            logger.info("Decoded cookie = " + decodedCookie);
            // Create cookie jar
            manager = new CookieManager();
            CookieHandler.setDefault(manager);
            cookieJar = manager.getCookieStore();
            theCookie = new HttpCookie("YBY", decodedCookie);
        } else {
            logger.info("Didn't find YBY");
        }
    }

    public void addToCookieJar(URL url) throws URISyntaxException {
        if ( theCookie == null ) {
            return;
        }

        cookieJar.add(url.toURI(), theCookie);
    }

    @Test
    public void LogviewerPagingTest() throws Exception {
        assumeTrue(cluster instanceof ModifiableStormCluster);
        StormTopology topology = buildTopology();

        String topoName = "logviewer-paging-test";
        String outputLoc = new File("/tmp", topoName).getCanonicalPath();

        SetUpCookie();

        Config config = new Config();
        config.put("test.output.location", outputLoc);
        config.setDebug(true);
        config.setNumWorkers(1);
        config.put(Config.NIMBUS_TASK_TIMEOUT_SECS, 200);
        config.put(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD, "123:456");
        List<String> whiteList = Arrays.asList("hadoop_re");
        config.put(Config.UI_USERS, whiteList);
        config.put(Config.LOGS_USERS, whiteList);
        // TODO turn this into a utility that has a conf setting
        File jar = new File(
                conf.getProperty("WORKSPACE")
                        + "/topologies/target/topologies-1.0-SNAPSHOT-jar-with-dependencies.jar");
        cluster.submitTopology(jar, topoName, config, topology);
        try {
            int uptime = 20;
            int cur_uptime = 0;
            cur_uptime = getUptime(topoName);
            if (cur_uptime < uptime) {
                Util.sleep(uptime - cur_uptime);
            }
            cur_uptime = getUptime(topoName);
            while (cur_uptime < uptime) {
                Util.sleep(1);
                cur_uptime = getUptime(topoName);
            }

            String topoId = getTS(topoName).get_id();

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

            String logPath = downloadFromLogviewer(topoId, host, logviewerPort,
                    workerPort);
            final String expectedRegex =
                    ".*TRANSFERR?ING.*an apple a day keeps the doctor away.*";
            assertTrue("Topology appears to be up-and-running.",
                    fileContainsRegex(logPath, expectedRegex));

            final int startByteNum = 42;
            final int byteLength = 1947;
            String logBytesString = readBytesFromFile(logPath, startByteNum,
                    byteLength);
            String expected = StringEscapeUtils.escapeXml(logBytesString);
            String actual = getLogviewerPageContent(topoId, host,
                    logviewerPort, workerPort, startByteNum, byteLength);
            assertEquals("Log page returns correct bytes", expected, actual);
        } finally {
            cluster.killTopology(topoName);
        }
    }

    private String getLogviewerPageContent(String topoId, String host,
            int logviewerPort, int workerPort, final int startByteNum,
            final int byteLength) throws MalformedURLException, IOException,
            FileNotFoundException, XPathExpressionException, URISyntaxException {
        URL logPageUrl = new URL("http", host, logviewerPort, "/log?file="
                + topoId + "-worker-" + workerPort + ".log&" + "start="
                + startByteNum + "&length=" + byteLength);
        addToCookieJar(logPageUrl);
        String logPagePath = downloadFile(logPageUrl);
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

    private String downloadFromLogviewer(String topoId, String host,
            int logviewerPort, int workerPort) throws MalformedURLException,
            IOException, URISyntaxException {
        URL logDownloadUrl = new URL("http", host, logviewerPort, "/download/"
                + topoId + "-worker-" + workerPort + ".log");
        addToCookieJar(logDownloadUrl);
        String result = null;
        result = this.downloadFile(logDownloadUrl);
        return result;
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

    private boolean fileContainsRegex(String logPath, final String expectedRegex)
            throws IOException {
        String line = "";
        LineIterator it = null;
        try {
            for (it = FileUtils.lineIterator(new File(logPath)); it.hasNext();
                    line = it.next()) {
                if (line.matches(expectedRegex)) {
                    return true;
                }
            }
            return false;
        } finally {
            LineIterator.closeQuietly(it);
        }
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
            
            int uptime = 20;
            int cur_uptime = 0;

            cur_uptime = getUptime(topoName);

            if (cur_uptime < uptime){
                Util.sleep(uptime - cur_uptime);
            }
            
            cur_uptime = getUptime(topoName);

            while (cur_uptime < uptime){
                Util.sleep(1);
                cur_uptime = getUptime(topoName);
            }
            
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

    /*
    @SuppressWarnings("serial")
    public static class Split extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String sentence = tuple.getString(0);
            for(String word: sentence.split(" ")) {
                collector.emit(new Values(word));                
            }
        }
    }
    */

        
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
