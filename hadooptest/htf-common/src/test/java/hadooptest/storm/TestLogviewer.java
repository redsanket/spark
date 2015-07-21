package hadooptest.storm;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assume.assumeTrue;
import hadooptest.SerialTests;
import hadooptest.TestSessionStorm;
import hadooptest.Util;
import hadooptest.automation.utils.http.HTTPHandle;
import hadooptest.automation.utils.http.Response;
import hadooptest.cluster.storm.ModifiableStormCluster;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.net.CookieManager;
import java.net.CookieStore;
import java.net.HttpCookie;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.thrift7.TException;
import org.json.simple.JSONValue;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.w3c.dom.Document;
import org.w3c.tidy.Tidy;

import backtype.storm.Config;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.utils.Utils;

@Category(SerialTests.class)
public class TestLogviewer extends TestSessionStorm {
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

    class LogviewerQueryStruct {
      public final String host;
      public final int workerPort;
      public final int logviewerPort;
      public LogviewerQueryStruct(String h, int wp, int lvp) {
        this.host = h;
        this.workerPort = wp;
        this.logviewerPort = lvp;
      }
    }

    @Test(timeout=300000)
    public void LogviewerPagingTest() throws Exception {
        assumeTrue(cluster instanceof ModifiableStormCluster);
        final String topoName = "logviewer-paging-test";
        createAndSubmitLogviewerTopology(topoName);
        try {
            final String topoId = getFirstTopoIdForName(topoName);
            waitForTopoUptimeSeconds(topoId, 30);
            LogviewerQueryStruct lqs = getLogviewerQueryInformation(topoId);
            HTTPHandle client = createAndPrepHttpClient();

            String getURL = "http://" + lqs.host + ":" + lqs.logviewerPort +
                    "/download/" + URLEncoder.encode(topoId + "/" + lqs.workerPort + "/worker.log", "UTF-8");
            String output = performHttpRequest(client, getURL);

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
            String actual = getLogviewerPageContent(client, topoId, lqs.host,
                    lqs.logviewerPort, lqs.workerPort, startByteNum,
                    byteLength);
            assertEquals("Log page returns correct bytes", expected, actual);

            //Now testing the gzip page viewing
            String getZipURL = "http://" + lqs.host + ":" + lqs.logviewerPort +
			        "/log?file=" + URLEncoder.encode(topoId + "/" + lqs.workerPort + "/worker", "UTF-8") +
                    ".log.1.gz" + "&start=0&length=51200";
            String outputPage1 = performHttpRequest(client, getZipURL);

            assertTrue("First page of gzip log returned correctly",
                    outputPage1.contains("Quick brown fox jumped over the lazy dog"));

            getZipURL = "http://" + lqs.host + ":" + lqs.logviewerPort +
			        "/log?file=" + URLEncoder.encode(topoId + "/" + lqs.workerPort + "/worker", "UTF-8") +
                    ".log.1.gz" + "&start=51200&length=51200";
            String outputPage2 = performHttpRequest(client, getZipURL);

            assertTrue("Second page of gzip log returned correctly",
                    outputPage2.contains("Take an apple a day keep the doctor away"));

            getZipURL = "http://" + lqs.host + ":" + lqs.logviewerPort +
			        "/log?file=" + URLEncoder.encode(topoId + "/" + lqs.workerPort + "/worker", "UTF-8") +
                    ".log.1.gz" + "&length=51200";
            String outputPageLast = performHttpRequest(client, getZipURL);

            assertTrue("Last page of gzip log returned correctly",
                    outputPageLast.contains("Don't cross your beidges until you come to them"));
        } finally {
            cluster.killTopology(topoName);
        }
    }

    /**
     * @param topoId
     * @return
     * @throws NotAliveException
     * @throws AuthorizationException
     * @throws TException
     */
    private LogviewerQueryStruct getLogviewerQueryInformation(
        final String topoId) throws NotAliveException, AuthorizationException,
        TException {
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

      LogviewerQueryStruct lqs =
              new LogviewerQueryStruct(host, workerPort, logviewerPort);
      return lqs;
    }

    /**
     * Creates a handle and obtains a bouncer cookie if possible.
     * @return the handle
     * @throws Exception
     */
    private HTTPHandle createAndPrepHttpClient() throws Exception {
      ModifiableStormCluster mc = (ModifiableStormCluster)cluster;

      String pw = null;
      String user = null;
      HTTPHandle client = new HTTPHandle();

      // Only get bouncer auth on secure storm cluster.
      if ( isUISecure() && mc != null) {
          user = mc.getBouncerUser();
          pw = mc.getBouncerPassword();
          client.logonToBouncer(user,pw);
      }

      assertNotNull("cookie is null", HTTPHandle.YBYCookie);
      logger.info("Cookie = " + HTTPHandle.YBYCookie);
      return client;
    }

    private void createAndSubmitLogviewerTopology(String topoName)
        throws Exception {
      StormTopology topology = TestWordCountTopology.buildTopology("TestLogViewer");

      String outputLoc = new File("/tmp", topoName).getCanonicalPath();

      Config config = new Config();
      config.put("test.output.location", outputLoc);
      config.setDebug(true);
      config.setNumWorkers(1);
      config.put(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD, "123:456");
      ModifiableStormCluster mc = (ModifiableStormCluster)cluster;
      List<String> whiteList = Arrays.asList(mc.getBouncerUser());
      config.put(Config.UI_USERS, whiteList);
      config.put(Config.LOGS_USERS, whiteList);
      cluster.submitTopology(getTopologiesJarFile(), topoName, config, topology);
    }

    private String getLogviewerPageContent(HTTPHandle client, String topoId,
            String host, int logviewerPort, int workerPort, final int
            startByteNum, final int byteLength) throws MalformedURLException,
            IOException, FileNotFoundException, XPathExpressionException,
            URISyntaxException {
        URL logPageUrl = new URL("http", host, logviewerPort, "/log?file=" +
                URLEncoder.encode(topoId + "/" + workerPort + "/worker.log", "UTF-8") +
                "&start=" + startByteNum + "&length=" + byteLength);
        return getLogviewerPageContent(client, logPageUrl);
    }

    private String getLogviewerPageContent(HTTPHandle client, URL url)
            throws IOException, XPathExpressionException {
        HttpMethod getMethod = client.makeGET(url.toString(), new String(""),
                null);
        Response response = new Response(getMethod, false);
        String output = response.getResponseBodyAsString();

        File tmpFile = File.createTempFile("logPage" +
                this.getClass().getName(), null);

        // Write out the original logfile
        String logPagePath = tmpFile.getCanonicalPath();
        PrintWriter out = new PrintWriter(logPagePath);
        out.println(output);
        out.close();

        Tidy tidy = new Tidy();
        tidy.setQuiet(true);
        tidy.setXmlSpace(true);
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

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test(timeout=300000)
    public void LogviewerDeepSearchTest() throws Exception {
        assumeTrue(cluster instanceof ModifiableStormCluster);
        final String topoName = "logviewer-deep-search-test";
        createAndSubmitLogviewerTopology(topoName);
        final String topoId = getFirstTopoIdForName(topoName);
        waitForTopoUptimeSeconds(topoId, 30);

        LogviewerQueryStruct lqs = getLogviewerQueryInformation(topoId);
        HTTPHandle client = createAndPrepHttpClient();

        String getURL = "http://" + lqs.host + ":" + lqs.logviewerPort +
                "/download/" + URLEncoder.encode(topoId + "/" + lqs.workerPort + "/worker.log", "UTF-8");
        String output = performHttpRequest(client, getURL);

        String searchSubstring = "an apple a day keep";
        final String expectedRegex =
                ".*TRANSFERR?ING.*" + searchSubstring + "*";
        Pattern p = Pattern.compile(expectedRegex, Pattern.DOTALL);
        Matcher regexMatcher = p.matcher(output);

        assertTrue("Topology appears to be up-and-running.",
                regexMatcher.find());

        String encodedSearchSubstring =
                URLEncoder.encode(searchSubstring, "UTF-8");
        final int numMatches = 25;
        logger.info("Will be connecting to the deep search page on UI at " + lqs.host + ":" + lqs.logviewerPort);

        getURL = "http://" + lqs.host + ":" + lqs.logviewerPort +
                "/deepSearch/" + topoId +
                "?search-string="+encodedSearchSubstring+
                "&num-matches="+numMatches+
                "&port=" + lqs.workerPort +
                "&search-archived=on" +
                "&start-file-offset=0";
        String searchOutput = performHttpRequest(client, getURL);

        assertTrue("Response returns at least a match", searchOutput.contains(searchSubstring));
        assertTrue("One of the matches is from the rolled log file",
                searchOutput.contains( topoId + "") && searchOutput.contains( lqs.workerPort + "") &&
				searchOutput.contains( "worker.log.1.gz&"));
        assertTrue("One of the matches is from the regular log file",
                searchOutput.contains( "worker.log&"));
    }


    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test(timeout=300000)
    public void LogviewerGrepTest() throws Exception {
        assumeTrue(cluster instanceof ModifiableStormCluster);
        final String topoName = "logviewer-search-test";
        createAndSubmitLogviewerTopology(topoName);
        try {
            final String topoId = getFirstTopoIdForName(topoName);
            waitForTopoUptimeSeconds(topoId, 30);
            LogviewerQueryStruct lqs = getLogviewerQueryInformation(topoId);
            HTTPHandle client = createAndPrepHttpClient();

            // Download the entire log
            String getURL = "http://" + lqs.host + ":" + lqs.logviewerPort +
                    "/download/" + URLEncoder.encode(topoId + "/" + lqs.workerPort + "/worker.log", "UTF-8");
            String logFileText = performHttpRequest(client, getURL);

            final String searchSubstring =
                    "an apple a day keeps the doctor away";
            final String expectedRegex =
                    ".*TRANSFERR?ING.*"+searchSubstring+".*";
            Pattern p = Pattern.compile(expectedRegex, Pattern.DOTALL);
            Matcher regexMatcher = p.matcher(logFileText);

            assertTrue("Topology appears to be up-and-running.",
                    regexMatcher.find());

            String encodedSearchSubstring =
                    URLEncoder.encode(searchSubstring, "UTF-8");
            final int numMatches = 3;
            getURL = "http://" + lqs.host + ":" + lqs.logviewerPort +
			        "/search/" + URLEncoder.encode(topoId + "/" + lqs.workerPort + "/worker", "UTF-8") +
                    ".log?search-string="+encodedSearchSubstring+
                    "&num-matches="+numMatches;
            String output = performHttpRequest(client, getURL);
            Map respMap = (Map) JSONValue.parseWithException(output);

            assertTrue("Response returns the pattern searched for.",
                    respMap.containsKey("searchString") &&
                    searchSubstring.equals(respMap.get("searchString")));
            assertTrue("Response returns some matches.",
                    respMap.containsKey("matches") &&
                    !((List)respMap.get("matches")).isEmpty());

            final String startByteOffsetKey = "startByteOffset";
            assertTrue("Match has starting offset, and it is a Long",
                    respMap.containsKey(startByteOffsetKey) &&
                    respMap.get(startByteOffsetKey) instanceof Long);
            long startOffset = (Long) respMap.get(startByteOffsetKey);

            final String nextByteOffsetKey = "nextByteOffset";
            assertTrue("Match has next offset, and it is a Long",
                    respMap.containsKey(nextByteOffsetKey) &&
                    respMap.get(nextByteOffsetKey) instanceof Long);
            long nextOffset = (Long) respMap.get(nextByteOffsetKey);

            final int searchStringSubstringSize =
                    searchSubstring.getBytes("UTF-8").length;

            for (Map match: ((List<Map>) respMap.get("matches"))) {
                assertTrue("Matches returned match the given pattern",
                        searchSubstring.equals(
                        (String)match.get("matchString")));
                for (String k : Arrays.asList("beforeString", "afterString",
                        "byteOffset", "logviewerURL", "matchString")) {
                  assertTrue("match has key '" +k+ "'",
                          match.containsKey(k));
                }

                long mOffset = (Long) match.get("byteOffset");
                assertTrue("match offset is within bounds",
                        startOffset <= mOffset &&
                        mOffset <= (nextOffset - searchStringSubstringSize));

                String logviewerOutput = getLogviewerPageContent(client,
                        new URL((String)match.get("logviewerURL")));

                // We assume the log has no multi-byte characters for this
                // assertion.
                assertEquals("Link shows a page of the log with match centered",
                    searchSubstring,
                    logviewerOutput.substring(25600-searchSubstring.length()/2,
                                25600+searchSubstring.length()/2));
            }

            assertTrue("Next byte offset is greater than the start offset.",
                    nextOffset > (Long) respMap.get("startByteOffset"));

            // Advance one page.
            getURL = "http://" + lqs.host + ":" + lqs.logviewerPort +
			        "/search/" + URLEncoder.encode(topoId + "/" + lqs.workerPort + "/worker", "UTF-8") +
                    ".log?search-string=" +
                    URLEncoder.encode(searchSubstring, "UTF-8")+
                    "&start-byte-offset="+nextOffset;
            output = performHttpRequest(client, getURL);
            respMap = (Map) JSONValue.parseWithException(output);

            assertEquals("Search started at the correct offset",
                (Long) respMap.get("startByteOffset"),
                (Long) nextOffset);

            String unexpectedPattern = UUID.randomUUID().toString();
            getURL = "http://" + lqs.host + ":" + lqs.logviewerPort +
			        "/search/" + URLEncoder.encode(topoId + "/" + lqs.workerPort + "/worker", "UTF-8") +
                    ".log?search-string=" + unexpectedPattern;
            output = performHttpRequest(client, getURL);
            respMap = (Map) JSONValue.parseWithException(output);
            assertFalse("There is no next byte offset after no-match",
                respMap.containsKey("nextByteOffset"));
            assertTrue("There are no matches",
                ((List)respMap.get("matches")).isEmpty());

        } finally {
            cluster.killTopology(topoName);
        }
    }

    /**
     * @param client
     * @param getURL
     * @return
     */
    private String performHttpRequest(HTTPHandle client, String getURL) {
      logger.info("URL to get is: " + getURL);
      HttpMethod getMethod = client.makeGET(getURL, new String(""), null);
      Response response = new Response(getMethod, false);
      String output = response.getResponseBodyAsString();
      logger.info("******* OUTPUT = " + output);
      return output;
    }
}
