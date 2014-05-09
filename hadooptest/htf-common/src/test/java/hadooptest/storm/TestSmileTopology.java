package hadooptest.storm;

import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import hadooptest.SerialTests;
import hadooptest.TestSessionStorm;
import hadooptest.Util;
import hadooptest.automation.utils.http.JSONUtil;
import hadooptest.cluster.storm.ModifiableStormCluster;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import backtype.storm.generated.TopologySummary;

@SuppressWarnings("deprecation")
@Category(SerialTests.class)
public class TestSmileTopology extends TestSessionStorm {
    static ModifiableStormCluster mc;
    static String smileJarFile = "/home/y/lib/jars/smile-standalone.jar";
    static String resultsFile = "target/surefire-reports/results.csv";
    static String resultsDir = "target/surefire-reports";
    static String stormVersion = null;
    static String smileVersion = null;
    static String rsVersion = null;
    int testInstance = 0;
    String registryURI;
    private static backtype.storm.Config _conf=null;
    static long startTime;

    public static void writeColumns() throws Exception {
        File resultDir = new File( resultsDir );
        resultDir.mkdir();
        File toWrite = new File( resultsFile );
        FileWriter writer = new FileWriter(toWrite.getPath(), false);
        // Write out column headers
        writer.append("Storm Version"); writer.append(',');
        writer.append("Smile Version"); writer.append(',');
        writer.append("Reg Server Version"); writer.append(',');
        writer.append("Data Set"); writer.append(',');
        writer.append("Model Type"); writer.append(',');
        writer.append("Score Start MS"); writer.append(',');
        writer.append("Score End MS"); writer.append(',');
        writer.append("True Positive"); writer.append(',');
        writer.append("False Positive"); writer.append(',');
        writer.append("True Negative"); writer.append(',');
        writer.append("False Negative"); writer.append(',');
        writer.append("Scoring Error"); writer.append('\n');
        writer.flush();
        writer.close();
    }

    public static String getPackageVersion(String pkg) throws Exception {
        String[] returnValue = exec.runProcBuilder(new String[] { "yinst", "ls", pkg }, true);
        assertTrue( "Could not find package " + pkg, returnValue[0].equals("0") );

        Pattern p = Pattern.compile("(.*)\n");
        Matcher m = p.matcher(returnValue[1]);
        assertTrue("Could not find yinst pattern in output", m.find());

        return m.group(1);
    }

    @BeforeClass
    public static void setup() throws Exception {
        assumeTrue(cluster instanceof ModifiableStormCluster);
        mc = (ModifiableStormCluster)cluster;
        if (mc != null) {
            if (_conf == null) {
                _conf = new backtype.storm.Config();
                _conf.putAll(backtype.storm.utils.Utils.readStormConfig());
            }
            String theURI=(String)_conf.get("http.registry.uri");
            mc.setRegistryServerURI(theURI);
            mc.startRegistryServer();
            stormVersion = getPackageVersion("ystorm");
            smileVersion = getPackageVersion("ystorm_smile");
            rsVersion = getPackageVersion("ystorm_registry");
        }
        
        cluster.setDrpcAuthAclForFunction("query", "mapredqa");
        cluster.setDrpcAuthAclForFunction("gradientquery", "mapredqa");
        
        startTime = System.currentTimeMillis();
        writeColumns();
    }

    @AfterClass
    public static void cleanup() throws Exception {
        if (mc != null) {
            killAll();
            mc.resetConfigsAndRestart();
            mc.stopRegistryServer();
            killAll();
        }
        stop();
        String[] returnValue = exec.runProcBuilder(new String[] { "/homes/mapredqa/test_models/rm_model" }, true);
    }
    
    @After
    public void cleanupTest() throws Exception {
        if (mc != null) {
            killAll();
        }
    }

    public static void killAll() throws Exception {
        for (TopologySummary ts: mc.getClusterInfo().get_topologies()) {
            mc.killTopology(ts.get_name());
        }
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

    public TestSmileTopology(){
        if (_conf == null) {
            _conf = new backtype.storm.Config();
            _conf.putAll(backtype.storm.utils.Utils.readStormConfig());
        }
    }

    public void fixConfFile( String configFile, SmileSession ss ) throws Exception {
        // Fix URI and ports
        Path path = Paths.get(configFile);
        Charset charset = StandardCharsets.UTF_8;

        // Read in file
        String content = new String(Files.readAllBytes(path), charset);
        String theURI=(String)_conf.get("http.registry.uri");
        mc.setRegistryServerURI(theURI);

        // Replace what needs replacing
        content = content.replaceAll( "registry.uri \".*\"" , "registry.uri \"" + theURI + "\"" );
        content = content.replaceAll( "injection.uri \".*\"" , "injection.uri \"" + ss.injectionURL + "\"");
        content = content.replaceAll( "refresh.uri \".*\"" , "refresh.uri \"" + ss.refreshURL + "\"");
        
        // Write it back out
        Files.write(path, content.getBytes(charset));
    }

    public void train( JSONUtil json, String hostname, int port ) throws Exception {
        // Set up http client

        String dirPath = json.getElement("pathToData").toString();
        String scoringTimeout = json.getElement("scoringTimeout").toString();
        int pauseBetween = Integer.parseInt(scoringTimeout);
        ArrayList<String> trainingFiles = (ArrayList<String>) json.getElement("trainingFiles");

        HttpClient client = new HttpClient();
        client.setIdleTimeout(30000);
        try {
            client.start();
        } catch (Exception e) {
            throw new IOException("Could not start Http Client", e);
        }

        String InjectionURI = "http://" + hostname + ":" + Integer.toString(port) ;
        logger.info("Attempting to connect to injection spout with " + InjectionURI);

        
        // Iterate over files 0..119
        for (Integer i = 0 ; i < trainingFiles.size() ; i++ ) {
            File toTrain = new File(dirPath, trainingFiles.get(i));
            String filename = toTrain.getPath();
            TestSessionStorm.logger.info("Doing file " + filename );
            int tryCount = 7;
            int sleepTime = pauseBetween;
            boolean sent = false;

            File f = new File (filename);
            assertTrue("File " + filename + " does not exist", f.exists());

            ContentResponse postResp = null;
            
            java.nio.file.Path inputPath = Paths.get(filename);
            while ( !sent ) {
                postResp = null;
                try {
                    postResp = client.POST(InjectionURI).file(inputPath).send();
                } catch (Exception e) {
                    TestSessionStorm.logger.error("Post failed to URL " + InjectionURI);
                }

                if ( postResp == null || postResp.getStatus() == 429 ) {
                    tryCount -= 1;
                    if ( postResp != null )
                        TestSessionStorm.logger.debug("POST returned wait.  Sleeping " + Integer.toString(sleepTime) + " ms.");
                    else 
                        TestSessionStorm.logger.debug("POST returned null.  Sleeping " + Integer.toString(sleepTime) + " ms.");
                    Thread.sleep(sleepTime);
                    sleepTime = sleepTime * 2;
                } else if ( postResp.getStatus() == 200 ) {
                    sent = true;
                } else {
                    TestSessionStorm.logger.error("Post failed to URL, resp code = " + postResp.getStatus() );
                    TestSessionStorm.logger.error("Post failed to URL, resp = " + postResp.getContentAsString() );
                    throw new IOException("POST failed");
                }
            }
            if ( !sent ) {
                throw new IOException("POST failed - num retries expired");
            }
            Thread.sleep(pauseBetween);
        }
        TestSessionStorm.logger.info("Finished traiing");
    }

    public void score( JSONUtil json ) throws Exception {
        String function = json.getElement("function").toString();
        String dataSet = json.getElement("dataSet").toString();
        String modelType = json.getElement("modelType").toString();
        String inputDir = json.getElement("pathToData").toString();
        String scoringInput = json.getElement("scoringInput").toString();
        String scoringOutput = json.getElement("scoringOutput").toString();
        File input = new File(inputDir,  scoringInput);
        File output = new File(inputDir, scoringOutput);
        BufferedReader brIn = new BufferedReader(new FileReader(input));
        BufferedReader brOut = new BufferedReader(new FileReader(output));
        int numLines = 0;
        int numIncorrect = 0;
        int numFalsePositive = 0;
        int numFalseNegative = 0;
        int numCorrectPositive = 0;
        int numCorrectNegative = 0;
        ArrayList<Long> startTimes = new ArrayList<Long>();
        ArrayList<Long> endTimes = new ArrayList<Long>();
        ArrayList<Integer> correctness = new ArrayList<Integer>(); // 0 = TP, 1 = FP, 2 = TN, 3 = FN, 4 = Invalid

        logger.info("Started scoring from " + input.getPath());
        logger.info("Started scoring against " + output.getPath());
        logger.info("function  = " + function );
        logger.info("dataSet  = " + dataSet );
        logger.info("modelType  = " + modelType );

        String inLine;
        while ((inLine = brIn.readLine()) != null) {
            numLines += 1;
            startTimes.add(System.currentTimeMillis());
            String drpcResult = cluster.DRPCExecute( function, inLine );
            endTimes.add(System.currentTimeMillis());
            String outLine = brOut.readLine();
            logger.debug("drpc result = " + drpcResult + " from file " + outLine);
            assertTrue("Couldn't get corresponding output", brOut != null);
            if ( drpcResult != null && drpcResult.equals("1") ) {
                if (drpcResult.equals(outLine)) {
                    numCorrectPositive += 1;
                    correctness.add(0);
                } else {
                    numFalsePositive += 1;
                    correctness.add(1);
                }
            } else {
                if ( drpcResult != null && drpcResult.equals("-1") ) {
                    if (drpcResult.equals(outLine)) {
                        numCorrectNegative += 1;
                        correctness.add(2);
                    } else {
                        numFalseNegative += 1;
                        correctness.add(3);
                    }
                } else {
                        correctness.add(4);
                }
            }
        }
        brIn.close();
        brOut.close();
        logger.info("Number of lines of scoring data = " + Integer.toString(numLines) );
        logger.info("Number of correct positives = " + Integer.toString(numCorrectPositive) );
        logger.info("Number of false positives = " + Integer.toString(numFalsePositive) );
        logger.info("Number of correct negatives = " + Integer.toString(numCorrectNegative) );
        logger.info("Number of false negatives = " + Integer.toString(numFalseNegative) );
        logger.info("Total correct = " + Integer.toString(numCorrectPositive + numCorrectNegative) );
        logger.info("Total incorrect = " + Integer.toString(numFalsePositive + numFalseNegative) );
        double percentageCorrect = ((double) (numCorrectPositive + numCorrectNegative)) / ((double) numLines);
        percentageCorrect = percentageCorrect * 100.0;
        logger.info("Percentage correct = " + String.format("%.2f", percentageCorrect));
        assertTrue("Percentage correct < 80 ", percentageCorrect >= 80.0 );

        // Let's write out everything
        File toWrite = new File( resultsFile );
        FileWriter writer = new FileWriter(toWrite.getPath(), true);

        for (int i = 0 ; i < numLines ; i++ ) {
            writer.append(stormVersion); writer.append(',');
            writer.append(smileVersion); writer.append(',');
            writer.append(rsVersion); writer.append(',');
            writer.append(dataSet); writer.append(',');
            writer.append(modelType); writer.append(',');
            writer.append(String.valueOf(startTimes.get(i) - startTime )); writer.append(',');
            writer.append(String.valueOf(endTimes.get(i) - startTime )); writer.append(',');
            int result = correctness.get(i);
            if ( result == 0 ) {
                writer.append("1"); writer.append(',');
            } else {
                writer.append("0"); writer.append(',');
            }
            if ( result == 1 ) {
                writer.append("1"); writer.append(',');
            } else {
                writer.append("0"); writer.append(',');
            }
            if ( result == 2 ) {
                writer.append("1"); writer.append(',');
            } else {
                writer.append("0"); writer.append(',');
            }
            if ( result == 3 ) {
                writer.append("1"); writer.append(',');
            } else {
                writer.append("0"); writer.append(',');
            }
            if ( result == 4 ) {
                writer.append("1"); writer.append('\n');
            } else {
                writer.append("0"); writer.append('\n');
            }
        }
        writer.flush();
        writer.close();
    }
    
    public class SmileSession {
        public String injectionURL;
        public String refreshURL;
        public String injectionHost;
        public String refreshHost;
        public int injectionPort;
        public int refreshPort;

        // Not thread safe, but this will not be called concurrently
        public SmileSession() {
            injectionPort = 4567 + testInstance;
            injectionHost = "smile" + Integer.toString(testInstance) + ".test";
            injectionURL = "http://" + injectionHost + ":" + Integer.toString(injectionPort) + "/";
            refreshPort = 5678 + testInstance;
            refreshHost = "smile" + Integer.toString(testInstance) + ".test.refresh";
            refreshURL = "http://" + refreshHost + ":" + Integer.toString(refreshPort) + "/";
            testInstance = testInstance + 1;
        }

        public void addVH() throws Exception {
            // Add the registy entries.
            String[] returnValue = exec.runProcBuilder(new String[] { "/home/y/bin/registry_client", "addvh", injectionURL }, true);
            assertTrue( "Could not add Injection VH " + injectionURL , returnValue[0].equals("0") );
            returnValue = exec.runProcBuilder(new String[] { "/home/y/bin/registry_client", "addvh", refreshURL }, true);
            assertTrue( "Could not add refresh VH " + refreshURL, returnValue[0].equals("0") );
        }
    }

    public String launchSmileTopology( String pathToConf, SmileSession ss) throws Exception {

        String[] returnValue = exec.runProcBuilder(new String[] { "storm", "jar", smileJarFile, "smile.classification.bootstrap.Bootstrap",  "conf-path", pathToConf }, true);
        assertTrue( "Could not launch topology", returnValue[0].equals("0") );

        // Let's get the YFOR info for the injection url.
        String YFORURL = "virtualHost/" + ss.injectionHost + "/ext/yahoo/yfor_config";
        String YForResult = null;
        int trycount = 100;
        String spoutHost = null;
        Pattern p = Pattern.compile("host (.*)\n");
        while( trycount > 0 && YForResult == null) {
            try {
                YForResult = mc.getFromRegistryServer(YFORURL);
            } catch (Exception e ) {
                Util.sleep(1);
                logger.info("Retrying for YFOR info for " + YFORURL);
            }
            if (YForResult != null) {
                logger.info("Got input\n" + YForResult);
                logger.info("Searching input for host: ");
                Matcher m = p.matcher(YForResult);
                if (m.find()) {
                    spoutHost = m.group(1);
                    logger.info("found host: "+ spoutHost);
                } else {
                    logger.info("Did not find host, retrying ");
                    YForResult = null;
                }
            }
            trycount = trycount - 1;
        }
        assertTrue( "Could not get YFOR information", spoutHost != null);

        return spoutHost;
    }

    public void testSmile(String pathToJson) throws Exception {
        // Read in json based config
        JSONUtil json = new JSONUtil();
        json.setContentFromFile(pathToJson);

        // Get the location of the clojure conf we need to submit with topology
        String pathToConf = json.getElement("pathToConf").toString();

        // This is different than most tests.  We are going to use yinst to find the location of the smile jar.  It should
        // be installed on the gateway node.  We are then going to launch it via the command line client, using process builder.
        logger.info("Starting testSmile");
        File jar = new File(smileJarFile);
        assertTrue( "Smile jar is not installed", jar.exists());

        // Add VH, and store off virtual host names and ports in class
        SmileSession ss = new SmileSession();
        ss.addVH();
 
        //Munge the config file to use our virutal hosts and ports
        fixConfFile( pathToConf, ss );

        String spoutHost = launchSmileTopology( pathToConf, ss);

        // All right.  We now have the location of the injection port.  Train away.
        train(json, spoutHost, ss.injectionPort);
        logger.info("Sleeping 10 seconds to give model time to replicate");
        Util.sleep(10);
        score(json);
    }

    public void testSmilePersist(String pathToJson) throws Exception {
        // Read in json based config
        JSONUtil json = new JSONUtil();
        json.setContentFromFile(pathToJson);

        // Get the location of the clojure conf we need to submit with topology
        String pathToConf = json.getElement("pathToConf").toString();

        // This is different than most tests.  We are going to use yinst to find the location of the smile jar.  It should
        // be installed on the gateway node.  We are then going to launch it via the command line client, using process builder.
        logger.info("Starting testSmile");
        File jar = new File(smileJarFile);
        assertTrue( "Smile jar is not installed", jar.exists());

        // Add VH, and store off virtual host names and ports in class
        SmileSession ss = new SmileSession();
        ss.addVH();
 
        //Munge the config file to use our virutal hosts and ports
        fixConfFile( pathToConf, ss );
        String spoutHost = launchSmileTopology( pathToConf, ss);

        // All right.  We now have the location of the injection port.  Train away.
        train(json, spoutHost, ss.injectionPort);

        logger.info("sleep 40 seconds to ensure model is written out");
        Util.sleep(40);

        // Now go ahead and kill the topology.  Wait 40 seconds, relaunch, and then score.  It should still work.
        logger.info("Kill topology and sleep 40 seconds");
        killAll();
        Util.sleep(40);

        logger.info("Relaunching topology");
        spoutHost = launchSmileTopology( pathToConf, ss);

        logger.info("sleep 40 seconds to ensure model is read in");
        Util.sleep(40);

        // Score it.
        logger.info("Scoring");
        score(json);

        killAll();
        Util.sleep(20);
        String[] returnValue = exec.runProcBuilder(new String[] { "/homes/mapredqa/test_models/rm_model" }, true);
    }

    @Test
    public void TestVW() throws Exception {
        testSmile("resources/storm/testinputoutput/TestSmileTopology/svm-vw.json");
    }

    @Test
    public void TestGradient() throws Exception {
        testInstance = 1;
        testSmile("resources/storm/testinputoutput/TestSmileTopology/svm-gd.json");
    }

    @Test
    public void TestFlickrVW() throws Exception {
        testInstance = 2;
        testSmile("resources/storm/testinputoutput/TestSmileTopology/flickr-vw.json");
    }

    @Test
    public void TestFlickrGD() throws Exception {
        testInstance = 3;
        testSmile("resources/storm/testinputoutput/TestSmileTopology/flickr-gd.json");
    }

    @Test
    public void TestSVMGradientPersist() throws Exception {
        testInstance = 4;
        String[] returnValue = exec.runProcBuilder(new String[] { "/homes/mapredqa/test_models/rm_model" }, true);
        testSmilePersist("resources/storm/testinputoutput/TestSmileTopology/svm-gd-persist.json");
    }
}
