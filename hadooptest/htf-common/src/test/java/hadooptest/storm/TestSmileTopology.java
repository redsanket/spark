package hadooptest.storm;

import java.io.File;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;

import static org.junit.Assume.*;
import java.net.URI;
import java.util.HashSet;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.regex.*;
import java.io.IOException;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import static org.junit.Assert.*;
import hadooptest.SerialTests;
import hadooptest.TestSessionStorm;
import hadooptest.cluster.storm.ModifiableStormCluster;
import hadooptest.automation.utils.http.JSONUtil;
import hadooptest.Util;

import backtype.storm.generated.*;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;

@SuppressWarnings("deprecation")
@Category(SerialTests.class)
public class TestSmileTopology extends TestSessionStorm {
    static ModifiableStormCluster mc;
    static String smileJarFile = "/home/y/lib/jars/smile-standalone.jar";
    static String smileConfigFile = "resources/storm/testinputoutput/TestSmileTopology/smile-http-conf.clj";
    static String smileGradientConfigFile = "resources/storm/testinputoutput/TestSmileTopology/smile-http-gradient-conf.clj";
    int testInstance = 0;
    String registryURI;
    private static backtype.storm.Config _conf=null;

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
        }
    }

    @AfterClass
    public static void cleanup() throws Exception {
        if (mc != null) {
            killAll();
            mc.resetConfigsAndRestart();
            mc.stopRegistryServer();
        }
        stop();
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

    public void train( String dirPath, ArrayList<String> trainingFiles, String hostname, int port, int pauseBetween ) throws Exception {
        // Set up http client

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

            File f = new File (filename);
            assertTrue("File " + filename + " does not exist", f.exists());

            ContentResponse postResp = null;
            
            java.nio.file.Path inputPath = Paths.get(filename);
            try {
                postResp = client.POST(InjectionURI).file(inputPath).send();
            } catch (Exception e) {
                TestSessionStorm.logger.error("Post failed to URL " + InjectionURI);
                throw new IOException("Could not put to Injection spout", e);
            }

            if (postResp == null || postResp.getStatus() != 200) {
                if (postResp == null) {
                    TestSessionStorm.logger.error("Post failed to URL " + InjectionURI);
                } else {
                    TestSessionStorm.logger.error("Post failed to URL, resop = " + postResp.getStatus() );
                }
                throw new IOException("POST returned null or bad status");
            }

            Thread.sleep(pauseBetween);
        }
        TestSessionStorm.logger.info("Finished traiing");
    }

    public void score( String inputDir, String scoringInput, String scoringOutput, String function ) throws Exception {
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

        logger.info("Started scoring from " + input.getPath());
        logger.info("Started scoring against " + output.getPath());

        String inLine;
        while ((inLine = brIn.readLine()) != null) {
            numLines += 1;
            String drpcResult = cluster.DRPCExecute( function, inLine );
            String outLine = brOut.readLine();
            logger.info("drpc result = " + drpcResult + " from file " + outLine);
            assertTrue("Couldn't get corresponding output", brOut != null);
            if ( outLine.equals("1") ) {
                if (drpcResult.equals(outLine)) {
                    numCorrectPositive += 1;
                } else {
                    numFalsePositive += 1;
                }
            } else {
                if (drpcResult.equals(outLine)) {
                    numCorrectNegative += 1;
                } else {
                    numFalseNegative += 1;
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

    public void testSmile(String pathToJson) throws Exception {
        JSONUtil json = new JSONUtil();

        json.setContentFromFile(pathToJson);

        // Get the location of the traing data, etc from json based config.
        String function = json.getElement("function").toString();
        String pathToConf = json.getElement("pathToConf").toString();
        String pathToData = json.getElement("pathToData").toString();
        String scoringTimeout = json.getElement("scoringTimeout").toString();
        ArrayList<String> trainingFiles = (ArrayList<String>) json.getElement("trainingFiles");
        String scoringInput = json.getElement("scoringInput").toString();
        String scoringOutput = json.getElement("scoringOutput").toString();
        logger.info("Starting testSmile");
        logger.info("function  = " + function );
        logger.info("pathToConf  = " + pathToConf );
        logger.info("pathToData  = " + pathToData );
        logger.info("scoringTimeout  = " + scoringTimeout );
        logger.info("trainingFiles  = " + trainingFiles );
        logger.info("scoringInput  = " + scoringInput );
        logger.info("scoringOutput  = " + scoringOutput );
        for ( int i = 0 ; i < trainingFiles.size() ; i++ ) {
            String s = trainingFiles.get(i);
            logger.info( "File " + s );
        }

        // This is different than most tests.  We are going to use yinst to find the location of the smile jar.  It should
        // be installed on the gateway node.  We are then going to launch it via the command line client, using process builder.
        logger.info("Starting testSmile");
        File jar = new File(smileJarFile);

        // Add VH, and store off virtual host names and ports in class
        SmileSession ss = new SmileSession();
        ss.addVH();
 
        assertTrue( "Smile jar is not installed", jar.exists());

        //Munge the config file to use our virutal hosts and ports
        fixConfFile( pathToConf, ss );

        // Launch topology
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
                trycount = trycount - 1;
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
        }
        assertTrue( "Could not get YFOR information", spoutHost != null);

        // All right.  We now have the location of the injection port.  Train away.
        train(pathToData, trainingFiles, spoutHost, ss.injectionPort, Integer.parseInt(scoringTimeout));
        score( pathToData, scoringInput, scoringOutput, function);
    }

    @Test
    public void TestVW() throws Exception {
        testSmile("resources/storm/testinputoutput/TestSmileTopology/svm-vw.json");
        killAll();
    }

    @Test
    public void TestGradient() throws Exception {
        testInstance = 1;
        testSmile("resources/storm/testinputoutput/TestSmileTopology/svm-gd.json");
        killAll();
    }

    @Test
    public void TestFlickrVW() throws Exception {
        testInstance = 2;
        testSmile("resources/storm/testinputoutput/TestSmileTopology/flickr-vw.json");
        killAll();
    }

    @Test
    public void TestFlickrGD() throws Exception {
        testInstance = 3;
        testSmile("resources/storm/testinputoutput/TestSmileTopology/flickr-gd.json");
        killAll();
    }
}
