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
    static String smileConfigFile = "smile-http-conf.clj";
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

    public void fixConfFile( ) throws Exception {
        // Extract the conf locally.
        String[] returnValue = exec.runProcBuilder(new String[] { "jar", "xf", smileJarFile,  smileConfigFile }, true);
        assertTrue( "Could not extract conf", returnValue[0].equals("0") );

        // Now fix URI and ports
        Path path = Paths.get(smileConfigFile);
        Charset charset = StandardCharsets.UTF_8;

        // Read in file
        String content = new String(Files.readAllBytes(path), charset);
        String theURI=(String)_conf.get("http.registry.uri");
        mc.setRegistryServerURI(theURI);

        // Replace what needs replacing
        content = content.replaceAll( "registry.uri \".*\"" , "registry.uri \"" + theURI + "\"" );
        content = content.replaceAll( "injection.uri \".*\"" , "injection.uri \"http://smile.test:4567/\"");
        content = content.replaceAll( "refresh.uri \".*\"" , "refresh.uri \"http://smile.test.refresh:5678/\"");
        
        // Write it back out
        Files.write(path, content.getBytes(charset));
    }

    public void train( String dirPath, String hostname, int port ) throws Exception {
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
        for (Integer i = 0 ; i < 120 ; i++ ) {
            String filename = dirPath + "/Day" + i.toString() + ".vw";
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

            Thread.sleep(1000);
        }
    }

    public void score( String inputDir ) throws Exception {
        File input = new File(inputDir + "/score.input");
        File output = new File(inputDir + "/score.output");
        BufferedReader brIn = new BufferedReader(new FileReader(input));
        BufferedReader brOut = new BufferedReader(new FileReader(output));
        int numLines = 0;
        int numIncorrect = 0;
        int numFalsePositive = 0;
        int numFalseNegative = 0;
        int numCorrectPositive = 0;
        int numCorrectNegative = 0;

        String inLine;
        while ((inLine = brIn.readLine()) != null) {
            numLines += 1;
            String drpcResult = cluster.DRPCExecute( "query", inLine );
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

    @Test
    public void TestSmile() throws Exception {
        // This is different than most tests.  We are going to use yinst to find the location of the smile jar.  It should
        // be installed on the gateway node.  We are then going to launch it via the command line client, using process builder.
        logger.info("Starting TestSmile");
        File jar = new File(smileJarFile);

        assertTrue( "Smile jar is not installed", jar.exists());

        //Munge the config file to use our virutal hosts and ports
        fixConfFile();

        // Add the registy entries.
        String[] returnValue = exec.runProcBuilder(new String[] { "/home/y/bin/registry_client", "addvh", "http://smile.test:4567/"}, true);
        assertTrue( "Could not add VH1", returnValue[0].equals("0") );
        returnValue = exec.runProcBuilder(new String[] { "/home/y/bin/registry_client", "addvh", "http://smile.test.refresh:5678/"}, true);
        assertTrue( "Could not add VH2", returnValue[0].equals("0") );

        // Launch topology
        returnValue = exec.runProcBuilder(new String[] { "storm", "jar", smileJarFile, "smile.classification.bootstrap.Bootstrap",  "conf-path", smileConfigFile }, true);
        assertTrue( "Could not launch topology", returnValue[0].equals("0") );

        // Let's get the YFOR info for the injection url.
        String YFORURL = "virtualHost/smile.test/ext/yahoo/yfor_config";
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
        train("/grid/0/smile-training", spoutHost, 4567 );
        score( "/grid/0/smile-training");
    }
}
