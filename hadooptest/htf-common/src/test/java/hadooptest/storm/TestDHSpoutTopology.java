package hadooptest.storm;

import static org.junit.Assume.assumeTrue;
import hadooptest.SerialTests;
import hadooptest.TestSessionStorm;
import hadooptest.Util;
import hadooptest.cluster.storm.ModifiableStormCluster;
import hadooptest.workflow.storm.topology.bolt.Aggregator;
import hadooptest.workflow.storm.topology.bolt.TestEventCountBolt;
import hadooptest.workflow.storm.topology.spout.TestDHSpout;

import java.io.File;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import backtype.storm.generated.StormTopology;
import backtype.storm.generated.TopologySummary;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.drpc.DRPCSpout;
import backtype.storm.drpc.ReturnResults;

import com.yahoo.dhrainbow.dhapi.AvroEventRecord;
import com.yahoo.spout.http.Config;
import com.yahoo.spout.http.RegistryStub;
import com.yahoo.spout.http.rainbow.DHSimulator;
import com.yahoo.spout.http.rainbow.KryoEventRecord;

@Category(SerialTests.class)
public class TestDHSpoutTopology extends TestSessionStorm {
    static ModifiableStormCluster mc;
    static String function = "wordQuery";
    //static String configURI="http://0.0.0.0:8090/registry/v1/";
    //static String serverURI=configURI;
    static String serverURI=null;
    static String vhURI="http://myvh-stormtest.corp.yahoo.com:9153/";
    static String vhEphemeralURI="http://myvh-stormtest-eph.corp.yahoo.com/";
    private backtype.storm.Config _conf;
    private String _ycaV1Role;
    private TestEventCountBolt theBolt = null;

    @BeforeClass
    public static void setup() throws Exception {
        assumeTrue(cluster instanceof ModifiableStormCluster);
        mc = (ModifiableStormCluster)cluster;
        if (mc != null) {
            backtype.storm.Config theconf = new backtype.storm.Config();
            theconf.putAll(backtype.storm.utils.Utils.readStormConfig());
            String theURI=(String)theconf.get("http.registry.uri");
            serverURI=theURI;
            mc.setRegistryServerURI(theURI);
            mc.startRegistryServer();
            cluster.setDrpcAclForFunction(function);
        }
    }

    public void addVirtualHost(URI serviceURI) throws Exception {
        String registryURI = serverURI;
        String registryProxy = (String)_conf.get(Config.REGISTRY_PROXY);
        logger.info("registry uri:"+registryURI);
        logger.info("registry proxy uri:"+registryProxy);
        RegistryStub registry = new RegistryStub(registryURI, registryProxy, null);
        registry.setYCAv1Role(_ycaV1Role);

        try {
            logger.info("service uri:"+serviceURI);
            String serviceID = com.yahoo.spout.http.Util.ServiceURItoID(serviceURI);
            HashSet<String> owners = new HashSet<String>();
            if (_ycaV1Role != null) owners.add("yca:"+_ycaV1Role);
            registry.addVirtualHost(serviceID, owners.toArray(new String[owners.size()]), com.yahoo.spout.http.Util.useHttps(serviceURI));
        } finally {
            if (registry != null) {
                registry.stop();
            }
        }
    }


    @AfterClass
    public static void cleanup() throws Exception {
        if (mc != null) {
            mc.resetConfigs();
            mc.stopRegistryServer();
        }
        stop();
    }

    public TestDHSpoutTopology(){
        //empty constructor
    }
    
    @Test(timeout=600000)
    public void TestDHSpoutTopologyHTTP() throws Exception{
        logger.info("Starting TestDHSpoutTopology");
        StormTopology topology = buildTopology(vhURI);

        String topoName = "dhspout-topology-test";

        _conf = new backtype.storm.Config();
        _conf.putAll(backtype.storm.utils.Utils.readStormConfig());
        _conf.setDebug(true);
        _conf.setNumWorkers(3);
        _conf.put(backtype.storm.Config.NIMBUS_TASK_TIMEOUT_SECS, 200);
        _conf.put(Config.REGISTRY_URI, serverURI);

        _conf.registerSerialization(AvroEventRecord.class, KryoEventRecord.class);

        String outputLoc = "/tmp/dhcount"; //TODO change this to use a shared directory or soemthing, so we can get to it simply
        _conf.put("test.output.location",outputLoc);

        addVirtualHost(new URI(vhURI));
        //TODO turn this into a utility that has a conf setting
        File jar = new File(conf.getProperty("WORKSPACE") + "/topologies/target/topologies-1.0-SNAPSHOT-jar-with-dependencies.jar");
        try {
            cluster.submitTopology(jar, topoName, _conf, topology);
        } catch (Exception e) {
            logger.error("Couldn't launch topology: " + e );
            throw new Exception();
        }

        // Now we need to wait until the spout shows up in the registry server's YFOR config.
        //
        // Actually, it __should__ wait for it to come up, but let's sleep a little.

        _conf.put(Config.REGISTRY_URI, serverURI);
        Integer dhCount = new Integer(20);


        logger.info("Launching DHSimulator:");
        DHSimulator dh = new DHSimulator(_conf, new URI(vhURI), null, null, 1, false, null, dhCount);

        try {
            logger.info("Starting DHSimulator:");
            dh.start();
            logger.info("Joining DHSimulator:");
            dh.join();
            logger.info("Leaving join of DHSimulator, sleeping for a little bit to make sure all batches processed:");
            Util.sleep(10);
        } finally {
            logger.info("Halting DHSimulator:");
            dh.halt();
        }

        boolean passed = true;
        // Need to fix this, but we are going to sleep for a while so I can check state of cluster to see if it really did anything.
        try {

            int uptime = 10;
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

            //get results

            //get expected results
            String file = conf.getProperty("WORKSPACE") + "/htf-common/resources/storm/testinputoutput/TestDHSpoutTopology/expected_results";
            logger.info("Read epected results from: "+ file);
            HashMap<String, Integer> expectedWordCount = Util.readMapFromFile(file);

            // Now check to see if the bolt counted the right number of packets
            for (String key: expectedWordCount.keySet()){
                logger.info("Checking to see if the value for " + key + "is " + expectedWordCount.get(key));
                String drpcResult = cluster.DRPCExecute( function, key );
                passed &= Integer.toString(expectedWordCount.get(key)).equals(drpcResult);
                if (!passed) {
                    logger.error("Expected (" + expectedWordCount.get(key) + ") and result (" + drpcResult +") do not match for " + key );
                }
            }

        } finally {
            cluster.killTopology(topoName);
        }

        if (!passed) {
            logger.error("A test case failed.  Throwing error");
            throw new Exception();
        }
    }

    @Test(timeout=600000)
    public void TestDHSpoutTopologyHTTPEphemeral() throws Exception{
        logger.info("Starting TestDHSpoutTopologyEphemeral");
        StormTopology topology = buildTopology(vhEphemeralURI);

        String topoName = "dhspout-ephemeral-topology-test";

        _conf = new backtype.storm.Config();
        _conf.putAll(backtype.storm.utils.Utils.readStormConfig());
        _conf.setDebug(true);
        _conf.setNumWorkers(3);
        _conf.put(backtype.storm.Config.NIMBUS_TASK_TIMEOUT_SECS, 200);
        _conf.put(Config.REGISTRY_URI, serverURI);
        _conf.put("topology.http_spout.port.range","10000-10100");

        _conf.registerSerialization(AvroEventRecord.class, KryoEventRecord.class);

        String outputLoc = "/tmp/dhcount"; //TODO change this to use a shared directory or soemthing, so we can get to it simply
        _conf.put("test.output.location",outputLoc);

        addVirtualHost(new URI(vhEphemeralURI));
        //TODO turn this into a utility that has a conf setting
        File jar = new File(conf.getProperty("WORKSPACE") + "/topologies/target/topologies-1.0-SNAPSHOT-jar-with-dependencies.jar");
        try {
            cluster.submitTopology(jar, topoName, _conf, topology);
        } catch (Exception e) {
            logger.error("Couldn't launch topology: " + e );
            throw new Exception();
        }

        // Now we need to wait until the spout shows up in the registry server's YFOR config.
        //
        // Actually, it __should__ wait for it to come up, but let's sleep a little.

        _conf.put(Config.REGISTRY_URI, serverURI);
        Integer dhCount = new Integer(20);


        logger.info("Launching DHSimulator:");
        DHSimulator dh = new DHSimulator(_conf, new URI(vhEphemeralURI), null, null, 1, false, null, dhCount);

        try {
            logger.info("Starting DHSimulator:");
            dh.start();
            logger.info("Joining DHSimulator:");
            dh.join();
            logger.info("Leaving join of DHSimulator, sleeping for a little bit to make sure all batches processed:");
            Util.sleep(10);
        } finally {
            logger.info("Halting DHSimulator:");
            dh.halt();
        }

        boolean passed = true;
        // Need to fix this, but we are going to sleep for a while so I can check state of cluster to see if it really did anything.
        try {

            int uptime = 10;
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

            //get results

            //get expected results
            String file = conf.getProperty("WORKSPACE") + "/htf-common/resources/storm/testinputoutput/TestDHSpoutTopology/expected_results";
            logger.info("Read epected results from: "+ file);
            HashMap<String, Integer> expectedWordCount = Util.readMapFromFile(file);

            // Now check to see if the bolt counted the right number of packets
            for (String key: expectedWordCount.keySet()){
                logger.info("Checking to see if the value for " + key + "is " + expectedWordCount.get(key));
                String drpcResult = cluster.DRPCExecute( function, key );
                passed &= Integer.toString(expectedWordCount.get(key)).equals(drpcResult);
                if (!passed) {
                    logger.error("Expected (" + expectedWordCount.get(key) + ") and result (" + drpcResult +") do not match for " + key );
                }
            }

        } finally {
            cluster.killTopology(topoName);
        }

        if (!passed) {
            logger.error("A test case failed.  Throwing error");
            throw new Exception();
        }
    }

    public StormTopology buildTopology(String uri) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        DRPCSpout drpcSpout = new DRPCSpout(function);
        URI spoutURI = new URI(uri);
        builder.setSpout("dh_spout", new TestDHSpout(spoutURI).setRegistryUri(serverURI).setUseSSLEncryption(false).setEventQueueSize(100).setAcking(false), 1);
        theBolt = new TestEventCountBolt("name");
        builder.setBolt("count", theBolt, 1).shuffleGrouping("dh_spout");

        builder.setSpout("drpc_spout", drpcSpout, 1);
        builder.setBolt("aggregator", new Aggregator())
            .globalGrouping("count")
            .globalGrouping("drpc_spout"); 

        builder.setBolt("rr", new ReturnResults()).globalGrouping("aggregator");

        return builder.createTopology();
    }    
}
