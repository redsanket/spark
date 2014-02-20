package hadooptest.storm;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;
import java.io.InputStreamReader;
import static org.junit.Assert.*;
import static org.junit.Assume.*;
import java.net.URI;
import java.util.HashSet;
import java.util.HashMap;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;


import hadooptest.SerialTests;
import hadooptest.TestSessionStorm;
import hadooptest.cluster.storm.ModifiableStormCluster;
import hadooptest.Util;
import hadooptest.workflow.storm.topology.spout.TestDHSpout;

import com.yahoo.spout.http.Config;
import com.yahoo.spout.http.RegistryStub;
import backtype.storm.generated.*;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class TestDHSpoutTopology extends TestSessionStorm {
    static ModifiableStormCluster mc;
    static String serverURI="http://fsbl350n13.blue.ygrid.yahoo.com:9080/registry/v1/";
    static String configURI="http://0.0.0.0:9080/registry/v1/";
    static String vhURI="http://myvh-stormtest.corp.yahoo.com:4080/";
    private backtype.storm.Config _conf;
    private String _ycaV1Role;

    @BeforeClass
    public static void setup() throws Exception {
        start();
        assumeTrue(cluster instanceof ModifiableStormCluster);
        mc = (ModifiableStormCluster)cluster;
	if (mc != null) {
		mc.setRegistryServerURI(configURI);
		mc.startRegistryServer();
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
          mc.resetConfigsAndRestart();
	  mc.stopRegistryServer();
        }
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


    @Test
    public void TestDHSpoutTopology() throws Exception{
	logger.info("Starting TestDHSpoutTopology");
        StormTopology topology = buildTopology(vhURI);

        String topoName = "dhspout-topology-test";
                           
        _conf = new backtype.storm.Config();
        _conf.putAll(backtype.storm.utils.Utils.readStormConfig());
        _conf.setDebug(true);
        _conf.setNumWorkers(2);
        _conf.put(backtype.storm.Config.NIMBUS_TASK_TIMEOUT_SECS, 200);

	addVirtualHost(new URI(vhURI));
        //TODO turn this into a utility that has a conf setting
        File jar = new File(conf.getProperty("STORM_TEST_HOME") + "/target/hadooptest-ci-1.0-SNAPSHOT-test-jar-with-dependencies.jar");
        cluster.submitTopology(jar, topoName, _conf, topology);
        try {
            
            int uptime = 270;
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
            
        } finally {
            cluster.killTopology(topoName);
        }
    }
        
    public static StormTopology buildTopology(String uri) {
        TopologyBuilder builder = new TopologyBuilder();
	try {
		URI spoutURI = new URI(uri);
        	builder.setSpout("dh_spout", new TestDHSpout(spoutURI).setRegistryUri(serverURI), 1);
	} catch (Exception e) {
	}
        
        
        return builder.createTopology();
    }    
}
