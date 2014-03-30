package hadooptest.storm;

import static org.junit.Assume.assumeTrue;
import hadooptest.SerialTests;
import hadooptest.TestSessionStorm;
import hadooptest.Util;
import hadooptest.cluster.storm.ModifiableStormCluster;
import backtype.storm.generated.TopologySummary;

import java.io.File;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.experimental.categories.Category;

import com.yahoo.spout.http.Config;
import com.yahoo.spout.http.RegistryStub;
import com.yahoo.spout.http.rainbow.DHSimulator;
import com.yahoo.spout.http.rainbow.KryoEventRecord;
import hadooptest.automation.utils.http.JSONUtil;

@Category(SerialTests.class)
public class TestRegistryClient extends TestSessionStorm {
    static ModifiableStormCluster mc;
    static String configURI="http://0.0.0.0:9080/registry/v1/";
    static String serverURI=configURI;
    static String vhURI="http://myvh-stormtest.corp.yahoo.com:9153/";
    static String RegClientCommand="/home/y/bin/registry_client";
    private backtype.storm.Config _conf;
    private String _ycaV1Role;

    @BeforeClass
    public static void setup() throws Exception {
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
    
    public String[]  runRegistryClientCommand( String[] command ) throws Exception {

        String[] returnValue = exec.runProcBuilder(command, true);
    
	return returnValue;
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

    public TestRegistryClient(){
        //empty constructor
    }
    
    @Test
    public void TestHelpOption() throws Exception {
        logger.info("Starting TestHelpOption");
        String[] output = runRegistryClientCommand(new String[] { RegClientCommand, "--help" } );
	
	for ( String test : new String[] { "addvh", "getvh", "delvh", "ARGS" } ) {
            boolean inOutput = false;
	    for ( String s: output ) {
                if ( s.indexOf(test) != -1 ) {
                    inOutput = true;
                }
	    }
            if (!inOutput) {
                logger.warn("Could not find " + test + " in the output of the command" );
                throw new Exception("Could not find " + test + " in the output of the command");
            }
        }
    }
    
    @Test
    public void TestAddHTTPVH() throws Exception {
        logger.info("Starting TestAddHTTPVH");

	String vhName = "myvirtualhost.basic.com";
	String vhURI = "http://" + vhName + ":9989";
        String[] output = runRegistryClientCommand(new String[] { RegClientCommand, "addvh", vhURI } );
	JSONUtil json = new JSONUtil();
	
	assertTrue("AddVH return value was not 0", output[0].equals("0"));

	// Output should be json object from server.
	json.setContent(output[1]);
	String fromJsonVH = json.getElement("virtualHost/name").toString();
        logger.info("Command returned the following vh name " + fromJsonVH);
	assertTrue("Names did not match", fromJsonVH.equals(vhName));
	
        // Now use the REST API to see if we really did add the virtual host.
        assertTrue("Did not get VH from REST api", mc.isVirtualHostDefined(vhName));
    }
    
    @Test
    public void TestDelHTTPVH() throws Exception {
        logger.info("Starting TestDelHTTPVH");

	String vhName = "goingtodelete.basic.com";
	String vhURI = "http://" + vhName + ":9999";
        String[] output = runRegistryClientCommand(new String[] { RegClientCommand, "addvh", vhURI } );
	JSONUtil json = new JSONUtil();
	
	assertTrue("AddVH return value was not 0", output[0].equals("0"));

	// Output should be json object from server.
	json.setContent(output[1]);
	String fromJsonVH = json.getElement("virtualHost/name").toString();
        logger.info("Command returned the following vh name " + fromJsonVH);
	assertTrue("Names did not match", fromJsonVH.equals(vhName));
	
        // Now use the REST API to see if we really did add the virtual host.
        assertTrue("Did not get VH from REST api", mc.isVirtualHostDefined(vhName));

	// Now let's do the delete
        output = runRegistryClientCommand(new String[] { RegClientCommand, "delvh", vhURI } );

	assertTrue("DelVH return value was not 0", output[0].equals("0"));

        assertFalse("Did not remove VH from REST api", mc.isVirtualHostDefined(vhName));
    }

    @Test
    public void TestNegatvieDelete() throws Exception {
        logger.info("Starting TestNegativeDelete");

	String vhName = "failtodelete.basic.com";
	String vhURI = "http://" + vhName + ":9999";
        String[] output = runRegistryClientCommand(new String[] { RegClientCommand, "delvh", vhURI } );
	
	assertFalse("DelVH return value was 0", output[0].equals("0"));
    }

    @Test
    public void TestNegativeAdd() throws Exception {
        logger.info("Starting TestNegativeAdd");

	String vhName = "doubleadd.basic.com";
	String vhURI = "http://" + vhName + ":9999";
        String[] output = runRegistryClientCommand(new String[] { RegClientCommand, "addvh", vhURI } );
	JSONUtil json = new JSONUtil();
	
	assertTrue("AddVH return value was not 0", output[0].equals("0"));

	// Output should be json object from server.
	json.setContent(output[1]);
	String fromJsonVH = json.getElement("virtualHost/name").toString();
        logger.info("Command returned the following vh name " + fromJsonVH);
	assertTrue("Names did not match", fromJsonVH.equals(vhName));

        // Now use the REST API to see if we really did add the virtual host.
        assertTrue("Did not get VH from REST api", mc.isVirtualHostDefined(vhName));

        output = runRegistryClientCommand(new String[] { RegClientCommand, "addvh", vhURI } );
	assertFalse("AddVH return value was 0", output[0].equals("0"));
        output = runRegistryClientCommand(new String[] { RegClientCommand, "delvh", vhURI } );
	assertTrue("DelVH return value was not 0", output[0].equals("0"));
    }

    @Test
    public void TestGetVH() throws Exception {
        logger.info("Starting TestGetVH");

	String vhName = "testgetvh.basic.com";
	String vhURI = "http://" + vhName + ":9999";
        String[] output = runRegistryClientCommand(new String[] { RegClientCommand, "addvh", vhURI } );
	JSONUtil json = new JSONUtil();
	
	assertTrue("AddVH return value was not 0", output[0].equals("0"));

	// Output should be json object from server.
	json.setContent(output[1]);
	String fromJsonVH = json.getElement("virtualHost/name").toString();
        logger.info("Command returned the following vh name " + fromJsonVH);
	assertTrue("Names did not match", fromJsonVH.equals(vhName));

        // Now use the REST API to see if we really did add the virtual host.
        assertTrue("Did not get VH from REST api", mc.isVirtualHostDefined(vhName));

        output = runRegistryClientCommand(new String[] { RegClientCommand, "getvh", vhURI } );
	assertTrue("GetVH return value was not 0", output[0].equals("0"));
        output = runRegistryClientCommand(new String[] { RegClientCommand, "delvh", vhURI } );
	assertTrue("DelVH return value was not 0", output[0].equals("0"));
    }

    @Test
    public void TestNegativeGetVH() throws Exception {
        logger.info("Starting TestNegativeGetVH");

	String vhName = "negativegettest.basic.com";
	String vhURI = "http://" + vhName + ":9999";

        String[] output = runRegistryClientCommand(new String[] { RegClientCommand, "getvh", vhURI } );
	assertTrue("GetVH return value was 0", output[0].equals("0"));
	assertTrue("Returned stdout: " + output[1], output[1].equals(""));
	assertTrue("Returned stderr: " + output[2], output[2].equals(""));
        logger.info("rc: " + output[0]);
        logger.info("stdout: " + output[1]);
        logger.info("stderr: " + output[2]);
    }

    @Test
    public void TestAddWithYCAV1() throws Exception {
       String v1Role = "yahoo.grid_re.storm." + conf.getProperty("CLUSTER_NAME");
	String vhName = "testgetvhycav1.basic.com";
	String vhURI = "https://" + vhName + ":9999";
        String[] output = runRegistryClientCommand(new String[] { RegClientCommand, "addvh", vhURI, "--yca_v1_role", v1Role } );
	JSONUtil json = new JSONUtil();
	
	assertTrue("AddVH return value was not 0", output[0].equals("0"));
	// Output should be json object from server.
	json.setContent(output[1]);
	String fromJsonVH = json.getElement("virtualHost/name").toString();
        logger.info("Command returned the following vh name " + fromJsonVH);
	assertTrue("Names did not match", fromJsonVH.equals(vhName));

        // Now use the REST API to see if we really did add the virtual host.
        assertTrue("Did not get VH from REST api", mc.isVirtualHostDefined(vhName));
    }

    @Test
    public void TestDelWithYCAV1() throws Exception {
       String v1Role = "yahoo.grid_re.storm." + conf.getProperty("CLUSTER_NAME");
	String vhName = "testgetvhycav1.basic.com";
	String vhURI = "https://" + vhName + ":9999";
        String[] output = runRegistryClientCommand(new String[] { RegClientCommand, "addvh", vhURI, "--yca_v1_role", v1Role } );
	JSONUtil json = new JSONUtil();
	
	assertTrue("AddVH return value was not 0", output[0].equals("0"));
	// Output should be json object from server.
	json.setContent(output[1]);
	String fromJsonVH = json.getElement("virtualHost/name").toString();
        logger.info("Command returned the following vh name " + fromJsonVH);
	assertTrue("Names did not match", fromJsonVH.equals(vhName));

        // Now use the REST API to see if we really did add the virtual host.
        assertTrue("Did not get VH from REST api", mc.isVirtualHostDefined(vhName));

	// Now let's do the delete
        output = runRegistryClientCommand(new String[] { RegClientCommand, "delvh", vhURI , "--yca_v1_role", v1Role } );
	assertTrue("DelVH return value was not 0", output[0].equals("0"));
        assertFalse("Did not remove VH from REST api", mc.isVirtualHostDefined(vhName));
    }

    @Test
    public void TestDelWithoutYCAV1() throws Exception {
       String v1Role = "yahoo.grid_re.storm." + conf.getProperty("CLUSTER_NAME");
	String vhName = "testgetvhyxxcav1.basic.com";
	String vhURI = "https://" + vhName + ":9999";
        String[] output = runRegistryClientCommand(new String[] { RegClientCommand, "addvh", vhURI, "--yca_v1_role", v1Role } );
	JSONUtil json = new JSONUtil();
	
	assertTrue("AddVH return value was not 0", output[0].equals("0"));
	// Output should be json object from server.
	json.setContent(output[1]);
	String fromJsonVH = json.getElement("virtualHost/name").toString();
        logger.info("Command returned the following vh name " + fromJsonVH);
	assertTrue("Names did not match", fromJsonVH.equals(vhName));

        // Now use the REST API to see if we really did add the virtual host.
        assertTrue("Did not get VH from REST api", mc.isVirtualHostDefined(vhName));

	// Now try the bad delete
        logger.info("About to try delete with different yca v1 role" + fromJsonVH);
        output = runRegistryClientCommand(new String[] { RegClientCommand, "delvh", vhURI , "--yca_v1_role", v1Role + ".nimbus" } ); 
        logger.info("rc=" + output[0]);
        logger.info("stdout=" + output[1]);
        logger.info("stderr=" + output[2]);
	assertTrue("DelVH did not return error.", !output[0].equals("0") || !output[2].equals(""));
        assertTrue("Falsely removed VH from REST api", mc.isVirtualHostDefined(vhName));

	// Now try another bad delete
        logger.info("About to try delete with nonexistent yca v1 role" + fromJsonVH);
        output = runRegistryClientCommand(new String[] { RegClientCommand, "delvh", vhURI , "--yca_v1_role", "bogus.yca.role.nimbus" } ); 
        logger.info("rc=" + output[0]);
        logger.info("stdout=" + output[1]);
        logger.info("stderr=" + output[2]);
	assertTrue("DelVH did not return error.", !output[0].equals("0") || !output[2].equals(""));
        assertTrue("Falsely removed VH from REST api", mc.isVirtualHostDefined(vhName));

	// Now let's do the real delete
        logger.info("About to try real delete with valid yca v1 role" + fromJsonVH);
        output = runRegistryClientCommand(new String[] { RegClientCommand, "delvh", vhURI , "--yca_v1_role", v1Role } );
        logger.info("rc=" + output[0]);
        logger.info("stdout=" + output[1]);
        logger.info("stderr=" + output[2]);
	assertTrue("DelVH return value was not 0", output[0].equals("0"));
        assertFalse("Did not remove VH from REST api", mc.isVirtualHostDefined(vhName));
    }
}
