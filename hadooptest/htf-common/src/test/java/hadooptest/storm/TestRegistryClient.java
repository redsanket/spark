package hadooptest.storm;

import static org.junit.Assume.assumeTrue;
import static org.junit.Assume.assumeFalse;
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
    static String vhURI="http://myvh-stormtest.corp.yahoo.com:9153/";
    static String RegClientCommand="/home/y/bin/registry_client";
    private String _ycaV1Role;

    @BeforeClass
    public static void setup() throws Exception {
        assumeTrue(cluster instanceof ModifiableStormCluster);
        mc = (ModifiableStormCluster)cluster;
        if (mc != null) {
            backtype.storm.Config conf = new backtype.storm.Config();
            conf.putAll(backtype.storm.utils.Utils.readStormConfig());
            String theURI=(String)conf.get("http.registry.uri");
            mc.setRegistryServerURI(theURI);
            mc.startRegistryServer();
        }
    }

    public String[]  runRegistryClientCommand( String[] command ) throws Exception {

        String[] returnValue = exec.runProcBuilder(command, true);
    
        return returnValue;
    }

    @AfterClass
    public static void cleanup() throws Exception {
        if (mc != null) {
            mc.resetConfigs();
            mc.stopRegistryServer();
        }
        stop();
    }

    public TestRegistryClient(){
        //empty constructor
    }
    
    @Test(timeout=600000)
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
    
    @Test(timeout=600000)
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
    
    @Test(timeout=600000)
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

    @Test(timeout=600000)
    public void TestNegatvieDelete() throws Exception {
        logger.info("Starting TestNegativeDelete");

        String vhName = "failtodelete.basic.com";
        String vhURI = "http://" + vhName + ":9999";
        String[] output = runRegistryClientCommand(new String[] { RegClientCommand, "delvh", vhURI } );
    
        assertFalse("DelVH return value was 0", output[0].equals("0"));
    }

    @Test(timeout=600000)
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

    @Test(timeout=600000)
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

    @Test(timeout=600000)
    public void TestNegativeGetVH() throws Exception {
        logger.info("Starting TestNegativeGetVH");

        String vhName = "negativegettest.basic.com";
        String vhURI = "http://" + vhName + ":9999";

        String[] output = runRegistryClientCommand(new String[] { RegClientCommand, "getvh", vhURI } );
        assertTrue("GetVH return value was 0", output[0].equals("0"));
        assertTrue("Returned stdout: " + output[1], output[1].contains("NOT FOUND"));
        assertTrue("Returned stderr: " + output[2], output[2].equals(""));
        logger.info("rc: " + output[0]);
        logger.info("stdout: " + output[1]);
        logger.info("stderr: " + output[2]);
    }

    @Test(timeout=600000)
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

    @Test(timeout=600000)
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

    @Test(timeout=600000)
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

    @Test(timeout=600000)
    public void TestAddWithYCAV2() throws Exception {
        String v1Role = "yahoo.griduser.hadoopqa";
        String v2Role = "ystorm.test.yca.users";
        String httpProxyRole = "grid.blue.res.httpproxy";
        String httpProxyServer = "http://httpproxy-res.blue.ygrid.yahoo.com:4080";

        // See if we are part of this v1role.  If not, skip this test.   We are running it somewhere it cannot work.
        String[] returnValue = exec.runProcBuilder(new String [] {"yca-cert-util", "--show", v1Role}, true);
        assumeTrue( returnValue[1].indexOf("NOT FOUND") < 0 );

        // So, we are running somewhere that has this V1 role.  Try to use it to get a v2 cert.
        String vhName = "testgetvhyxxcav2.basic.com";
        String vhURI = "https://" + vhName + ":9999";
        String[] output = runRegistryClientCommand(new String[] { RegClientCommand, "addvh", vhURI, "--yca_v1_role", v1Role, "--yca_v2_role", v2Role, "--yca_proxy_role", httpProxyRole, "-P", httpProxyServer } );
        JSONUtil json = new JSONUtil();
    
        assertTrue("AddVH return value was not 0", output[0].equals("0"));
        // Output should be json object from server.
        json.setContent(output[1]);
        String fromJsonVH = json.getElement("virtualHost/name").toString();
        logger.info("Command returned the following vh name " + fromJsonVH);
        assertTrue("Names did not match", fromJsonVH.equals(vhName));
     
        // Now let's do the real delete
        logger.info("About to try real delete with valid yca v2 role " + fromJsonVH);
        output = runRegistryClientCommand(new String[] { RegClientCommand, "delvh", vhURI , "--yca_v1_role", v1Role,  "--yca_v2_role", v2Role, "--yca_proxy_role", httpProxyRole, "-P", httpProxyServer} );
        assertTrue("DelVH return value was not 0", output[0].equals("0"));
        assertFalse("Did not remove VH from REST api", mc.isVirtualHostDefined(vhName));
    }

    @Test(timeout=600000)
    public void TestAddWithBadYCAV2() throws Exception {
        String v1Role = "yahoo.griduser.hadoopqa";
        String badv1Role = "yahoo.grid_re.storm." + conf.getProperty("CLUSTER_NAME");
        String v2Role = "ystorm.test.yca.users";
        String httpProxyRole = "grid.blue.flubber.httpproxy";

        // See if we are part of this v1role.  If not, skip this test.   We are running it somewhere it cannot work.
        String[] returnValue = exec.runProcBuilder(new String [] {"yca-cert-util", "--show", v1Role}, true);
        assumeTrue( returnValue[1].indexOf("NOT FOUND") < 0 );

        // So, we are running somewhere that has this V1 role.  Try to use it to get a v2 cert.
        String vhName = "testfailonycav2.basic.com";
        String vhURI = "https://" + vhName + ":9999";
        String[] output = runRegistryClientCommand(new String[] { RegClientCommand, "addvh", vhURI, "--yca_v1_role", badv1Role, "--yca_v2_role", v2Role, "--yca_proxy_role", "grid.blue.flubber.httpproxy" } );
        logger.info("rc=" + output[0]);
        logger.info("stdout=" + output[1]);
        logger.info("stderr=" + output[2]);
    
        assertFalse("AddVH return value was 0, when we expected command to fail", output[0].equals("0"));
    }
}
