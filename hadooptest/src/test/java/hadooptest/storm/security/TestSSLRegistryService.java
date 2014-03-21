package hadooptest.storm.security;


import com.yahoo.spout.http.Config;
import com.yahoo.spout.http.RegistryStub;
import hadooptest.TestSessionStorm;
import hadooptest.cluster.storm.ModifiableStormCluster;
import hadooptest.workflow.storm.topology.bolt.TestEventCountBolt;
import org.apache.hadoop.util.Shell;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class TestSSLRegistryService extends TestSessionStorm {

    private static final ThreadLocal<ObjectMapper> mapper =
            new ThreadLocal<ObjectMapper>() {
                @Override
                protected ObjectMapper initialValue() {
                    return new ObjectMapper(new JsonFactory());
                }
            };

    static ModifiableStormCluster mc;
    static String configURI = "http://0.0.0.0:9080/registry/v1/";
    static String configSecureURI = "https://0.0.0.0:9443/registry/v1/";
    private String vhURI = "http://myvh-stormtest-ssl.corp.yahoo.com:9153/";
    private backtype.storm.Config _conf;
    private String _ycaV1Role;
    private TestEventCountBolt theBolt = null;
    private static File tempPkcs12 = null;
    private static File tempCert = null;
    private static String password = "password";
    public static final String KEY_STORE_TYPE = "PKCS12";
    private String physicalServerId ="testhost.yahoo.local";

    @BeforeClass
    public static void setup() throws Exception {
        start();
        assumeTrue(cluster instanceof ModifiableStormCluster);
        mc = (ModifiableStormCluster) cluster;

        if (mc != null) {
            createKeyStore();
            mc.setRegistryServerURI(configURI + "," + configSecureURI);
            mc.setRegistryConf("ssl.server.keystore.location", tempPkcs12.getPath());
            mc.setRegistryConf("ssl.server.keystore.password", password);
            mc.setRegistryConf("ssl.server.keystore.keypassword", password);
            mc.setRegistryConf("ssl.server.keystore.type", KEY_STORE_TYPE);
            mc.setRegistryServerURI(configURI + "," + configSecureURI);
            mc.startRegistryServer();
        }
    }


    private static void createKeyStore() throws IOException {
        String dname = "CN=0.0.0.0, OU=yarn, O=registry";
        String alias = "selfsigned";
        Integer validity = (365 * 2); //2 years by default

        tempPkcs12 = File.createTempFile("keystore", ".pkcs12");

        tempPkcs12.delete(); //keytool complains if the keystore is empty
        runCommand("keytool", "-genkey", "-keyalg", "RSA", "-alias", alias,
                "-keystore", tempPkcs12.getAbsolutePath(),
                "-storetype", KEY_STORE_TYPE, "-storepass", password,
                "-validity", validity.toString(), "-keysize", "2048",
                "-dname", dname);

        tempCert = File.createTempFile("keystore", ".cert");
        tempCert.delete();
        runCommand("keytool", "-export", "-alias", alias,
                "-file", tempCert.getAbsolutePath(), "-storetype", KEY_STORE_TYPE,
                "-storepass", password, "-keystore", tempPkcs12.getAbsolutePath(), "-rfc");

    }

    @Before
    public void SetUp() throws Exception {
        addVirtualHost(configURI, new URI(vhURI));
    }

    public void addVirtualHost(String registryURI, URI serviceURI) throws IOException {
        _conf = new backtype.storm.Config();
        _conf.putAll(backtype.storm.utils.Utils.readStormConfig());
        String registryProxy = (String) _conf.get(Config.REGISTRY_PROXY);
        logger.info("registry uri:" + registryURI);
        logger.info("registry proxy uri:" + registryProxy);
        RegistryStub registry = new RegistryStub(registryURI, registryProxy, null);
        registry.setYCAv1Role(_ycaV1Role);

        try {
            logger.info("service uri:" + serviceURI);
            String serviceID = com.yahoo.spout.http.Util.ServiceURItoID(serviceURI);
            JsonNode virtualHostNode = registry.getVirtualHost(serviceID);
            registry.addVirtualHost(serviceID, null
                    , com.yahoo.spout.http.Util.useHttps(serviceURI));
            virtualHostNode = registry.getVirtualHost(serviceID);
            String vhDetails = null;
            if (virtualHostNode != null) {
                vhDetails = mapper.get().writeValueAsString((virtualHostNode));
                logger.info(vhDetails);
            }
            registry.addServer(serviceID, physicalServerId, 9153);
        }
        catch (IOException io)
        {
           logger.warn(String.format("Could not create VH [%s]",vhURI), io);
        }
        finally {
            if (registry != null) {
                registry.stop();
            }
        }
    }

    @Test
    public void testSSLInterface() throws Exception {
        testGetServerSSL();
        testGetVirtualHostSSL();
        testGetyForConfigSSL();
    }

    public void testGetServerSSL() throws Exception {
        URI serviceURI = new URI(vhURI);
        String serviceID = com.yahoo.spout.http.Util.ServiceURItoID(serviceURI);
        String getURIForServer = configSecureURI + "virtualHost/" + serviceID + "/server";
        String response = runCommand("curl", "--cacert", tempCert.getAbsolutePath(), getURIForServer);
        logger.info(String.format("For Request to [%s] received [%s]", getURIForServer, response));
        Map<String, Object> props = new ObjectMapper().readValue(response, HashMap.class);
        assertNotNull(props);
        assertNotNull(props.get("server"));

    }

    public void testGetVirtualHostSSL() throws Exception {
        URI serviceURI = new URI(vhURI);
        String serviceID = com.yahoo.spout.http.Util.ServiceURItoID(serviceURI);
        String getURIForVH = configSecureURI + "virtualHost/" + serviceID;
        String response = runCommand("curl", "--cacert", tempCert.getAbsolutePath(), getURIForVH);
        logger.info(String.format("For Request to [%s] received [%s]", getURIForVH, response));
        Map<String, Object> props = new ObjectMapper().readValue(response, HashMap.class);
        assertNotNull(props);
        assertNotNull(props.get("virtualHost"));
    }

    public void testGetyForConfigSSL() throws Exception {
        URI serviceURI = new URI(vhURI);
        String serviceID = com.yahoo.spout.http.Util.ServiceURItoID(serviceURI);
        String getURIForVH = configSecureURI + "virtualHost/" + serviceID + "/ext/yahoo/yfor_config";
        String response = runCommand("curl", "--cacert", tempCert.getAbsolutePath(), getURIForVH);
        logger.info(String.format("For Request to [%s] received [%s]", getURIForVH, response));
        assertTrue(response.contains(serviceID));
    }


    @AfterClass
    public static void cleanup() throws Exception {
        if (mc != null) {
            mc.resetConfigsAndRestart();
            mc.stopRegistryServer();
        }
        stop();
    }


    private static String runCommand(String... cmd) throws IOException {
        Shell.ShellCommandExecutor exec = new Shell.ShellCommandExecutor(cmd, null, null, 15000);
        try {
            exec.execute();
        } catch (Shell.ExitCodeException e) {
            logger.warn("Error running command " + exec +
                    "\nEXIT CODE:" + exec.getExitCode() +
                    "\nOUT" + exec.getOutput(), e);
            throw e;
        }
        return exec.getOutput();
    }


}
