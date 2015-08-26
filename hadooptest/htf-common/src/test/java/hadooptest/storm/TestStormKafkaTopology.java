package hadooptest.storm;

import hadooptest.SerialTests;
import hadooptest.TestSessionStorm;
import hadooptest.Util;
import hadooptest.cluster.storm.ModifiableStormCluster;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

@Category(SerialTests.class)
public class TestStormKafkaTopology extends TestSessionStorm {

    static ModifiableStormCluster mc = null;
    private String topic;
    private static String function = "stormkafka";
    private String zookeeperHostPort;
    private String brokerHostPortInfo;
    private String pathToScripts;
    private final static Logger LOG = LoggerFactory.getLogger(TestStormKafkaTopology.class);

    @BeforeClass
    public static void setup() throws Exception {
        cluster.setDrpcAclForFunction(function);
        mc = (ModifiableStormCluster) cluster;
        assumeTrue(mc != null);
    }

    @AfterClass
    public static void cleanup() throws Exception {
        stop();
    }

    public void initiateKafkaProducer() throws Exception {
        pathToScripts = conf.getProperty("KAFKA_HOME");
        brokerHostPortInfo = conf.getProperty("KAFKA_BROKER_HOST_PORT_LIST");
        zookeeperHostPort = conf.getProperty("KAFKA_ZOOKEEPER_HOST_PORT");
        UUID uuid = UUID.randomUUID();
        topic = uuid.toString();
        LOG.info("Topic: " + topic);
        // for secure kafka you have to create the topic as the super user.
        kinit(conf.getProperty("KAFKA_KEYTAB"), conf.getProperty("KAFKA_PRINCIPAL") );
        Map<String, String> newEnv = new HashMap<String, String>();
        newEnv.put("KAFKA_KERBEROS_PARAMS", "-Djava.security.auth.login.config=" + conf.getProperty("KAFKA_CLIENT_JAAS"));
        String[] returnTopicValue = exec.runProcBuilder(new String[]{
                pathToScripts + "kafka-topics.sh", "--create", "--zookeeper",
                zookeeperHostPort, "--replication-factor", "1", "--partitions", "1", "--topic", topic}, newEnv, true);
        assertTrue("Could not create topic for consuming", returnTopicValue[0].equals("0"));
        String byUser = mc.getBouncerUser();
        String user = conf.getProperty("USER");
        String[] returnAclValue = exec.runProcBuilder(new String[]{
                pathToScripts + "kafka-acl.sh", "--topic", topic, "--add", "--operations", "WRITE,READ",
                "--allowprincipals", "user:" + user + ",user:" + byUser, "--config", 
                pathToScripts + "/../config/server.properties"},
                newEnv, true);
        kinit();
        assertTrue("Could set acls", returnAclValue[0].equals("0"));

        System.setProperty("java.security.auth.login.config", conf.getProperty("KAFKA_CLIENT_JAAS"));
        Properties props = new Properties();
        props.put("bootstrap.servers", brokerHostPortInfo);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("security.protocol", "PLAINTEXTSASL");

        // New JAVA KAFKA CLIENT API
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        LOG.info("KafkaProducer Created ");
        ProducerRecord<String, String> line1 = new ProducerRecord<String, String>(topic, "1", "hello Yahoo");
        ProducerRecord<String, String> line2 = new ProducerRecord<String, String>(topic, "2", "hello Champaign");
        LOG.debug("creating line1 " + line1);
        LOG.debug("creating line2 " + line1);
        producer.send(line1);
        producer.send(line2);
        LOG.debug("sent line1 and line2 ");
        producer.close();
        LOG.info("producer closed");
    }

    public void launchKafkaTopology(String className, String topoName) throws Exception {
        String pathToJar = conf.getProperty("WORKSPACE") + "/topologies/target/topologies-1.0-SNAPSHOT-jar-with-dependencies.jar";
        String byUser = mc.getBouncerUser();
        String[] returnValue = null;
        if (conf.getProperty("KAFKA_AUTO_JAAS")!=null && conf.getProperty("KAFKA_AUTO_JAAS").length() > 0) {
            returnValue = exec.runProcBuilder(new String[]{"storm", "jar", pathToJar, className, topoName, topic, function, zookeeperHostPort, "-c",
                    "ui.users=[\"" + byUser + "\"]", "-c", "logs.users=[\"" + byUser + "\"]",
                    "-c", "topology.worker.childopts=\"-Djava.security.auth.login.config="+conf.getProperty("KAFKA_AUTO_JAAS")+"\""}, true);
        } else {
            returnValue = exec.runProcBuilder(new String[]{"storm", "jar", pathToJar, className, topoName, topic, function, zookeeperHostPort, "-c",
                    "ui.users=[\"" + byUser + "\"]", "-c", "logs.users=[\"" + byUser + "\"]"}, true);
        }
        assertTrue("Problem running Storm jar command", returnValue[0].equals("0"));
    }

    @Test(timeout = 600000)
    public void StormKafkaTest() throws Exception {
        try {
            initiateKafkaProducer();
            LOG.info("Intiated kafka topic:" + topic + " and entered data");
            launchKafkaTopology("hadooptest.topologies.StormKafkaTopology", "testStormKafka");
            LOG.info("Topology Launched");
            Util.sleep(30);
            String drpcResult = cluster.DRPCExecute(function, "hello");
            logger.debug("drpc result = " + drpcResult);
            assertTrue("Did not get expected result back from stormkafka topology", drpcResult.equals("2"));
        } finally {
            cluster.killTopology("testStormKafka");
            kinit(conf.getProperty("KAFKA_KEYTAB"), conf.getProperty("KAFKA_PRINCIPAL") );
            Map<String, String> newEnv = new HashMap<String, String>();
            newEnv.put("KAFKA_KERBEROS_PARAMS", "-Djava.security.auth.login.config=" + conf.getProperty("KAFKA_CLIENT_JAAS"));
            String[] returnTopicValue = exec.runProcBuilder(new String[]{
                    pathToScripts + "kafka-topics.sh", "--zookeeper",
                    zookeeperHostPort, "--delete", "--topic", topic}, newEnv, true);
            assertTrue("Could not delete topic", returnTopicValue[0].equals("0"));
            kinit();
        }
    }

    @Test(timeout = 600000)
    public void StormKafkaOpaqueTridentTest() throws Exception {
        try {
            initiateKafkaProducer();
            LOG.info("Intiated kafka topic:" + topic + " and entered data");
            launchKafkaTopology("hadooptest.topologies.StormKafkaOpaqueTridentTopology", "testTrident");
            LOG.info("Topology Launched");
            Util.sleep(30);
            String drpcResult = cluster.DRPCExecute(function, "hello");
            logger.debug("drpc result = " + drpcResult);
            assertEquals("Did not get expected result back from stormkafka topology", 2, Integer.parseInt(drpcResult));
        } finally {
            cluster.killTopology("testTrident");
            String[] returnTopicValue = exec.runProcBuilder(new String[]{pathToScripts + "kafka-topics.sh", "--zookeeper",
                    zookeeperHostPort, "--delete", "--topic", topic}, true);
            assertTrue("Could not delete topic", returnTopicValue[0].equals("0"));
        }
    }
}
