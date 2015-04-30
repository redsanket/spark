package hadooptest.storm;

import static org.junit.Assume.assumeTrue;
import static org.junit.Assert.*;
import hadooptest.SerialTests;
import hadooptest.Util;
import hadooptest.cluster.storm.ModifiableStormCluster;
import hadooptest.TestSessionStorm;

import java.util.UUID;
import java.util.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

@Category(SerialTests.class)
public class TestStormKafkaTopology extends TestSessionStorm {

    static ModifiableStormCluster mc = null;
    private String topic;
    private static String function = "stormkafka";
    private String zookeeperHost;
    private String zookeeperPort;
    private String brokerHostPortInfo;
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
        String pathToFile = conf.getProperty("WORKSPACE") + "/htf-common/resources/storm/testinputoutput/TestStormKafkaTopology/producer";
        String pathToScripts = conf.getProperty("KAFKA_HOME");
        brokerHostPortInfo = conf.getProperty("KAFKA_BROKER_HOST_PORT_LIST");//"gsbl90786.blue.ygrid.yahoo.com";
        zookeeperHost = conf.getProperty("ZOOKEEPER_HOST");//"gsbl90782.blue.ygrid.yahoo.com";
        zookeeperPort = conf.getProperty("ZOOKEEPER_PORT");//4080
        UUID uuid = UUID.randomUUID();
        topic = uuid.toString();
        LOG.info("Topic: " + topic);
        String[] returnTopicValue = exec.runProcBuilder(new String[]{pathToScripts + "kafka-topics.sh", "--create", "--zookeeper",
                zookeeperHost + ":" + zookeeperPort, "--replication-factor", "1", "--partitions", "1", "--topic", topic}, true);
        assertTrue("Could not create topic for consuming", returnTopicValue[0].equals("0"));

        Properties props = new Properties();
        props.put("bootstrap.servers", brokerHostPortInfo);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

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

    public void launchKafkaTopology() throws Exception {
        String pathToJar = conf.getProperty("WORKSPACE") + "/topologies/target/topologies-1.0-SNAPSHOT-jar-with-dependencies.jar";
        String byUser = mc.getBouncerUser();
        String[] returnValue = exec.runProcBuilder(new String[]{"storm", "jar", pathToJar, "hadooptest.topologies.StormKafkaTopology", "test", topic, function, zookeeperHost + ":" + zookeeperPort ,"-c",
                "ui.users=[\"" + byUser + "\"]", "-c", "logs.users=[\"" + byUser + "\"]"}, true);
        assertTrue("Problem running Storm jar command", returnValue[0].equals("0"));
    }

    @Test(timeout = 600000)
    public void StormKafkaTest() throws Exception {
        try {
            initiateKafkaProducer();
            LOG.info("Intiated kafka topic:" + topic + " and entered data");
            launchKafkaTopology();
            LOG.info("Topology Launched");
            Util.sleep(30);
            String drpcResult = cluster.DRPCExecute(function, "hello");
            logger.debug("drpc result = " + drpcResult);
            assertTrue("Did not get expected result back from stormkafka topology", drpcResult.equals("2"));
        } finally {
            cluster.killTopology("test");
        }
    }
}
