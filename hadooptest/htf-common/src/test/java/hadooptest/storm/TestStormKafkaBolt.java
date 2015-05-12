package hadooptest.storm;

import hadooptest.SerialTests;
import hadooptest.TestSessionStorm;
import hadooptest.Util;
import hadooptest.cluster.storm.ModifiableStormCluster;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.UUID;

import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

@Category(SerialTests.class)
public class TestStormKafkaBolt extends TestSessionStorm {

    final static int kafkaProducerBufferSize = 64 * 1024;
    final static String clientId = "SimpleConsumerDemoClient";
    private final static int connectionTimeOut = 100000;
    private final static Logger LOG = LoggerFactory.getLogger(TestStormKafkaBolt.class);
    static ModifiableStormCluster mc = null;
    private static String function = "kafkabolt";
    private static String topologyName = "testKafkaBolt";
    private String topic;
    private String zookeeperHostPort;
    private String brokerHostPortInfo;
    private String pathToScripts;

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

    public static String fetchMessages(ByteBufferMessageSet messageSet) throws UnsupportedEncodingException {
        String messages = "";
        for (MessageAndOffset messageAndOffset : messageSet) {
            ByteBuffer payload = messageAndOffset.message().payload();
            byte[] bytes = new byte[payload.limit()];
            payload.get(bytes);
            messages += new String(bytes, "UTF-8");
            LOG.info("Message" + messages);
        }
        return messages;
    }

    public void createKafkaTopic() throws Exception {
        pathToScripts = conf.getProperty("KAFKA_HOME");
        brokerHostPortInfo = conf.getProperty("KAFKA_BROKER_HOST_PORT_LIST");
        zookeeperHostPort = conf.getProperty("KAFKA_ZOOKEEPER_HOST_PORT");
        UUID uuid = UUID.randomUUID();
        topic = uuid.toString();
        LOG.info("Topic: " + topic);
        LOG.info("LAUNCHED ZOOKEEPER");
        String[] returnTopicValue = exec.runProcBuilder(new String[]{pathToScripts + "kafka-topics.sh", "--create", "--zookeeper",
                zookeeperHostPort, "--replication-factor", "1", "--partitions", "1", "--topic", topic}, true);
        assertTrue("Could not create topic for consuming", returnTopicValue[0].equals("0"));
    }

    public void launchKafkaBoltTopology() throws Exception {
        String pathToJar = conf.getProperty("WORKSPACE") + "/topologies/target/topologies-1.0-SNAPSHOT-jar-with-dependencies.jar";
        String byUser = mc.getBouncerUser();
        String[] returnValue = exec.runProcBuilder(new String[]{"storm", "jar", pathToJar, "hadooptest.topologies.StormKafkaBoltTopology", topologyName, topic, brokerHostPortInfo, "-c",
                "ui.users=[\"" + byUser + "\"]", "-c", "logs.users=[\"" + byUser + "\"]"}, true);
        assertTrue("Problem running Storm jar command", returnValue[0].equals("0"));
    }

    public boolean kafkaConsumer() throws Exception {
        String[] serverDetails = brokerHostPortInfo.split(":");
        LOG.info("server details" + serverDetails[0] + " " + serverDetails[1]);
        SimpleConsumer simpleConsumer = new SimpleConsumer(serverDetails[0],
                Integer.parseInt(serverDetails[1]),
                connectionTimeOut,
                kafkaProducerBufferSize,
                clientId);

        System.out.println("Testing single fetch");
        FetchRequest req = new FetchRequestBuilder()
                .clientId(clientId)
                .addFetch(topic, 0, 0L, 100)
                .build();
        FetchResponse fetchResponse = simpleConsumer.fetch(req);
        String messages = fetchMessages((ByteBufferMessageSet) fetchResponse.messageSet(topic, 0));
        simpleConsumer.close();
        return messages.contains("Storm");
    }

    @Test(timeout = 600000)
    public void StormKafkaBoltTest() throws Exception {
        try {
            createKafkaTopic();
            LOG.info("Created kafka topic:" + topic);
            launchKafkaBoltTopology();
            LOG.info("Topology Launched");
            Util.sleep(150);
            cluster.killTopology(topologyName);
            LOG.info("Topology Killed");
            LOG.info("Starting simple consumer to consume the messages");
            assertTrue("No relevant messages found in the server", kafkaConsumer());
        } finally {
            String[] returnTopicValue = exec.runProcBuilder(new String[]{pathToScripts + "kafka-topics.sh", "--zookeeper",
                    zookeeperHostPort, "--delete", "--topic", topic}, true);
            assertTrue("Could not delete topic", returnTopicValue[0].equals("0"));
        }
    }
}
