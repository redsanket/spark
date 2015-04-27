package hadooptest.storm;

import static org.junit.Assert.assertEquals;
import hadooptest.SerialTests;
import hadooptest.Util;
import hadooptest.automation.utils.http.Response;
import hadooptest.cluster.storm.ModifiableStormCluster;
import hadooptest.automation.utils.http.HTTPHandle;
import org.apache.commons.httpclient.HttpMethod;
import hadooptest.TestSessionStorm;
import hadooptest.Util;
import static org.junit.Assume.assumeTrue;
import org.json.simple.JSONValue;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import static org.junit.Assume.assumeTrue;
import static org.junit.Assert.*;
import java.util.Map;
import java.util.*;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;

@Category(SerialTests.class)
public class TestStormKafkaTopology extends TestSessionStorm {

    static ModifiableStormCluster mc = null;

    @BeforeClass
    public static void setup() throws Exception {
        cluster.setDrpcAclForFunction("stormkafka");
        mc = (ModifiableStormCluster) cluster;
        assumeTrue(mc != null);
    }

    @AfterClass
    public static void cleanup() throws Exception {
        stop();
    }


    public void initiateKafkaProducer() throws Exception
    {
        String pathToFile = conf.getProperty("WORKSPACE") + "/htf-common/resources/storm/testinputoutput/TestStormKafkaTopology/producer";
        String pathToScripts = conf.getProperty("KAFKA_HOME");
        String hostAddress = "gsbl90782.blue.ygrid.yahoo.com";
        String brokerPort = "9092";
        String[] returnTopicValue = exec.runProcBuilder(new String[]{ pathToScripts + "kafka-topics.sh", "--create", "--zookeeper",
                hostAddress+":2181", "--replication-factor", "1", "--partitions", "1" ,"--topic", "test"}, true);
        assertTrue( "Could not create topic for consuming", returnTopicValue[0].equals("0") );

//        String[] returnProducerValue = exec.runProcBuilder(new String[]{ pathToScripts + "kafka-console-producer.sh", "--broker-list",
//                 hostAddress + ":" + brokerPort,  "--topic", "test", "<", pathToFile }, true);
//        assertTrue("Could not write to the producer", returnProducerValue[0].equals("0"));

        
        Map<String, Object> config = new HashMap<String, Object>();
        config.put("metadata.broker.list", hostAddress + ":9092");
        config.put("serializer.class", "kafka.serializer.StringEncoder");
        config.put("request.required.acks", "1");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(config);

        ProducerRecord<String, String> line1 = new ProducerRecord<String, String>("test", "1","hello Yahoo");
        ProducerRecord<String, String> line2 = new ProducerRecord<String, String>("test", "2","hello Champaign");

        producer.send(line1);
        producer.send(line2);

        producer.close();



    }

    public void launchKafkaTopology() throws Exception
    {
        String pathToJar = conf.getProperty("WORKSPACE") + "/topologies/target/topologies-1.0-SNAPSHOT-jar-with-dependencies.jar";
        String byUser = mc.getBouncerUser();
        String[] returnValue = exec.runProcBuilder(new String[]{"storm", "jar", pathToJar, "hadooptest.topologies.StormKafkaTopology", "test", "-c",
               "ui.users=[\"" + byUser + "\"]", "-c", "logs.users=[\"" + byUser + "\"]"}, true);
        assertTrue("Problem running Storm jar command", returnValue[0].equals("0"));
    }

    @Test(timeout=600000)
    public void StormKafkaTest() throws Exception {
        initiateKafkaProducer();
        launchKafkaTopology();

        Util.sleep(50);
        String drpcResult = cluster.DRPCExecute("stormkafka", "hello");
        logger.debug("drpc result = " + drpcResult);
        assertTrue("Did not get expected result back from stormkafka topology", drpcResult.equals("2"));


    }
}
