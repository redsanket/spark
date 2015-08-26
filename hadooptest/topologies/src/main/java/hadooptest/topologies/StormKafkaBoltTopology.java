package hadooptest.topologies;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import hadooptest.workflow.storm.topology.spout.KeyMessageSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.*;
import storm.kafka.bolt.KafkaBolt;

import java.util.Properties;


public class StormKafkaBoltTopology {

    private final static Logger LOG = LoggerFactory.getLogger(StormKafkaBoltTopology.class);

    public static void main(String[] args) throws Exception {
        try {
            LOG.info("Triggering StormKafkaTopology with name " + args[0] + " topic " + args[1]);
            String topology_name = args[0];
            String topic = args[1];
            String brokerHostPortInfo = args[2];
            StormKafkaBoltTopology skt = new StormKafkaBoltTopology();
            skt.kafkaBoltSetUp(topology_name, topic, brokerHostPortInfo);
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
        }

    }

    public void kafkaBoltSetUp(String topology_name, String topic, String brokerHostPortInfo) throws Exception {
        Config storm_conf = new Config();
        storm_conf.setDebug(true);
        LOG.info("Setting Properties for Kafka Bolt");
        Properties props = new Properties();
        props.put("metadata.broker.list", brokerHostPortInfo);
        props.put("security.protocol", "PLAINTEXTSASL");
        props.put("request.required.acks", "1");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        storm_conf.put(KafkaBolt.KAFKA_BROKER_PROPERTIES, props);
        storm_conf.put("topic", topic);
        LOG.info("Launching Topology ...");
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("KeyMessageSpout", new KeyMessageSpout(), 1);
        LOG.info("KafkaBolt Triggered");
        builder.setBolt("KafkaBolt", new KafkaBolt()).shuffleGrouping("KeyMessageSpout");

        if (topology_name != null) {
            StormSubmitter.submitTopology(topology_name, storm_conf, builder.createTopology());
        } else {
            StormSubmitter.submitTopology("default", storm_conf, builder.createTopology());
        }


    }

}
