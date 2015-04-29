package hadooptest.topologies;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.drpc.DRPCSpout;
import backtype.storm.drpc.ReturnResults;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.commons.math.linear.FieldDecompositionSolver;
import storm.kafka.KafkaSpout;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.ZkState;
import storm.kafka.SpoutConfig;
import storm.kafka.BrokerHosts;
import backtype.storm.spout.SchemeAsMultiScheme;
import storm.kafka.KafkaConfig;
import hadooptest.workflow.storm.topology.bolt.StormKafkaBolt;
import hadooptest.workflow.storm.topology.bolt.StormKafkaAggregator;
import hadooptest.workflow.storm.topology.bolt.WordCount;
import backtype.storm.task.ShellBolt;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.StormSubmitter;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;


public class StormKafkaTopology {

    private final static Logger LOG = LoggerFactory.getLogger(StormKafkaTopology.class);

    public void stormkafkaSetup(String topology_name, String topic, String function) throws Exception
    {
        Config storm_conf = new Config();
        storm_conf.setDebug(true);

        LOG.info("Launching Topology ...");
        DRPCSpout drpcSpout = new DRPCSpout(function);
        BrokerHosts hosts = new ZkHosts("gsbl90782.blue.ygrid.yahoo.com:4080");
        SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, "/"+topic, "KafkaSpout");
        spoutConfig.forceFromStart =true;
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("line-reader-spout", new KafkaSpout(spoutConfig), 2);

        builder.setBolt("line-splitter", new StormKafkaBolt()).shuffleGrouping("line-reader-spout");
        builder.setBolt("word-count", new WordCount()).fieldsGrouping("line-splitter", new Fields("word"));

        builder.setSpout("drpc", drpcSpout, 1);
        builder.setBolt("check-count", new StormKafkaAggregator())
                       .globalGrouping("word-count")
                       .globalGrouping("drpc");

        builder.setBolt("rr", new ReturnResults()).globalGrouping("check-count");

        if(topology_name != null)
        {
            StormSubmitter.submitTopology(topology_name, storm_conf,builder.createTopology());
        }
        else
        {
            StormSubmitter.submitTopology("default", storm_conf,builder.createTopology());
        }


    }

    public static void main(String[] args) throws Exception
    {
       try
       {
           LOG.info("Triggering StormKafkaTopology with name " + args[0] + "topic " + args[1] + " and drpcfunction" + args[2]);
           String topology_name = args[0];
           String topic = args[1];
           String function = args[2];
           StormKafkaTopology skt = new StormKafkaTopology();
           skt.stormkafkaSetup(topology_name, topic, function);
       }
       catch(Exception e)
       {
           e.printStackTrace();
           LOG.error(e.getMessage());
       }

    }

}
