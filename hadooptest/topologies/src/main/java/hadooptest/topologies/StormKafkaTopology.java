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

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class StormKafkaTopology {

    //private final static Logger LOG = Logger.getLogger("StormKafkaTopology.class");

    public void stormkafkaSetup(String topology_name) throws Exception
    {
        Config storm_conf = new Config();
        storm_conf.setDebug(true);
        DRPCSpout drpcSpout = new DRPCSpout("storm-kafka");
        BrokerHosts hosts = new ZkHosts("gsbl90782.blue.ygrid.yahoo.com:2181");
        SpoutConfig spoutConfig = new SpoutConfig(hosts, "test27", "/test", "id1");
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("line-reader-spout", new KafkaSpout(spoutConfig));

        builder.setBolt("line-splitter", new StormKafkaBolt()).shuffleGrouping("line-reader-spout");
        builder.setBolt("word-count", new WordCount()).shuffleGrouping("line-splitter");

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
           String topology_name = args[0];
           StormKafkaTopology skt = new StormKafkaTopology();
           skt.stormkafkaSetup(topology_name);
       }
       catch(Exception e)
       {
           e.printStackTrace();
           //LOG(e.getMessage());
       }

    }

}
