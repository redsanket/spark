package hadooptest.topologies;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.drpc.DRPCSpout;
import backtype.storm.drpc.ReturnResults;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import hadooptest.workflow.storm.topology.bolt.LineSplit;
import hadooptest.workflow.storm.topology.bolt.StormKafkaWordCountAggregator;
import hadooptest.workflow.storm.topology.bolt.WordCount;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.*;


public class StormKafkaTopology {

    private final static Logger LOG = LoggerFactory.getLogger(StormKafkaTopology.class);

    public void stormkafkaSetup(String topology_name, String topic, String function, String zookeeperHostPortInfo) throws Exception
    {
        Config storm_conf = new Config();
        storm_conf.setDebug(true);

        LOG.info("Launching Topology ...");
        DRPCSpout drpcSpout = new DRPCSpout(function);
        BrokerHosts hosts = new ZkHosts(zookeeperHostPortInfo);
        SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, "/"+topic, "KafkaSpout");
        spoutConfig.forceFromStart = true;
        spoutConfig.securityProtocol = "PLAINTEXTSASL";
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("line-reader-spout", new KafkaSpout(spoutConfig), 2);

        builder.setBolt("line-splitter", new LineSplit()).shuffleGrouping("line-reader-spout");
        builder.setBolt("word-count", new WordCount()).fieldsGrouping("line-splitter", new Fields("word"));

        builder.setSpout("drpc", drpcSpout, 1);
        builder.setBolt("check-count", new StormKafkaWordCountAggregator())
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
           String zookeeperHostPortInfo = args[3];
           StormKafkaTopology skt = new StormKafkaTopology();
           skt.stormkafkaSetup(topology_name, topic, function,zookeeperHostPortInfo);
       }
       catch(Exception e)
       {
           e.printStackTrace();
           LOG.error(e.getMessage());
       }

    }

}
