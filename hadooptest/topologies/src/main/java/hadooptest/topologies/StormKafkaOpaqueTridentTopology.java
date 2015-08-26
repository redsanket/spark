package hadooptest.topologies;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.*;
import storm.trident.operation.builtin.*;
import storm.trident.testing.Split;
import storm.trident.testing.MemoryMapState;



public class StormKafkaOpaqueTridentTopology {

    private final static Logger LOG = LoggerFactory.getLogger(StormKafkaOpaqueTridentTopology.class);

    public void stormkafkaSetup(String topology_name, String topic, 
        String function, String zookeeperHostPortInfo, String useOpaque) throws Exception
    {
        Config storm_conf = new Config();
        storm_conf.setDebug(true);

        LOG.info("Launching Topology ...");
        TridentTopology topology = new TridentTopology();        
        BrokerHosts hosts = new ZkHosts(zookeeperHostPortInfo);
        TridentKafkaConfig spoutConf = new TridentKafkaConfig(hosts, topic, "OpaqueTridentKafkaSpout");
        spoutConf.forceFromStart = true;
        spoutConf.securityProtocol = "PLAINTEXTSASL";
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());

        OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);

        TridentState wordCounts = 
            topology.newStream("line-reader-spout", spout).name("kafka-line-spout").parallelismHint(1)
                .each(spout.getOutputFields(), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))                
                .parallelismHint(6);

        topology.newDRPCStream(function)
           .each(new Fields("args"), new Split(), new Fields("word"))
           .groupBy(new Fields("word"))
           .stateQuery(wordCounts, null, new MapGet(), new Fields("count"))
           .each(new Fields("count"), new FilterNull())
           .aggregate(new Fields("count"), new Sum(), new Fields("sum"));

        if(topology_name != null) {
            StormSubmitter.submitTopology(topology_name, storm_conf, topology.build());
        } else {
            StormSubmitter.submitTopology("default", storm_conf, topology.build());
        }
    }

    public static void main(String[] args) throws Exception
    {
       try
       {
           LOG.info("Triggering StormKafkaTopology with name " + 
               args[0] + "topic " + args[1] + " and drpcfunction" + args[2]);
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
