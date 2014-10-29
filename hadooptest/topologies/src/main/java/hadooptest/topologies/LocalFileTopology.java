package hadooptest.topologies;

import java.util.Map;
import java.util.Date;
import java.net.URL;
import java.io.IOException;
import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.utils.Utils;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.drpc.DRPCSpout;
import backtype.storm.drpc.ReturnResults;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import hadooptest.workflow.storm.topology.bolt.LocalFileBolt;

public class LocalFileTopology {
    private static Logger LOG = Logger.getLogger(LocalFileTopology.class);

    public static void main(String args[]) {
        LocalFileTopology app = new LocalFileTopology();
        try {
            String topologyName = args[0];
            LOG.info("topology: " + topologyName);

            app.setupStorm(topologyName);
        } catch (Exception ex) {
            ex.printStackTrace();
            LOG.error(ex.getMessage());
        }
    }

    public void setupStorm(String topology_name) throws Exception {
        //setup topology
        TopologyBuilder builder = new TopologyBuilder();
        DRPCSpout drpcSpout = new DRPCSpout("blobstore");
        builder.setSpout("drpc", drpcSpout, 1);
        builder.setBolt("blobstore", new LocalFileBolt(), 1).shuffleGrouping("drpc");

        builder.setBolt("rr", new ReturnResults()).globalGrouping("blobstore");

        Config storm_conf = new Config();
        storm_conf.putAll(Utils.readStormConfig());
        storm_conf.setDebug(true);
        storm_conf.setNumWorkers(1);

        if (topology_name != null) {
            StormSubmitter.submitTopology(topology_name, storm_conf, builder.createTopology());
        } else {
            StormSubmitter.submitTopology("default", storm_conf, builder.createTopology());
        }
    }
}
