package hadooptest.topologies;

import java.util.Map;
import java.util.Date;
import java.net.URL;
import java.io.IOException;

import backtype.storm.Config;
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

import hadooptest.workflow.storm.topology.bolt.HBaseLookupBolt;
import hadooptest.workflow.storm.topology.bolt.HBaseInjectionBolt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;

import org.apache.log4j.Logger;

public class HBaseTopology {
    private final static byte[] FAMILY = Bytes.toBytes("date");
    private final static byte[] COLUMN = Bytes.toBytes("foo");
    private static Logger LOG = Logger.getLogger(HBaseTopology.class);

    public static void main(String args[]) {
        HBaseTopology app = new HBaseTopology();
        try {
            String topologyName = args[0];
            LOG.info("topology: " + topologyName);

            String hbase_site_config_path = args[1];
            LOG.info("hbase_site_config_path: " + hbase_site_config_path);

            Configuration hbase_conf = new Configuration();
            hbase_conf.addResource(new URL("file://"+hbase_site_config_path));
            LOG.info(hbase_conf);

            String table_name = args[2];
            app.setupStorm(hbase_conf, Bytes.toBytes(table_name), topologyName);
        } catch (Exception ex) {
            ex.printStackTrace();
            LOG.error(ex.getMessage());
        }
    }

    public void setupStorm(Configuration hbase_conf, byte[] table_name, String topology_name) throws Exception {
        //setup topology
        TopologyBuilder builder = new TopologyBuilder();
        DRPCSpout drpcSpout = new DRPCSpout("hbase");
        builder.setSpout("drpc", drpcSpout, 1);
        //builder.setSpout("word", new MyWordSpout(), 4);        
        //builder.setBolt("hb-injection", new HBaseInjectionBolt(hbase_conf, table_name), 3).shuffleGrouping("word");
        builder.setBolt("hb-injection", new HBaseInjectionBolt(hbase_conf, table_name), 3).shuffleGrouping("drpc");
        builder.setBolt("hb-lookup", new HBaseLookupBolt(hbase_conf, table_name), 3).shuffleGrouping("hb-injection");

        builder.setBolt("rr", new ReturnResults()).globalGrouping("hb-lookup");

        Config storm_conf = new Config();
        storm_conf.setDebug(true);
        storm_conf.setNumWorkers(1);

        if (topology_name != null) {
            StormSubmitter.submitTopology(topology_name, storm_conf, builder.createTopology());
        } else {
            StormSubmitter.submitTopology("default", storm_conf, builder.createTopology());
        }
    }
}
