package hadooptest.workflow.storm.topology.bolt;

import java.util.Map;
import java.util.Date;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

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

public class HBaseInjectionBolt extends BaseRichBolt {
    private final static byte[] FAMILY = Bytes.toBytes("date");
    private final static byte[] COLUMN = Bytes.toBytes("foo");
    private static final Logger LOG = LoggerFactory.getLogger(HBaseInjectionBolt.class);
    byte[] hbase_conf_buff;
    byte[] table_name;
    transient OutputCollector collector;
    transient HTableInterface table;
    //transient HConnection connection;
    String topology_name;

    public HBaseInjectionBolt(Configuration hbase_conf, byte[] table_name) throws IOException {
        LOG.info("constructing HBaseInjectionBolt");
        this.table_name = table_name;
        this.hbase_conf_buff = Util.ToBytes(hbase_conf);
    }

    @Override
    public void prepare(Map storm_conf, TopologyContext context,
                            OutputCollector collector) {
        try {
            LOG.info("preparing HBaseInjectionBolt");
            this.topology_name = (String)storm_conf.get(Config.TOPOLOGY_NAME);
            this.collector = collector;
            Configuration hbase_conf = Util.FromBytes(hbase_conf_buff);
            //connection = HConnectionManager.createConnection(hbase_conf);
            //table = connection.getTable(table_name);
            //table = new HTable(hbase_conf, table_name);
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            throw new RuntimeException(ex.getMessage());
        }
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            Configuration hbase_conf = Util.FromBytes(hbase_conf_buff);
            HConnection myConnection = HConnectionManager.createConnection(hbase_conf);
            HTableInterface mytable = myConnection.getTable(table_name);
            LOG.info("myConnection (writer)= " + System.identityHashCode(myConnection));
            String word = (String)tuple.getValue(0);
            Put row = new Put(Bytes.toBytes(word));
            String val = new Date().toString();
            LOG.info("injecting into HBase:"+word+" <-- "+val);
            row.add(FAMILY, COLUMN, Bytes.toBytes(val));
            mytable.put(row);
            String args = tuple.getStringByField("args");
            LOG.info("Args =" + args);
            String returnInfo = tuple.getStringByField("return-info");

            collector.emit(tuple, new Values(args, returnInfo));
            collector.ack(tuple);

            mytable.close();
            //myConnection.close();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("args", "return-info"));
    }
}
