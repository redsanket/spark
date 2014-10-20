package hadooptest.workflow.storm.topology.bolt;

import java.util.Map;
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

public class HBaseLookupBolt extends BaseRichBolt{
    private final static byte[] FAMILY = Bytes.toBytes("date");
    private final static byte[] COLUMN = Bytes.toBytes("foo");
    private static final Logger LOG = LoggerFactory.getLogger(HBaseLookupBolt.class);
    byte[] hbase_conf_buff;
    byte[] table_name;
    transient Configuration hbase_conf;
    transient OutputCollector collector;
    transient String topology_name;
    //transient HConnection connection;

    public HBaseLookupBolt(Configuration hbase_conf, byte[] table_name) throws IOException {
        LOG.debug("constructing HBaseLookupBolt");
        this.hbase_conf_buff = Util.ToBytes(hbase_conf);
        this.table_name = table_name;
    }

    @Override
    public void prepare(Map storm_conf, TopologyContext context, OutputCollector collector)  {
        try {
            LOG.info("preparing HBaseLookupBolt");
            this.topology_name = (String)storm_conf.get(Config.TOPOLOGY_NAME);
            this.collector = collector;
            hbase_conf = Util.FromBytes(hbase_conf_buff);
            //connection = HConnectionManager.createConnection(hbase_conf);
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            throw new RuntimeException(ex.getMessage());
        }
    }


    @Override
    public void execute(Tuple tuple) {
        try {
            String word = (String)tuple.getValue(0);
            //LOG.debug("got word:"+word);
            HConnection myConnection;
            myConnection = HConnectionManager.createConnection(hbase_conf);
            LOG.info("myConnection (reader)= " + System.identityHashCode(myConnection));
            HTableInterface mytable = myConnection.getTable(table_name);
            Get row = new Get(Bytes.toBytes(word));
            Result result = mytable.get(row);
            String val = Bytes.toString(result.getValue(FAMILY, COLUMN));
            //LOG.info("HBase record found: "+word+" = "+val);

            //collector.emit(tuple, new Values(tuple.getString(0)));
            String returnInfo = tuple.getStringByField("return-info");
            collector.emit( tuple, new Values( tuple.getString(0), returnInfo ) );
            collector.ack(tuple);
            mytable.close();
            //myConnection.close();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //declarer.declare(new Fields("word"));
        declarer.declare(new Fields("result", "return-info"));
    }
}
