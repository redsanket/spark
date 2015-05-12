package hadooptest.workflow.storm.topology.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class KeyMessageSpout extends BaseRichSpout {
    private final static org.slf4j.Logger LOG = LoggerFactory.getLogger(KeyMessageSpout.class);
    SpoutOutputCollector _collector;
    Integer _count;

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        _count = 0;
    }

    public void nextTuple() {
        String[] messages = {"Storm", "Kafka", "Spark", "Hadoop", "Zookeeper"};
        if (_count < messages.length) {
            _collector.emit(new Values("" + (_count + 1), messages[_count]));
            _count++;
        }
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", "message"));
    }

}
