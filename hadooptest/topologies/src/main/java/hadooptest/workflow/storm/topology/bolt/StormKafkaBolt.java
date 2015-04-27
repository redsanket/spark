package hadooptest.workflow.storm.topology.bolt;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
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

public class StormKafkaBolt extends BaseRichBolt{

    OutputCollector _collector;
    private final static Logger LOG = LoggerFactory.getLogger(StormKafkaAggregator.class);
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }
    // Split the incoming strings
    @Override
    public void execute(Tuple tuple) {
        LOG.info("Tuple:" + tuple.getString(0));
        String line = tuple.getString(0);
        String[] split = line.split(" ");

        for(String word : split) {
            _collector.emit(tuple, new Values(word));

        }
        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("word"));
    }


}
