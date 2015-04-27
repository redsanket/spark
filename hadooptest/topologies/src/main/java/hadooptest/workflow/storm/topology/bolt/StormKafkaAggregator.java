package hadooptest.workflow.storm.topology.bolt;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.tuple.Fields;

public class StormKafkaAggregator extends BaseRichBolt {
    private PrintWriter out = null;
    private HashMap<String, Integer> wordCountMap = new HashMap<String, Integer>();
    String outputFileName;
    private final static Logger LOG = LoggerFactory.getLogger(StormKafkaAggregator.class);
    OutputCollector _collector;

    public void handleDrpcTuple(Tuple tuple) {
        LOG.info("Got DRPC Tuple");
        String args = tuple.getStringByField("args");
        LOG.info("Args =" + args);
        String returnInfo = tuple.getStringByField("return-info");

        LOG.info("WordcountMap = " + wordCountMap);
        Integer returnValue = wordCountMap.get(args);
        if (returnValue == null) {
            returnValue = 0;
        }
        LOG.info("returnValue =" + returnValue);
        _collector.emit( tuple, new Values( Integer.toString(returnValue), returnInfo ) );
    }

    public void handleStormKafkaTuple(Tuple tuple) {
        LOG.info("Got StormKafka Tuple");

        String word = tuple.getString(0);
        Integer count = tuple.getInteger(1);

        LOG.info("Putting " + count + " at " + word);
        wordCountMap.put(word, count);
    }

    public void execute(Tuple tuple) {
        String streamId = tuple.getSourceComponent();
        LOG.info("Got Stream ID" + streamId);
        String sourceComponent = tuple.getSourceComponent();
        LOG.info("Got component" + sourceComponent);

        if (streamId.equals("word-count")) {
            handleStormKafkaTuple(tuple);
        }

        if (streamId.equals("drpc")) {
            handleDrpcTuple(tuple);
        }

        _collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // TODO Auto-generated method stub
        declarer.declare(new Fields("result", "return-info"));
    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        //outputFileName = (String) stormConf.get("test.output.location");
        _collector = collector;
    }

    public void cleanup() {
        //if (out != null) out.close();
    }
}
