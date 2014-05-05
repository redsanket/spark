package hadooptest.workflow.storm.topology.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.yahoo.dhrainbow.dhapi.AvroEventRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.avro.generic.GenericRecord;

public class TestEventCountBolt extends BaseBasicBolt {
    private final static Logger LOG = LoggerFactory.getLogger(TestEventCountBolt.class);
    public Map<String, Integer> counts = new HashMap<String, Integer>();
    String[] path;

    public TestEventCountBolt(String ... path) {
        this.path = path;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        LOG.info("Bolt EventCount initiated");
    }

    public void execute(Tuple tuple, BasicOutputCollector collector) {
        LOG.info("TestEventCountBolt Received tuple " + tuple);
        AvroEventRecord rec = (AvroEventRecord)tuple.getValue(0);
        Object at = rec.getData();
	LOG.info("Iterating over " + Arrays.toString(path) );
	LOG.info("at = " + rec.getData());
        for (String part : path) {
	    LOG.info("Looking for " + part );
            if (at instanceof GenericRecord) {
                at = ((GenericRecord)at).get(part);
            } else {
                LOG.error("Could not find "+Arrays.toString(path)+" inside "+rec.getData());
                return;
            }
        }
        if (at == null) {
            LOG.error("Could not find "+Arrays.toString(path)+" inside "+rec.getData());
            return;
        }
        String val = at.toString();
	LOG.info("The value is " + val);
        int count = 0;
        if (counts.get(val) != null) {
            count = counts.get(val);
        }
        count++;
	LOG.info("The count is " + count + " for " + val);
        counts.put(val, count);

        collector.emit(new Values(val, count));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("event", "count"));
    }
}
