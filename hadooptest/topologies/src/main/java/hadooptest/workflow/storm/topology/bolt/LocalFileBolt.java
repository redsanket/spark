package hadooptest.workflow.storm.topology.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.utils.Utils;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.blobstore.InputStreamWithMeta;
import backtype.storm.blobstore.ClientBlobStore;
import backtype.storm.generated.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalFileBolt extends BaseBasicBolt {

    private final static Logger LOG = LoggerFactory.getLogger(LocalFileBolt.class);

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
	LOG.info("Called LocalFileBolt prepare");
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String distFile = tuple.getString(0);
	String output = null;
	String returnInfo = tuple.getStringByField("return-info");
        LOG.info("Received file name " + distFile);
	try {
	    output = getBlobContent(distFile);
	} catch (IOException io) {
            output = "Got IO Exception";
            LOG.error("Got IO exception");
	}
        collector.emit(new Values(output, returnInfo));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("result", "return-info"));
    }

    private String getBlobContent(String filename) throws IOException {
        BufferedReader r = new BufferedReader(new FileReader(filename));
        return r.readLine();
    }
}
