package hadooptest.workflow.storm.topology.bolt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.tuple.Tuple;
import com.yahoo.storm.security.yca.AutoYCA;
import yjava.security.yca.YCAException;


public class GetYCACertForAppIdBolt extends BaseBasicBolt {
    private static final Logger LOG = LoggerFactory.getLogger(GetTicketTimeBolt.class);

    private String getYCACerts(String appId) {
        String cert = null;
        try {
            cert = AutoYCA.getYcaV2Cert(appId);
        } catch (YCAException e) {
            LOG.info("Got runtime exception: ", e);
        }
        return cert;
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String appId = tuple.getString(1);
        collector.emit(new Values(tuple.getValue(0),getYCACerts(appId)));
    }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "ycacert"));
    }
}
