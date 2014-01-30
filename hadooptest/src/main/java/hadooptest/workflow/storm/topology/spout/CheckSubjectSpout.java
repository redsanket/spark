package hadooptest.workflow.storm.topology.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.Principal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CheckSubjectSpout extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(CheckSubjectSpout.class);

    SpoutOutputCollector _collector;
    String _openName;
    boolean _wasSent = false;

    public static String getCurrentName() {
        AccessControlContext context = AccessController.getContext();
        Subject subject = Subject.getSubject(context);
        LOG.warn("Got Subject: "+subject);
        Principal p = subject.getPrincipals().iterator().next();
        return p.getName();
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        _openName = getCurrentName();
    }

    @Override
    public void nextTuple() {
        if (!_wasSent) {
            String name = getCurrentName();
            _collector.emit(new Values(name, _openName));
            _wasSent = true;
        }
    }        

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tupleName", "openName"));
    }
}
