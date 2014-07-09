package hadooptest.workflow.storm.topology.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.tuple.Tuple;

public class ExclaimBoltWithErrors extends ExclaimBolt {
    private static final long serialVersionUID = 1L;

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        collector.reportError(new RuntimeException("Dummy Error"));
        super.execute(tuple, collector);
    }
}
