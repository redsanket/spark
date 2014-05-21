package hadooptest.workflow.storm.topology.bolt;

import storm.trident.operation.BaseFunction;
import storm.trident.tuple.TridentTuple;
import storm.trident.operation.TridentCollector;
import backtype.storm.tuple.Values;

public class Split extends BaseFunction {

    public void execute(TridentTuple tuple, TridentCollector collector) {
        String sentence = tuple.getString(0);
        for(String word: sentence.split(" ")) {
            collector.emit(new Values(word));                
        }
    }
}
