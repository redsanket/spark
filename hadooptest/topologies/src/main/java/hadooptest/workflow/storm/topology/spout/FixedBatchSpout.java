package hadooptest.workflow.storm.topology.spout;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;


public class FixedBatchSpout implements IBatchSpout {

    Fields fields;
    List<Object>[] outputs;
    int maxBatchSize;
    HashMap<Long, List<List<Object>>> batches = new HashMap<Long, List<List<Object>>>();
    
    public FixedBatchSpout(Fields fields, int maxBatchSize, List<Object>... outputs) {
        this.fields = fields;
        this.outputs = outputs;
        this.maxBatchSize = maxBatchSize;
    }
    
    int index = 0;
    int cycle = 1;
    
    public void setCycle(int cycle) {
        this.cycle = cycle;
    }
    
    public void open(Map conf, TopologyContext context) {
        index = 0;
    }

    public void emitBatch(long batchId, TridentCollector collector) {
        List<List<Object>> batch = this.batches.get(batchId);
        if(batch == null){
            batch = new ArrayList<List<Object>>();
            if(index>=outputs.length && cycle > 1) {
                cycle -= 1;
                index = 0;
            }
            for(int i=0; index < outputs.length && i < maxBatchSize; index++, i++) {
                batch.add(outputs[index]);
            }
            this.batches.put(batchId, batch);
        }
        for(List<Object> list : batch){
            collector.emit(list);
        }
    }

    public void ack(long batchId) {
        this.batches.remove(batchId);
    }

    public void close() {
    }

    public Map getComponentConfiguration() {
        Config conf = new Config();
        conf.setMaxTaskParallelism(1);
        return conf;
    }

    public Fields getOutputFields() {
        return fields;
    }
    
}
