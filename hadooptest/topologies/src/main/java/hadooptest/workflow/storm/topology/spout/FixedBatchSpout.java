package hadooptest.workflow.storm.topology.spout;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Properties;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.PumpStreamHandler;

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

    private String pathToStromJar () {
        File userDir = new File(System.getProperty("user.dir"));
        String thePath = null;
        try {
            for (File file: userDir.listFiles()) {
                String canonPath = file.getCanonicalPath();
                if (canonPath.endsWith("resources")) {
                    thePath = canonPath.substring(0, canonPath.length() - "resources".length()) + "stormjar.jar";
                }
            }
        } catch (IOException e) {
            throw new RuntimeException (e);
        }
        return thePath;
    }

    private void extractSampleLog () {
        String stormJarPath = pathToStromJar();
        String dstDummyLog = System.getProperty("storm.home") + "/logs/" + System.getProperty("logfile.name") + ".1.gz";
        String line = "jar xf " + stormJarPath + " sample-log-file.log.gz";
        try {
            CommandLine cmdline = CommandLine.parse(line);
            DefaultExecutor executor = new DefaultExecutor();
            DefaultExecuteResultHandler handler = new DefaultExecuteResultHandler();
            ByteArrayOutputStream stdout = new ByteArrayOutputStream();
            executor.setStreamHandler(new PumpStreamHandler(stdout));
            executor.execute(cmdline, handler);
            while (!handler.hasResult()) {
                try {
                    handler.waitFor();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            if (!new File(dstDummyLog).exists()) {
                Files.copy(new File("sample-log-file.log.gz").toPath(), new File(dstDummyLog).toPath());
            } else {
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void open(Map conf, TopologyContext context) {
        index = 0;
        for (Object obj: outputs) {
            if (obj.toString().contains("TestLogViewer")) {
                extractSampleLog();
            }
        }
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
