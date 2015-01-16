package hadooptest.workflow.storm.topology.bolt;

//import backtype.storm.generated.*;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.io.ByteArrayOutputStream;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.PumpStreamHandler;


public class MD5Bolt extends BaseBasicBolt {

    private final static Logger LOG = LoggerFactory.getLogger(MD5Bolt.class);
    /** The process executor for the test session */

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
        output = blobFileMD5(distFile);
	} catch (Exception io) {
            output = "Exception";
            LOG.error("Got exception", io);
	}
        collector.emit(new Values(output, returnInfo));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("result", "return-info"));
    }

    public String blobFileMD5 (String blobFile) throws Exception {
        String line = "md5sum " + blobFile;
        CommandLine cmdline = CommandLine.parse(line);
        DefaultExecutor executor = new DefaultExecutor();
        DefaultExecuteResultHandler handler=new DefaultExecuteResultHandler();
        ByteArrayOutputStream stdout=new ByteArrayOutputStream();
        executor.setStreamHandler(new PumpStreamHandler(stdout));
        executor.execute(cmdline, handler);
        while (!handler.hasResult()) {
            try {
                handler.waitFor();
            }
            catch (InterruptedException e) {
                LOG.error("There was an exception in MD5Bolt: ", e);
            }
        }
        if (handler.getExitValue()!=0) {
            return ("MD5 return value is not 0, bad execution. STDOUT: " + stdout.toString() );
        } else {
            String[] split = stdout.toString().split(" ");
            LOG.info("md5 results for the file = " + blobFile + " is = " + split[0]);
            return split[0];
        }
    }
}
