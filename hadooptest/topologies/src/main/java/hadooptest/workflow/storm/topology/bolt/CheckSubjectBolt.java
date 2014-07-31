package hadooptest.workflow.storm.topology.bolt;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import javax.security.auth.Subject;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.Principal;

public class CheckSubjectBolt extends BaseBasicBolt {
    private PrintWriter out = null;
    String outputFileName;
    String _prepareName;
    boolean _wasSent = false;

    public static String getCurrentName() {
        AccessControlContext context = AccessController.getContext();
        Subject subject = Subject.getSubject(context);
        Principal p = subject.getPrincipals().iterator().next();
        return p.getName();
    }

    public void execute(Tuple tuple, BasicOutputCollector collector) {
        try {
            out = new PrintWriter(new BufferedWriter(new FileWriter(outputFileName, false)));                
        
            String name1 = tuple.getString(0);
            String name2 = tuple.getString(1);    
            out.append("SPOUT\t"+name1+"\t"+name2+"\n");
            out.append("BOLT\t"+_prepareName+"\t"+getCurrentName()+"\n");
            out.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally { 
            out.close();
        }            
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // TODO Auto-generated method stub
        
    }
    
    public void prepare(Map stormConf, TopologyContext context) {
        outputFileName = (String) stormConf.get("test.output.location");
        _prepareName = getCurrentName();
    }

    public void cleanup() {
        if (out != null) out.close();
    }   
}
