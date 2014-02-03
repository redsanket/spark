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

public class Aggregator extends BaseBasicBolt {
	private PrintWriter out = null;
	private HashMap<String, Integer> wordCountMap = new HashMap<String, Integer>();
	String outputFileName;
	
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		try {
			out = new PrintWriter(new BufferedWriter(new FileWriter(outputFileName, false)));				
		} catch (IOException e) {
			out = null;
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}    
		
		String word = tuple.getString(0);
		Integer count = tuple.getInteger(1);
		
		wordCountMap.put(word, count);
		Iterator<String> itr = wordCountMap.keySet().iterator();
		while (itr.hasNext()){
			word = itr.next();
			count = wordCountMap.get(word);
			out.append(word+": "+count.toString()+"\n");
		}    		    		
		if (out != null) out.close();    		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}
	
	public void prepare(Map stormConf, TopologyContext context) {
		outputFileName = (String) stormConf.get("test.output.location");
    }

    public void cleanup() {
    	if (out != null) out.close();
    }   
}