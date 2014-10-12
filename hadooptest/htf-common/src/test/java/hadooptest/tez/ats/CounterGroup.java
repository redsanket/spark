package hadooptest.tez.ats;

import hadooptest.TestSession;

import java.util.ArrayList;
import java.util.List;

public class CounterGroup {
	public String counterGroupName;
	public String counterGroupDisplayName;
	List<Counterr> counters;

	CounterGroup() {
		counters = new ArrayList<Counterr>();
	}
	public void addCounter(Counterr c){
		this.counters.add(c);
	}
	public static class Counterr {
		public String counterName;
		public String counterDisplayName;
		public Long counterValue;
		
		public void dump(){
			TestSession.logger.info("counterName:" + counterName);
			TestSession.logger.info("counterDisplayName:" + counterDisplayName);
			TestSession.logger.info("counterValue:" + counterValue);
		}
	}
	public void dump(){
		TestSession.logger.info("DUMPING COUNTER GROUPS...");
		TestSession.logger.info("counterGroupName:" + counterGroupName);
		TestSession.logger.info("counterGroupDisplayName:" + counterGroupDisplayName);
		for (Counterr aCounter:counters){
			aCounter.dump();
		}
		
	}
}
