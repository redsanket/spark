package hadooptest.tez.ats;

import hadooptest.TestSession;

import java.util.ArrayList;
import java.util.List;
/**
 * The REST called into timelineserver returns a large number of counters
 * as a part of their response (contained within "otherinfo" key). Since
 * the JSON schema of the response was similar, I abstracted the functionality
 * into this common class, such that it can be easily reused. This code
 * is exercised in the {@code ATSUtils} class by methods that expect
 * counters in the response.
 * @author tiwari
 *
 */
public class CounterGroup {
	public String counterGroupName;
	public String counterGroupDisplayName;
	List<Counter> counters;

	CounterGroup() {
		counters = new ArrayList<Counter>();
	}
	public void addCounter(Counter c){
		this.counters.add(c);
	}
	public static class Counter {
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
		for (Counter aCounter:counters){
			aCounter.dump();
		}
		
	}
}
