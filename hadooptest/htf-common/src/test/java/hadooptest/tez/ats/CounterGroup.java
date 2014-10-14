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

	public CounterGroup() {
		counters = new ArrayList<Counter>();
	}
	@Override
	public boolean equals(Object arg){
		if (!(arg instanceof CounterGroup)){
			TestSession.logger.error("Equality failed here!");
			return false;
		}
		CounterGroup other = (CounterGroup) arg;
		if (!this.counterGroupDisplayName.equals(other.counterGroupDisplayName) ||
				!this.counterGroupName.equals(other.counterGroupName)){
			TestSession.logger.error("Equality failed here!");
			return false;
		}
		if (this.counters.size() != other.counters.size()){
			TestSession.logger.error("Equality failed here!");
			return false;
		}
		for (int xx=0;xx<this.counters.size();xx++){
			if (!this.counters.get(xx).equals(other.counters.get(xx))){
				TestSession.logger.error("Equality failed here!");
				return false;
			}
		}
		return true;
	}
	
	@Override
	public int hashCode(){
		int hash = 0;
		hash = hash *37 + counterGroupName.hashCode();
		hash = hash * 37 + counterGroupDisplayName.hashCode();
		hash = hash * 37 + counters.hashCode();
		
		return hash;
				
	}
	public void addCounter(Counter c){
		this.counters.add(c);
	}
	public static class Counter {
		public String counterName;
		public String counterDisplayName;
		public Long counterValue;
		
		@Override
		public boolean equals(Object arg){
			if (!(arg instanceof Counter)){
				TestSession.logger.error("Equality failed here!");
				return false;
			}
			Counter other = (Counter) arg;
			if (!(this.counterName.equals(other.counterName)) ||
					!(this.counterDisplayName.equals(other.counterDisplayName))||
					this.counterValue.longValue()!=other.counterValue.longValue()){
				TestSession.logger.error("Equality failed here!");
				return false;
			}
			return true;
		}
		
		@Override
		public int hashCode(){
			int hash = 0;
			hash = hash * 37 + this.counterDisplayName.hashCode();
			hash = hash *37 + this.counterName.hashCode();
			hash = hash * 37 + this.counterValue.hashCode();
			
			return hash;
		}
		
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
