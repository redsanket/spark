package hadooptest.tez.ats;

import hadooptest.TestSession;
import hadooptest.tez.ats.OtherInfoTezDagIdBO.DagPlanBO;

import java.util.ArrayList;
import java.util.List;

/**
 * This class provides the real Object to be used for the abstract parent class
 * {@code ATSOtherInfoEntityBO}. The object is instantiated in individual
 * methods in {@code ATSUtils} that consume the "otherinfo" key in the response.
 * 
 * @author tiwari
 * 
 */

public class OtherInfoTezTaskAttemptIdBO extends ATSOtherInfoEntityBO {
	public Long startTime;
	public String status;
	public Long timeTaken;
	public String inProgressLogsURL;
	public String completedLogsURL;
	public Long endTime;
	public String diagnostics;
	public List<CounterGroup> counters;

	public OtherInfoTezTaskAttemptIdBO(){
		this.counters = new ArrayList<CounterGroup>();
	}


	@Override
	public boolean equals(Object arg) {
		if (!(arg instanceof OtherInfoTezTaskAttemptIdBO)){
			TestSession.logger.error("Equality failed here!");
			return false;
		}
		OtherInfoTezTaskAttemptIdBO other = (OtherInfoTezTaskAttemptIdBO) arg;
		if (this.counters.size() != other.counters.size()){
			TestSession.logger.error("Equality failed here!");
			return false;
		}
		for (int xx = 0; xx < this.counters.size(); xx++) {
			if (!(this.counters.get(xx).equals(other.counters.get(xx)))) {
				TestSession.logger.error("Equality failed here!");
				return false;
			}
		}
		if( (this.startTime.longValue() != other.startTime.longValue()
				|| !this.status.equals(other.status)
				|| this.timeTaken.longValue() != other.timeTaken.longValue()
				|| !this.inProgressLogsURL.equals(other.inProgressLogsURL)
				|| !this.completedLogsURL.equals(other.completedLogsURL)
				|| this.endTime.longValue() != other.endTime.longValue() 
				|| !this.diagnostics.equals(other.diagnostics))){
			TestSession.logger.error("Equality failed here!");
			return false;
		}
		return true;
	}

	@Override
	public int hashCode() {
		int hash = 0;
		hash = hash * 37 + this.startTime.hashCode();
		hash = hash * 37 + this.status.hashCode();
		hash = hash * 37 + this.timeTaken.hashCode();
		hash = hash * 37 + this.inProgressLogsURL.hashCode();
		hash = hash * 37 + this.completedLogsURL.hashCode();
		hash = hash * 37 + this.endTime.hashCode();
		hash = hash * 37 + this.diagnostics.hashCode();
		return hash;
	}

	public void dump() {
		TestSession.logger.info("DUMPING OTHER INFO FOR TASK ATTEMPT ID");
		TestSession.logger.info("startTime:" + startTime);
		TestSession.logger.info("status:" + status);
		TestSession.logger.info("timeTaken:" + timeTaken);
		TestSession.logger.info("inProgressLogsURL:" + inProgressLogsURL);
		TestSession.logger.info("completedLogsURL:" + completedLogsURL);
		TestSession.logger.info("endTime:" + endTime);
		TestSession.logger.info("diagnostics:" + diagnostics);

		for (CounterGroup aCounterGroup : counters) {
			aCounterGroup.dump();
		}
	}

}
