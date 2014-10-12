package hadooptest.tez.ats;

import hadooptest.TestSession;

import java.util.ArrayList;
import java.util.List;

public class OtherInfoTezTaskIdBO extends ATSOtherInfoEntityBO {
	public Long startTime;
	public String status;
	public Long timeTaken;
	public Long scheduledTime;
	public Long endTime;
	public String diagnostics;

	public List<CounterGroup> counters;

	public OtherInfoTezTaskIdBO() {
		this.counters = new ArrayList<CounterGroup>();
	}

	public void dump() {
		TestSession.logger.info("DUMPING OTHER INFO FOR TASK ID");
		TestSession.logger.info("status:" + status);
		TestSession.logger.info("endTime:" + endTime);
		TestSession.logger.info("startTime:" + startTime);
		TestSession.logger.info("timeTaken:" + timeTaken);
		TestSession.logger.info("scheduledTime:" + scheduledTime);
		TestSession.logger.info("diagnostics:" + diagnostics);

		for (CounterGroup aCounterGroup : counters) {
			aCounterGroup.dump();
		}
	}

}
