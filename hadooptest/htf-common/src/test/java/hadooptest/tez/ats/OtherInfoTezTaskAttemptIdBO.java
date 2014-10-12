package hadooptest.tez.ats;

import hadooptest.TestSession;

import java.util.ArrayList;
import java.util.List;

public class OtherInfoTezTaskAttemptIdBO extends ATSOtherInfoEntityBO {
	public Long startTime;
	public String status;
	public Long timeTaken;
	public String inProgressLogsURL;
	public String completedLogsURL;
	public Long endTime;
	public String diagnostics;

	public List<CounterGroup> counters;

	public OtherInfoTezTaskAttemptIdBO() {
		this.counters = new ArrayList<CounterGroup>();
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
