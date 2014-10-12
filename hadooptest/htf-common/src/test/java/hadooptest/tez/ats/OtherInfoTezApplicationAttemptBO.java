package hadooptest.tez.ats;

import hadooptest.TestSession;

public class OtherInfoTezApplicationAttemptBO extends ATSOtherInfoEntityBO {
	public Long appSubmitTime;

	public void dump() {
		TestSession.logger.info("DUMPING OTHER INFO");
		TestSession.logger.info("appSubmitTime:" + appSubmitTime);
	}

}
