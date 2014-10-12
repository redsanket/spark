package hadooptest.tez.ats;

import hadooptest.TestSession;

public class OtherInfoTezContainerIdBO extends ATSOtherInfoEntityBO {
	public Long exitStatus;
	public Long endTime;

	public void dump() {
		TestSession.logger.info("DUMPING OTHER INFO");
		TestSession.logger.info("exitStatus:" + exitStatus);
		TestSession.logger.info("endTime:" + endTime);
	}

}
