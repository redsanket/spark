package hadooptest.tez.ats;

import hadooptest.TestSession;

/**
 * This class provides the real Object to be used for the abstract
 * parent class {@code ATSOtherInfoEntityBO}. The object is instantiated
 * in individual methods in {@code ATSUtils} that consume the "otherinfo"
 * key in the response.
 * @author tiwari
 *
 */
public class OtherInfoTezContainerIdBO extends ATSOtherInfoEntityBO {
	public Long exitStatus;
	public Long endTime;

	public void dump() {
		TestSession.logger.info("DUMPING OTHER INFO");
		TestSession.logger.info("exitStatus:" + exitStatus);
		TestSession.logger.info("endTime:" + endTime);
	}

}
