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

public class OtherInfoTezApplicationAttemptBO extends ATSOtherInfoEntityBO {
	public Long appSubmitTime;

	public void dump() {
		TestSession.logger.info("DUMPING OTHER INFO");
		TestSession.logger.info("appSubmitTime:" + appSubmitTime);
	}

}
