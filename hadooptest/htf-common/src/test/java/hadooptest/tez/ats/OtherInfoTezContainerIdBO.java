package hadooptest.tez.ats;

import hadooptest.TestSession;

/**
 * This class provides the real Object to be used for the abstract parent class
 * {@code ATSOtherInfoEntityBO}. The object is instantiated in individual
 * methods in {@code ATSUtils} that consume the "otherinfo" key in the response.
 * 
 * @author tiwari
 * 
 */
public class OtherInfoTezContainerIdBO extends ATSOtherInfoEntityBO {
	public Long exitStatus;
	public Long endTime;

	@Override
	public boolean equals(Object arg) {
		if (!(arg instanceof OtherInfoTezContainerIdBO))
			return false;
		OtherInfoTezContainerIdBO other = (OtherInfoTezContainerIdBO) arg;
		if (this.exitStatus.longValue() != other.exitStatus.longValue()
				|| this.endTime.longValue() != other.endTime.longValue()) {
			TestSession.logger.error("Equality failed here!");
			return false;
		}
		return true;
	}

	@Override
	public int hashCode() {
		int hash = 0;
		hash = hash * 37 + this.exitStatus.hashCode();
		hash = hash * 37 + this.endTime.hashCode();
		return hash;

	}

	public void dump() {
		TestSession.logger.info("DUMPING OTHER INFO");
		TestSession.logger.info("exitStatus:" + exitStatus);
		TestSession.logger.info("endTime:" + endTime);
	}

}
