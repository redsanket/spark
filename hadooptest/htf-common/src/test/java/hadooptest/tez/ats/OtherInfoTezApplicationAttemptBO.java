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

public class OtherInfoTezApplicationAttemptBO extends ATSOtherInfoEntityBO {
	public Long appSubmitTime;

	@Override
	public boolean equals(Object arg) {
		if (!(arg instanceof OtherInfoTezApplicationAttemptBO))
			return false;
		OtherInfoTezApplicationAttemptBO other = (OtherInfoTezApplicationAttemptBO) arg;
		 if(this.appSubmitTime.longValue() != other.appSubmitTime.longValue()){
			 TestSession.logger.error("Equality failed here!");
			 return false;
		 }
		 return true;
	}

	@Override
	public int hashCode() {
		int hash = 0;
		hash = hash * 37 + this.appSubmitTime.hashCode();
		return hash;
	}

	public void dump() {
		TestSession.logger.info("DUMPING OTHER INFO");
		TestSession.logger.info("appSubmitTime:" + appSubmitTime);
	}

}
