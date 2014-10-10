package hadooptest.tez.ats.dag;

import hadooptest.TestSession;

public class DagOtherInfoBO {
	Long startTime;
	String status;
	Long initTime;
	Long timeTaken;
	String applicationId;
	DagPlan dagPlan;
	DagOtherInfoBO(){
		this.dagPlan = new DagPlan();
	}
	public void dump(){
		TestSession.logger.info("DUMPING OTHER INFO");
		TestSession.logger.info("Starttime:" + startTime);
		TestSession.logger.info("status:" + status);
		TestSession.logger.info("initTime:" + initTime);
		TestSession.logger.info("timeTaken:" + timeTaken);
		TestSession.logger.info("applicationId:" + applicationId);
		dagPlan.dump();
		
	}
}
