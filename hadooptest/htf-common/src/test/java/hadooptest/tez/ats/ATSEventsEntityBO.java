package hadooptest.tez.ats;
import hadooptest.TestSession;

import org.json.simple.JSONObject;

public class ATSEventsEntityBO {
	public Long timestamp;
	public String eventtype;
	public JSONObject eventinfo;
	
	public void dump(){
		TestSession.logger.info("DUMPING EVENT ENTITY");
		TestSession.logger.info("timestamp:" + timestamp);
		TestSession.logger.info("eventtype:" + eventtype);
		TestSession.logger.info("eventinfo:" + eventinfo.toString());
	}
}
