package hadooptest.tez.ats;

import hadooptest.TestSession;

import org.json.simple.JSONObject;

public class ATSEventsEntityBO {
	public boolean expectedInResponse;
	public Long timestamp;
	public String eventtype;
	public JSONObject eventinfo;

	public void dump() {
		TestSession.logger.info("DUMPING EVENT ENTITY");
		TestSession.logger.info("timestamp:" + timestamp);
		TestSession.logger.info("eventtype:" + eventtype);
		TestSession.logger.info("eventinfo:" + eventinfo.toString());
	}

	public ATSEventsEntityBO(Boolean expectedInResponse){
		this.expectedInResponse = expectedInResponse;
	}
	@Override
	public boolean equals(Object argument) {
		if (!(argument instanceof ATSEventsEntityBO)){
			TestSession.logger.error("Equality failed here!");
			return false;
		}
		ATSEventsEntityBO other = (ATSEventsEntityBO) argument;
		if( this.timestamp.longValue() != other.timestamp.longValue()){
			TestSession.logger.info("timestamp:[" + this.timestamp + "]while other timestampe:[" + other.timestamp+"]");
			TestSession.logger.error("Equality failed here!");
			return false;
		}
		if(!this.eventtype.equalsIgnoreCase(other.eventtype)){
			TestSession.logger.info("EvtntType:[" + this.eventtype + "]while other eventtype:[" + other.eventtype+"]");
			TestSession.logger.error("Equality failed here!");
			return false;
		}

		return true;
	}

	@Override
	public int hashCode() {
		 int hashCode = 0;
		 hashCode = hashCode * 37 + this.timestamp.hashCode();
		 hashCode = hashCode * 37 + this.eventtype.hashCode();
		 hashCode = hashCode * 37 + this.eventinfo.hashCode();
		 return hashCode;
	}
}
