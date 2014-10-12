package hadooptest.tez.ats;

import hadooptest.TestSession;

import java.util.ArrayList;
import java.util.List;

/**
 * This class provides the real Object to be used for the abstract
 * parent class {@code ATSOtherInfoEntityBO}. The object is instantiated
 * in individual methods in {@code ATSUtils} that consume the "otherinfo"
 * key in the response.
 * @author tiwari
 *
 */

public class OtherInfoTezVertexIdBO extends ATSOtherInfoEntityBO {
	public Long numFailedTasks;
	public Long numSucceededTasks;
	public String status;
	public String vertexName;
	public String processorClassName;
	public Long endTime;
	public List<CounterGroup> counters;
	public Long startTime;
	public Long initTime;
	public Long numTasks;
	public Long timeTaken;
	public Long numKilledTasks;
	public Long numCompletedTasks;
	public Long initRequestedTime;
	public String diagnostics;
	public Long startRequestedTime;
	public Stats stats;

	public OtherInfoTezVertexIdBO() {
		this.counters = new ArrayList<CounterGroup>();
	}

	public void dump() {
		TestSession.logger.info("DUMPING OTHER INFO FOR VERTEX ID");
		TestSession.logger.info("numFailedTasks:" + numFailedTasks);
		TestSession.logger.info("numSucceededTasks:" + numSucceededTasks);
		TestSession.logger.info("status:" + status);
		TestSession.logger.info("vertexName:" + vertexName);
		TestSession.logger.info("processorClassName:" + processorClassName);
		TestSession.logger.info("endTime:" + endTime);
		TestSession.logger.info("startTime:" + startTime);
		TestSession.logger.info("initTime:" + initTime);
		TestSession.logger.info("numTasks:" + numTasks);
		TestSession.logger.info("timeTaken:" + timeTaken);
		TestSession.logger.info("numKilledTasks:" + numKilledTasks);
		TestSession.logger.info("numCompletedTasks:" + numCompletedTasks);
		TestSession.logger.info("initRequestedTime:" + initRequestedTime);
		TestSession.logger.info("diagnostics:" + diagnostics);
		TestSession.logger.info("startRequestedTime:" + startRequestedTime);
		stats.dump();
		for (CounterGroup aCounterGroup : counters) {
			aCounterGroup.dump();
		}
	}

	public static class Stats {
		public Long firstTaskStartTime;
		public ArrayList<String> firstTasksToStart;
		public Long lastTaskFinishTime;
		public ArrayList<String> lastTasksToFinish;
		public ArrayList<String> shortestDurationTasks;
		public ArrayList<String> longestDurationTasks;
		public Long minTaskDuration;
		public Long maxTaskDuration;
		public Double avgTaskDuration;

		public Stats() {
			firstTasksToStart = new ArrayList<String>();
			lastTasksToFinish = new ArrayList<String>();
			shortestDurationTasks = new ArrayList<String>();
			longestDurationTasks = new ArrayList<String>();
		}

		public void dump() {
			TestSession.logger.info("firstTaskStartTime:" + firstTaskStartTime);
			TestSession.logger.info("first tasks to start:");
			for (String aString : firstTasksToStart) {
				TestSession.logger.info(aString);
			}
			TestSession.logger.info("firstTasksToStart:");
			for (String aString : firstTasksToStart) {
				TestSession.logger.info(aString);
			}

			TestSession.logger.info("lastTaskFinishTime:" + lastTaskFinishTime);
			TestSession.logger.info("last tasks to finish:");
			for (String aString : lastTasksToFinish) {
				TestSession.logger.info(aString);
			}
			TestSession.logger.info("minTaskDuration:" + minTaskDuration);
			TestSession.logger.info("maxTaskDuration:" + maxTaskDuration);
			TestSession.logger.info("avgTaskDuration:" + avgTaskDuration);
			TestSession.logger.info("shortest duration tasks:");
			for (String aString : shortestDurationTasks) {
				TestSession.logger.info(aString);
			}
			TestSession.logger.info("longest duration tasks:");
			for (String aString : longestDurationTasks) {
				TestSession.logger.info(aString);
			}

		}
	}

}
