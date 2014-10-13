package hadooptest.tez.ats;

import hadooptest.TestSession;

import java.util.ArrayList;
import java.util.List;

/**
 * This class provides the real Object to be used for the abstract parent class
 * {@code ATSOtherInfoEntityBO}. The object is instantiated in individual
 * methods in {@code ATSUtils} that consume the "otherinfo" key in the response.
 * 
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

	@Override
	public boolean equals(Object arg) {
		if (!(arg instanceof OtherInfoTezVertexIdBO)) {
			TestSession.logger.error("Equality failed here!");
			return false;
		}
		OtherInfoTezVertexIdBO other = (OtherInfoTezVertexIdBO) arg;
		if (this.counters.size() != other.counters.size()) {
			TestSession.logger.error("Equality failed here!");
			return false;
		}
		for (int xx = 0; xx < this.counters.size(); xx++) {
			if (!(this.counters.get(xx).equals(other.counters.get(xx)))) {
				TestSession.logger.error("Equality failed here!");
				return false;
			}
		}
		if ((this.numFailedTasks.longValue() != other.numFailedTasks.longValue()
				|| this.numSucceededTasks.longValue() != other.numSucceededTasks.longValue()
				|| !this.status.equals(other.status)
				|| !this.vertexName.equals(other.vertexName)
				|| !this.processorClassName.equals(other.processorClassName)
				|| this.endTime.longValue() != other.endTime.longValue()
				|| this.startTime.longValue() != other.startTime.longValue()
				|| this.initTime.longValue() != other.initTime.longValue()
				|| this.numTasks.longValue() != other.numTasks.longValue()
				|| this.timeTaken.longValue() != other.timeTaken.longValue()
				|| this.numKilledTasks.longValue() != other.numKilledTasks.longValue()
				|| this.numCompletedTasks.longValue() != other.numCompletedTasks.longValue()
				|| this.initRequestedTime.longValue() != other.initRequestedTime.longValue()
				|| !this.diagnostics.equals(other.diagnostics)
				|| this.startRequestedTime.longValue() != other.startRequestedTime.longValue() 
				|| !this.stats.equals(other.stats))) {
			TestSession.logger.error("Equality failed here!");
			return false;
		}
		return true;
	}

	@Override
	public int hashCode() {

		int hash = 0;
		hash = hash * 37 + this.numFailedTasks.hashCode();
		hash = hash * 37 + this.numSucceededTasks.hashCode();
		hash = hash * 37 + this.status.hashCode();
		hash = hash * 37 + this.vertexName.hashCode();
		hash = hash * 37 + this.processorClassName.hashCode();
		hash = hash * 37 + this.endTime.hashCode();
		hash = hash * 37 + this.counters.hashCode();
		hash = hash * 37 + this.startTime.hashCode();
		hash = hash * 37 + this.initTime.hashCode();
		hash = hash * 37 + this.numTasks.hashCode();
		hash = hash * 37 + this.timeTaken.hashCode();
		hash = hash * 37 + this.numKilledTasks.hashCode();
		hash = hash * 37 + this.numCompletedTasks.hashCode();
		hash = hash * 37 + this.initRequestedTime.hashCode();
		hash = hash * 37 + this.diagnostics.hashCode();
		hash = hash * 37 + this.startRequestedTime.hashCode();
		hash = hash * 37 + this.stats.hashCode();

		return hash;

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

		@Override
		public boolean equals(Object arg) {
			if (!(arg instanceof Stats)) {
				TestSession.logger.error("Equality failed here!");
				return false;
			}
			Stats other = (Stats) arg;
			if (this.firstTasksToStart.size() != other.firstTasksToStart.size()
					|| this.lastTasksToFinish.size() != other.lastTasksToFinish
							.size()
					|| this.shortestDurationTasks.size() != other.shortestDurationTasks
							.size()
					|| this.longestDurationTasks.size() != other.longestDurationTasks
							.size()) {
				TestSession.logger.error("Equality failed here!");
				return false;
			}
			for (int xx = 0; xx < firstTasksToStart.size(); xx++) {
				if (!(this.firstTasksToStart.get(xx)
						.equals(other.firstTasksToStart.get(xx)))) {
					TestSession.logger.error("Equality failed here!");
					return false;
				}

			}
			for (int xx = 0; xx < lastTasksToFinish.size(); xx++) {
				if (!(this.lastTasksToFinish.get(xx)
						.equals(other.lastTasksToFinish.get(xx)))) {
					TestSession.logger.error("Equality failed here!");
					return false;
				}
			}

			for (int xx = 0; xx < shortestDurationTasks.size(); xx++) {
				if (!(this.shortestDurationTasks.get(xx)
						.equals(other.shortestDurationTasks.get(xx)))) {
					TestSession.logger.error("Equality failed here!");
					return false;
				}
			}

			for (int xx = 0; xx < longestDurationTasks.size(); xx++) {
				if (!(this.longestDurationTasks.get(xx)
						.equals(other.longestDurationTasks.get(xx)))) {
					TestSession.logger.error("Equality failed here!");
					return false;
				}
			}

			if ((this.firstTaskStartTime.longValue() != other.firstTaskStartTime.longValue()
					|| this.lastTaskFinishTime.longValue() != other.lastTaskFinishTime.longValue()
					|| this.minTaskDuration.longValue() != other.minTaskDuration.longValue()
					|| this.maxTaskDuration.longValue() != other.maxTaskDuration.longValue() 
					|| this.avgTaskDuration.longValue() != other.avgTaskDuration.longValue())) {
				TestSession.logger.error("Equality failed here!");
				return false;
			}
			return true;
		}

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
