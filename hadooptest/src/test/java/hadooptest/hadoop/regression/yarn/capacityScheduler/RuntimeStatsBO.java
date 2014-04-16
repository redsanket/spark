package hadooptest.hadoop.regression.yarn.capacityScheduler;

import hadooptest.TestSession;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.eclipse.jetty.util.ConcurrentHashSet;

public class RuntimeStatsBO {
	CopyOnWriteArrayList<String> users;
	CopyOnWriteArrayList<String> queues;

	Set<String> unique_user_set;
	Set<String> unique_queue_set;
	private ConcurrentHashSet<Job> jobSet;
	public ConcurrentHashSet<JobStats> jobStatsSet;

	ArrayList<Thread> jobsThreads;

	public RuntimeStatsBO() {

		users = new CopyOnWriteArrayList<String>();
		queues = new CopyOnWriteArrayList<String>();
		unique_user_set = new LinkedHashSet<String>();
		unique_queue_set = new LinkedHashSet<String>();
		jobSet = new ConcurrentHashSet<Job>();
		jobsThreads = new ArrayList<Thread>();
		jobStatsSet = new ConcurrentHashSet<JobStats>();
	}

	class JobStats {
		Job job;
		ArrayList<Integer> memoryConsumed;

		JobStats(Job job) {
			this.job = job;
			memoryConsumed = new ArrayList<Integer>();
		}

		void tick() {
			try {
				memoryConsumed.add(job.getStatus().getUsedMem());
				TestSession.logger.info("JobId:" + job.getJobID() + " added mem usage:" + job.getStatus().getUsedMem());
				
				TestSession.logger.info("JobId:" + job.getJobID() + " map progress:" + job.getStatus().getMapProgress());
				TestSession.logger.info("JobId:" + job.getJobID() + " needed mem:" + job.getStatus().getNeededMem());
				TestSession.logger.info("JobId:" + job.getJobID() + " num reserved slots:" + job.getStatus().getNumReservedSlots());
				TestSession.logger.info("JobId:" + job.getJobID() + " num used slots:" + job.getStatus().getNumUsedSlots());
				TestSession.logger.info("JobId:" + job.getJobID() + " reduce progress" + job.getStatus().getReduceProgress());
				TestSession.logger.info("JobId:" + job.getJobID() + " reserved mem:" + job.getStatus().getReservedMem());
				
				
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	void startCollectingStats(){
		StatMonitorTicker statMonitorTicker = new StatMonitorTicker();
		Thread statMonitor = new Thread(statMonitorTicker);
		statMonitor.start();
		try {
			statMonitor.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	class StatMonitorTicker implements Runnable {

		@Override
		public void run() {
			while (isAtleaseOneJobRunning()) {
				try {
					for (JobStats aJobStatsObj : jobStatsSet) {
						if (aJobStatsObj.job.getStatus().getState() == State.RUNNING){
							aJobStatsObj.tick();	
						}
					}
					TestSession.logger.info("TICK!!!");
					Thread.sleep(1000);

				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
		}

	}

	boolean isAtleaseOneJobRunning() {
		boolean atleastOneJobRunning = false;
		for (Job aJob : jobSet) {
			try {
				if (aJob.getStatus().getState() == State.RUNNING) {
					atleastOneJobRunning = true;
					break;
				}
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		return atleastOneJobRunning;
	}

	public void registerJob(Job aMapRedJob) {
		jobStatsSet.add(new JobStats(aMapRedJob));
		jobSet.add(aMapRedJob);
		

		TestSession.logger.info("Consuming user:" + aMapRedJob.getUser());
		users.add(aMapRedJob.getUser());
		unique_user_set.add(aMapRedJob.getUser());
		try {
			TestSession.logger.info("Consuming queue:"
					+ aMapRedJob.getStatus().getQueue());
			queues.add(aMapRedJob.getStatus().getQueue());
			unique_queue_set.add(aMapRedJob.getStatus().getQueue());
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		TestSession.logger.info("Consuming job:" + aMapRedJob.getJobID());

	}
	

	boolean isSingleUser() {
		return unique_user_set.size() == 1 ? true : false;
	}

	boolean isSingleQueue() {
		return unique_queue_set.size() == 1 ? true : false;
	}

	@Override
	public String toString() {
		for (String aQueue : queues) {
			TestSession.logger.info("A queue in the list:" + aQueue);
		}
		for (String aUniqQueue : unique_queue_set) {
			TestSession.logger.info("A Uniq queue in the list:" + aUniqQueue);
		}
		for (String aUniqUser : unique_user_set) {
			TestSession.logger.info("A Uniq user in the list:" + aUniqUser);
		}

		StringBuilder sb = new StringBuilder();
		sb.append("--> Testing capacity scheduler with: '" + jobSet.size()
				+ "' jobs, '" + unique_user_set.size() + "' users, '"
				+ unique_queue_set.size() + "' queues, '");

		return sb.toString();

	}

}
