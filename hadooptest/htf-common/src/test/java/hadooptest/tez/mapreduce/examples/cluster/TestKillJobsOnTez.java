package hadooptest.tez.mapreduce.examples.cluster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.junit.After;
import org.junit.Test;

/**
 * This test class is there to check for backward compatibility, to ensure that
 * legacy MR jobs continue to run on Tez, with the framework set to yarn-tez
 * 
 */
public class TestKillJobsOnTez {

	@Test
	public void testKillAllTezJobs() throws Exception {
		System.out.println("Running testKillAllTezJobs");
		JobConf jobConf = new JobConf(new Configuration());
		JobClient jobClient ;
		JobStatus[] jobStatuses;
		//Pass the framework name
		Configuration conf = new Configuration();
		conf.set("mapreduce.framework.name", "yarn-tez");
		jobClient = new JobClient(conf);
		jobStatuses = jobClient.getAllJobs();

		for (JobStatus aJobStatus : jobStatuses) {
			System.out.println(aJobStatus.getJobName() + " "
					+ aJobStatus.getState() + " " + aJobStatus.getJobID());
			if (aJobStatus.getState() == State.RUNNING){
				RunningJob runningJob = jobClient.getJob((JobID) aJobStatus.getJobID());
				runningJob.killJob();
			}
		}

	}

	@After
	public void logTaskReportSummary() {

	}
}
