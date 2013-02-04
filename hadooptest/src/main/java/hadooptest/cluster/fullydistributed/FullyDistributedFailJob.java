package hadooptest.cluster.fullydistributed;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import hadooptest.TestSession;
import hadooptest.cluster.fullydistributed.FullyDistributedJob;

public class FullyDistributedFailJob extends FullyDistributedJob {
	
	private String HADOOP_VERSION;
	private String HADOOP_INSTALL;
	private String CONFIG_BASE_DIR;
	private String USER;
	
	/*
	 * Class constructor.
	 */
	public FullyDistributedFailJob() {

		HADOOP_VERSION = TestSession.conf.getProperty("HADOOP_VERSION", "");
		HADOOP_INSTALL = TestSession.conf.getProperty("HADOOP_INSTALL", "");
		CONFIG_BASE_DIR = TestSession.conf.getProperty("CONFIG_BASE_DIR", "");
		USER = TestSession.conf.getProperty("USER", "");
	}
	
	/*
	 * Submit a single default sleep job to the cluster.
	 * 
	 * @return String the ID of the job submitted.
	 */
	public String submit() {
		//return this.submit(10, 10, 50000, 50000, 1);
		return this.submit(true, true);
	}
	
	public String submit(int mappers, int reducers, int map_time,
			int reduce_time, int numJobs, int map_memory, int reduce_memory) {
		TestSession.logger.debug("submit(int, int, int, int, int, int) is unimplemented for FullyDistributedFailJob.");
		return null;
	}
	
	/*
	 * Submit a fail job to the cluster, while being able to specify whether the mappers or reducers should fail.
	 * 
	 * @param failMappers set to true if the mappers should fail
	 * @param failReducers set to true if the reducers should fail
	 * 
	 * @return String the ID of the sleep job that was submitted to the cluster.
	 */
	public String submit(boolean failMappers, boolean failReducers) {			
		Process hadoopProc = null;
		String jobID = "";

		String hadoop_mapred_test_jar = HADOOP_INSTALL + "/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-" + HADOOP_VERSION + "-tests.jar";
		String hadoop_exe = HADOOP_INSTALL + "share/hadoop/bin/hadoop";

		String strFailMappers = "";
		if (failMappers) {
			strFailMappers = "-failMappers";
		}
		
		String strFailReducers = "";
		if (failReducers) {
			strFailReducers = "-failReducers";
		}
		
		String hadoopCmd = hadoop_exe + " --config " + CONFIG_BASE_DIR 
				+ " jar " + hadoop_mapred_test_jar 
				+ " fail -Dmapreduce.job.user.name=" + USER 
				+ " " + strFailMappers
				+ " " + strFailReducers;

		TestSession.logger.debug("COMMAND: " + hadoopCmd);

		String jobPatternStr = " Running job: (.*)$";
		Pattern jobPattern = Pattern.compile(jobPatternStr);

		try {
			hadoopProc = Runtime.getRuntime().exec(hadoopCmd);
			BufferedReader reader=new BufferedReader(new InputStreamReader(hadoopProc.getInputStream())); 
			String line=reader.readLine(); 

			while(line!=null) 
			{ 
				TestSession.logger.debug(line);

				Matcher jobMatcher = jobPattern.matcher(line);

				if (jobMatcher.find()) {
					jobID = jobMatcher.group(1);
					TestSession.logger.debug("JOB ID: " + jobID);
					break;
				}

				line=reader.readLine();
			} 
		}
		catch (Exception e) {
			if (hadoopProc != null) {
				hadoopProc.destroy();
			}
			e.printStackTrace();
		}

		this.ID = jobID;

		return jobID;
	}

}
