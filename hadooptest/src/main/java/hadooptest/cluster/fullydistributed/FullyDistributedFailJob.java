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
	
	private static TestSession TSM;
	
	/*
	 * Class constructor.
	 */
	public FullyDistributedFailJob(TestSession testSession) {
		super(testSession);
		
		TSM = testSession;

		HADOOP_VERSION = TSM.conf.getProperty("HADOOP_VERSION", "");
		HADOOP_INSTALL = TSM.conf.getProperty("HADOOP_INSTALL", "");
		CONFIG_BASE_DIR = TSM.conf.getProperty("CONFIG_BASE_DIR", "");
		USER = TSM.conf.getProperty("USER", "");
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
	
	/*
	 * Submit a sleep job to the cluster, while being able to specify the sleep job parameters.
	 * 
	 * @param mappers The "-m" param to the sleep job (number of mappers)
	 * @param reducers The "-r" param to the sleep job (number of reducers)
	 * @param mt_param The "-rt" param to the sleep job (map time)
	 * @param rt_param The "-mt" param to the sleep job (reduce time)
	 * @param numJobs The number of sleep jobs to run.
	 * @param map_memory The memory assigned for map jobs.  If -1, it will use the default.
	 * @param reduce_memory The memory assigned for reduce jobs.  If -1, it will use the default.
	 * 
	 * @return String the ID of the sleep job that was submitted to the pseudodistributed cluster.
	 */
	public String submit(boolean failMappers, boolean failReducers) {			
		Process hadoopProc = null;
		String jobID = "";

		String hadoop_mapred_test_jar = HADOOP_INSTALL + "/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-" + HADOOP_VERSION + "-tests.jar";
		String hadoop_exe = HADOOP_INSTALL + "/bin/hadoop";

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

		TSM.logger.debug("COMMAND: " + hadoopCmd);

		String jobPatternStr = " Running job: (.*)$";
		Pattern jobPattern = Pattern.compile(jobPatternStr);

		try {
			hadoopProc = Runtime.getRuntime().exec(hadoopCmd);
			BufferedReader reader=new BufferedReader(new InputStreamReader(hadoopProc.getInputStream())); 
			String line=reader.readLine(); 

			while(line!=null) 
			{ 
				TSM.logger.debug(line);

				Matcher jobMatcher = jobPattern.matcher(line);

				if (jobMatcher.find()) {
					jobID = jobMatcher.group(1);
					TSM.logger.debug("JOB ID: " + jobID);
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
