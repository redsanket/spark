package hadooptest.cluster.pseudodistributed;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import hadooptest.cluster.JobState;

public class SleepJob extends PseudoDistributedJob {
	
	public SleepJob() {
		super();
	}
	
	/*
	 * Submit the job to the cluster.
	 * 
	 * @return String the ID of the job submitted.
	 */
	public String submit() {
		return this.submit(10, 10, 50000, 50000, 1);
	}
	
	private String submit(int m_param, int r_param, int mt_param, int rt_param, int numJobs) {			
		Process hadoopProc = null;
		String jobID = "";
		//String taskAttemptID = "";
		
		String hadoop_version = "0.23.4"; // should come from prop variable in fw conf
		String hadoop_install = "/Users/rbernota/workspace/eclipse/branch-0.23.4/hadoop-dist/target/hadoop-0.23.4"; // this should come from env $HADOOP_INSTALL or prop variable in fw conf
		String hadoop_mapred_test_jar = hadoop_install + "/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-" + hadoop_version + "-tests.jar";
		String artifacts_dir = "/Users/rbernota/workspace/artifacts";
		String hadoop_conf_dir = "/Users/rbernota/workspace/hadoop/test/pseudodistributed_configs/test/";
		String hadoop_exe = hadoop_install + "/bin/hadoop";
		String user = "rbernota"; // not sure where this should go... probably in fw conf for now, but possibly extract from system.
		
		for (int i = 0; i < numJobs; i++) {			
			String hadoopCmd = hadoop_exe + " --config " + hadoop_conf_dir 
					+ " jar " + hadoop_mapred_test_jar 
					+ " sleep -Dmapreduce.job.user.name=" + user 
					+ " -m " + m_param 
					+ " -r " + r_param 
					+ " -mt " + mt_param 
					+ " -rt " + rt_param
					+ " > " + artifacts_dir + "/sleep." + i + ".log";
			
			System.out.println("COMMAND: " + hadoop_exe + hadoopCmd);
			
			String jobPatternStr = " - Running job: (.*)$";
			Pattern jobPattern = Pattern.compile(jobPatternStr);
			
			//String mapTaskPatternStr = " - Starting task: (.*)$";
			//Pattern mapTaskPattern = Pattern.compile(mapTaskPatternStr);
			
			try {
				hadoopProc = Runtime.getRuntime().exec(hadoopCmd);
				//hadoopProc.waitFor();
				BufferedReader reader=new BufferedReader(new InputStreamReader(hadoopProc.getInputStream())); 
				String line=reader.readLine(); 
				while(line!=null) 
				{ 
					System.out.println(line); 
					
					Matcher jobMatcher = jobPattern.matcher(line);
					//Matcher mapTaskMatcher = mapTaskPattern.matcher(line);
					
					if (jobMatcher.find()) {
						jobID = jobMatcher.group(1);
						System.out.println("JOB ID: " + jobID);
						break;
					}
					//else if (mapTaskMatcher.find()) {
					//	taskAttemptID = mapTaskMatcher.group(1);
					//	System.out.println("TASK ATTEMPT ID: " + taskAttemptID);
					//	break;
					//}
					
					line=reader.readLine();
				} 
			}
			catch (Exception e) {
				if (hadoopProc != null) {
					hadoopProc.destroy();
				}
				e.printStackTrace();
			}
		}

		//this.mapTaskID = taskAttemptID;
		
		this.ID = jobID;
		
		return jobID;
	}

	
}
