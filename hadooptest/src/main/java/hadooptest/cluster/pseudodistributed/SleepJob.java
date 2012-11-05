package hadooptest.cluster.pseudodistributed;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SleepJob extends PseudoDistributedJob {
	
	private final String HADOOP_VERSION = "0.23.4";
	private final String HADOOP_INSTALL = "/Users/rbernota/workspace/eclipse/branch-0.23.4/hadoop-dist/target/hadoop-0.23.4";
	private final String CONFIG_BASE_DIR = "/Users/rbernota/workspace/hadoop/test/pseudodistributed_configs/test/";
	private final String USER = "rbernota";
	
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
		
		String hadoop_mapred_test_jar = HADOOP_INSTALL + "/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-" + HADOOP_VERSION + "-tests.jar";
		String hadoop_exe = HADOOP_INSTALL + "/bin/hadoop";
		
		for (int i = 0; i < numJobs; i++) {			
			String hadoopCmd = hadoop_exe + " --config " + CONFIG_BASE_DIR 
					+ " jar " + hadoop_mapred_test_jar 
					+ " sleep -Dmapreduce.job.user.name=" + USER 
					+ " -m " + m_param 
					+ " -r " + r_param 
					+ " -mt " + mt_param 
					+ " -rt " + rt_param;
			
			System.out.println("COMMAND: " + hadoop_exe + hadoopCmd);
			
			String jobPatternStr = " - Running job: (.*)$";
			Pattern jobPattern = Pattern.compile(jobPatternStr);
			
			try {
				hadoopProc = Runtime.getRuntime().exec(hadoopCmd);
				BufferedReader reader=new BufferedReader(new InputStreamReader(hadoopProc.getInputStream())); 
				String line=reader.readLine(); 
				
				while(line!=null) 
				{ 
					System.out.println(line); 
					
					Matcher jobMatcher = jobPattern.matcher(line);
					
					if (jobMatcher.find()) {
						jobID = jobMatcher.group(1);
						System.out.println("JOB ID: " + jobID);
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
		}
		
		this.ID = jobID;
		
		return jobID;
	}

	
}
