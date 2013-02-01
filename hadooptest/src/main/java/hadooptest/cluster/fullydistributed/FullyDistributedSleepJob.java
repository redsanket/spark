/*
 * YAHOO
 */

package hadooptest.cluster.fullydistributed;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import hadooptest.TestSession;

/*
 * A class which represents a fully distributed MapReduce Sleep Job.
 */
public class FullyDistributedSleepJob extends FullyDistributedJob {
	
	/*
	 * Class constructor.
	 */
	public FullyDistributedSleepJob() {
	}
	
	/*
	 * Submit a single default sleep job to the cluster.
	 * 
	 * @return String the ID of the job submitted.
	 */
	public String submit() {
		//return this.submit(10, 10, 50000, 50000, 1);
		return this.submit(5, 5, 500, 500, 1, -1, -1);
	}
	
	public String submit(boolean failMappers, boolean failReducers) {
		TestSession.logger.debug("submit(boolean, boolean) is unimplemented for FullyDistributedSleepJob.");
		return null;
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
	public String submit(int mappers, int reducers, int map_time,
			int reduce_time, int numJobs, int map_memory, int reduce_memory) {

		Process hadoopProc = null;
		String jobID = "";
		
		String hadoop_mapred_test_jar = HADOOP_INSTALL + "/share/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-" + HADOOP_VERSION + "-tests.jar";
		String hadoop_exe = HADOOP_INSTALL + "share/hadoop/bin/hadoop";
		
		String strMapMemory = "";
		if (map_memory != -1) {
			//-Dmapred.job.map.memory.mb=6144 -Dmapred.job.reduce.memory.mb=8192 
			strMapMemory = " -Dmapred.job.map.memory.mb=" + map_memory;
		}
		
		String strReduceMemory = "";
		if (reduce_memory != -1) {
			strReduceMemory = " -Dmapred.job.reduce.memory.mb=" + reduce_memory;
		}
		
		String strQueue = "";
		if (QUEUE != "") {
			strQueue = " -Dmapreduce.job.queuename=" + QUEUE;
		}
		
		for (int i = 0; i < numJobs; i++) {			
			String[] hadoopCmd = { hadoop_exe, "--config", CONFIG_BASE_DIR,
					"jar", hadoop_mapred_test_jar,
					"sleep", "-Dmapreduce.job.user.name=" + USER,
					strQueue,
					strMapMemory,
					strReduceMemory,
					"-m", Integer.toString(mappers), 
					"-r", Integer.toString(reducers), 
					"-mt", Integer.toString(map_time), 
					"-rt", Integer.toString(reduce_time) };
			
			String jobPatternStr = " Running job: (.*)$";
			Pattern jobPattern = Pattern.compile(jobPatternStr);
			
			try {
				hadoopProc = hadoop.runHadoopProcBuilderGetProc(hadoopCmd, USER);
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
		}
		
		this.ID = jobID;
		
		return jobID;
	}
	
    // Putting this here temporary
    public String[] runSleepJob() {
		String user = TestSession.conf.getProperty("USER", 
				System.getProperty("user.name"));		
		return runSleepJob(user);
    }

    // Putting this here temporary
    public String[] runSleepJob(String user) {    	
        String sleepJobJar = TestSession.cluster.getConf().getHadoopProp("HADOOP_SLEEP_JAR");
        String hadoopPath = TestSession.cluster.getConf().getHadoopProp("HADOOP_BIN");
        	
    	// -Dmapred.job.queue.name=default
    	String[] cmd = { hadoopPath,
    			"--config", CONFIG_BASE_DIR, "jar", sleepJobJar, "sleep", 
    			"-m", "1", "-r", "1", "-mt", "1", "-rt", "1" };
    	return hadoop.runHadoopProcBuilder(cmd, user);
    }    

    // Putting this here temporary
    public String[] listJobs() {
    	String mapredPath = TestSession.cluster.getConf().getHadoopProp("MAPRED_BIN");
    	String[] cmd = { mapredPath,
    			"--config", CONFIG_BASE_DIR, "job", "-list" };
		return hadoop.runProcBuilder(cmd);   		
    }    

}
