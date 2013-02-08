package hadooptest.job;

import hadooptest.TestSession;
import hadooptest.config.TestConfiguration;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SleepJob extends Job {

	private int numMappers = 1;
	private int numReducers = 1;
	private int mapDuration = 500;
	private int reduceDuration = 500;
	private int numberOfJobs = 1;
	private int mapMemory = -1;
	private int reduceMemory = -1;
	
	public void setNumMappers(int mappers) {
		this.numMappers = mappers;
	}
	
	public void setNumReducers(int reducers) {
		this.numReducers = reducers;
	}
	
	public void setMapDuration(int mapTime) {
		this.mapDuration = mapTime;
	}
	
	public void setReduceDuration(int reduceTime) {
		this.reduceDuration = reduceTime;
	}
	
	public void setNumJobs(int numJobs) {
		this.numberOfJobs = numJobs;
	}
	
	public void setMapMemory(int memory) {
		mapMemory = memory;
	}
	
	public void setReduceMemory(int memory) {
		reduceMemory = memory;
	}
	
	public void submit() {			
		Process hadoopProc = null;
		String jobID = "";
		
		String strMapMemory = "";
		if (this.mapMemory != -1) {
			//-Dmapred.job.map.memory.mb=6144 -Dmapred.job.reduce.memory.mb=8192 
			strMapMemory = " -Dmapred.job.map.memory.mb=" + this.mapMemory;
		}
		
		String strReduceMemory = "";
		if (this.reduceMemory != -1) {
			strReduceMemory = " -Dmapred.job.reduce.memory.mb=" + this.reduceMemory;
		}
		
		String strQueue = "";
		if (QUEUE != "") {
			strQueue = " -Dmapreduce.job.queuename=" + this.QUEUE;
		}
		
		for (int i = 0; i < this.numberOfJobs; i++) {			
			String[] hadoopCmd = {
					TestSession.cluster.getConf().getHadoopProp("HADOOP_BIN"),
					"--config", 
					TestSession.cluster.getConf().getHadoopConfDirPath(),
					"jar",
					TestSession.cluster.getConf().getHadoopProp("HADOOP_SLEEP_JAR"),
					"sleep", "-Dmapreduce.job.user.name=" + this.USER,
					strQueue,
					strMapMemory,
					strReduceMemory,
					"-m", Integer.toString(this.numMappers), 
					"-r", Integer.toString(this.numReducers), 
					"-mt", Integer.toString(this.mapDuration), 
					"-rt", Integer.toString(this.reduceDuration) };
			
			String jobPatternStr = " Running job: (.*)$";
			Pattern jobPattern = Pattern.compile(jobPatternStr);
			
			try {
				hadoopProc = TestSession.exec.runHadoopProcBuilderGetProc(hadoopCmd, this.USER);
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
	} 
	
}
