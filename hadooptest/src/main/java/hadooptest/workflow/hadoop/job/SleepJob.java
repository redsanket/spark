/*
 * YAHOO!
 */

package hadooptest.workflow.hadoop.job;

import hadooptest.TestSession;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An instance of Job that represents a sleep job.
 */
public class SleepJob extends Job {

	/** The number of mappers to use for the job */
	private int numMappers = 1;
	
	/** The number of reducers to use for the job */
	private int numReducers = 1;
	
	/** The duration of the sleep job map phase */
	private int mapDuration = 500;
	
	/** The duration of the sleep job reduce phase */
	private int reduceDuration = 500;
	
	/** The memory to use for the map phase */
	private int mapMemory = -1;
	
	/** The memory to use for the reduce phase */
	private int reduceMemory = -1;
	
	/**
	 * Set the number of mappers to use for the sleep job.
	 * 
	 * @param mappers the number of mappers to use for the sleep job.
	 */
	public void setNumMappers(int mappers) {
		this.numMappers = mappers;
	}
	
	/**
	 * Set the number of reducers to use for the sleep job.
	 * 
	 * @param reducers the number of reducers to use for the sleep job.
	 */
	public void setNumReducers(int reducers) {
		this.numReducers = reducers;
	}
	
	/**
	 * Set the duration of the map phase.
	 * 
	 * @param mapTime the duration of the map phase.
	 */
	public void setMapDuration(int mapTime) {
		this.mapDuration = mapTime;
	}

	/**
	 * Set the duration of the reduce phase.
	 * 
	 * @param reduceTime the duration of the reduce phase.
	 */
	public void setReduceDuration(int reduceTime) {
		this.reduceDuration = reduceTime;
	}
	
	/**
	 * Set the memory to be used by the mappers.
	 * 
	 * @param memory the memory to be used by the mappers.
	 */
	public void setMapMemory(int memory) {
		mapMemory = memory;
	}
	
	/**
	 * Set the memory to be used by the reducers.
	 * 
	 * @param memory the memory to be used by the reducers.
	 */
	public void setReduceMemory(int memory) {
		reduceMemory = memory;
	}

	/**
	 * Submit the job.  This should be done only by the Job.start() as Job should
	 * remain threaded.
	 * 
	 * @throws Exception if there is a fatal error running the process to submit the job.
	 */
	protected void submit() throws Exception {
		String jobPatternStr = " Running job: (.*)$";
		Pattern jobPattern = Pattern.compile(jobPatternStr);

		try {
			this.process = TestSession.exec.runProcBuilderSecurityGetProc(this.assembleCommand(), this.USER);
			BufferedReader reader=new BufferedReader(new InputStreamReader(this.process.getInputStream())); 
			String line=reader.readLine(); 

			while(line!=null) 
			{ 
				TestSession.logger.debug(line);

				Matcher jobMatcher = jobPattern.matcher(line);

				if (jobMatcher.find()) {
					this.ID = jobMatcher.group(1);
					TestSession.logger.debug("JOB ID: " + this.ID);
					break;
				}

				line=reader.readLine();
			} 
		}
		catch (Exception e) {
			if (this.process != null) {
				this.process.destroy();
			}
			
			TestSession.logger.error("Exception " + e.getMessage(), e);
			throw e;
		}
	} 

	/**
	 * Submit the job and don't wait for the ID.  This should be done only by the Job.start() as Job should
	 * remain threaded.
	 * 
	 * @throws Exception if there is a fatal error running the process to submit the job.
	 */
	protected void submitNoID() throws Exception {
		try {
			this.process = TestSession.exec.runProcBuilderSecurityGetProc(this.assembleCommand(), this.USER);
		}
		catch (Exception e) {
			if (this.process != null) {
				this.process.destroy();
			}
			
			TestSession.logger.error("Exception " + e.getMessage(), e);
			throw e;
		}
	} 

	/**
	 * Assemble the system command to launch the sleep job.
	 * 
	 * @return String[] the string array representation of the system command to launch the job.
	 */
	private String[] assembleCommand() {		
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

		return new String[] { TestSession.cluster.getConf().getHadoopProp("HADOOP_BIN"), 
				"--config",
				TestSession.cluster.getConf().getHadoopConfDir(),
				"jar", TestSession.cluster.getConf().getHadoopProp("HADOOP_TEST_JAR"),
				"sleep", "-Dmapreduce.job.user.name=" + this.USER,
				strQueue,
				strMapMemory,
				strReduceMemory,
				"-m", Integer.toString(this.numMappers), 
				"-r", Integer.toString(this.numReducers), 
				"-mt", Integer.toString(this.mapDuration), 
				"-rt", Integer.toString(this.reduceDuration) };
	}
}
