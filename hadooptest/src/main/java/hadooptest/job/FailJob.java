/*
 * YAHOO!
 */

package hadooptest.job;

import hadooptest.TestSession;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An instance of Job that represents a fail job.
 */
public class FailJob extends Job {

	/** Whether or not to fail the mappers */
	protected boolean failMappers = false;

	/** Whether or not to fail the reducers */
	protected boolean failReducers = false;
	
	/**
	 * Set whether or not the mappers should fail.
	 * 
	 * @param state whether or not the mappers should fail.
	 */
	public void setMappersFail(boolean state) {
		failMappers = state;
	}
	
	/**
	 * Set whether or not the reducers should fail.
	 * 
	 * @param state whether or not the reducers should fail.
	 */
	public void setReducersFail(boolean state) {
		failReducers = state;
	}
	
	/**
	 * Submit a fail job to the cluster.  This should only be called 
	 * by the Job.start() to keep the Job threaded.
	 */
	protected void submit() {
		String jobPatternStr = " Running job: (.*)$";
		Pattern jobPattern = Pattern.compile(jobPatternStr);

		try {
			this.process = TestSession.exec.runHadoopProcBuilderGetProc(this.assembleCommand(), this.USER);
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
			e.printStackTrace();
		}
	}

	/**
	 * Submit a fail job to the cluster, and don't wait for the ID.  This should
	 * only be called by the Job.start() to keep the Job threaded.
	 */
	protected void submitNoID() {
		try {
			this.process = TestSession.exec.runHadoopProcBuilderGetProc(this.assembleCommand(), this.USER);
		}
		catch (Exception e) {
			if (this.process != null) {
				this.process.destroy();
			}
			e.printStackTrace();
		}
	}

	/**
	 * Assemble the system command to launch the fail job.
	 * 
	 * @return String[] the string array representation of the system command to launch the job.
	 */
	private String[] assembleCommand() {
		String strFailMappers = "";
		if (failMappers) {
			strFailMappers = "-failMappers";
		}
		
		String strFailReducers = "";
		if (failReducers) {
			strFailReducers = "-failReducers";
		}
		
		return new String[] { TestSession.cluster.getConf().getHadoopProp("HADOOP_BIN"), 
				"--config", TestSession.cluster.getConf().getHadoopConfDirPath(),
				"jar", TestSession.cluster.getConf().getHadoopProp("HADOOP_SLEEP_JAR"),
				"fail", "-Dmapreduce.job.user.name=" + this.USER, 
				strFailMappers,
				strFailReducers };
	}

}
