package hadooptest.job;

import hadooptest.TestSession;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FailJob extends Job {
	
	protected boolean failMappers = false;
	protected boolean failReducers = false;
	
	public void setMappersFail(boolean state) {
		failMappers = state;
	}
	
	public void setReducersFail(boolean state) {
		failReducers = state;
	}
	
	/*
	 * Submit a fail job to the cluster, while being able to specify whether the mappers or reducers should fail.
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

	/*
	 * Submit a fail job to the cluster, while being able to specify whether the mappers or reducers should fail.
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

	private String[] assembleCommand() {
		String hadoop_mapred_test_jar = TestSession.conf.getProperty("HADOOP_INSTALL", "") + "/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-" + TestSession.conf.getProperty("HADOOP_VERSION", "") + "-tests.jar";
		String hadoop_exe = TestSession.conf.getProperty("HADOOP_INSTALL", "") + "/bin/hadoop";

		String strFailMappers = "";
		if (failMappers) {
			strFailMappers = "-failMappers";
		}
		
		String strFailReducers = "";
		if (failReducers) {
			strFailReducers = "-failReducers";
		}
		
		return new String[] { hadoop_exe, 
				"--config", TestSession.conf.getProperty("CONFIG_BASE_DIR", ""),
				"jar", hadoop_mapred_test_jar,
				"fail", "-Dmapreduce.job.user.name=" + USER, 
				strFailMappers,
				strFailReducers };
	}

}
