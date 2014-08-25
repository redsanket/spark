/*
 * YAHOO!
 */

package hadooptest.workflow.hadoop.job;

import hadooptest.TestSession;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An instance of Job that represents a generic job.
 */
public class GenericJob extends Job {

	/** The jar file path to use */
	protected String jobJar =
	        TestSession.cluster.getConf().getHadoopProp("HADOOP_TEST_JAR");

	/** The config dir to use */
	protected String jobConf = TestSession.cluster.getConf().getHadoopConfDir();
	
	/** The job args to use*/
	protected String[] jobArgs = null;

	/** The job command*/
	protected String[] command = null;
	
    /** The job name to run*/
    private String jobName = null;

	/**
	 * Get the job jar file path.
	 * 
	 */
	public String getJobJar() {
		return this.jobJar;
	}
	
	/**
	 * Set the job jar file path.
	 * 
	 * @param job jar file path.
	 */
	public void setJobJar(String jobJar) {
		this.jobJar = jobJar;
	}
	
	/**
	 * Get job conf dir.
	 * 
	 */
	public String getJobConf() {
		return this.jobConf;
	}
	
	/**
	 * Set job configuration directory.
	 * 
	 * @param conf dir path.
	 */
	public void setJobConf(String confDir) {
		this.jobConf = confDir;
	}
	
	/**
	 * Get the job name.
	 * 
	 */
	public String getJobName() {
		return this.jobName;
	}
	
	/**
	 * Set the job name.
	 * 
	 * @param job name.
	 */
	public void setJobName(String name) {
		this.jobName = name;
	}
	
	/**
	 * Get the job args.
	 * 
	 */
	public String[] getJobArgs() {
		return this.jobArgs;
	}
	
	/**
	 * Set the job args.
	 * 
	 * @param job args.
	 */
	public void setJobArgs(String[] jobArgs) {
		this.jobArgs = jobArgs;
	}	
	
	/**
	 * Submit the job.  This should be done only by the Job.start() as Job 
	 * should remain threaded.
	 * 
	 * @throws Exception if there is a fatal error running the job process,
	 * or the InputStream can not be read.
	 */
	protected void submit() throws Exception {
		String jobPatternStr = " Running job: (.*)$";
		Pattern jobPattern = Pattern.compile(jobPatternStr);

		this.process =
		        TestSession.exec.runProcBuilderSecurityGetProc(
		                this.assembleCommand(), this.USER);
		BufferedReader reader =
		        new BufferedReader(new InputStreamReader(
		                this.process.getInputStream())); 
		String line=reader.readLine(); 

		StringBuffer lineBuffer = new StringBuffer();

		while(line!=null) 
		{ 
			TestSession.logger.debug(line);
			lineBuffer.append(line + "\n");

			Matcher jobMatcher = jobPattern.matcher(line);

			if (jobMatcher.find()) {
				this.ID = jobMatcher.group(1);
				TestSession.logger.debug("JOB ID: " + this.ID);
				reader.close();
				break;
			}

			line=reader.readLine();
		}

		if (this.ID.equals("0")) {
	        TestSession.logger.error(
	                "Did not find JOB ID for the submitted job: '" +
	                        lineBuffer + "'");
		}
	} 

	/**
	 * Submit the job and don't wait for the ID.  This should be done only by
	 * the Job.start() as Job should remain threaded.
	 * 
	 * @throws Exception if there is a fatal error running the job process.
	 */
	protected void submitNoID() throws Exception {
		this.process =
		        TestSession.exec.runProcBuilderSecurityGetProc(
		                this.assembleCommand(), this.USER);
	} 

	/**
	 * Submit the job and wait until it is completed. 
	 * Use this if you do not want the job run as threaded. 
	 * This will block until the job has completed. 
	 * 
	 * @throws Exception if there is a fatal error funning the job process.
	 */
	public String[] submitUnthreaded() throws Exception {
		boolean verbose = true;
		return submitUnthreaded(verbose);
	}

	/**
	 * Submit the job and wait until it is completed. 
	 * Use this if you do not want the job run as threaded. 
	 * This will block until the job has completed. 
	 * 
	 * @throws Exception if there is a fatal error funning the job process.
	 */
	public String[] submitUnthreaded(boolean verbose) throws Exception {
		String[] output = null;
		output = TestSession.exec.runProcBuilderSecurity(
		        this.assembleCommand(), verbose);
		return output;
	}

	/**
	 * Assemble the system command to launch the job.
	 * 
	 * @return String[] the string array representation of the system command to
	 * launch the job.
	 */
	protected String[] assembleCommand() {
		ArrayList<String> cmd = new ArrayList<String>();	
		cmd.add(TestSession.cluster.getConf().getHadoopProp("HADOOP_BIN"));
		cmd.add("--config");
		cmd.add(this.jobConf);
		cmd.add("jar");
		cmd.add(this.jobJar);
		cmd.add(this.jobName);
		if (!Arrays.asList(this.jobArgs).contains("mapreduce.job.name")) {
	        cmd.add("-Dmapreduce.job.name=" + this.jobName + "-" +
	                this.getTimestamp());
		}
		cmd.addAll(Arrays.asList(this.jobArgs));
		this.command = cmd.toArray(new String[0]);
		return this.command;		
	}

	/**
	 * Get the system command for launching the job.
	 * 
	 * @return String[] the string array representation of the system command to
	 *  launch the job.
	 */
	public String[] getCommand() {
		return this.command;		
	}

}
