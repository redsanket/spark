/*
 * YAHOO!
 */

package hadooptest.job;

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
	private String jobJar = null;

	/** The config dir to use */
	private String jobConf = TestSession.cluster.getConf().getHadoopConfDirPath();
	
	/** The job name to run*/
	private String jobName = null;

	/** The job args to use*/
	private String[] jobArgs = null;

	/** The job command*/
	private String[] command = null;
	
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
	 * Submit the job.  This should be done only by the Job.start() as Job should
	 * remain threaded.
	 * 
	 * @throws Exception if there is a fatal error running the job process, or the 
	 *         InputStream can not be read.
	 */
	protected void submit() 
			throws Exception {
		String jobPatternStr = " Running job: (.*)$";
		Pattern jobPattern = Pattern.compile(jobPatternStr);

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

	/**
	 * Submit the job and don't wait for the ID.  This should be done only by the Job.start() as Job should
	 * remain threaded.
	 * 
	 * @throws Exception if there is a fatal error running the job process.
	 */
	protected void submitNoID() 
			throws Exception {
		this.process = TestSession.exec.runHadoopProcBuilderGetProc(this.assembleCommand(), this.USER);
	} 

	/**
	 * Submit the job and wait until it is completed. 
	 * Use this if you do not want the job run as threaded. 
	 * This will block until the job has completed. 
	 * 
	 * @throws Exception if there is a fatal error funning the job process.
	 */
	public String[] submitUnthreaded() 
			throws Exception {
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
	public String[] submitUnthreaded(boolean verbose) 
			throws Exception {
		String[] output = null;
		
		output = TestSession.exec.runHadoopProcBuilder(this.assembleCommand(), verbose);

		return output;
	}

	/**
	 * Assemble the system command to launch the job.
	 * 
	 * @return String[] the string array representation of the system command to launch the job.
	 */
	private String[] assembleCommand() {
		ArrayList<String> cmd = new ArrayList<String>();	
		cmd.add(TestSession.cluster.getConf().getHadoopProp("HADOOP_BIN"));
		cmd.add("--config");
		cmd.add(this.jobConf);
		cmd.add("jar");
		cmd.add(this.jobJar);
		cmd.add(this.jobName);
		cmd.addAll(Arrays.asList(this.jobArgs));
		this.command = cmd.toArray(new String[0]);
		return this.command;		
	}

	/**
	 * Get the system command for launching the job.
	 * 
	 * @return String[] the string array representation of the system command to launch the job.
	 */
	public String[] getCommand() {
		return this.command;		
	}

}
