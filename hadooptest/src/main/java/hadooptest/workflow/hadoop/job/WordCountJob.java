package hadooptest.workflow.hadoop.job;

import hadooptest.TestSession;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An instance of Job that represents a sleep job.
 */
public class WordCountJob extends Job {
	
	/** The input path for the wordcount job */
	private String inputFile;
	
	/** The output path for the wordcount job */
	private String outputPath;
	
	/**
	 * Set the -input file for the streaming job.
	 * 
	 * @param file the input file for the job.
	 */
	public void setInputFile(String file) {
		this.inputFile = file;
	}
	
	/**
	 * Set the -output path for the streaming job.
	 * 
	 * @param path the output path for the job.
	 */
	public void setOutputPath(String path) {
		this.outputPath = path;
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
			// copy the file from local disc to the HDFS
			// do the job
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

		// set up the cmd
		if(this.QUEUE == "") {
			return new String[] { TestSession.cluster.getConf().getHadoopProp("HADOOP_BIN"), 
					"--config",
					TestSession.cluster.getConf().getHadoopConfDir(),
					"jar", TestSession.cluster.getConf().getHadoopProp("HADOOP_EXAMPLE_JAR"),
					"wordcount",
					this.inputFile,
					this.outputPath };
			
		} else { 	
			return new String[] { TestSession.cluster.getConf().getHadoopProp("HADOOP_BIN"), 
					"--config",
					TestSession.cluster.getConf().getHadoopConfDir(),
					"jar", TestSession.cluster.getConf().getHadoopProp("HADOOP_EXAMPLE_JAR"),
					"wordcount",
					"-Dmapred.job.queue.name=" + this.QUEUE,
					this.inputFile,
					this.outputPath };	
		}
	}
}
