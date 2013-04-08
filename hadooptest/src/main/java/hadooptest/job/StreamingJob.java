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
 * An instance of Job that represents a streaming job.
 */
public class StreamingJob extends Job {

	/** The number of mappers to use for the job */
	private int numMappers = 1;
	
	/** The number of reducers to use for the job */
	private int numReducers = 1;
	
	/** The name of the streaming job. */
	private String name = "streamingJob";
	
	/** The mapper for the streaming job. */
	private String mapper;
	
	/** The reducer for the streaming job. */
	private String reducer;
	
	/** The input file for the streaming job. */
	private String inputFile;
	
	/** The output path for the streaming job. */
	private String outputPath;
	
	/** The cache archive path for the streaming job. */
	private String cacheArchivePath = "";

	/** The archive path for the streaming job. */
	private String archivePath = "";

	/** The cacheFile path for the streaming job. */
	private String cacheFilePath = "";
	
	/** The yarn options for the streaming job. */
	private String yarnOptions;
	
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
	 * Set the name of the streaming job.
	 * 
	 * @param jobName the name of the job.
	 */
	public void setJobName(String jobName) {
		this.name = jobName;
	}
	
	/**
	 * Set the mapper for the streaming job.
	 * 
	 * @param streamingMapper the mapper for the job.
	 */
	public void setMapper(String streamingMapper) {
		this.mapper = streamingMapper;
	}

	/**
	 * Set the reducer for the streaming job.
	 * 
	 * @param streamingReducer the reducer for the job.
	 */
	public void setReducer(String streamingReducer) {
		this.reducer = streamingReducer;
	}
	
	/**
	 * Set the input file for the streaming job.
	 * 
	 * @param file the input file for the job.
	 */
	public void setInputFile(String file) {
		this.inputFile = file;
	}
	
	/**
	 * Set the output path for the streaming job.
	 * 
	 * @param path the output path for the job.
	 */
	public void setOutputPath(String path) {
		this.outputPath = path;
	}
	
	/**
	 * Set the cache archive path for the streaming job.
	 * 
	 * @param path the cache archive path for the job.
	 */
	public void setCacheArchivePath(String path) {
		this.cacheArchivePath = path;
	}
	
	/**
	 * Set the archive path for the streaming job.
	 * 
	 * @param path the cache archive path for the job.
	 */
	public void setArchivePath(String path) {
		this.archivePath = path;
	}

	/**
	 * Set the cacheFile path for the streaming job.
	 * 
	 * @param path the cache archive path for the job.
	 */
	public void setCacheFilePath(String path) {
		this.cacheFilePath = path;
	}
	
	/** 
	 * Set the YARN options for the streaming job.
	 * 
	 * @param options the YARN options for the job.
	 */
	public void setYarnOptions(String options) {
		this.yarnOptions = options;
	}

	/**
	 * Submit the job.  This should be done only by the Job.start() as Job 
	 * should remain threaded.
	 * 
	 * @throws Exception if there is a fatal error running the process to 
	 *         submit the job.
	 */
	protected void submit() throws Exception {
		String jobPatternStr = " Running job: (.*)$";
		Pattern jobPattern = Pattern.compile(jobPatternStr);

		try {
			this.process = TestSession.exec.runHadoopProcBuilderGetProc(
					this.assembleCommand(), this.USER);
			BufferedReader reader=new BufferedReader(
					new InputStreamReader(this.process.getInputStream())); 
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
	 * Submit the job and don't wait for the ID.  This should be done only by 
	 * the Job.start() as Job should remain threaded.
	 * 
	 * @throws Exception if there is a fatal error running the process to 
	 *         submit the job.
	 */
	protected void submitNoID() throws Exception {
		try {
			this.process = TestSession.exec.runHadoopProcBuilderGetProc(
					this.assembleCommand(), this.USER);
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
	 * Submit the job and wait until it is completed. 
	 * Use this if you do not want the job run as threaded. 
	 * This will block until the job has completed. 
	 * 
	 * @throws Exception if there is a fatal error running the job process.
	 */
	public String[] submitUnthreaded() 
			throws Exception {
		String[] output = null;
		
		output = TestSession.exec.runHadoopProcBuilder(this.assembleCommand(), 
				true);

		return output;
	}

	/**
	 * Assemble the system command to launch the sleep job.
	 * 
	 * @return String[] the string array representation of the system command 
	 *         to launch the job.
	 */
	private String[] assembleCommand() {

		String[] command = null;
		String hadoopBin = 
				TestSession.cluster.getConf().getHadoopProp("HADOOP_BIN");
		String config = "--config";
		String confPath = TestSession.cluster.getConf().getHadoopConfDirPath();
		String jar = "jar";
		String jarPath = TestSession.cluster.getConf().getHadoopProp(
				"HADOOP_STREAMING_JAR");
		String yarnOpts = this.yarnOptions;
		String MRusernameOpt = "-Dmapreduce.job.user.name=" + this.USER;
		String MRMapsOpt = "-Dmapreduce.job.maps=" + 
				Integer.toString(this.numMappers);
		String MRReducesOpt = "-Dmapreduce.job.reduces=" + 
				Integer.toString(this.numReducers);
		String MRJobNameOpt = "-Dmapreduce.job.name=" + this.name;
		String mapperOpt = "-mapper";
		String mapperVal = this.mapper;
		String reducerOpt = "-reducer";
		String reducerVal = this.reducer;
		String inputOpt = "-input";
		String inputVal = this.inputFile;
		String outputOpt = "-output";
		String outputVal = this.outputPath;
		
		if (!this.archivePath.equals("")) {			
			command = new String[] { hadoopBin, config, confPath, jar, jarPath,
					yarnOpts, MRusernameOpt, MRMapsOpt, MRReducesOpt, 
					MRJobNameOpt, "-archives", this.archivePath,
					mapperOpt, mapperVal, reducerOpt, reducerVal,
					inputOpt, inputVal, outputOpt, outputVal };
		}
		else if (!this.cacheArchivePath.equals("")) {
			command = new String[] { hadoopBin, config, confPath, jar, jarPath, 
					yarnOpts, MRusernameOpt, MRMapsOpt, MRReducesOpt, 
					MRJobNameOpt, mapperOpt, mapperVal, reducerOpt, reducerVal,
					inputOpt, inputVal, outputOpt, outputVal,
					"-cacheArchive", this.cacheArchivePath };
		}
		else if (!this.cacheFilePath.equals("")) {
			command = new String[] { hadoopBin, config, confPath, jar, jarPath, 
					yarnOpts, MRusernameOpt, MRMapsOpt, MRReducesOpt, 
					MRJobNameOpt, mapperOpt, mapperVal, reducerOpt, reducerVal,
					inputOpt, inputVal, outputOpt, outputVal,
					"-cacheFile", this.cacheFilePath };
		}
		
		return command;
	}
}
