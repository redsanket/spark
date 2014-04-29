package hadooptest.workflow.hadoop.job;

import hadooptest.TestSession;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

/**
 * An instance of Job that represents a TeraSort job.
 */
public class TeraSortJob extends Job {
	
	/** The output path for the teragen job */
	private String teraGenOutputPath;
    private String teraSortOutputPath;
	// Number of 100-byte rows
	private long numInputDataRows = 1000;
	private long numMapTasks=1000;
	
    public void setNumDataRows(long numRows){
        this.numInputDataRows = numRows;
    }

    public void setNumMapTasks(long size){
        this.numMapTasks = size;
    }

	/**
	 * Set the -output path for the streaming job.
	 * 
	 * @param path the output path for the job.
	 */
	public void setTeraGenOutputPath(String path) {
		this.teraGenOutputPath = path;
	}
	
    /**
     * Set the -output path for the streaming job.
     * 
     * @param path the output path for the job.
     */
    public void setTeraSortOutputPath(String path) {
        this.teraSortOutputPath = path;
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
					reader.close();
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
	 * Assemble the system command to launch the job.
	 * 
	 * @return String[] the string array representation of the system command to launch the job.
	 */
	private String[] assembleCommand() {	    
		// set up the cmd
	    
	    // tergen
	    ArrayList<String> cmd = new ArrayList<String>();    
            cmd.add("/usr/local/bin/bash");
	    cmd.add(TestSession.cluster.getConf().getHadoopProp("HADOOP_BIN"));
	    cmd.add("--config");
	    cmd.add(TestSession.cluster.getConf().getHadoopConfDir());
	    cmd.add("jar");
	    cmd.add(TestSession.cluster.getConf().getHadoopProp("HADOOP_EXAMPLE_JAR"));
	    cmd.add("teragen");
	    if (this.QUEUE != "") {
	        cmd.add("-Dmapred.job.queue.name=" + this.QUEUE);            
	    }
	    cmd.add("-Dmapred.map.tasks=" + Long.toString(numMapTasks));            
	    cmd.add(Long.toString(numInputDataRows));
	    cmd.add(this.teraGenOutputPath + ";");
	        
        // terasort
        cmd.add(TestSession.cluster.getConf().getHadoopProp("HADOOP_BIN"));
        cmd.add("--config");
        cmd.add(TestSession.cluster.getConf().getHadoopConfDir());
        cmd.add("jar");
        cmd.add(TestSession.cluster.getConf().getHadoopProp("HADOOP_EXAMPLE_JAR"));
        cmd.add("terasort");        
        cmd.add("-Dmapreduce.job.acl-view-job=*");
        cmd.add("-Dmapreduce.reduce.input.limit=-1");
        if (this.QUEUE != "") {
            cmd.add("-Dmapred.job.queue.name=" + this.QUEUE);            
        }
	    
        // Not sure if this is applicable...
        cmd.add("-Dmapred.map.tasks=" + Long.toString(numMapTasks));

        // teragen output, terasort input
        cmd.add(this.teraGenOutputPath);
        cmd.add(this.teraSortOutputPath);

	    // Convert the commands to String Array
	    String[] command = cmd.toArray(new String[0]);
	    System.out.println(command);

	    String shCmd = StringUtils.join(command, " ");
	    return new String[] {"bash", "-c", shCmd};
	}
}



