package hadooptest.workflow.hadoop.job;

import hadooptest.TestSession;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An instance of Job that represents a sleep job.
 */
public class RandomWriterJob extends Job {
	
    /** The jar file path to use */
    private String jobJar =
            TestSession.cluster.getConf().getHadoopProp("HADOOP_EXAMPLE_JAR");

    /** The config dir to use */
    private String jobConf = TestSession.cluster.getConf().getHadoopConfDir();
    
    /** The job args to use*/
    private String[] jobArgs = null;

    /** The job command*/
    private String[] command = null;
    
    /** The job name to run*/
    private String jobName = "randomwriter";

    /** The output dirto use */
    private String outputDir = null;

    /**
     * Set the data input dir.
     * 
     * @param directory
     */
    public void setOutputDir(String outputDir) {
        this.outputDir = outputDir;
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
     * @throws Exception if there is a fatal error running the job process, or
     * the InputStream can not be read.
     */
    protected void submit() throws Exception {
        String jobPatternStr = " Running job: (.*)$";
        Pattern jobPattern = Pattern.compile(jobPatternStr);

        this.process =
                TestSession.exec.runProcBuilderSecurityGetProc(
                        this.assembleCommand(), this.USER);
        BufferedReader reader =
                new BufferedReader(
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

    /**
     * Submit the job and don't wait for the ID.  This should be done only by
     * the Job.start() as Job should remain threaded.
     * 
     * @throws Exception if there is a fatal error running the job process.
     */
    protected void submitNoID() throws Exception {
        submit();
    } 

    /**
     * Assemble the system command to launch the job.
     * 
     * @return String[] the string array representation of the system command 
     * to launch the job.
     */
    private String[] assembleCommand() {
        ArrayList<String> cmd = new ArrayList<String>();    
        cmd.add(TestSession.cluster.getConf().getHadoopProp("HADOOP_BIN"));
        cmd.add("--config");
        cmd.add(this.jobConf);
        cmd.add("jar");
        cmd.add(this.jobJar);
        cmd.add(this.jobName);
        
        ArrayList<String> jobArgs = new ArrayList<String>();
        jobArgs.add("-Dmapreduce.job.acl-view-job=*");
        jobArgs.add(this.outputDir);
        this.setJobArgs(jobArgs.toArray(new String[0]));
                
        cmd.addAll(Arrays.asList(this.jobArgs));
        this.command = cmd.toArray(new String[0]);
        return this.command;
    }        
    
    /**
     * Get the system command for launching the job.
     * 
     * @return String[] the string array representation of the system command
     * to launch the job.
     */
    public String[] getCommand() {
        return this.command;
    }
}
