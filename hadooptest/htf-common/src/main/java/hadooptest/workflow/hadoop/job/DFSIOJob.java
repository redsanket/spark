package hadooptest.workflow.hadoop.job;

import hadooptest.TestSession;
import hadooptest.cluster.hadoop.dfs.DFS;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// import junit.framework.Assert;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;

import org.junit.Assert;
import org.junit.BeforeClass;

/**
 * An instance of Job that represents a sleep job.
 */
public class DFSIOJob extends Job {

    private static final int FILE_SIZE = 320;
    private static int ttCount=0;
    private int percentage;
    private int numFiles=0;
    private String testDir;
    private String writeJobID;
    
    /** The DFSIO operation */
    private String operation;
    
    private static final String[] OPERATIONS = {
        "write",
        "read"
    };

    public void setupTestDir() throws Exception {
        if (this.testDir == null) {
            this.setTestDir(this.getTimestamp());
        }
        
        FileSystem fs = TestSession.cluster.getFS();
        FsShell fsShell = TestSession.cluster.getFsShell();

        if (fs.exists(new Path(testDir))) {
            TestSession.logger.info("Delete existing test directory: " +
                testDir);
            fsShell.run(new String[] {"-rm", "-r", testDir});           
        }
        TestSession.logger.info("Create new test directory: " + testDir);
        fsShell.run(new String[] {"-mkdir", "-p", testDir});
    }

    public void initNumTT() throws Exception {
        Cluster clusterInfo = TestSession.cluster.getClusterInfo();
        ttCount = clusterInfo.getClusterStatus().getTaskTrackerCount();
        TestSession.logger.info("tasktracker count = " +
                Integer.toString(ttCount));
    }

    public void setNumFiles(int numFiles) throws Exception {
        this.numFiles = numFiles;
    }

    public int getNumFiles() throws Exception {
        if (this.numFiles == 0) {
            if (DFSIOJob.ttCount == 0) {
                initNumTT();
            }
            // ttCount might be zero for example when the RM is just starting up, 
            // and the tasktrackers has not heartbeated in.
            this.numFiles = Math.min((ttCount * this.percentage)/100, 1); 
        }
        return this.numFiles;
    }

    /**
     * Set the testDir.
     * 
     * @param timestamp for the testDir
     */
    public void setTestDir(String timestamp) throws Exception {
        this.timestamp = timestamp;
        DFS dfs = new DFS();        
        this.testDir = dfs.getBaseUrl() + "/user/" +
                System.getProperty("user.name") + "/benchmarks_dfsio/" +
                timestamp;              
    }    
    
    /**
     * Set the DFSIO operation.
     * 
     * @param operation
     */
    public void setOperation(String operation) {
        this.operation = operation;
    }

    /**
     * Set the DFSIO operation percentage.
     * 
     * @param operation
     */
    public void setPercentage(int percentage) {
        this.percentage = percentage;
    }

    /**
     * Set the dependent DFSIO write Job ID.
     * 
     * @param job ID
     */
    public void setWriteJobID(String writeJobID) {
        this.writeJobID = writeJobID;
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
     * Assemble the system command to launch the sleep job.
     * 
     * @return String[] the string array representation of the system command to launch the job.
     */
    private String[] assembleCommand(String operation) throws Exception {
        ArrayList<String> cmd = new ArrayList<String>();    
        cmd.add(TestSession.cluster.getConf().getHadoopProp("HADOOP_BIN"));
        cmd.add("--config");
        cmd.add(TestSession.cluster.getConf().getHadoopConfDir());
        cmd.add("jar");
        cmd.add(TestSession.cluster.getConf().getHadoopProp("HADOOP_TEST_JAR"));
        cmd.add("TestDFSIO");
        
        // Custom Output directory        
        String outputDir = this.testDir + "/" + this.percentage;
        cmd.add("-D");
        cmd.add("test.build.data=" + outputDir);
        cmd.add("-D");
        cmd.add("mapreduce.job.name=dfsio-" + operation + "-" + 
                this.getTimestamp());        
        cmd.add("-" + operation);
        
        cmd.add("-nrFiles");
        cmd.add(Integer.toString(this.getNumFiles()));

        cmd.add("-fileSize");
        cmd.add(Integer.toString(FILE_SIZE));
        
        String[] command = cmd.toArray(new String[0]);
        return command; 
    }
    
    /**
     * Assemble the wait for job success loop.
     * 
     * @return String[] the string array representation of the system command.
     */
    private String[] assembleWaitForCommand(String jobID) throws Exception {
        ArrayList<String> cmd = new ArrayList<String>();    
        int maxWaitTime=20;
        int waitInterval=60;
        cmd.add(TestSession.conf.getProperty("WORKSPACE") + "/scripts/wait_for_job");
        cmd.add("-cluster=" + TestSession.cluster.getClusterName());
        cmd.add("-job_id=" + jobID);        
        cmd.add("-max_wait_time=" + maxWaitTime);
        cmd.add("-wait_interval=" + waitInterval + ";");       
        String[] command = cmd.toArray(new String[0]);
        return command; 
    }
    
	/**
	 * Assemble the system command to launch the sleep job.
	 * 
	 * @return String[] the string array representation of the system command to launch the job.
	 */
	private String[] assembleCommand() throws Exception {
        // set up the cmd
        if (this.operation.equals("write")) {
            return assembleCommand(this.operation);
        } else {
            if (this.writeJobID == null) {
                throw new Exception("ERROR: Cannot run DFSIO read tests without dependent DFSIO write job ID");
            }
            String[] command1 = assembleWaitForCommand(this.writeJobID);
            String[] command2 = assembleCommand(this.operation);
            String[] command = (String[]) ArrayUtils.addAll( command1, command2);
            String shCmd = StringUtils.join(command, " ");
            return new String[] {"bash", "-c", shCmd};
        }
	}
}
