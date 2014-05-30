package hadooptest.workflow.hadoop.job;

import static org.junit.Assert.assertNotNull;
import hadooptest.TestSession;
import hadooptest.cluster.hadoop.HadoopCluster;
import hadooptest.cluster.hadoop.HadoopCluster.Action;
import hadooptest.cluster.hadoop.dfs.DFS;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl;
import org.apache.commons.lang.ArrayUtils;

import org.junit.BeforeClass;

/**
 * An instance of Job that represents a sleep job.
 */
public class DFSIOJob extends Job {

    private static final int FILE_SIZE = 320;
    private static int ttCount;
    private int percentage;
    private String testDir;

    /** The DFSIO operation */
    private String operation;
    
    private static final String[] OPERATIONS = {
        "write",
        "read"
    };

    public void setup() throws Exception {
        setupTestDir();
        initNumTT();
    }
    
    public void setupTestDir() throws Exception {
        FileSystem fs = TestSession.cluster.getFS();
        FsShell fsShell = TestSession.cluster.getFsShell();
        DFS dfs = new DFS();
        this.testDir = dfs.getBaseUrl() + "/user/" +
            System.getProperty("user.name") + "/benchmarks_dfsio/" +
            new SimpleDateFormat("yyyyMMddhhmm'.txt'").format(new Date());
       
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
        // set up the cmd
        initNumTT();
        
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
        
        cmd.add("-" + operation);
        cmd.add("-nrFiles");
        cmd.add(Integer.toString((ttCount * this.percentage)/100));
        cmd.add("-fileSize");
        cmd.add(Integer.toString(FILE_SIZE));
        
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
	    String[] command1 = assembleCommand(OPERATIONS[0]);
	    String[] command2 = assembleCommand(OPERATIONS[1]);
	    
	    String[] command = 
	            (String[]) ArrayUtils.addAll(
	                    ArrayUtils.add(command1, ";"), command2);
	    return command;
	}
}
