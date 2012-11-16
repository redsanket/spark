package hadooptest.cluster.standalone;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import hadooptest.cluster.Job;
import hadooptest.cluster.JobState;

public abstract class StandaloneJob implements Job {

	public String ID = "0";	// The ID of the job.
	public JobState state;

	private final String HADOOP_INSTALL = "/Users/rbernota/workspace/eclipse/branch-0.23.4/hadoop-dist/target/hadoop-0.23.4";
	private final String CONFIG_BASE_DIR = "/Users/rbernota/workspace/hadoop/test/pseudodistributed_configs/test/";
	
	
	/*
	 * Class Constructor.
	 */
	public StandaloneJob() {
		super();
	}

	/*
	 * Returns the state of the job in the JobState format.
	 * 
	 * @return JobState The state of the job.
	 */
	public JobState state() {
		return state;
	}
	
	/*
	 * Verifies that the job ID matches the expected format.
	 * 
	 * @return boolean Whether the job ID matches the expected format.
	 */
	public boolean verifyID() {
		if (this.ID == "0") {
			System.out.println("JOB ID DID NOT MATCH FORMAT AND WAS ZERO");
			return false;
		}

		String jobPatternStr = "job_local_(.*)$";
		Pattern jobPattern = Pattern.compile(jobPatternStr);
		
		Matcher jobMatcher = jobPattern.matcher(this.ID);
		
		if (jobMatcher.find()) {
			System.out.println("JOB ID MATCHED EXPECTED FORMAT");
			System.out.println("JOB ID: " + this.ID);
			return true;
		}
		else {
			System.out.println("JOB ID DID NOT MATCH FORMAT");
			return false;
		}
	}
	
}
