package hadooptest.hadoop.regression;

import static org.junit.Assert.assertTrue;
import hadooptest.TestSession;
import hadooptest.workflow.hadoop.job.JobClient;
import hadooptest.workflow.hadoop.job.TaskReportSummary;

import org.apache.hadoop.mapred.JobStatus;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

/**
 * Sample test for using JobClient to get task summary report .
 */
public class TestJobHistory extends TestSession {
    
	@BeforeClass
	public static void startTestSession() throws IOException {
		TestSession.start();
	}
	
	@Test
	public void testCluster() throws Exception {	    
	    JobClient jobClient = TestSession.cluster.getJobClient();
	    // js.printTasksSummary();
	    
        TestSession.logger.info("********************************************");
        TestSession.logger.info("Display All Jobs:");
        TestSession.logger.info("********************************************");
        JobStatus[] jobsStatus1 = jobClient.getAllJobs();
        /* 
         * We can also get job status for jobs based on start time, or job IDs.
         * 
        JobStatus[] jobsStatus2 = js.getJobs(Long.parseLong("1385138589547"));
        JobStatus[] jobsStatus3 = js.getJobs(new String[] {
            "job_1385138355868_0006", "job_1385138355868_0007"});
         */
        JobStatus[] jobsStatus = jobsStatus1;
        jobClient.displayJobList(jobsStatus);
        
        TestSession.logger.info("********************************************");
        TestSession.logger.info("Aggregate Task Summary For Each Job:");
        TestSession.logger.info("********************************************");
        TaskReportSummary taskReportSummary = jobClient.getTaskReportSummary(jobsStatus);
        
        TestSession.logger.info("********************************************");
        TestSession.logger.info("Print Task Summary For All Jobs:");
        TestSession.logger.info("********************************************");        
        taskReportSummary.printSummary();
        
        // Check that there are no non-complete tasks
        int numNonCompleteMapTasks = taskReportSummary.getNonCompleteMapTasks();
        int numNonCompleteReduceTasks = taskReportSummary.getNonCompleteReduceTasks();
        
        assertTrue("There are " + numNonCompleteMapTasks +
                " map tasks that did not complete successfully",  
                numNonCompleteMapTasks == 0);
        assertTrue("There are " + numNonCompleteReduceTasks +
                " reduce tasks that did not complete successfully",  
                numNonCompleteReduceTasks == 0);
	}
}