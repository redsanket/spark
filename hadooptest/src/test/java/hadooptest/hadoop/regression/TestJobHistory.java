package hadooptest.hadoop.regression;

import static org.junit.Assert.assertTrue;
import hadooptest.TestSession;
import hadooptest.workflow.hadoop.job.JobClient;
import hadooptest.workflow.hadoop.job.TaskReportSummary;

import org.apache.hadoop.mapred.JobStatus;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class TestJobHistory extends TestSession {
    
	@BeforeClass
	public static void startTestSession() throws IOException {
		TestSession.start();
	}
	
	@Test
	public void testCluster() throws Exception {	    
	    JobClient js = TestSession.cluster.getJobClient();
	    // js.printTasksSummary();
	    
        TestSession.logger.info("********************************************");
        TestSession.logger.info("---> Display All Jobs:");
        TestSession.logger.info("********************************************");
        JobStatus[] jobsStatus = js.getAllJobs();
        js.displayJobList(jobsStatus);
        TestSession.logger.info("Total Number of jobs = " + jobsStatus.length);
        
        TestSession.logger.info("********************************************");
        TestSession.logger.info("--> Aggregate Task Summary For Each Job:");
        TestSession.logger.info("********************************************");
        TaskReportSummary taskReportSummary = js.getTaskReportSummary(jobsStatus);
        
        TestSession.logger.info("********************************************");
        TestSession.logger.info("--> Print Task Summary For All Jobs:");
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