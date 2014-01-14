package hadooptest.hadoop.regression;

import hadooptest.TestSession;
import hadooptest.workflow.hadoop.job.JobClient;

import org.junit.Test;

/**
 * Sample test for using JobClient to get task summary report .
 */
public class TestJobHistory extends TestSession {
    
	@Test
	public void testCluster() throws Exception {
        /* 
         * We can also get job status for jobs based on start time or job IDs.
         * 
        jobClient.validateTaskReportSummary(
                jobClient.logTaskReportSummary("tasks_report.log", TestSession.startTime), 0, 0);        
        */
        JobClient jobClient = TestSession.cluster.getJobClient();
        jobClient.validateTaskReportSummary(
                jobClient.logTaskReportSummary("tasks_report.log"), 0, 0);        
	}
}