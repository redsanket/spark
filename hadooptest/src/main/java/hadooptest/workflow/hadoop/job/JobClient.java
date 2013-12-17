/*
 * YAHOO!
 */

package hadooptest.workflow.hadoop.job;

import hadooptest.TestSession;
import hadooptest.config.hadoop.HadoopConfiguration;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.TIPStatus;
import org.apache.hadoop.mapred.TaskReport;

/**
 * A class which should represent the base capability of any job
 * submitted to a cluster.
 */
public class JobClient extends org.apache.hadoop.mapred.JobClient {

    public static enum TaskType { MPA, REDUCE }
    
	public JobClient() throws IOException{
	    super(TestSession.cluster.getConf());
	}
	
    public JobClient(HadoopConfiguration conf) throws IOException{
        super(conf);
    }
    
    public HashMap<TIPStatus, Integer> getTaskReportSummary(
            TaskReport[] taskReports) {
        return this.getTaskReportSummary(
                taskReports, new HashMap<TIPStatus, Integer>());
    }

    public HashMap<TIPStatus, Integer> getTaskReportSummary(
            TaskReport[] taskReports, 
            HashMap<TIPStatus, Integer> statusCounter) {
        for ( TaskReport report : taskReports) {
            String taskId = report.getTaskID().toString();
            TestSession.logger.trace("task id=" + taskId);

            TIPStatus taskStatus = report.getCurrentStatus();
            if (!statusCounter.containsKey(taskStatus)) {
                statusCounter.put(taskStatus, 1);
                TestSession.logger.info("init count for " +
                taskStatus.toString());
            } else {
                statusCounter.put(taskStatus,
                        statusCounter.get(taskStatus)+1);
                TestSession.logger.info("increment count for " + 
                        taskStatus.toString());
            }
            TestSession.logger.info("Status of task id '" + taskId + 
                    "' = '" + report.getCurrentStatus().toString() + "'");
            if (!taskStatus.equals(TIPStatus.COMPLETE)) {
                TestSession.logger.trace("Found non-complete status '" +
                        report.getCurrentStatus().toString() +
                        "' for task id '" + taskId + "'");                      
            }
        }
        return statusCounter;
    }
    
    public TaskReportSummary getTaskReportSummary(
            JobID jobId, 
            TaskReportSummary taskReportSummary) 
                    throws InterruptedException, IOException {        

        TestSession.logger.info("job id = " + jobId.toString());
        
        TaskReport[] mapReports = this.getMapTaskReports(jobId);
        TestSession.logger.trace("Num map task reports = " + mapReports.length);
        taskReportSummary.incrementMapTasks(mapReports.length);
        taskReportSummary.setMapStatusCounter(
                this.getTaskReportSummary(
                        mapReports,
                        taskReportSummary.getMapStatusCounter()));
        
        TaskReport[] reduceReports = this.getReduceTaskReports(jobId);
        TestSession.logger.trace("Num reduce task reports = " + reduceReports.length);
        taskReportSummary.incrementReduceTasks(reduceReports.length);
        taskReportSummary.setReduceStatusCounter(
                this.getTaskReportSummary(
                        reduceReports,
                        taskReportSummary.getReduceStatusCounter()));
        return taskReportSummary;
    }
    
    public TaskReportSummary getTaskReportSummary(JobStatus[] jobsStatus) 
            throws InterruptedException, IOException {
        TaskReportSummary taskReportSummary = new TaskReportSummary();
        for ( JobStatus js : jobsStatus) {
            taskReportSummary = getTaskReportSummary(
                    js.getJobID(),
                    taskReportSummary);
        }
        return taskReportSummary;
    }
    
    public TaskReportSummary getTaskReportSummary()
        throws InterruptedException, IOException {
        TestSession.logger.info("********************************************");
        TestSession.logger.info("---> Display All Jobs:");
        TestSession.logger.info("********************************************");
        JobStatus[] jobsStatus = this.getAllJobs();
        this.displayJobList(jobsStatus);
        TestSession.logger.info("Total Number of jobs = " + jobsStatus.length);
        
        TestSession.logger.info("********************************************");
        TestSession.logger.info("--> Aggregate Task Summary For Each Job:");
        TestSession.logger.info("********************************************");
        return this.getTaskReportSummary(jobsStatus);        
    }
    
    /**
     * Get an array of current mapper TaskReport statuses for the current Job.
     * 
     * @return TaskReport[] an array of TaskReport map task statuses.
     * 
     * @throws InterruptedException if there is a problem sleeping the current Thread.
     * @throws IOException if there is a fatal error getting the Hadoop JobClient.
     */
    public void printTasksSummary() 
            throws InterruptedException, IOException {
        TaskReportSummary taskReportSummary = this.getTaskReportSummary();
        TestSession.logger.info("********************************************");
        TestSession.logger.info("--> Print Task Report Summary For All Jobs:");
        TestSession.logger.info("********************************************");        
        taskReportSummary.printSummary();
    }
}

