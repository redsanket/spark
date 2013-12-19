/*
 * YAHOO!
 */

package hadooptest.workflow.hadoop.job;

import hadooptest.TestSession;
import hadooptest.config.hadoop.HadoopConfiguration;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;

import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.TIPStatus;
import org.apache.hadoop.mapred.TaskReport;

/**
 * Wrapper to the Apache Hadoop JobClient. Contains additional methods that are
 * specific for testing.
 */
public class JobClient extends org.apache.hadoop.mapred.JobClient {

    public static enum TaskType { MPA, REDUCE }
    
	public JobClient() throws IOException{
	    super(TestSession.cluster.getConf());
	}
	
    public JobClient(HadoopConfiguration conf) throws IOException{
        super(conf);
    }
    
    /**
     * Generate the task report summary given a task report.
     * 
     * @throws InterruptedException 
     * @throws IOException 
     */
    public HashMap<TIPStatus, Integer> getTaskReportSummary(
            TaskReport[] taskReports) {
        return this.getTaskReportSummary(
                taskReports, new HashMap<TIPStatus, Integer>());
    }

    /**
     * Get JobStatus for all jobs for a given set of job IDs.
     * 
     * @param String array of job IDs
     * @throws IOException 
     */
    public JobStatus[] getJobs(String[] jobIds) throws IOException {
        JobStatus[] jobsStatus = this.getAllJobs();
        String jobId;
        ArrayList<JobStatus> filteredJs = new ArrayList<JobStatus>();
        for ( JobStatus js : jobsStatus) {
            jobId = js.getJobID().toString();
            
            if (Arrays.asList(jobIds).contains(jobId)) {
                TestSession.logger.debug("Include matching job '" + jobId +
                        "'");                
                filteredJs.add(js);
            } else {
                TestSession.logger.debug("Exclude non-matching job '" + jobId +
                        "'");                
            }
        }
        return filteredJs.toArray(new JobStatus[filteredJs.size()]);
    }
    
    /**
     * Get JobStatus for all jobs that ran at or after a given start time.
     * 
     * @param String array of job IDs
     * @throws IOException 
     */
    public JobStatus[] getJobs(long startTime) throws IOException {
        JobStatus[] jobsStatus = this.getAllJobs();
        String jobId;
        ArrayList<JobStatus> filteredJs = new ArrayList<JobStatus>();
        long jobStart;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        for ( JobStatus js : jobsStatus) {
            jobId = js.getJobID().toString();
            jobStart = js.getStartTime();
            if (jobStart >= startTime) {
                TestSession.logger.debug("Include job '" + jobId +
                        "': job start time '" + sdf.format(new Date(jobStart)) +
                        "' => cutoff time '" +
                        sdf.format(new Date(startTime)) + "'");                
                filteredJs.add(js);
            } else {
                TestSession.logger.debug("Exclude job '" + jobId +
                        "': job start time '" + sdf.format(new Date(jobStart)) +
                        "' < cutoff time '" +
                        sdf.format(new Date(startTime)) + "'");                
            }
        }
        return filteredJs.toArray(new JobStatus[filteredJs.size()]);
    }
    
    /**
     * Generate the task report summary given a task report.
     * 
     * @throws InterruptedException 
     * @throws IOException 
     */
    public HashMap<TIPStatus, Integer> getTaskReportSummary(
            TaskReport[] taskReports, 
            HashMap<TIPStatus, Integer> statusCounter) {
        for ( TaskReport report : taskReports) {
            String taskId = report.getTaskID().toString();
            TestSession.logger.trace("task id=" + taskId);

            TIPStatus taskStatus = report.getCurrentStatus();
            if (!statusCounter.containsKey(taskStatus)) {
                statusCounter.put(taskStatus, 1);
                TestSession.logger.trace("init count for " +
                taskStatus.toString());
            } else {
                statusCounter.put(taskStatus,
                        statusCounter.get(taskStatus)+1);
                TestSession.logger.trace("increment count for " + 
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
    
    /**
     * Get the task report for a particular job ID.
     * 
     * @throws InterruptedException 
     * @throws IOException 
     */
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
    
    /**
     * Get the task report summary for the given job status array.
     * 
     * @throws InterruptedException 
     * @throws IOException 
     */
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
    
    /**
     * Get the task report summary for all the jobs.
     * 
     * @throws InterruptedException 
     * @throws IOException 
     */
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
     * Print the task summary report.
     * 
     * @throws InterruptedException 
     * @throws IOException 
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

