/*
 * YAHOO!
 */

package hadooptest.workflow.hadoop.job;

import static org.junit.Assert.assertTrue;
import hadooptest.TestSession;
import hadooptest.config.hadoop.HadoopConfiguration;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.TIPStatus;
import org.apache.hadoop.mapred.TaskReport;

/**
 * Wrapper to the Apache Hadoop JobClient. Contains additional methods that are
 * specific for testing.
 */
public class JobClient extends org.apache.hadoop.mapred.JobClient {

    public static enum TaskType { MAP, REDUCE }
    
	public JobClient() throws IOException{
	    super(TestSession.cluster.getConf());
	}
	
    public JobClient(HadoopConfiguration conf) throws IOException{
        super(conf);
    }
    
    /**
     * Get JobStatus for all jobs for a given set of job IDs.
     * 
     * @param String array of job IDs
     * 
     * @throws IOException 
     */
    public JobStatus[] getJobs(String[] jobIds) throws IOException {
        TestSession.logger.debug("Get job status for jobs based on job IDs:" +
                StringUtils.join(jobIds,","));
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
     * @param long startTime
     * 
     * @throws IOException 
     */
    public JobStatus[] getJobs(long startTime) throws IOException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        TestSession.logger.debug(
                "Get job status for jobs based on start time '" +
                        sdf.format(new Date(startTime)) + "':");
        JobStatus[] jobsStatus = this.getAllJobs();
        String jobId;
        ArrayList<JobStatus> filteredJs = new ArrayList<JobStatus>();
        long jobStart;
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
     * Display Job List for a given set of JobStatus. This overrides the parent
     * class of apache hadoop JobClient because we want to be able to log this 
     * information to external log file via the TestSession logger.
     * 
     * @param Array of job status. 
     * 
     * @throws IOException 
     */
    public void displayJobList(JobStatus[] jobs) {
        TestSession.logger.info("--------------------------------------------");
        TestSession.logger.info("Display jobs:");
        TestSession.logger.info("--------------------------------------------");
        TestSession.logger.info("Total jobs:" + jobs.length);
        TestSession.logger.info(
                "JobId" + "\t" + "State" + "\t" + "StartTime" + "\t" +
                "UserName" + "\t" + "Queue" + "\t" + "Priority" + "\t" + 
                "UsedContainers" + "\t" + "\t" + "RsvdContainers" + "\t" + 
                "UsedMem" + "\t" + "RsvdMem" + "\t" + "NeededMem" + "\t" + 
                "AM info");
        for (JobStatus job : jobs) {
              int numUsedSlots = job.getNumUsedSlots();
              int numReservedSlots = job.getNumReservedSlots();
              int usedMem = job.getUsedMem();
              int rsvdMem = job.getReservedMem();
              int neededMem = job.getNeededMem();
              TestSession.logger.info(
                  job.getJobID().toString() + "\t" + job.getState() + "\t" +
                  job.getStartTime() + "\t" + job.getUsername() + "\t" + 
                  job.getQueue() + "\t" + job.getPriority().name() + "\t" + 
                  (numUsedSlots < 0 ? "UNAVAILABLE" : numUsedSlots) + "\t" + 
                  (numReservedSlots < 0 ? "UNAVAILABLE" : numReservedSlots) + "\t" + 
                  (usedMem < 0 ? "UNAVAILABLE" : String.format("%dM", usedMem)) + "\t" + 
                  (rsvdMem < 0 ? "UNAVAILABLE" : String.format("%dM", rsvdMem)) + "\t" + 
                  (neededMem < 0 ? "UNAVAILABLE" : String.format("%dM", neededMem)) + "\t" + 
                  job.getSchedulingInfo());
        }
    }
    
    
    /**
     * Generate the task report summary given a task report.
     * @param JobID
     * @param TaskType 
     * @param TaskReportSummary
     * 
     * @throws InterruptedException 
     * @throws IOException 
     */
    public HashMap<TIPStatus, Integer> getTaskReportSummaryByTaskType(
            JobID jobID, TaskType taskType, TaskReportSummary taskReportSummary) 
            throws InterruptedException, IOException {
        TaskReport[] taskTypeReports;
        HashMap<TIPStatus, Integer> statusCounter;
        if (taskType.equals(TaskType.MAP)) {
            taskTypeReports = this.getMapTaskReports(jobID);
            statusCounter = taskReportSummary.getMapStatusCounter();
        } else {
            taskTypeReports = this.getReduceTaskReports(jobID);            
            statusCounter = taskReportSummary.getReduceStatusCounter();
        }        
        TestSession.logger.trace("Num " + taskType + " task reports=" +
                taskTypeReports.length);
        for ( TaskReport taskTypereport : taskTypeReports) {
            String taskId = taskTypereport.getTaskID().toString();
            TestSession.logger.trace("task id=" + taskId);
            TIPStatus taskStatus = taskTypereport.getCurrentStatus();
            statusCounter.put(taskStatus,
                    statusCounter.get(taskStatus)+1);
            TestSession.logger.trace("increment count for " + 
                    taskStatus.toString());
            TestSession.logger.info("'" + jobID.toString() + 
                    "'->'" + taskId + "'='" +
                    taskTypereport.getCurrentStatus().toString() + "'");
            if (!taskStatus.equals(TIPStatus.COMPLETE)) {
                TestSession.logger.warn("Found non-complete status '" +
                        taskTypereport.getCurrentStatus().toString() +
                        "' for task id '" + taskId + "'");                      
            }
        }
        return statusCounter;
    }
        
    /**
     * Get the full task report for a particular job ID, for both map and reduce
     * tasks.
     * 
     * @param JobID
     * @param TaskReportSummary
     * 
     * @throws InterruptedException 
     * @throws IOException 
     */
    public TaskReportSummary getFullTaskReportSummaryByJobID(
            JobID jobID, TaskReportSummary taskReportSummary) 
                    throws InterruptedException, IOException {        
        TestSession.logger.trace("job id='" + jobID.toString() + "'");
        for (TaskType taskType : TaskType.values()) {
            HashMap<TIPStatus, Integer> typeStatusCounter = 
                    this.getTaskReportSummaryByTaskType(
                            jobID, taskType, taskReportSummary);
            if (taskType.equals(TaskType.MAP)) {
                taskReportSummary.setMapStatusCounter(typeStatusCounter);
            } else {
                taskReportSummary.setReduceStatusCounter(typeStatusCounter);
            }
        }
        return taskReportSummary;
    }
    
    /**
     * Get the full task report summary for the given job status array, for both
     * map and reduce tasks.
     * 
     * @param Array of JobStatus
     * 
     * @throws InterruptedException 
     * @throws IOException 
     */
    public TaskReportSummary getFullTaskReportSummary(JobStatus[] jobsStatus) 
            throws InterruptedException, IOException {
        this.displayJobList(jobsStatus);
        TestSession.logger.info("--------------------------------------------");
        TestSession.logger.info("Aggregate task report summary for jobs:");
        TestSession.logger.info("--------------------------------------------");
        TaskReportSummary taskReportSummary = new TaskReportSummary();
        for (JobStatus js : jobsStatus) {
            taskReportSummary = getFullTaskReportSummaryByJobID(
                    js.getJobID(), taskReportSummary);
        }
        return taskReportSummary;
    }
    
    /**
     * Get the task report summary for all the jobs.
     * 
     * @throws InterruptedException 
     * @throws IOException 
     */
    public TaskReportSummary getFullTaskReportSummary()
        throws InterruptedException, IOException {
        return this.getFullTaskReportSummary(this.getAllJobs());        
    }
    
    /**
     * Get the task report summary for all the jobs since a given start time.
     * 
     * @param long startTime
     * 
     * @throws InterruptedException 
     * @throws IOException 
     */
    public TaskReportSummary getFullTaskReportSummary(long startTime)
        throws InterruptedException, IOException {
        return this.getFullTaskReportSummary(this.getJobs(startTime));
    }

    /**
     * Display the full task report summary for a given set of JobStatus
     * 
     * @param JobStatus[]
     * 
     * @throws InterruptedException 
     * @throws IOException 
     */
    public void displayFullTaskReportSummary(JobStatus[] jobsStatus)
            throws InterruptedException, IOException {
        TaskReportSummary taskReportSummary =
                this.getFullTaskReportSummary(jobsStatus);
        taskReportSummary.displaySummary();
    }
    
    /**
     * Display the full task report summary for all jobs.
     * 
     * @throws InterruptedException 
     * @throws IOException 
     */
    public void displayFullTaskReportSummary()
            throws InterruptedException, IOException {
        displayFullTaskReportSummary(this.getAllJobs());
    }
    
    /**
     * Display the full task report summary for all jobs after a given 
     * start time
     * 
     * @param long startTime
     * 
     * @throws InterruptedException 
     * @throws IOException 
     */
    public void displayFullTaskReportSummary(long startTime)
            throws InterruptedException, IOException {
        displayFullTaskReportSummary(this.getJobs(startTime));
    }
    
    /**
     * Log task report summary to external log file
     * 
     * @param JobStatus
     * 
     * @throws InterruptedException 
     * @throws IOException 
     */
    public TaskReportSummary logTaskReportSummary(
            String fileName, JobStatus[] jobsStatus) 
                    throws InterruptedException, IOException {
        TestSession.addLoggerFileAppender(fileName);    
        TaskReportSummary taskReportSummary =
                this.getFullTaskReportSummary(jobsStatus);
        taskReportSummary.displaySummary();
        TestSession.removeLoggerFileAppender(fileName);
        return taskReportSummary;
    }

    /**
     * Log task report summary to external log file
     * 
     * @param long startTime
     * @param String fileName
     * 
     * @throws InterruptedException 
     * @throws IOException 
     */
    public TaskReportSummary logTaskReportSummary(String fileName) 
                    throws InterruptedException, IOException {
        return this.logTaskReportSummary(fileName, this.getAllJobs());
    }

    /**
     * Log task report summary to external log file
     * 
     * @param long startTime
     * @param String fileName
     * 
     * @throws InterruptedException 
     * @throws IOException 
     */
    public TaskReportSummary logTaskReportSummary(
            String fileName, long startTime) 
                    throws InterruptedException, IOException {
        return this.logTaskReportSummary(fileName, this.getJobs(startTime));
    }

    /**
     * Validate task report summary
     * 
     * @param TaskReportSummary
     * 
     * @throws InterruptedException 
     * @throws IOException 
     */
    public void validateTaskReportSummary(TaskReportSummary taskReportSummary) 
            throws InterruptedException, IOException {
        validateTaskReportSummary(taskReportSummary, 0, 0);
    }
    
    /**
     * Validate Task Report Summary
     */
    public void validateTaskReportSummary(TaskReportSummary taskReportSummary,
            int acceptableMapFailure, int acceptableRedFailure) 
            throws InterruptedException, IOException {
        // Check that there are no non-complete tasks
        int numNonCompleteMapTasks =
                taskReportSummary.getNonCompleteMapTasks();
        int numNonCompleteReduceTasks =
                taskReportSummary.getNonCompleteReduceTasks();
        String mapMsg = "There are " +
                (numNonCompleteMapTasks - acceptableMapFailure) + 
                " more map task failures than the acceptable failure of " +
                acceptableMapFailure;
        String redMsg = "There are " + 
                (numNonCompleteReduceTasks - acceptableRedFailure) + 
                " more reduce task failures than the acceptable failure of " + 
                acceptableRedFailure;
        assertTrue(mapMsg, numNonCompleteMapTasks <= acceptableMapFailure);
        assertTrue(redMsg, numNonCompleteReduceTasks <= acceptableRedFailure);           
    }
}

