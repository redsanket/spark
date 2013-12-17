/*
 * YAHOO!
 */

package hadooptest.workflow.hadoop.job;

import hadooptest.TestSession;
import hadooptest.config.hadoop.HadoopConfiguration;

import java.io.IOException;
import java.util.HashMap;

// import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.TIPStatus;
import org.apache.hadoop.mapred.TaskReport;

/**
 * A class which should represent the base capability of any job
 * submitted to a cluster.
 */
public class TaskReportSummary {
    
    // Track map and reduce tasks total, and sub total for each task type
    static int totalMapTasks = 0;
    static int totalReduceTasks = 0;        
    static HashMap<TIPStatus, Integer> mapStatusCounter =
            new HashMap<TIPStatus, Integer>();
    static HashMap<TIPStatus, Integer> reduceStatusCounter =
            new HashMap<TIPStatus, Integer>();

    public TaskReportSummary() {
        this.totalMapTasks = 0;
        this.totalReduceTasks = 0;            
    }
        
    public int getMapTasks() {
        return totalMapTasks;
    }
    
    public int getReduceTasks() {
        return totalReduceTasks;
    }
        
    public int getMapTasks(TIPStatus status) {
        return mapStatusCounter.get(status);
    }
    
    public int getReduceTasks(TIPStatus status) {
        return reduceStatusCounter.get(status);
    }

    public int getNonCompleteMapTasks() {
        return (this.getMapTasks() - 
                this.getMapTasks(TIPStatus.COMPLETE));
    }    
    
    public int getNonCompleteReduceTasks() {
        return (this.getReduceTasks() - 
                this.getReduceTasks(TIPStatus.COMPLETE));
    }    
    
    public void incrementMapTasks(int numTasks) {
        totalMapTasks = totalMapTasks + numTasks;
    }

    public void incrementReduceTasks(int numTasks) {
        totalReduceTasks = totalReduceTasks + numTasks;
    }
        
    public HashMap<TIPStatus, Integer> getMapStatusCounter() {
        return mapStatusCounter;
    }

    public HashMap<TIPStatus, Integer> getReduceStatusCounter() {
        return reduceStatusCounter;
    }

    public void setMapStatusCounter(HashMap<TIPStatus, Integer> counter) {
        this.mapStatusCounter = counter;
    }

    public void setReduceStatusCounter(HashMap<TIPStatus, Integer> counter) {
        this.reduceStatusCounter = counter;
    }
    
    public void printSummary() {
        // TestSession.logger.info("---> Tasks Summary: ");
        // TestSession.logger.info("********************************************");
        /*
        int numJobs = jobsStatus.length;
        TestSession.logger.info("Total Number of jobs = " + numJobs);
        */
        
        TestSession.logger.info("Total Number of map tasks = " + this.getMapTasks());
        TestSession.logger.info("Total Number of reduce tasks = " + this.getReduceTasks());

        for(TIPStatus taskStatus : this.getMapStatusCounter().keySet()){
            TestSession.logger.info("Number of map tasks with status " + taskStatus.toString() +
                    " = "+ this.getMapStatusCounter().get(taskStatus));
        }
        
        for(TIPStatus taskStatus : this.getReduceStatusCounter().keySet()){
            TestSession.logger.info("Number of reduce tasks with status " + taskStatus.toString() +
                    " = "+ this.getReduceStatusCounter().get(taskStatus));
        }
    }
}

