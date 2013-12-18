/*
 * YAHOO!
 */

package hadooptest.workflow.hadoop.job;

import hadooptest.TestSession;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.mapred.TIPStatus;

/**
 * Container class for task report summary for a set of jobs.
 */
public class TaskReportSummary {
    
    // Track map and reduce tasks total, and sub total for each task type
    private int totalMapTasks = 0;
    private int totalReduceTasks = 0;        
    private HashMap<TIPStatus, Integer> mapStatusCounter =
            new HashMap<TIPStatus, Integer>();
    private HashMap<TIPStatus, Integer> reduceStatusCounter =
            new HashMap<TIPStatus, Integer>();

    public TaskReportSummary() {
    }
        
    /**
     * Get the total number of map tasks
     */
    public int getMapTasks() {
        return totalMapTasks;
    }
    
    /**
     * Get the total number of reduce tasks
     */
    public int getReduceTasks() {
        return totalReduceTasks;
    }
        
    /**
     * Get the total number of map tasks for a particular task status
     */
    public int getMapTasks(TIPStatus status) {
        return mapStatusCounter.get(status);
    }
    
    /**
     * Get the total number of reduce tasks for a particular task status
     */
    public int getReduceTasks(TIPStatus status) {
        return reduceStatusCounter.get(status);
    }

    /**
     * Get the total number of non-complete map tasks
     */
    public int getNonCompleteMapTasks() {
        return (this.getMapTasks() - 
                this.getMapTasks(TIPStatus.COMPLETE));
    }    
    
    /**
     * Get the total number of non-complete reduce tasks
     */
    public int getNonCompleteReduceTasks() {
        return (this.getReduceTasks() - 
                this.getReduceTasks(TIPStatus.COMPLETE));
    }    
    
    /**
     * Increment the number of map tasks
     */
    public void incrementMapTasks(int numTasks) {
        totalMapTasks = totalMapTasks + numTasks;
    }

    /**
     * Increment the number of reduce tasks
     */
    public void incrementReduceTasks(int numTasks) {
        totalReduceTasks = totalReduceTasks + numTasks;
    }
        
    /**
     * Set the map task status counter
     */
    public HashMap<TIPStatus, Integer> getMapStatusCounter() {
        return mapStatusCounter;
    }

    /**
     * Get the reduce task status counter
     */
    public HashMap<TIPStatus, Integer> getReduceStatusCounter() {
        return reduceStatusCounter;
    }

    /**
     * Set the map task status counter
     */
    public void setMapStatusCounter(HashMap<TIPStatus, Integer> counter) {
        this.mapStatusCounter = counter;
    }

    /**
     * Set the reduce task status counter
     */
    public void setReduceStatusCounter(HashMap<TIPStatus, Integer> counter) {
        this.reduceStatusCounter = counter;
    }
    
    /**
     * Print the task report summary.
     */
    public void printSummary() {
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

