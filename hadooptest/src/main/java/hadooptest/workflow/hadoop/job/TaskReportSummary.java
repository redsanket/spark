/*
 * YAHOO!
 */

package hadooptest.workflow.hadoop.job;

import hadooptest.TestSession;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.mapred.TIPStatus;

/**
 * Container class for task report summary for a set of jobs.
 */
public class TaskReportSummary {
    private HashMap<TIPStatus, Integer> mapStatusCounter =
            new HashMap<TIPStatus, Integer>();
    private HashMap<TIPStatus, Integer> reduceStatusCounter =
            new HashMap<TIPStatus, Integer>();

    public TaskReportSummary() {
        for (TIPStatus status: TIPStatus.values()) {
            mapStatusCounter.put(status, 0);
            reduceStatusCounter.put(status, 0);
        }
    }
        
    /**
     * Get the total number of tasks by task type
     * 
     * @param HashMap<TIPStatus, Integer> statusCounter
     */
    public int getTotalTasks(HashMap<TIPStatus, Integer> statusCounter) {
        int totalTasks = 0;
        for (Integer count : statusCounter.values()) {
            totalTasks = totalTasks + count;
        }
        return totalTasks;
    }

    /**
     * Get the total number of map tasks
     */
    public int getTotalMapTasks() {
        return getTotalTasks(mapStatusCounter);
    }
    
    /**
     * Get the total number of reduce tasks
     */
    public int getTotalReduceTasks() {
        return getTotalTasks(reduceStatusCounter);
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
        return (this.getTotalMapTasks() - this.getMapTasks(TIPStatus.COMPLETE));
    }    
    
    /**
     * Get the total number of non-complete reduce tasks
     */
    public int getNonCompleteReduceTasks() {
        return (this.getTotalReduceTasks() - 
                this.getReduceTasks(TIPStatus.COMPLETE));
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
     * Print the task report summary for a given task type counter
     */
    public String getSummaryString(HashMap<TIPStatus, Integer> taskCounter) {
        StringBuffer sb = new StringBuffer();
        Iterator<Entry<TIPStatus, Integer>> iter = 
                taskCounter.entrySet().iterator();
        while (iter.hasNext()) {
            Entry<TIPStatus, Integer> entry = iter.next();
            sb.append(entry.getKey());
            sb.append('=').append('"');
            sb.append(entry.getValue());
            sb.append('"');
            if (iter.hasNext()) {
                sb.append(',').append(' ');
            }
        }
        return sb.toString();
    }

    /**
     * Print the full task report summary for all summary types.
     */
    public void displaySummary() {
        TestSession.logger.info("--------------------------------------------");
        TestSession.logger.info("Display task report summary for jobs:");
        TestSession.logger.info("--------------------------------------------");
        HashMap<TIPStatus, Integer> taskTypeCounter;
        for (String taskType : new String[] {"map", "reduce"}) {
            taskTypeCounter = (taskType.equals("map")) ? 
                    this.getMapStatusCounter() : this.getReduceStatusCounter();
            TestSession.logger.info("Total " + taskType + "tasks=" + 
                    this.getTotalTasks(taskTypeCounter) +
                    ": (" + this.getSummaryString(taskTypeCounter) + ")");
        }
    }
}

