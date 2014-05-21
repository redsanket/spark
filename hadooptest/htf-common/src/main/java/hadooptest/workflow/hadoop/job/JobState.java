/*
 * YAHOO!
 */

package hadooptest.workflow.hadoop.job;

/**
 * A class which represents an enumerated type of the state of a job.
 */
public enum JobState {
	
    RUNNING(1),
    SUCCEEDED(2),
    FAILED(3),
    PREP(4),
    KILLED(5),
    UNKNOWN(6);
    
    int value;
    
    JobState(int value) {
      this.value = value;
    }
    
    public int getValue() {
      return value; 
    }
    
    public static JobState getState(int state) {
    	switch (state) {
    	case 1: return RUNNING;
    	case 2: return SUCCEEDED;
    	case 3: return FAILED;
    	case 4: return PREP;
    	case 5: return KILLED;
    	case 6: return UNKNOWN;
    	default: return UNKNOWN;
    	}
    }
	
}
