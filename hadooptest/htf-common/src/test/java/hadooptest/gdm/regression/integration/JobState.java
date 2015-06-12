package hadooptest.gdm.regression.integration;

public interface JobState {
	String FAILED = "FAIL";
	String SUCCESS = "SUCCESS";
	String RUNNING = "RUNNING";
	String UNKNOWN = "UNKNOWN";
	String STARTED = "STARTED";
}
