package hadooptest.tez.ats;

/**
 * When a REST call is made, for example
 * http:<hostname>:8188/ws/v1/timeline/TEZ_TASK_ATTEMPT_ID then the response
 * contains two portions. One is common across different response and the other
 * portion is specific to the type of REST call made. Since that can vary across
 * different responses, this class is made abstract. This is overriden, and the
 * correct class (object) for the implementation is provided in {@code ATSUtils}
 * methods that process the specific OtherInfo.
 * 
 * 
 * @author tiwari
 * 
 */
public abstract class ATSOtherInfoEntityBO {
	abstract public void dump();
}
