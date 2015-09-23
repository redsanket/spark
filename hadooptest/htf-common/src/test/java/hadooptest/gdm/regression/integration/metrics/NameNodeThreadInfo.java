package hadooptest.gdm.regression.integration.metrics;

import static com.jayway.restassured.RestAssured.given;

import java.text.SimpleDateFormat;
import java.util.Calendar;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;
import hadooptest.TestSession;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.HTTPHandle;

public class NameNodeThreadInfo {
	private String cookie;
	private ConsoleHandle consoleHandle;
	private String nameNodeName ;
	private String currentState;
	private String hadoopVersion;
	private String currentTimeStamp;
	private String newThread;
	private String runnableThread;
	private String blockedThread;
	private String waitingThread;
	private String timedWaitingThread;
	private String terminatedThread;
	private static final String NAME_NODE_NAME = "gsbl90101.blue.ygrid.yahoo.com";

	public NameNodeThreadInfo() {
		this.consoleHandle = new ConsoleHandle();
		HTTPHandle httpHandle = new HTTPHandle();
		this.cookie = httpHandle.getBouncerCookie();
	}
	
	public void setNameNodeCurrentState(String currentState) {
		this.currentState = currentState;
	}
	
	public String getNameNodeCurrentState() {
		return this.currentState;
	}

	public void setNameNodeName(String nameNodeName) {
		this.nameNodeName = nameNodeName;
	}
	public String getNameNodeName() {
		return this.nameNodeName;
	}

	private void setHadoopVersion(String hadoopVersion) {
		this.hadoopVersion = hadoopVersion;
	}

	public String getHadoopVersion() {
		return this.hadoopVersion;
	}

	private void setCurrentTimeStamp(String currentTimeStamp) {
		this.currentTimeStamp = currentTimeStamp;
	}
	public String getCurrentTimeStamp(){
		return this.currentTimeStamp;
	}

	private void setNewThread(String newThread) {
		this.newThread = newThread;
	}

	public String getNewThread() {
		return this.newThread;
	}

	private void setRunnableThread(String runnableThread) {
		this.runnableThread = runnableThread;
	}

	public String getRunnableThread() {
		return this.runnableThread;
	}

	private void setBlockedThread(String blockedThread){
		this.blockedThread = blockedThread;
	}

	public String getBlockedThread() {
		return this.blockedThread;
	}

	private void setWaitingThread(String waitingThread) {
		this.waitingThread = waitingThread;
	}

	public String getWaitingThread() {
		return this.waitingThread;
	}

	private void setTimedWaitingThread(String timedWaitingThread) {
		this.timedWaitingThread = timedWaitingThread;
	}

	public String getTimedWaitingThread() {
		return this.timedWaitingThread;
	}

	private void setTerminatedThread(String terminatedThread) {
		this.terminatedThread = terminatedThread;
	}

	public String getTerminatedThread() {
		return this.terminatedThread;
	}

	public void getNameNodeThreadInfo() {
		Calendar initialCal = Calendar.getInstance();
		SimpleDateFormat feed_sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		String currentiMin = feed_sdf.format(initialCal.getTime());
		String url = null;
		String nameNode = this.getNameNodeName().trim();
		if ( nameNode == null) {
			url = "http://" + NAME_NODE_NAME + ":" + MetricFields.NAME_NAME_METRIC_PORT + "/" + MetricFields.JMX_METRIC;
		} else {
			url = "http://" + nameNode + ":" + MetricFields.NAME_NAME_METRIC_PORT + "/" + MetricFields.JMX_METRIC;
		}
		System.out.println("url = " + url);
		com.jayway.restassured.response.Response response = given().cookie(this.cookie).get(url);
		String responseString = response.getBody().asString();

		JSONObject obj =  (JSONObject) JSONSerializer.toJSON(responseString.toString());
		JSONArray jsonArray = obj.getJSONArray("beans");

		for ( int i=0; i<jsonArray.size() - 1 ; i++) {
			JSONObject threadJsonObject = jsonArray.getJSONObject(i);
			if (threadJsonObject.getString("name").equals("Hadoop:service=NameNode,name=JvmMetrics")) {
				String nameNodeName = threadJsonObject.getString("tag.Hostname");
				String newThread = threadJsonObject.getString("ThreadsNew");
				String runnableThread = threadJsonObject.getString("ThreadsRunnable");
				String blockedThread = threadJsonObject.getString("ThreadsBlocked");
				String waitingThread = threadJsonObject.getString("ThreadsWaiting");
				String timedWaitingThread = threadJsonObject.getString("ThreadsTimedWaiting");
				String terminatedThread = threadJsonObject.getString("ThreadsTerminated");

				this.setNameNodeName(nameNodeName);
				this.setCurrentTimeStamp(currentiMin);
				this.setNewThread(newThread);
				this.setRunnableThread(runnableThread);
				this.setBlockedThread(blockedThread);
				this.setWaitingThread(timedWaitingThread);
				this.setTimedWaitingThread(timedWaitingThread);
				this.setTerminatedThread(terminatedThread);
			}
			if (threadJsonObject.getString("name").equals("Hadoop:service=NameNode,name=NameNodeStatus")) {
				String cState = threadJsonObject.getString("State").trim();
				this.setNameNodeCurrentState(cState);
			} 
			if (threadJsonObject.getString("name").equals("Hadoop:service=NameNode,name=NameNodeInfo")) {
				String version = threadJsonObject.getString("Version").trim();
				this.setHadoopVersion(version);
			}
		}
	}

	public JSONObject constructNameNodeThreadInfoJsonObject() {
		JSONObject nameNodeThreadInfoJsonObject = new JSONObject();
		nameNodeThreadInfoJsonObject.put("NameNodeName", nameNodeName);
		nameNodeThreadInfoJsonObject.put("NewThreads", newThread);
		nameNodeThreadInfoJsonObject.put("RunnableThreads", runnableThread);
		nameNodeThreadInfoJsonObject.put("BlockedThreads", blockedThread);
		nameNodeThreadInfoJsonObject.put("WaitingThreads", waitingThread);
		nameNodeThreadInfoJsonObject.put("TimedWaitingThreads", timedWaitingThread);
		nameNodeThreadInfoJsonObject.put("TerminatedThreads", terminatedThread);
		return  nameNodeThreadInfoJsonObject;
	}
}
