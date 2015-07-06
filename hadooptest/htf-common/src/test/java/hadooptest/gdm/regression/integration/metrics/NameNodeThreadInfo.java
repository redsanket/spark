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

	private void setNameNodeName(String nameNodeName) {
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


		String url = "http://" + NAME_NODE_NAME + ":" + MetricFields.NAME_NAME_METRIC_PORT + "/" + MetricFields.JMX_METRIC;
		System.out.println("url = " + url);
		com.jayway.restassured.response.Response response = given().cookie(this.cookie).get(url);
		String responseString = response.getBody().asString();

		JSONObject obj =  (JSONObject) JSONSerializer.toJSON(responseString.toString());
		//TestSession.logger.info("obj = " + obj.toString());
		JSONArray jsonArray = obj.getJSONArray("beans");
		System.out.println("size = " + jsonArray.size());

		for ( int i=0; i<jsonArray.size() - 1 ; i++) {
			JSONObject theadJsonObject = jsonArray.getJSONObject(i);
			System.out.println(i + "   theadJsonObject = " + theadJsonObject.getString("name"));
			if (theadJsonObject.getString("name").equals("Hadoop:service=NameNode,name=JvmMetrics")) {
				System.out.println("Name = " + theadJsonObject.getString("name"));
				String nameNodeName = theadJsonObject.getString("tag.Hostname");
				String newThread = theadJsonObject.getString("ThreadsNew");
				String runnableThread = theadJsonObject.getString("ThreadsRunnable");
				String blockedThread = theadJsonObject.getString("ThreadsBlocked");
				String waitingThread = theadJsonObject.getString("ThreadsWaiting");
				String timedWaitingThread = theadJsonObject.getString("ThreadsTimedWaiting");
				String terminatedThread = theadJsonObject.getString("ThreadsTerminated");
				System.out.println("nameNodeName -  " + nameNodeName );
				System.out.println("ThreadsNew - " + newThread);
				System.out.println("ThreadsRunnable - " + runnableThread);
				System.out.println("ThreadsBlocked - " + blockedThread);
				System.out.println("ThreadsWaiting - " + waitingThread);
				System.out.println("ThreadsTimedWaiting - " + timedWaitingThread);
				System.out.println("ThreadsTerminated - " + terminatedThread);

				this.setNameNodeName(nameNodeName);
				this.setHadoopVersion("2.6.0.12.1504130654");
				this.setCurrentTimeStamp(currentiMin);
				this.setNewThread(newThread);
				this.setRunnableThread(runnableThread);
				this.setBlockedThread(blockedThread);
				this.setWaitingThread(timedWaitingThread);
				this.setTimedWaitingThread(timedWaitingThread);
				this.setTerminatedThread(terminatedThread);
			}
		}
	}

	public JSONObject constructNameNodeThreadInfoJsonObject(/*String nameNodeName , String newThread, String runnableThread, String blockedThread , String waitingThread, String timedWaitingThread, String terminatedThread*/) {
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
