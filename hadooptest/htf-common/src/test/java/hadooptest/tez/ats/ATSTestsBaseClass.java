package hadooptest.tez.ats;

import static com.jayway.restassured.RestAssured.given;
import static com.jayway.restassured.config.HttpClientConfig.httpClientConfig;
import static com.jayway.restassured.config.RestAssuredConfig.newConfig;
import static org.apache.http.client.params.ClientPNames.COOKIE_POLICY;
import static org.apache.http.client.params.CookiePolicy.BROWSER_COMPATIBILITY;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.automation.utils.http.HTTPHandle;
import hadooptest.cluster.hadoop.HadoopComponent;
import hadooptest.node.hadoop.HadoopNode;
import hadooptest.tez.ats.ATSTestsBaseClass.ResponseComposition;
import hadooptest.tez.ats.ATSTestsBaseClass.ResponseComposition.EVENTS;
import hadooptest.tez.examples.cluster.TestHtfOrderedWordCount;
import hadooptest.tez.examples.cluster.TestSimpleSessionExample;
import hadooptest.tez.examples.extensions.OrderedWordCountExtendedForHtf;
import hadooptest.tez.examples.extensions.SimpleSessionExampleExtendedForTezHTF;
import hadooptest.tez.mapreduce.examples.extensions.MRRSleepJobExtendedForTezHTF;
import hadooptest.tez.utils.HtfTezUtils.TimelineServer;
import hadooptest.tez.utils.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.httpclient.cookie.CookiePolicy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.http.client.params.ClientPNames;
import org.json.simple.parser.ParseException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.jayway.restassured.response.Header;
import com.jayway.restassured.response.Response;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.UserInfo;

public class ATSTestsBaseClass extends TestSession {
	public static boolean timelineserverStarted = false;
	public static HashMap<String, String> userCookies = new HashMap<String, String>();
	Queue<GenericATSResponseBO> dagIdQueue = new ConcurrentLinkedQueue<GenericATSResponseBO>();
	Queue<GenericATSResponseBO> containerIdQueue = new ConcurrentLinkedQueue<GenericATSResponseBO>();
	Queue<GenericATSResponseBO> applicationAttemptQueue = new ConcurrentLinkedQueue<GenericATSResponseBO>();
	Queue<GenericATSResponseBO> vertexIdQueue = new ConcurrentLinkedQueue<GenericATSResponseBO>();
	Queue<GenericATSResponseBO> taskIdQueue = new ConcurrentLinkedQueue<GenericATSResponseBO>();
	Queue<GenericATSResponseBO> taskAttemptIdQueue = new ConcurrentLinkedQueue<GenericATSResponseBO>();
	AtomicInteger errorCount = new AtomicInteger();

	public static Boolean jobsLaunchedOnceToSeedData = false;
	
	public static SeedData sleepJobSleepData = null;
	public static SeedData simpleSessionExampleSeedData=null;
	public static SeedData orderedWordCountSeedData=null;
	
	public enum EntityTypes {
		TEZ_APPLICATION_ATTEMPT, TEZ_CONTAINER_ID, TEZ_DAG_ID, TEZ_VERTEX_ID, TEZ_TASK_ID, TEZ_TASK_ATTEMPT_ID,
	};

	public static class ResponseComposition {
		public enum EVENTS {
			EXPECTED, NOT_EXPECTED
		}

		public enum ENTITYTYPE {
			EXPECTED, NOT_EXPECTED
		}

		public enum ENTITY {
			EXPECTED, NOT_EXPECTED
		}

		public enum STARTTIME {
			EXPECTED, NOT_EXPECTED
		}

		public enum RELATEDENTITIES {
			EXPECTED, NOT_EXPECTED
		}

		public enum PRIMARYFILTERS {
			EXPECTED, NOT_EXPECTED
		}

		public enum OTHERINFO {
			EXPECTED, NOT_EXPECTED
		}

	}

	@Before
	public void cleanupAndPrepareForTestRun() throws Exception {
		// Fetch cookies
		HTTPHandle httpHandle = new HTTPHandle();
		String hitusr_1_cookie = null;
		String hitusr_2_cookie = null;
		String hitusr_3_cookie = null;
		String hitusr_4_cookie = null;
		

		hitusr_1_cookie = httpHandle
				.loginAndReturnCookie(HadooptestConstants.UserNames.HITUSR_1);
		TestSession.logger.info("Got cookie:" + hitusr_1_cookie );
		userCookies
				.put(HadooptestConstants.UserNames.HITUSR_1, hitusr_1_cookie);
		hitusr_2_cookie = httpHandle
				.loginAndReturnCookie(HadooptestConstants.UserNames.HITUSR_2);
		userCookies
				.put(HadooptestConstants.UserNames.HITUSR_2, hitusr_2_cookie);
		hitusr_3_cookie = httpHandle
				.loginAndReturnCookie(HadooptestConstants.UserNames.HITUSR_3);
		userCookies
				.put(HadooptestConstants.UserNames.HITUSR_3, hitusr_3_cookie);
		hitusr_4_cookie = httpHandle
				.loginAndReturnCookie(HadooptestConstants.UserNames.HITUSR_4);
		userCookies
				.put(HadooptestConstants.UserNames.HITUSR_4, hitusr_4_cookie);
		

		// Reset the error count
		errorCount.set(0);

		drainQueues();
		launchJobsOnceToSeedData();
	}

	public void launchJobsOnceToSeedData() throws Exception {
		if (!jobsLaunchedOnceToSeedData) {
			launchOrderedWordCountExtendedForHtf(HadooptestConstants.UserNames.HITUSR_1);
//			String[] sleepJobArgs = new String[] { "-m 5", "-r 4", "-ir 4",
//					"-irs 4", "-mt 500", "-rt 200", "-irt 100", "-recordt 100" };
//			launchMRRSleepJob(HadooptestConstants.UserNames.HITUSR_2,
//					sleepJobArgs);
//			launchSimpleSessionExampleExtendedForTezHTF(HadooptestConstants.UserNames.HITUSR_3);
			jobsLaunchedOnceToSeedData = true;
		}
	}

	public void launchOrderedWordCountExtendedForHtf(String user)
			throws IOException, InterruptedException {
		UserGroupInformation ugi = getUgiForUser(user);
		DoAs doAs = new DoAs(ugi, new OrderedWordCountExtendedForHtf(),
				new String[0]);
		doAs.doAction();
		 orderedWordCountSeedData = doAs.getSeedDataForAppThatJustRan();
		 GenericATSResponseBO processedDagIdResponses = getDagIdResponses(orderedWordCountSeedData);
		 populateAdditionalSeedData(processedDagIdResponses, orderedWordCountSeedData);
		 orderedWordCountSeedData.dump();
	}

	public void launchSimpleSessionExampleExtendedForTezHTF(String user)
			throws IOException, InterruptedException {
		UserGroupInformation ugi = getUgiForUser(user);
		DoAs doAs = new DoAs(ugi, new SimpleSessionExampleExtendedForTezHTF(),
				new String[0]);
		doAs.doAction();
		simpleSessionExampleSeedData = doAs.getSeedDataForAppThatJustRan();
	}

	public void launchMRRSleepJob(String user, String[] sleepJobArgs)
			throws Exception {
		UserGroupInformation ugi = getUgiForUser(user);
		DoAs doAs = new DoAs(ugi, new MRRSleepJobExtendedForTezHTF(),
				sleepJobArgs);
		doAs.doAction();
		 sleepJobSleepData = doAs.getSeedDataForAppThatJustRan();
	}

	void populateAdditionalSeedData(GenericATSResponseBO dagIdResponse, SeedData seedData) throws InterruptedException{
		String appId = seedData.appId.replace("application_", "");
		for (EntityInGenericATSResponseBO anEntityInResponse:dagIdResponse.entities){
			if(anEntityInResponse.entity.contains(appId)){
				String rxDagName = anEntityInResponse.primaryfilters.get("dagName").get(0);
				TestSession.logger.info("Got rxDagName:" + rxDagName);
				SeedData.DAG aSeedDag = seedData.getDagObject(rxDagName); 
				 aSeedDag.id = anEntityInResponse.entity; 
				
				//Get the Vertex details
				for (String aVertexId:anEntityInResponse.relatedentities.get("TEZ_VERTEX_ID")){
					populateVertexDetails(seedData, aSeedDag, aVertexId);
				}
			}
		}
	}
	public GenericATSResponseBO getDagIdResponses(SeedData seedData) throws InterruptedException{
		ExecutorService execService = Executors.newFixedThreadPool(10);
		//TODO: Change the user below to seedData.appStartedByUser
		makeHttpCallAndEnqueueConsumedResponse(execService,
//				getATSUrl()+ "TEZ_DAG_ID/" , seedData.appStartedByUser,
				getATSUrl()+ "TEZ_DAG_ID/" , HadooptestConstants.UserNames.HITUSR_1,
				EntityTypes.TEZ_DAG_ID, dagIdQueue, expectEverythingMap());
		execService.shutdown();
		while (!execService.isTerminated()) {
			Thread.sleep(1000);
		}
		GenericATSResponseBO dagIdResponse = dagIdQueue.poll();		
		return dagIdResponse;

	}
	public void populateVertexDetails(SeedData seedData, SeedData.DAG dag, String vertexId) throws InterruptedException{
		ExecutorService execService = Executors.newFixedThreadPool(10);
		makeHttpCallAndEnqueueConsumedResponse(execService,
				//TODO: Change the user below to seedData.appStartedByUser
				getATSUrl()+ "TEZ_VERTEX_ID/" + vertexId , HadooptestConstants.UserNames.HITUSR_1,
				EntityTypes.TEZ_VERTEX_ID, vertexIdQueue, expectEverythingMap());
		execService.shutdown();
		while (!execService.isTerminated()) {
			Thread.sleep(1000);
		}
		GenericATSResponseBO vertexIdResponse = vertexIdQueue.poll();		
		String vertexName = ((OtherInfoTezVertexIdBO)(vertexIdResponse.entities.get(0).otherinfo)).vertexName;
		SeedData.DAG.Vertex aSeedVertex = dag.getVertexObject(vertexName); 
		aSeedVertex.id = vertexId;
		
		for (String aTaskId:vertexIdResponse.entities.get(0).relatedentities.get(EntityTypes.TEZ_TASK_ID.name())){
			SeedData.DAG.Vertex.Task aSeedTask = new SeedData.DAG.Vertex.Task();
			aSeedTask.id = aTaskId;
			aSeedVertex.tasks.add(aSeedTask);
			populateTaskDetails(seedData, aSeedTask, aTaskId);
		}

	}
	public void populateTaskDetails(SeedData seedData, SeedData.DAG.Vertex.Task aSeedTask, String taskId) throws InterruptedException{
		ExecutorService execService = Executors.newFixedThreadPool(10);
		makeHttpCallAndEnqueueConsumedResponse(execService,
				//TODO: Change the user below to seedData.appStartedByUser
				getATSUrl()+ "TEZ_TASK_ID/" + taskId , HadooptestConstants.UserNames.HITUSR_1,
				EntityTypes.TEZ_TASK_ID, taskIdQueue, expectEverythingMap());
		execService.shutdown();
		while (!execService.isTerminated()) {
			Thread.sleep(1000);
		}
		GenericATSResponseBO taskIdResponse = taskIdQueue.poll();
		for (String aTaskAttemptId:taskIdResponse.entities.get(0).relatedentities.get(EntityTypes.TEZ_TASK_ATTEMPT_ID.name())){
			SeedData.DAG.Vertex.Task.Attempt aTaskAttempt = new SeedData.DAG.Vertex.Task.Attempt();
			aTaskAttempt.id = aTaskAttemptId;
			aSeedTask.attempts.add(aTaskAttempt);
		}
		
	}

	public void drainQueues() {
		// "Drain" the queues
		dagIdQueue = new ConcurrentLinkedQueue<GenericATSResponseBO>();
		containerIdQueue = new ConcurrentLinkedQueue<GenericATSResponseBO>();
		applicationAttemptQueue = new ConcurrentLinkedQueue<GenericATSResponseBO>();
		vertexIdQueue = new ConcurrentLinkedQueue<GenericATSResponseBO>();
		taskIdQueue = new ConcurrentLinkedQueue<GenericATSResponseBO>();
		taskAttemptIdQueue = new ConcurrentLinkedQueue<GenericATSResponseBO>();

	}

	public Map<String, Boolean> expectEverythingMap() {
		Map<String, Boolean> expectedFieldsMap = new HashMap<String, Boolean>();

		expectedFieldsMap.put("events", true);
		expectedFieldsMap.put("entitytype", true);
		expectedFieldsMap.put("entity", true);
		expectedFieldsMap.put("starttime", true);
		expectedFieldsMap.put("relatedentities", true);
		expectedFieldsMap.put("primaryfilters", true);
		expectedFieldsMap.put("otherinfo", true);

		return expectedFieldsMap;

	}

	public Map<String, Boolean> getExpectedFieldsMap(
			ResponseComposition.EVENTS events,
			ResponseComposition.ENTITYTYPE entitytype,
			ResponseComposition.ENTITY entity,
			ResponseComposition.STARTTIME starttime,
			ResponseComposition.RELATEDENTITIES relatedentities,
			ResponseComposition.PRIMARYFILTERS primaryfilters,
			ResponseComposition.OTHERINFO otherinfo) {
		Map<String, Boolean> expectedFieldsMap = new HashMap<String, Boolean>();
		if (events == EVENTS.EXPECTED) {
			expectedFieldsMap.put("events", true);
		} else {
			expectedFieldsMap.put("events", false);
		}
		if (entitytype == ResponseComposition.ENTITYTYPE.EXPECTED) {
			expectedFieldsMap.put("entitytype", true);
		} else {
			expectedFieldsMap.put("entitytype", false);
		}
		if (entity == ResponseComposition.ENTITY.EXPECTED) {
			expectedFieldsMap.put("entity", true);
		} else {
			expectedFieldsMap.put("entity", false);
		}
		if (starttime == ResponseComposition.STARTTIME.EXPECTED) {
			expectedFieldsMap.put("starttime", true);
		} else {
			expectedFieldsMap.put("starttime", false);
		}
		if (relatedentities == ResponseComposition.RELATEDENTITIES.EXPECTED) {
			expectedFieldsMap.put("relatedentities", true);
		} else {
			expectedFieldsMap.put("relatedentities", false);
		}
		if (primaryfilters == ResponseComposition.PRIMARYFILTERS.EXPECTED) {
			expectedFieldsMap.put("primaryfilters", true);
		} else {
			expectedFieldsMap.put("primaryfilters", false);
		}
		if (otherinfo == ResponseComposition.OTHERINFO.EXPECTED) {
			expectedFieldsMap.put("otherinfo", true);
		} else {
			expectedFieldsMap.put("otherinfo", false);
		}

		return expectedFieldsMap;

	}

	public void ensureTimelineserverStarted(String resourceManagerHost)
			throws Exception {

		String url = "http://" + resourceManagerHost + ":"
				+ HadooptestConstants.Ports.HTTP_ATS_PORT + "/ws/v1/timeline/";
		int MAX_COUNT = 10;
		int count = 1;

		do {
			Thread.sleep(1000);
			try {
				Response response = given()
						.log()
						.all()
						.cookie(userCookies
								.get(HadooptestConstants.UserNames.HITUSR_1))
						.param("User-Agent", "Mozilla/5.0")
						.param("Accept",
								"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
						.param(ClientPNames.COOKIE_POLICY,
								CookiePolicy.RFC_2965)
						.param("Content-Type", "application/json")
						.config(newConfig().httpClient(
								httpClientConfig().setParam(COOKIE_POLICY,
										BROWSER_COMPATIBILITY))).redirects()
						.follow(false).get(url);

				TestSession.logger.info("Response status Line:"
						+ response.getStatusLine());
				TestSession.logger.info("Response status code:"
						+ response.getStatusCode());

				for (Header aResponseHeader : response.getHeaders()) {
					TestSession.logger.info(aResponseHeader.getName() + " : "
							+ aResponseHeader.getValue());
				}

				TestSession.logger.info(response.body().asString());
				TestSession.logger.info("Response code:"
						+ response.getStatusCode() + " received.");

				if (response.getStatusCode() == 202) {
					break;
				}
			} catch (Exception e) {
				TestSession.logger
						.error("Exception:"
								+ e.getClass()
								+ " Received and the timeline server has not started yet. Loop count ["
								+ count + "/" + MAX_COUNT + "]");
				TestSession.logger.error(e.getCause());

			}
		} while (++count <= MAX_COUNT);

	}

	public void startTimelineServerOnRM(String rmHost) throws Exception {
		String command = "/home/gs/gridre/yroot."
				+ System.getProperty("CLUSTER_NAME")
				+ "/share/hadoop/sbin/yarn-daemon.sh start timelineserver";
		doJavaSSHClientExec(
				HadooptestConstants.UserNames.MAPREDQA,
				rmHost,
				command,
				HadooptestConstants.Location.Identity.HADOOPQA_AS_MAPREDQA_IDENTITY_FILE);
		timelineserverStarted = true;
		ensureTimelineserverStarted(rmHost);
		TestSession.logger.info("Timelineserver started");
	}

	public void stopTimelineServerOnRM(String rmHost) {
		String command = "/home/gs/gridre/yroot."
				+ System.getProperty("CLUSTER_NAME")
				+ "/share/hadoop/sbin/yarn-daemon.sh stop timelineserver";
		doJavaSSHClientExec(
				HadooptestConstants.UserNames.MAPREDQA,
				rmHost,
				command,
				HadooptestConstants.Location.Identity.HADOOPQA_AS_MAPREDQA_IDENTITY_FILE);
		timelineserverStarted = false;
		TestSession.logger.info("Timelineserver stopped");
	}

	String printResponseAndReturnItAsString(Process process)
			throws InterruptedException, IOException {
		StringBuffer sb = new StringBuffer();
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				process.getInputStream()));
		String line;
		line = reader.readLine();
		while (line != null) {
			sb.append(line);
			sb.append("\n");
			TestSession.logger.debug(line);
			line = reader.readLine();
		}

		process.waitFor();
		return sb.toString();
	}

	public String doJavaSSHClientExec(String user, String host, String command,
			String identityFile) {
		JSch jsch = new JSch();
		TestSession.logger.info("SSH Client is about to run command:" + command
				+ " on host:" + host + "as user:" + user
				+ " using identity file:" + identityFile);
		Session session;
		StringBuilder sb = new StringBuilder();
		try {
			session = jsch.getSession(user, host, 22);
			jsch.addIdentity(identityFile);
			UserInfo ui = new MyUserInfo();
			session.setUserInfo(ui);
			session.setConfig("StrictHostKeyChecking", "no");
			session.connect();
			Channel channel = session.openChannel("exec");
			((ChannelExec) channel).setCommand(command);
			channel.setInputStream(null);
			((ChannelExec) channel).setErrStream(System.err);

			InputStream in = channel.getInputStream();

			channel.connect();

			byte[] tmp = new byte[1024];
			while (true) {
				while (in.available() > 0) {
					int i = in.read(tmp, 0, 1024);
					if (i < 0)
						break;
					String outputFragment = new String(tmp, 0, i);
					TestSession.logger.info(outputFragment);
					sb.append(outputFragment);
				}
				if (channel.isClosed()) {
					TestSession.logger.info("exit-status: "
							+ channel.getExitStatus());
					break;
				}
				try {
					Thread.sleep(1000);
				} catch (Exception ee) {
				}
			}
			channel.disconnect();
			session.disconnect();

		} catch (JSchException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return sb.toString();
	}

	public class MyUserInfo implements UserInfo {

		public String getPassphrase() {
			// TODO Auto-generated method stub
			return null;
		}

		public String getPassword() {
			// TODO Auto-generated method stub
			return null;
		}

		public boolean promptPassphrase(String arg0) {
			// TODO Auto-generated method stub
			return false;
		}

		public boolean promptPassword(String arg0) {
			// TODO Auto-generated method stub
			return false;
		}

		public boolean promptYesNo(String arg0) {
			// TODO Auto-generated method stub
			return false;
		}

		public void showMessage(String arg0) {
			// TODO Auto-generated method stub

		}

	}

	String getHostNameFromIp(String ip) throws Exception {

		InetAddress iaddr = InetAddress.getByName(ip);
		System.out.println("And the Host name of the g/w is:"
				+ iaddr.getHostName());
		return iaddr.getHostName();

	}

	public String getATSUrl() {
		String rmHostname = null;
		HadoopComponent hadoopComp = TestSession.cluster.getComponents().get(
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER);

		Hashtable<String, HadoopNode> nodesHash = hadoopComp.getNodes();
		for (String key : nodesHash.keySet()) {
			TestSession.logger.info("Key:" + key);
			TestSession.logger.info("The associated hostname is:"
					+ nodesHash.get(key).getHostname());
			rmHostname = nodesHash.get(key).getHostname();
		}

		String url = "http://" + rmHostname + ":"
				+ HadooptestConstants.Ports.HTTP_ATS_PORT + "/ws/v1/timeline/";
		return url;
	}

	class RunnableHttpGetAndEnqueue implements Runnable {
		String url;
		String user;
		EntityTypes entityType;
		Queue<GenericATSResponseBO> queue;
		Map<String, Boolean> expectedEntities;

		public RunnableHttpGetAndEnqueue(String url, String user,
				EntityTypes entityType, Queue<GenericATSResponseBO> queue,
				Map<String, Boolean> expectedEntities) {
			this.url = url;
			this.user = user;
			this.entityType = entityType;
			this.queue = queue;
			this.expectedEntities = expectedEntities;
		}

		public void run() {
			TestSession.logger
					.info("RRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR");
			TestSession.logger.info("Url:" + url);
			TestSession.logger
					.info("RRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR");
			Response response = given().cookie(userCookies.get(user)).get(url);

			String responseAsString = response.getBody().asString();
			TestSession.logger.info("R E S P O N S E  B O D Y :"
					+ responseAsString);
			HtfATSUtils atsUtils = new HtfATSUtils();
			GenericATSResponseBO consumedResponse = null;
			try {
				consumedResponse = atsUtils.processATSResponse(
						responseAsString, entityType, expectedEntities);
			} catch (ParseException e) {
				TestSession.logger.error(e);
				errorCount.incrementAndGet();
			} catch (Exception e) {
				errorCount.incrementAndGet();
			}
			queue.add(consumedResponse);

		}
	}

	void makeHttpCallAndEnqueueConsumedResponse(ExecutorService execService,
			String url, String user, EntityTypes entityType,
			Queue<GenericATSResponseBO> queue,
			Map<String, Boolean> expectedEntities) throws InterruptedException {
		RunnableHttpGetAndEnqueue runnableHttpGetAndEnqueue = new RunnableHttpGetAndEnqueue(
				url, user, entityType, queue, expectedEntities);
		execService.execute(runnableHttpGetAndEnqueue);

	}



	UserGroupInformation getUgiForUser(String aUser) {

		String keytabDir = HadooptestConstants.Location.Keytab.HDFSQA;
		if (aUser.equals(HadooptestConstants.UserNames.HDFSQA)) {
			keytabDir = HadooptestConstants.Location.Keytab.HDFSQA;
		}
		UserGroupInformation ugi;
		try {

			ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(aUser,
					keytabDir);
			TestSession.logger.info("UGI=" + ugi);
			TestSession.logger.info("credentials:" + ugi.getCredentials());
			TestSession.logger.info("group names" + ugi.getGroupNames());
			TestSession.logger.info("real user:" + ugi.getRealUser());
			TestSession.logger
					.info("short user name:" + ugi.getShortUserName());
			TestSession.logger.info("token identifiers:"
					+ ugi.getTokenIdentifiers());
			TestSession.logger.info("tokens:" + ugi.getTokens());
			TestSession.logger.info("username:" + ugi.getUserName());
			TestSession.logger.info("current user:"
					+ UserGroupInformation.getCurrentUser());
			TestSession.logger.info("login user:"
					+ UserGroupInformation.getLoginUser());

		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return ugi;
	}

	class DoAs {
		UserGroupInformation ugi;
		Configuration configuration;
		Object jobObjectToRun;
		String[] sleepJobArgs;
		SeedData seedData;
		DoAs(UserGroupInformation ugi, Object jobObjectToRun,
				String[] sleepJobArgs) throws IOException {
			this.ugi = ugi;
			this.jobObjectToRun = jobObjectToRun;
			this.sleepJobArgs = sleepJobArgs;
		}

		public void doAction() throws AccessControlException,
				IOException, InterruptedException {
			PrivilegedExceptionActionImpl privilegedExceptionActor = new PrivilegedExceptionActionImpl(
					ugi, jobObjectToRun, sleepJobArgs);
			ugi.doAs(privilegedExceptionActor);
			this.seedData = privilegedExceptionActor.getSeedDataForAppThatJustRan();
		}
		public SeedData getSeedDataForAppThatJustRan(){
			return this.seedData;
		}
	}

	class PrivilegedExceptionActionImpl implements
			PrivilegedExceptionAction<String> {
		UserGroupInformation ugi;

		Object theJobToRun;
		String appIdThatJustRan = null;
		Set<String> dagNamesThatJustRan = null;
		String[] sleepJobArgs = null;
		SeedData seedData = new SeedData();

		PrivilegedExceptionActionImpl(UserGroupInformation ugi,
				Object jobObjectToRun, String[] sleepJobArgs)
				throws IOException {
			this.ugi = ugi;
			this.theJobToRun = jobObjectToRun;
			this.sleepJobArgs = sleepJobArgs;
			this.seedData.appStartedByUser = ugi.getUserName();
		}

		public String run() throws Exception {
			String returnString = null;
			if (this.theJobToRun instanceof OrderedWordCountExtendedForHtf) {
				TestHtfOrderedWordCount test = new TestHtfOrderedWordCount();
				test.copyTheFileOnHdfs();
				
				boolean returnCode = ((OrderedWordCountExtendedForHtf) theJobToRun)
						.run(TestHtfOrderedWordCount.INPUT_FILE,
								TestHtfOrderedWordCount.OUTPUT_LOCATION
										+ System.currentTimeMillis(), null, 2,
								HadooptestConstants.Execution.TEZ_CLUSTER,
								HtfTezUtils.Session.NO, TimelineServer.ENABLED,
								"OrdWrdCnt", ugi, seedData);

				Assert.assertTrue(returnCode == true);

			} else if (this.theJobToRun instanceof SimpleSessionExampleExtendedForTezHTF) {
				TestSimpleSessionExample test = new TestSimpleSessionExample();
				test.copyTheFileOnHdfs();
				boolean returnCode = ((SimpleSessionExampleExtendedForTezHTF) theJobToRun)
						.run(TestSimpleSessionExample.inputFilesOnHdfs,
								TestSimpleSessionExample.outputPathsOnHdfs,
								HtfTezUtils.setupConfForTez(
										TestSession.cluster.getConf(),
										HadooptestConstants.Execution.TEZ_CLUSTER,
										HtfTezUtils.Session.YES,
										TimelineServer.ENABLED, "SimpleSessionEx"),
								2, ugi, seedData);
				test.deleteTezStagingDirs();
				Assert.assertTrue(returnCode == true);

			} else if (this.theJobToRun instanceof MRRSleepJobExtendedForTezHTF) {

				/**
				 * [-m numMapper][-r numReducer] [-ir numIntermediateReducer]
				 * [-irs numIntermediateReducerStages] [-mt mapSleepTime (msec)]
				 * [-rt reduceSleepTime (msec)] [-irt
				 * intermediateReduceSleepTime] [-recordt recordSleepTime
				 * (msec)] [-generateSplitsInAM (false)/true] [-writeSplitsToDfs
				 * (false)/true]
				 */
				int returnCode = ((MRRSleepJobExtendedForTezHTF) theJobToRun)
						.run(sleepJobArgs,
								HadooptestConstants.Execution.TEZ_CLUSTER,
								HtfTezUtils.Session.YES,
								TimelineServer.DISABLED, "MRRSleepJob",
								ugi, seedData);
				Assert.assertTrue(returnCode == 0);

			}

			return returnString;
		}

		public SeedData getSeedDataForAppThatJustRan() {
			return this.seedData;
		}

	}

	@After
	public void logTaskReportSummary() throws Exception {
	}

}
