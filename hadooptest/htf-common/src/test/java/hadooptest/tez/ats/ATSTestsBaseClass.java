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
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Force;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Recursive;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.SkipTrash;
import hadooptest.hadoop.regression.yarn.YarnCliCommands;
import hadooptest.hadoop.regression.yarn.YarnCliCommands.GenericYarnCliResponseBO;
import hadooptest.hadoop.regression.yarn.YarnTestsBaseClass.YarnApplicationSubCommand;
import hadooptest.node.hadoop.HadoopNode;
import hadooptest.tez.ats.ATSTestsBaseClass.ResponseComposition.EVENTS;
import hadooptest.tez.examples.cluster.TestHtfOrderedWordCount;
import hadooptest.tez.examples.cluster.TestSimpleSessionExample;
import hadooptest.tez.examples.extensions.OrderedWordCountExtendedForHtf;
import hadooptest.tez.examples.extensions.SimpleSessionExampleExtendedForTezHTF;
import hadooptest.tez.mapreduce.examples.extensions.MRRSleepJobExtendedForTezHTF;
import hadooptest.tez.utils.HtfATSUtils;
import hadooptest.tez.utils.HtfPigBaseClass;
import hadooptest.tez.utils.HtfPigBaseClass.TIMELINE_SEVICE;
import hadooptest.tez.utils.HtfTezUtils;
import hadooptest.tez.utils.HtfTezUtils.TimelineServer;

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

import org.apache.commons.httpclient.cookie.CookiePolicy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.http.client.params.ClientPNames;
import org.apache.tez.dag.api.TezConfiguration;
import org.json.simple.parser.ParseException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

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
	public static Map<String, String> userCookies = new HashMap<String, String>();
	Queue<GenericATSResponseBO> dagIdQueue = new ConcurrentLinkedQueue<GenericATSResponseBO>();
	Queue<GenericATSResponseBO> containerIdQueue = new ConcurrentLinkedQueue<GenericATSResponseBO>();
	Queue<GenericATSResponseBO> applicationAttemptQueue = new ConcurrentLinkedQueue<GenericATSResponseBO>();
	Queue<GenericATSResponseBO> vertexIdQueue = new ConcurrentLinkedQueue<GenericATSResponseBO>();
	Queue<GenericATSResponseBO> taskIdQueue = new ConcurrentLinkedQueue<GenericATSResponseBO>();
	Queue<GenericATSResponseBO> taskAttemptIdQueue = new ConcurrentLinkedQueue<GenericATSResponseBO>();
	AtomicInteger errorCount = new AtomicInteger();
	public static Map<String, String> userGroupMapping = new HashMap<String, String>();

	public static Boolean jobsLaunchedOnceToSeedData = Boolean.FALSE;

	public static SeedData seedDataForAutoLaunchedSleepJob = null;
	public static SeedData seedDataForAutoLaunchedSimpleSessionExample = null;
	public static SeedData seedDataForAutoLaunchedOrderedWordCount = null;
	public static SeedData seedDataForAutoLaunchedPigJob = new SeedData();
	
	public static String YAHOO_TEAM_MEMBERS = "tiwari,jeagles,mitdesai";

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

		TestSession.logger.info("Running cleanupAndPrepareForTestRun");
		// Fetch cookies
		HTTPHandle httpHandle = new HTTPHandle();
		String hitusr_1_cookie = null;
		String hitusr_2_cookie = null;
		String hitusr_3_cookie = null;
		String hitusr_4_cookie = null;

		hitusr_1_cookie = httpHandle
				.loginAndReturnCookie(HadooptestConstants.UserNames.HITUSR_1);
		TestSession.logger.info("Got cookie hitusr_1:" + hitusr_1_cookie);
		userCookies
				.put(HadooptestConstants.UserNames.HITUSR_1, hitusr_1_cookie);
		hitusr_2_cookie = httpHandle
				.loginAndReturnCookie(HadooptestConstants.UserNames.HITUSR_2);
		TestSession.logger.info("Got cookie hitusr_2:" + hitusr_2_cookie);
		userCookies
				.put(HadooptestConstants.UserNames.HITUSR_2, hitusr_2_cookie);
		hitusr_3_cookie = httpHandle
				.loginAndReturnCookie(HadooptestConstants.UserNames.HITUSR_3);
		TestSession.logger.info("Got cookie hitusr_3:" + hitusr_3_cookie);
		userCookies
				.put(HadooptestConstants.UserNames.HITUSR_3, hitusr_3_cookie);
		hitusr_4_cookie = httpHandle
				.loginAndReturnCookie(HadooptestConstants.UserNames.HITUSR_4);
		TestSession.logger.info("Got cookie hitusr_4:" + hitusr_4_cookie);
		userCookies
				.put(HadooptestConstants.UserNames.HITUSR_4, hitusr_4_cookie);

		// Reset the error count
		errorCount.set(0);

		drainQueues();
		launchJobsOnceToSeedData();

		HadoopNode hadoopNode = TestSession.cluster
				.getNode(HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
		String rmHost = hadoopNode.getHostname();
		TestSession.logger.info("RESOURCE MANAGER HOST:::::::::::::::::::::"
				+ rmHost);

	}

	public void createUserGroupMapping() {
		String user;
		// hitusr_1
		user = HadooptestConstants.UserNames.HITUSR_1;
		userGroupMapping.put(user, HadooptestConstants.UserGroups.HADOOP);

		// hitusr_2
		user = HadooptestConstants.UserNames.HITUSR_2;
		userGroupMapping.put(user, HadooptestConstants.UserGroups.HADOOPQA);

		// hitusr_3
		user = HadooptestConstants.UserNames.HITUSR_3;
		userGroupMapping.put(user, HadooptestConstants.UserGroups.GDMDEV);

		// hitusr_4
		user = HadooptestConstants.UserNames.HITUSR_4;
		userGroupMapping.put(user, HadooptestConstants.UserGroups.HADOOPQA
				+ "," + HadooptestConstants.UserGroups.GDMQA + ","
				+ HadooptestConstants.UserGroups.GDMDEV);

	}

	public void restartATSWithTheseArgs(String rmHost, List<String> args)
			throws Exception {
		String command = "/home/gs/gridre/yroot."
				+ System.getProperty("CLUSTER_NAME")
				+ "/share/hadoop/sbin/yarn-daemon.sh stop timelineserver";
		doJavaSSHClientExec(
				HadooptestConstants.UserNames.MAPREDQA,
				rmHost,
				command,
				HadooptestConstants.Location.Identity.HADOOPQA_AS_MAPREDQA_IDENTITY_FILE);

		StringBuilder sb = new StringBuilder();
		for (String anArg : args) {
			sb.append(" " + anArg + " ");
		}
		command = "YARN_ROOT_LOGGER=DEBUG,RFA /home/gs/gridre/yroot."
				+ System.getProperty("CLUSTER_NAME")
				+ "/share/hadoop/sbin/yarn-daemon.sh" + sb.toString()
				+ " start timelineserver";
		doJavaSSHClientExec(
				HadooptestConstants.UserNames.MAPREDQA,
				rmHost,
				command,
				HadooptestConstants.Location.Identity.HADOOPQA_AS_MAPREDQA_IDENTITY_FILE);

	}

	public void restartRMWithTheseArgs(String rmHost, List<String> args)
			throws Exception {
		String command = "/home/gs/gridre/yroot."
				+ System.getProperty("CLUSTER_NAME")
				+ "/share/hadoop/sbin/yarn-daemon.sh stop resourcemanager";
		doJavaSSHClientExec(
				HadooptestConstants.UserNames.MAPREDQA,
				rmHost,
				command,
				HadooptestConstants.Location.Identity.HADOOPQA_AS_MAPREDQA_IDENTITY_FILE);

		StringBuilder sb = new StringBuilder();
		for (String anArg : args) {
			sb.append(" -D" + anArg + " ");
		}

		command = "/home/gs/gridre/yroot." + System.getProperty("CLUSTER_NAME")
				+ "/share/hadoop/sbin/yarn-daemon.sh start resourcemanager"
				+ sb.toString();
		TestSession.logger.info("About to start RM command:" + command);
		doJavaSSHClientExec(
				HadooptestConstants.UserNames.MAPREDQA,
				rmHost,
				command,
				HadooptestConstants.Location.Identity.HADOOPQA_AS_MAPREDQA_IDENTITY_FILE);
		TestSession.logger.info("After running the command");

	}

	public String getAllTheGroupsThatUserBelongsTo(String username) {
		StringBuilder sb = new StringBuilder();
		if (username.equalsIgnoreCase(HadooptestConstants.UserNames.HADOOPQA)) {
			sb.append(HadooptestConstants.UserGroups.HADOOP).append(",")
					.append(HadooptestConstants.UserGroups.HADOOPQA);
		} else if (username
				.equalsIgnoreCase(HadooptestConstants.UserNames.HITUSR_1)) {
			sb.append(HadooptestConstants.UserGroups.HADOOP);
		} else if (username
				.equalsIgnoreCase(HadooptestConstants.UserNames.HITUSR_2)) {
			sb.append(HadooptestConstants.UserGroups.HADOOPQA);
		} else if (username
				.equalsIgnoreCase(HadooptestConstants.UserNames.HITUSR_3)) {
			sb.append(HadooptestConstants.UserGroups.GDMDEV);
		} else if (username
				.equalsIgnoreCase(HadooptestConstants.UserNames.HITUSR_4)) {
			sb.append(HadooptestConstants.UserGroups.HADOOP).append(",")
					.append(HadooptestConstants.UserGroups.HADOOPQA)
					.append(",").append(HadooptestConstants.UserGroups.GDMDEV)
					.append(",").append(HadooptestConstants.UserGroups.GDMQA);
		}
		return sb.toString();
	}

	public void launchJobsOnceToSeedData() throws Exception {
		TestSession.logger.info("I launchJobsOnceToSeedData");
		if (jobsLaunchedOnceToSeedData.booleanValue() == Boolean.FALSE) {
			jobsLaunchedOnceToSeedData = Boolean.TRUE;
			groundWorkForPigScriptExecution();

			// Run a OrderedWordCount as hitusr_1
			seedDataForAutoLaunchedOrderedWordCount = launchOrderedWordCountExtendedForHtfAndGetSeedData(
					HadooptestConstants.UserNames.HITUSR_1,
					HadooptestConstants.UserNames.HITUSR_1);

			// Run a Sleep Job, as hitusr_2
			String[] sleepJobArgs = new String[] { "-m 5", "-r 4", "-ir 4",
					"-irs 4", "-mt 500", "-rt 200", "-irt 100", "-recordt 100" };
			seedDataForAutoLaunchedSleepJob = launchMRRSleepJobAndGetSeedData(
					HadooptestConstants.UserNames.HITUSR_2, sleepJobArgs,
					HadooptestConstants.UserNames.HITUSR_2);

			// Run a SimpleSessionExample as hitusr_3
			seedDataForAutoLaunchedSimpleSessionExample = launchSimpleSessionExampleExtendedForTezHTFAndGetSeedData(
					HadooptestConstants.UserNames.HITUSR_3,
					HadooptestConstants.UserNames.HITUSR_3);

		}
	}

	public void groundWorkForPigScriptExecution() throws Exception {
		TestSession.logger.info("Doing groundWorkForPigScriptExecution");
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		String MIMIC_THIS_NAME_ON_HDFS = TestHtfOrderedWordCount.YINST_DIR_LOCATION_OF_HTF_DATA;
		String DATA_FILE = "excite-small.log";
		GenericCliResponseBO quickCheck = dfsCliCommands.test(
				DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "",
				System.getProperty("CLUSTER_NAME"), MIMIC_THIS_NAME_ON_HDFS
						+ DATA_FILE, DfsCliCommands.FILE_SYSTEM_ENTITY_FILE);

		if (quickCheck.process.exitValue() != 0) {
			dfsCliCommands
					.mkdir(DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
							HadooptestConstants.UserNames.HDFSQA, "",
							System.getProperty("CLUSTER_NAME"),
							MIMIC_THIS_NAME_ON_HDFS);
			dfsCliCommands.put(DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
					HadooptestConstants.UserNames.HDFSQA, "",
					System.getProperty("CLUSTER_NAME"),
					TestHtfOrderedWordCount.SOURCE_FILE,
					MIMIC_THIS_NAME_ON_HDFS);
			dfsCliCommands.chmod(DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
					HadooptestConstants.UserNames.HDFSQA,
					System.getProperty("CLUSTER_NAME"),
					System.getProperty("CLUSTER_NAME"), "/"
							+ MIMIC_THIS_NAME_ON_HDFS.split("/")[1], "777",
					Recursive.YES);
		}
		// Check if o/p dir exists
		quickCheck = dfsCliCommands.test(DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "",
				System.getProperty("CLUSTER_NAME"), "/tmp/pigout/",
				DfsCliCommands.FILE_SYSTEM_ENTITY_DIRECTORY);

		// Delete the output dir, if it exists
		if (quickCheck.process.exitValue() == 0) {
			dfsCliCommands.rm(DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
					HadooptestConstants.UserNames.HDFSQA, "",
					System.getProperty("CLUSTER_NAME"), Recursive.YES,
					Force.YES, SkipTrash.YES, "/tmp/pigout/");
		}

	}

	public List<String> pigJobsOnTheCluster() throws Exception {
		List<String> pigJobsRan = new ArrayList<String>();
		YarnCliCommands yarnCliCommands = new YarnCliCommands();
		GenericYarnCliResponseBO yarnResponse = yarnCliCommands.application(
				DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "",
				System.getProperty("CLUSTER_NAME"),
				YarnApplicationSubCommand.LIST, "-appStates" + " " + "ALL");
		String[] allLines = yarnResponse.response.split("\n");
		for (String aLine : allLines) {
			if (!aLine.contains("PigLatin")) {
				continue;
			}
			pigJobsRan.add(aLine.split("\\s+")[0]);
		}
		return pigJobsRan;
	}

	public void runPigOnTezScriptOnCluster() throws Exception {
		TestSession.logger.info("Running seed Pig script on cluster");
		List<String> params = new ArrayList<String>();
		params.add("outdir=/tmp/pigout/script2-mapreduce");
		String scriptLocation = "/home/y/share/htf-data/script2-local.pig ";
		HtfPigBaseClass htfPigBaseClass = new HtfPigBaseClass(
				TIMELINE_SEVICE.ENABLED);
		int returnCode = htfPigBaseClass.runPigScriptOnCluster(params,
				scriptLocation);
		Assert.assertTrue(returnCode == 0);
	}

	public SeedData launchOrderedWordCountExtendedForHtfAndGetSeedData(
			String user, String acls) throws IOException, InterruptedException {
		UserGroupInformation ugi = getUgiForUser(user);
		DoAs doAs = new DoAs(ugi, new OrderedWordCountExtendedForHtf(),
				new String[0], acls);
		doAs.doAction();
		SeedData seedDataForLaunchedJob;
		seedDataForLaunchedJob = doAs.getSeedDataForAppThatJustRan();
		GenericATSResponseBO processedDagIdResponses = getDagIdResponses(seedDataForLaunchedJob.appStartedByUser);
		populateAdditionalSeedData(processedDagIdResponses,
				seedDataForLaunchedJob);
		seedDataForLaunchedJob.dump();
		return seedDataForLaunchedJob;
	}

	public SeedData launchSimpleSessionExampleExtendedForTezHTFAndGetSeedData(
			String user, String acls) throws IOException, InterruptedException {
		UserGroupInformation ugi = getUgiForUser(user);
		DoAs doAs = new DoAs(ugi, new SimpleSessionExampleExtendedForTezHTF(),
				new String[0], acls);
		doAs.doAction();
		SeedData seedDataForLaunchedJob;
		seedDataForLaunchedJob = doAs.getSeedDataForAppThatJustRan();
		GenericATSResponseBO processedDagIdResponses = getDagIdResponses(seedDataForLaunchedJob.appStartedByUser);
		populateAdditionalSeedData(processedDagIdResponses,
				seedDataForLaunchedJob);

		seedDataForLaunchedJob.dump();
		return seedDataForLaunchedJob;
	}

	public SeedData launchMRRSleepJobAndGetSeedData(String user,
			String[] sleepJobArgs, String acls) throws Exception {
		TestSession.logger.info("About to launch MRRSleepJob as user:" + user);
		UserGroupInformation ugi = getUgiForUser(user);
		DoAs doAs = new DoAs(ugi, new MRRSleepJobExtendedForTezHTF(),
				sleepJobArgs, acls);
		doAs.doAction();
		SeedData seedDataForLaunchedJob;
		seedDataForLaunchedJob = doAs.getSeedDataForAppThatJustRan();
		GenericATSResponseBO processedDagIdResponses = getDagIdResponses(seedDataForLaunchedJob.appStartedByUser);
		populateAdditionalSeedData(processedDagIdResponses,
				seedDataForLaunchedJob);

		seedDataForLaunchedJob.dump();
		return seedDataForLaunchedJob;
	}

	void populateAdditionalSeedData(GenericATSResponseBO dagIdResponse,
			SeedData seedData) throws InterruptedException {
		String appId = seedData.appId.replace("application_", "");
		for (EntityInGenericATSResponseBO anEntityInResponse : dagIdResponse.entities) {
			if (anEntityInResponse.entity.contains(appId)) {
				String rxDagName = anEntityInResponse.primaryfilters.get(
						"dagName").get(0);
				TestSession.logger.info("Got rxDagName:" + rxDagName);
				SeedData.DAG aSeedDag = seedData.getDagObject(rxDagName);
				aSeedDag.id = anEntityInResponse.entity;

				// Get the Vertex details
				for (String aVertexId : anEntityInResponse.relatedentities
						.get("TEZ_VERTEX_ID")) {
					populateVertexDetails(seedData, aSeedDag, aVertexId);
				}
			}
		}
	}

	public GenericATSResponseBO getDagIdResponses(String user)
			throws InterruptedException {
		ExecutorService execService = Executors.newFixedThreadPool(1);
		String filter = "?fields=events,relatedentities,primaryfilters";
		Map<String, Boolean> expectedEntities = getExpectedFieldsMap(
				ResponseComposition.EVENTS.EXPECTED,
				ResponseComposition.ENTITYTYPE.EXPECTED,
				ResponseComposition.ENTITY.EXPECTED,
				ResponseComposition.STARTTIME.EXPECTED,
				ResponseComposition.RELATEDENTITIES.EXPECTED,
				ResponseComposition.PRIMARYFILTERS.EXPECTED,
				ResponseComposition.OTHERINFO.NOT_EXPECTED);

		// TODO: Change the user below to seedData.appStartedByUser
		makeHttpCallAndEnqueueConsumedResponse(execService, getATSUrl()
				+ "TEZ_DAG_ID" + filter, user, EntityTypes.TEZ_DAG_ID,
				dagIdQueue, expectedEntities);
		execService.shutdown();
		while (!execService.isTerminated()) {
			Thread.sleep(1000);
		}
		GenericATSResponseBO dagIdResponse = dagIdQueue.poll();
		return dagIdResponse;

	}

	int countNumberOfFailedJobs(String user) throws InterruptedException {
		int count = 0;
		ExecutorService execService = Executors.newFixedThreadPool(10);

		// TODO: Change the user below to user
		makeHttpCallAndEnqueueConsumedResponse(execService, getATSUrl()
				+ "TEZ_DAG_ID/", user, EntityTypes.TEZ_DAG_ID, dagIdQueue,
				expectEverythingMap());
		execService.shutdown();
		while (!execService.isTerminated()) {
			Thread.sleep(1000);
		}
		TestSession.logger.info("dagIdqueue size:" + dagIdQueue.size());
		GenericATSResponseBO dagIdResponse = dagIdQueue.poll();
		for (EntityInGenericATSResponseBO anEntity : dagIdResponse.entities) {
			try {
				if (((OtherInfoTezDagIdBO) anEntity.otherinfo).status
						.equals("FAILED")) {
					count++;
				}
			} catch (Exception e) {
				TestSession.logger.error(e.getMessage());
				TestSession.logger.info("exception received while processing:"
						+ anEntity.entity);
			}
		}
		return count;

	}

	public void populateVertexDetails(SeedData seedData, SeedData.DAG dag,
			String vertexId) throws InterruptedException {
		ExecutorService execService = Executors.newFixedThreadPool(1);
		makeHttpCallAndEnqueueConsumedResponse(
				execService,
				// TODO: Change the user below to seedData.appStartedByUser
				getATSUrl() + "TEZ_VERTEX_ID/" + vertexId,
				seedData.appStartedByUser, EntityTypes.TEZ_VERTEX_ID,
				vertexIdQueue, expectEverythingMap());
		execService.shutdown();
		while (!execService.isTerminated()) {
			Thread.sleep(1000);
		}
		GenericATSResponseBO vertexIdResponse = vertexIdQueue.poll();
		String vertexName = ((OtherInfoTezVertexIdBO) (vertexIdResponse.entities
				.get(0).otherinfo)).vertexName;
		SeedData.DAG.Vertex aSeedVertex = dag.getVertexObject(vertexName);
		aSeedVertex.id = vertexId;

		for (String aTaskId : vertexIdResponse.entities.get(0).relatedentities
				.get(EntityTypes.TEZ_TASK_ID.name())) {
			SeedData.DAG.Vertex.Task aSeedTask = new SeedData.DAG.Vertex.Task();
			aSeedTask.id = aTaskId;
			aSeedVertex.tasks.add(aSeedTask);
			populateTaskDetails(seedData, aSeedTask, aTaskId);
		}

	}

	public void populateTaskDetails(SeedData seedData,
			SeedData.DAG.Vertex.Task aSeedTask, String taskId)
			throws InterruptedException {
		ExecutorService execService = Executors.newFixedThreadPool(10);
		makeHttpCallAndEnqueueConsumedResponse(execService, getATSUrl()
				+ "TEZ_TASK_ID/" + taskId, seedData.appStartedByUser,
				EntityTypes.TEZ_TASK_ID, taskIdQueue, expectEverythingMap());
		execService.shutdown();
		while (!execService.isTerminated()) {
			Thread.sleep(1000);
		}
		GenericATSResponseBO taskIdResponse = taskIdQueue.poll();
		for (String aTaskAttemptId : taskIdResponse.entities.get(0).relatedentities
				.get(EntityTypes.TEZ_TASK_ATTEMPT_ID.name())) {
			SeedData.DAG.Vertex.Task.Attempt aTaskAttempt = new SeedData.DAG.Vertex.Task.Attempt();
			aTaskAttempt.id = aTaskAttemptId;
			aSeedTask.attempts.add(aTaskAttempt);
		}

	}

	public void drainQueues() {
		// "Drain" the queues
		dagIdQueue.clear();
		containerIdQueue.clear();
		applicationAttemptQueue.clear();
		vertexIdQueue.clear();
		taskIdQueue.clear();
		taskAttemptIdQueue.clear();

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
			TestSession.logger.debug("XML change:" + line);
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
			jsch.addIdentity(identityFile,"");
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
			TestSession.logger.info("USer:" + user + " Cookie:"
					+ userCookies.get(user));
			TestSession.logger
					.info("RRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR");
			TestSession.logger.trace(userCookies);
			Response response = given().cookie(userCookies.get(user)).get(url);

			String responseAsString = response.getBody().asString();
			TestSession.logger.info(" R E S P O N S E  C O D E: "
					+ response.getStatusCode());
			TestSession.logger.info("R E S P O N S E  B O D Y :"
					+ responseAsString);
			HtfATSUtils atsUtils = new HtfATSUtils();

			try {

				atsUtils.processATSResponse(responseAsString, entityType,
						expectedEntities, queue);

			} catch (ParseException e) {
				TestSession.logger.error(e);
				errorCount.incrementAndGet();
			} catch (Exception e) {
				for (StackTraceElement x : e.getStackTrace()) {
					TestSession.logger.error(x);
				}
				TestSession.logger.error(e);
				errorCount.incrementAndGet();
			}

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
		String keyTabLocation = null;

		try {
			TestSession.logger.info("Even before the call Current user:"
					+ UserGroupInformation.getCurrentUser());

		} catch (IOException e1) {
			e1.printStackTrace();
		}
		if (aUser.equalsIgnoreCase(HadooptestConstants.UserNames.HITUSR_1)) {
			keyTabLocation = HadooptestConstants.Location.Keytab.HITUSR_1;
		} else if (aUser
				.equalsIgnoreCase(HadooptestConstants.UserNames.HITUSR_2)) {
			keyTabLocation = HadooptestConstants.Location.Keytab.HITUSR_2;
		} else if (aUser
				.equalsIgnoreCase(HadooptestConstants.UserNames.HITUSR_3)) {
			keyTabLocation = HadooptestConstants.Location.Keytab.HITUSR_3;
		} else if (aUser
				.equalsIgnoreCase(HadooptestConstants.UserNames.HITUSR_4)) {
			keyTabLocation = HadooptestConstants.Location.Keytab.HITUSR_4;
		}
		UserGroupInformation ugi;
		try {
			TestSession.logger.info("Is login keytab based:"
					+ UserGroupInformation.isLoginKeytabBased());
			TestSession.logger.info("Is security enabled:"
					+ UserGroupInformation.isSecurityEnabled());
			TestSession.logger.info("Fetching keytab as " + aUser
					+ " from keytab:" + keyTabLocation);
			ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(aUser,
					keyTabLocation);
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

	public void chmodOutputPath777Tmp() throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		dfsCliCommands.chmod(new HashMap<String, String>(),
				HadooptestConstants.UserNames.HDFSQA,
				HadooptestConstants.Schema.HDFS,
				System.getProperty("CLUSTER_NAME"), "/tmp", "777",
				Recursive.YES);

	}

	class DoAs {
		UserGroupInformation ugi;
		Configuration configuration;
		Object jobObjectToRun;
		String[] sleepJobArgs;
		SeedData seedData;
		String acls = null;

		DoAs(UserGroupInformation ugi, Object jobObjectToRun,
				String[] sleepJobArgs, String acls) throws IOException {
			this.ugi = ugi;
			this.jobObjectToRun = jobObjectToRun;
			this.sleepJobArgs = sleepJobArgs;
			this.acls = acls;
		}

		public void doAction() throws AccessControlException, IOException,
				InterruptedException {
			TestSession.logger.info("In DoAs as user:" + ugi.getShortUserName()
					+ " the same will be passed downstream");
			PrivilegedExceptionActionImpl privilegedExceptionActor = new PrivilegedExceptionActionImpl(
					ugi, jobObjectToRun, sleepJobArgs, acls);
			ugi.doAs(privilegedExceptionActor);
			this.seedData = privilegedExceptionActor
					.getSeedDataForAppThatJustRan();
		}

		public SeedData getSeedDataForAppThatJustRan() {
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
		String acls = null;

		PrivilegedExceptionActionImpl(UserGroupInformation ugi,
				Object jobObjectToRun, String[] sleepJobArgs, String acls)
				throws IOException {
			this.ugi = ugi;
			this.theJobToRun = jobObjectToRun;
			this.sleepJobArgs = sleepJobArgs;
			this.seedData.appStartedByUser = ugi.getShortUserName();
			this.acls = acls;
		}

		public String run() throws Exception {
			TestSession.logger.info("In PrivilegedExceptionActionImpl as user:"
					+ ugi.getShortUserName());
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
								"OrdWrdCnt", ugi, seedData, acls);

				Assert.assertTrue(returnCode == true);

			} else if (this.theJobToRun instanceof SimpleSessionExampleExtendedForTezHTF) {
				TestSimpleSessionExample test = new TestSimpleSessionExample();
				test.copyTheFileOnHdfs();
				chmodOutputPath777Tmp();
				int returnCode = ((SimpleSessionExampleExtendedForTezHTF) theJobToRun).runJob(new String[]{TestSimpleSessionExample.inputFilesOnHdfs,
				TestSimpleSessionExample.outputPathsOnHdfs,"2"},
				(TezConfiguration)HtfTezUtils.setupConfForTez(
				TestSession.cluster.getConf(),
				HadooptestConstants.Execution.TEZ_CLUSTER,
				HtfTezUtils.Session.YES,
				TimelineServer.ENABLED,
				"SimpleSessionEx"), ugi, seedData,
				acls);
				test.deleteTezStagingDirs();
				Assert.assertTrue(returnCode == 0);
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
								TimelineServer.DISABLED, "MRRSleepJob", ugi,
								seedData, acls);
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
