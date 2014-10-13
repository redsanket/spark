package hadooptest.tez.ats;

import static com.jayway.restassured.RestAssured.given;
import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.cluster.hadoop.HadoopComponent;
import hadooptest.node.hadoop.HadoopNode;

import java.util.Hashtable;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.json.simple.parser.ParseException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.jayway.restassured.response.Response;

@Category(SerialTests.class)
public class TestConcurrentRequests extends ATSTestsBaseClass {

	@Test
	public void testDagIdResponse() throws Exception {

		Queue<GenericATSResponseBO> dagIdQueue = new ConcurrentLinkedQueue<GenericATSResponseBO>();
		Queue<GenericATSResponseBO> containerIdQueue = new ConcurrentLinkedQueue<GenericATSResponseBO>();
		Queue<GenericATSResponseBO> applicationAttemptQueue = new ConcurrentLinkedQueue<GenericATSResponseBO>();
		Queue<GenericATSResponseBO> tezVertexIdQueue = new ConcurrentLinkedQueue<GenericATSResponseBO>();
		Queue<GenericATSResponseBO> tezTaskIdQueue = new ConcurrentLinkedQueue<GenericATSResponseBO>();
		Queue<GenericATSResponseBO> tezTaskAttemptIdQueue = new ConcurrentLinkedQueue<GenericATSResponseBO>();

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

		if (!timelineserverStarted) {
			// startTimelineServerOnRM(rmHostname);
		}

		String url = "http://" + rmHostname + ":"
				+ HadooptestConstants.Ports.HTTP_ATS_PORT
				+ "/ws/v1/timeline/TEZ_DAG_ID/";
		Response response = given().cookie(
				userCookies.get(HadooptestConstants.UserNames.HITUSR_1)).get(
				url);
		String responseAsString = response.getBody().asString();
		TestSession.logger
				.info("R E S P O N S E  B O D Y :" + responseAsString);
		ATSUtils atsUtils = new ATSUtils();
		GenericATSResponseBO consumedResponse = atsUtils.processATSResponse(
				responseAsString, EntityTypes.TEZ_DAG_ID);
		consumedResponse.dump();
		RestCaller tezDagIdCaller = new RestCaller("http://" + rmHostname + ":"
				+ HadooptestConstants.Ports.HTTP_ATS_PORT
				+ "/ws/v1/timeline/TEZ_DAG_ID/",
				HadooptestConstants.UserNames.HITUSR_1, EntityTypes.TEZ_DAG_ID,
				dagIdQueue);
		RestCaller tezContainerIdCaller = new RestCaller("http://" + rmHostname
				+ ":" + HadooptestConstants.Ports.HTTP_ATS_PORT
				+ "/ws/v1/timeline/TEZ_CONTAINER_ID/",
				HadooptestConstants.UserNames.HITUSR_1,
				EntityTypes.TEZ_CONTAINER_ID, containerIdQueue);
		RestCaller applicationAttemptCaller = new RestCaller("http://" + rmHostname
				+ ":" + HadooptestConstants.Ports.HTTP_ATS_PORT
				+ "/ws/v1/timeline/TEZ_APPLICATION_ATTEMPT/",
				HadooptestConstants.UserNames.HITUSR_1,
				EntityTypes.TEZ_APPLICATION_ATTEMPT, applicationAttemptQueue);

		RestCaller tezVertexIdCaller = new RestCaller("http://" + rmHostname
				+ ":" + HadooptestConstants.Ports.HTTP_ATS_PORT
				+ "/ws/v1/timeline/TEZ_VERTEX_ID/",
				HadooptestConstants.UserNames.HITUSR_1,
				EntityTypes.TEZ_VERTEX_ID, tezVertexIdQueue);
		RestCaller tezTaskIdCaller = new RestCaller("http://" + rmHostname
				+ ":" + HadooptestConstants.Ports.HTTP_ATS_PORT
				+ "/ws/v1/timeline/TEZ_TASK_ID/",
				HadooptestConstants.UserNames.HITUSR_1,
				EntityTypes.TEZ_TASK_ID, tezTaskIdQueue);
		RestCaller tezTaskAttemptIdCaller = new RestCaller("http://" + rmHostname
				+ ":" + HadooptestConstants.Ports.HTTP_ATS_PORT
				+ "/ws/v1/timeline/TEZ_TASK_ATTEMPT_ID/",
				HadooptestConstants.UserNames.HITUSR_1,
				EntityTypes.TEZ_TASK_ATTEMPT_ID, tezTaskAttemptIdQueue);

		//tezTaskAttemptIdQueue

		ExecutorService execService = Executors.newFixedThreadPool(10);
		for (int xx = 0; xx < 2; xx++) {
			execService.execute(tezDagIdCaller);
			execService.execute(tezContainerIdCaller);
			execService.execute(applicationAttemptCaller);
			execService.execute(tezVertexIdCaller);
			execService.execute(tezTaskIdCaller);
			execService.execute(tezTaskAttemptIdCaller);
		}
		execService.shutdown();
		while (!execService.isTerminated()) {
			TestSession.logger
					.info("Thread sleeping while awaiting REST calls");
			Thread.sleep(1000);
		}

		TestSession.logger.info("DagId queue size:" + dagIdQueue.size());
		TestSession.logger.info("ContainerId queue size:"
				+ containerIdQueue.size());
		Assert.assertTrue(atsUtils.areAllTheResponsesSame(dagIdQueue));
//		Assert.assertTrue(atsUtils.areAllTheResponsesSame(containerIdQueue));
		Assert.assertTrue(atsUtils.areAllTheResponsesSame(applicationAttemptQueue));
		Assert.assertTrue(atsUtils.areAllTheResponsesSame(tezVertexIdQueue));

	}

	class RestCaller implements Runnable {
		String url;
		String user;
		EntityTypes entityType;
		Queue<GenericATSResponseBO> queue;

		public RestCaller(String url, String user, EntityTypes entityType,
				Queue<GenericATSResponseBO> queue) {
			this.url = url;
			this.user = user;
			this.entityType = entityType;
			this.queue = queue;
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
			ATSUtils atsUtils = new ATSUtils();
			GenericATSResponseBO consumedResponse = null;
			try {
				consumedResponse = atsUtils.processATSResponse(
						responseAsString, entityType);
			} catch (ParseException e) {
				TestSession.logger.error(e);
			}
			queue.add(consumedResponse);
			consumedResponse.dump();

		}

	}


}
