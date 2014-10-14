package hadooptest.tez.ats;

import static com.jayway.restassured.RestAssured.given;
import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.cluster.hadoop.HadoopComponent;
import hadooptest.node.hadoop.HadoopNode;
import hadooptest.tez.ats.ATSTestsBaseClass.EntityTypes;
import hadooptest.tez.utils.HtfATSUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.json.simple.parser.ParseException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.jayway.restassured.response.Response;

@Category(SerialTests.class)
public class TestConcurrentRequests extends ATSTestsBaseClass {

	// @Test
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
		HtfATSUtils atsUtils = new HtfATSUtils();
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
		RestCaller applicationAttemptCaller = new RestCaller("http://"
				+ rmHostname + ":" + HadooptestConstants.Ports.HTTP_ATS_PORT
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
		RestCaller tezTaskAttemptIdCaller = new RestCaller("http://"
				+ rmHostname + ":" + HadooptestConstants.Ports.HTTP_ATS_PORT
				+ "/ws/v1/timeline/TEZ_TASK_ATTEMPT_ID/",
				HadooptestConstants.UserNames.HITUSR_1,
				EntityTypes.TEZ_TASK_ATTEMPT_ID, tezTaskAttemptIdQueue);

		// tezTaskAttemptIdQueue

		ExecutorService execService = Executors.newFixedThreadPool(10);
		for (int xx = 0; xx < 50; xx++) {
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
		Assert.assertTrue(atsUtils.compareSimilarResponses(dagIdQueue));
		// Assert.assertTrue(atsUtils.compareSimilar(containerIdQueue));
		Assert.assertTrue(atsUtils
				.compareSimilarResponses(applicationAttemptQueue));
		Assert.assertTrue(atsUtils.compareSimilarResponses(tezVertexIdQueue));

	}

	@Test
	public void testCascading() throws Exception {
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

		HtfATSUtils atsUtils = new HtfATSUtils();

		ExecutorService execService = Executors.newFixedThreadPool(10);

		Queue<GenericATSResponseBO> dagIdQueue = new ConcurrentLinkedQueue<GenericATSResponseBO>();
		Queue<GenericATSResponseBO> containerIdQueue = new ConcurrentLinkedQueue<GenericATSResponseBO>();
		Queue<GenericATSResponseBO> applicationAttemptQueue = new ConcurrentLinkedQueue<GenericATSResponseBO>();
		Queue<GenericATSResponseBO> tezVertexIdQueue = new ConcurrentLinkedQueue<GenericATSResponseBO>();
		Queue<GenericATSResponseBO> tezTaskIdQueue = new ConcurrentLinkedQueue<GenericATSResponseBO>();
		Queue<GenericATSResponseBO> tezTaskAttemptIdQueue = new ConcurrentLinkedQueue<GenericATSResponseBO>();
		// PrimaryFilters
		List<String> tezDagIdPrimaryFilter = new ArrayList<String>();
		Map<String, List<String>> expectedPrimaryfilters = new HashMap<String, List<String>>();
		List<String> vertexIds = new ArrayList<String>();
		List<String> taskIds = new ArrayList<String>();;
		List<String> taskAttemptIds = new ArrayList<String>();;

		// TEZ_DAG_ID
		makeRESTCall(execService, rmHostname, EntityTypes.TEZ_DAG_ID, "dag_1412943684044_0003_1", dagIdQueue);
		vertexIds = atsUtils.givenDagIdResponseRetrieveVertexIds(dagIdQueue.poll());
		tezDagIdPrimaryFilter.add("dag_1412943684044_0003_1");
		expectedPrimaryfilters.put("TEZ_DAG_ID", tezDagIdPrimaryFilter);
		
		// TEZ_VERTEX_ID
		for (String aVertexId : vertexIds) {
			makeRESTCall(execService, rmHostname, EntityTypes.TEZ_VERTEX_ID, aVertexId, tezVertexIdQueue);
		}
		GenericATSResponseBO vertexIdResponse;		
		// Drain the queue
		while ((vertexIdResponse = tezVertexIdQueue.poll()) != null) {
			assertPrimaryFiltersDetails(vertexIdResponse,expectedPrimaryfilters);
			taskIds.addAll(atsUtils.givenVertexIdResponseRetrieveTaskIds(vertexIdResponse));
		}
		expectedPrimaryfilters.put("TEZ_VERTEX_ID", vertexIds);

		// TEZ_TASK_ID
		for (String aTaskId : taskIds) {
			makeRESTCall(execService, rmHostname, EntityTypes.TEZ_TASK_ID, aTaskId, tezTaskIdQueue);
		}		
		GenericATSResponseBO taskIdResponse;
		// Drain the queue
		while ((taskIdResponse = tezTaskIdQueue.poll()) != null) {
			assertPrimaryFiltersDetails(taskIdResponse, expectedPrimaryfilters);
			taskAttemptIds.addAll(atsUtils.givenTaskIdResponseRetrieveTaskAttemptIds(taskIdResponse));
		}
		expectedPrimaryfilters.put("TEZ_TASK_ID", taskIds);
		
		// TASK_ATTEMPT_ID
		for (String aTaskAttemptId : taskAttemptIds) {
			makeRESTCall(execService, rmHostname, EntityTypes.TEZ_TASK_ATTEMPT_ID, aTaskAttemptId, tezTaskAttemptIdQueue);
		}		
		GenericATSResponseBO taskAttemptIdResponse;
		// Drain the queue
		while ((taskAttemptIdResponse = tezTaskAttemptIdQueue.poll()) != null) {
			assertPrimaryFiltersDetails(taskAttemptIdResponse, expectedPrimaryfilters);
			taskAttemptIds.addAll(atsUtils.givenTaskIdResponseRetrieveTaskAttemptIds(taskIdResponse));
		}

	}

	void makeRESTCall(ExecutorService execService,String rmHostname,
			EntityTypes entityType, String resource, Queue<GenericATSResponseBO> queue)
			throws InterruptedException {
		RestCaller tezDagIdCaller = new RestCaller("http://" + rmHostname + ":"
				+ HadooptestConstants.Ports.HTTP_ATS_PORT
				+ "/ws/v1/timeline/" + entityType+"/" + resource,
				HadooptestConstants.UserNames.HITUSR_1, entityType,
				queue);
		execService.execute(tezDagIdCaller);
		execService.awaitTermination(2, TimeUnit.SECONDS);

	}

	void assertPrimaryFiltersDetails(GenericATSResponseBO responseBO,
			Map<String, List<String>> expectedPrimaryfiltersMap) {

		Map<String, List<String>> primaryfiltersInResponseMap = responseBO.entities
				.get(0).primaryfilters;
		for (String aFilterKey:primaryfiltersInResponseMap.keySet()){
			for (String aFilter:primaryfiltersInResponseMap.get(aFilterKey)){
				Assert.assertTrue(expectedPrimaryfiltersMap.get(aFilterKey).contains(aFilter));
			}
		}
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
			HtfATSUtils atsUtils = new HtfATSUtils();
			GenericATSResponseBO consumedResponse = null;
			try {
				consumedResponse = atsUtils.processATSResponse(
						responseAsString, entityType);
			} catch (ParseException e) {
				TestSession.logger.error(e);
			}
			queue.add(consumedResponse);
			// consumedResponse.dump();

		}

	}

}
