package hadooptest.tez.ats;

import static com.jayway.restassured.RestAssured.given;
import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.cluster.hadoop.HadoopComponent;
import hadooptest.node.hadoop.HadoopNode;
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

import org.junit.Assert;
import org.junit.experimental.categories.Category;

import com.jayway.restassured.response.Response;

@Category(SerialTests.class)
public class TestConcurrentRequests extends ATSTestsBaseClass {
	// @Test
	public void testCompleteResponses() throws Exception {

		if (!timelineserverStarted) {
			// startTimelineServerOnRM(rmHostname);
		}

		String url = getATSUrl()+ "/TEZ_DAG_ID/";
		HtfATSUtils atsUtils = new HtfATSUtils();
		GenericATSResponseBO consumedResponse = atsUtils
				.processATSResponse(responseAsString, EntityTypes.TEZ_DAG_ID,
						expectEverythingMap());
		consumedResponse.dump();
		RunnableRESTCaller tezDagIdCaller = new RunnableRESTCaller("http://" + rmHostname + ":"
				+ HadooptestConstants.Ports.HTTP_ATS_PORT
				+ "/ws/v1/timeline/TEZ_DAG_ID/",
				HadooptestConstants.UserNames.HITUSR_1, EntityTypes.TEZ_DAG_ID,
				dagIdQueue, expectEverythingMap());
		RunnableRESTCaller tezContainerIdCaller = new RunnableRESTCaller("http://" + rmHostname
				+ ":" + HadooptestConstants.Ports.HTTP_ATS_PORT
				+ "/ws/v1/timeline/TEZ_CONTAINER_ID/",
				HadooptestConstants.UserNames.HITUSR_1,
				EntityTypes.TEZ_CONTAINER_ID, containerIdQueue,
				expectEverythingMap());
		RunnableRESTCaller applicationAttemptCaller = new RunnableRESTCaller("http://"
				+ rmHostname + ":" + HadooptestConstants.Ports.HTTP_ATS_PORT
				+ "/ws/v1/timeline/TEZ_APPLICATION_ATTEMPT/",
				HadooptestConstants.UserNames.HITUSR_1,
				EntityTypes.TEZ_APPLICATION_ATTEMPT, applicationAttemptQueue,
				expectEverythingMap());

		RunnableRESTCaller tezVertexIdCaller = new RunnableRESTCaller("http://" + rmHostname
				+ ":" + HadooptestConstants.Ports.HTTP_ATS_PORT
				+ "/ws/v1/timeline/TEZ_VERTEX_ID/",
				HadooptestConstants.UserNames.HITUSR_1,
				EntityTypes.TEZ_VERTEX_ID, tezVertexIdQueue,
				expectEverythingMap());
		RunnableRESTCaller tezTaskIdCaller = new RunnableRESTCaller("http://" + rmHostname
				+ ":" + HadooptestConstants.Ports.HTTP_ATS_PORT
				+ "/ws/v1/timeline/TEZ_TASK_ID/",
				HadooptestConstants.UserNames.HITUSR_1,
				EntityTypes.TEZ_TASK_ID, tezTaskIdQueue, expectEverythingMap());
		RunnableRESTCaller tezTaskAttemptIdCaller = new RunnableRESTCaller("http://"
				+ rmHostname + ":" + HadooptestConstants.Ports.HTTP_ATS_PORT
				+ "/ws/v1/timeline/TEZ_TASK_ATTEMPT_ID/",
				HadooptestConstants.UserNames.HITUSR_1,
				EntityTypes.TEZ_TASK_ATTEMPT_ID, tezTaskAttemptIdQueue,
				expectEverythingMap());

		// tezTaskAttemptIdQueue

		ExecutorService execService = Executors.newFixedThreadPool(10);
		execService.execute(tezDagIdCaller);
		execService.execute(tezContainerIdCaller);
		execService.execute(applicationAttemptCaller);
		execService.execute(tezVertexIdCaller);
		execService.execute(tezTaskIdCaller);
		execService.execute(tezTaskAttemptIdCaller);
		execService.shutdown();
		while (!execService.isTerminated()) {
			TestSession.logger
					.info("Thread sleeping while awaiting REST calls");
			Thread.sleep(1000);
		}

		TestSession.logger.info("DagId queue size:" + dagIdQueue.size());
		TestSession.logger.info("ContainerId queue size:"
				+ containerIdQueue.size());
		Assert.assertTrue(atsUtils.takeFirstItemAndCompareItAgainstAllTheOtherItemsInQueue(dagIdQueue));
		Assert.assertTrue(atsUtils
				.takeFirstItemAndCompareItAgainstAllTheOtherItemsInQueue(applicationAttemptQueue));
		Assert.assertTrue(atsUtils.takeFirstItemAndCompareItAgainstAllTheOtherItemsInQueue(tezVertexIdQueue));

	}

	// @Test
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

		ExecutorService dagIdExecService = Executors.newFixedThreadPool(10);
		ExecutorService vertexIdExecService = Executors.newFixedThreadPool(10);
		ExecutorService taskIdExecService = Executors.newFixedThreadPool(10);
		ExecutorService taskAttemptIdExecService = Executors
				.newFixedThreadPool(10);

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
		List<String> taskIds = new ArrayList<String>();

		List<String> taskAttemptIds = new ArrayList<String>();

		// TEZ_DAG_ID
		makeRESTCall(dagIdExecService, rmHostname, EntityTypes.TEZ_DAG_ID,
				"dag_1412943684044_0003_1", dagIdQueue, expectEverythingMap());
		dagIdExecService.shutdown();
		while (!dagIdExecService.isTerminated()) {
			TestSession.logger
					.info("Thread sleeping while awaiting DAG ID REST calls");
			Thread.sleep(1000);
		}

		vertexIds = atsUtils.getRelatedentities(dagIdQueue.poll(),
				EntityTypes.TEZ_VERTEX_ID.name());
		tezDagIdPrimaryFilter.add("dag_1412943684044_0003_1");
		expectedPrimaryfilters.put("TEZ_DAG_ID", tezDagIdPrimaryFilter);

		// TEZ_VERTEX_ID
		for (String aVertexId : vertexIds) {
			makeRESTCall(vertexIdExecService, rmHostname,
					EntityTypes.TEZ_VERTEX_ID, aVertexId, tezVertexIdQueue,
					expectEverythingMap());
		}
		vertexIdExecService.shutdown();
		while (!vertexIdExecService.isTerminated()) {
			TestSession.logger
					.info("Thread sleeping while awaiting VERTEX ID REST calls");
			Thread.sleep(1000);
		}

		GenericATSResponseBO vertexIdResponse;
		// Drain the queue
		while ((vertexIdResponse = tezVertexIdQueue.poll()) != null) {
			assertPrimaryFiltersDetails(vertexIdResponse,
					expectedPrimaryfilters);
			taskIds.addAll(atsUtils.getRelatedentities(vertexIdResponse,
					EntityTypes.TEZ_TASK_ID.name()));
		}
		expectedPrimaryfilters.put("TEZ_VERTEX_ID", vertexIds);

		// TEZ_TASK_ID
		for (String aTaskId : taskIds) {
			makeRESTCall(taskIdExecService, rmHostname,
					EntityTypes.TEZ_TASK_ID, aTaskId, tezTaskIdQueue,
					expectEverythingMap());
		}
		taskIdExecService.shutdown();
		while (!taskIdExecService.isTerminated()) {
			TestSession.logger
					.info("Thread sleeping while awaiting DAG ID REST calls");
			Thread.sleep(1000);
		}

		GenericATSResponseBO taskIdResponse;
		// Drain the queue
		while ((taskIdResponse = tezTaskIdQueue.poll()) != null) {
			assertPrimaryFiltersDetails(taskIdResponse, expectedPrimaryfilters);
			taskAttemptIds.addAll(atsUtils.getRelatedentities(taskIdResponse,
					EntityTypes.TEZ_TASK_ATTEMPT_ID.name()));
		}
		expectedPrimaryfilters.put("TEZ_TASK_ID", taskIds);

		// TASK_ATTEMPT_ID
		for (String aTaskAttemptId : taskAttemptIds) {
			makeRESTCall(taskAttemptIdExecService, rmHostname,
					EntityTypes.TEZ_TASK_ATTEMPT_ID, aTaskAttemptId,
					tezTaskAttemptIdQueue, expectEverythingMap());
		}

		GenericATSResponseBO taskAttemptIdResponse;
		// Drain the queue
		while ((taskAttemptIdResponse = tezTaskAttemptIdQueue.poll()) != null) {
			assertPrimaryFiltersDetails(taskAttemptIdResponse,
					expectedPrimaryfilters);
		}
		taskAttemptIdExecService.shutdown();
		while (!taskAttemptIdExecService.isTerminated()) {
			TestSession.logger
					.info("Thread sleeping while awaiting REST calls");
			Thread.sleep(1000);
		}

	}


	void assertPrimaryFiltersDetails(GenericATSResponseBO responseBO,
			Map<String, List<String>> expectedPrimaryfiltersMap) {

		Map<String, List<String>> primaryfiltersInResponseMap = responseBO.entities
				.get(0).primaryfilters;
		for (String aFilterKey : primaryfiltersInResponseMap.keySet()) {
			for (String aFilter : primaryfiltersInResponseMap.get(aFilterKey)) {
				Assert.assertTrue(expectedPrimaryfiltersMap.get(aFilterKey)
						.contains(aFilter));
			}
		}
	}

}
