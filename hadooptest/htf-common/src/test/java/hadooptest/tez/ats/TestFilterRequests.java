package hadooptest.tez.ats;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.cluster.hadoop.HadoopComponent;
import hadooptest.node.hadoop.HadoopNode;

import java.util.Hashtable;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SerialTests.class)
public class TestFilterRequests extends ATSTestsBaseClass {

//	@Test
	public void testFilteringOnEvents() throws Exception {
		ExecutorService execService = Executors.newFixedThreadPool(10);
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


		Map<String, Boolean> expectedEntities = getExpectedFieldsMap(
				EVENTS.EXPECTED, ENTITYTYPE.EXPECTED, ENTITY.EXPECTED,
				STARTTIME.EXPECTED, RELATEDENTITIES.NOT_EXPECTED,
				PRIMARYFILTERS.NOT_EXPECTED, OTHERINFO.NOT_EXPECTED);

		makeRESTCall(execService, rmHostname, EntityTypes.TEZ_DAG_ID,
				"?primaryFilter=dagName:MRRSleepJob&fields=events", dagIdQueue,
				expectedEntities);
		makeRESTCall(execService, rmHostname, EntityTypes.TEZ_CONTAINER_ID,
				"?fields=events", containerIdQueue,
				expectedEntities);
		makeRESTCall(execService, rmHostname, EntityTypes.TEZ_APPLICATION_ATTEMPT,
				"?fields=events", applicationAttemptQueue,
				expectedEntities);
		makeRESTCall(execService, rmHostname, EntityTypes.TEZ_VERTEX_ID,
				"?fields=events", tezVertexIdQueue,
				expectedEntities);
		makeRESTCall(execService, rmHostname, EntityTypes.TEZ_TASK_ID,
				"?fields=events", tezTaskIdQueue,
				expectedEntities);
		makeRESTCall(execService, rmHostname, EntityTypes.TEZ_TASK_ATTEMPT_ID,
				"?fields=events", tezTaskAttemptIdQueue,
				expectedEntities);

		execService.shutdown();
		while (!execService.isTerminated()) {
			TestSession.logger
					.info("Thread sleeping while awaiting DAG ID REST calls");
			Thread.sleep(1000);
		}

		Assert.assertEquals(errorCount.get(), 0);
	}

	@Test
	public void testFilteringOnRelatedentities() throws Exception {
		ExecutorService execService = Executors.newFixedThreadPool(10);
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


		Map<String, Boolean> expectedEntities = getExpectedFieldsMap(
				EVENTS.NOT_EXPECTED, ENTITYTYPE.EXPECTED, ENTITY.EXPECTED,
				STARTTIME.EXPECTED, RELATEDENTITIES.EXPECTED,
				PRIMARYFILTERS.NOT_EXPECTED, OTHERINFO.NOT_EXPECTED);

		makeRESTCall(execService, rmHostname, EntityTypes.TEZ_DAG_ID,
				"?primaryFilter=dagName:MRRSleepJob&fields=relatedentities", dagIdQueue,
				expectedEntities);
		makeRESTCall(execService, rmHostname, EntityTypes.TEZ_CONTAINER_ID,
				"?fields=relatedentities", containerIdQueue,
				expectedEntities);
		makeRESTCall(execService, rmHostname, EntityTypes.TEZ_APPLICATION_ATTEMPT,
				"?fields=relatedentities", applicationAttemptQueue,
				expectedEntities);
		makeRESTCall(execService, rmHostname, EntityTypes.TEZ_VERTEX_ID,
				"?fields=relatedentities", tezVertexIdQueue,
				expectedEntities);
		makeRESTCall(execService, rmHostname, EntityTypes.TEZ_TASK_ID,
				"?fields=relatedentities", tezTaskIdQueue,
				expectedEntities);
		makeRESTCall(execService, rmHostname, EntityTypes.TEZ_TASK_ATTEMPT_ID,
				"?fields=relatedentities", tezTaskAttemptIdQueue,
				expectedEntities);

		execService.shutdown();
		while (!execService.isTerminated()) {
			TestSession.logger
					.info("Thread sleeping while awaiting DAG ID REST calls");
			Thread.sleep(1000);
		}
		Assert.assertEquals(errorCount.get(), 0);
	}

}
