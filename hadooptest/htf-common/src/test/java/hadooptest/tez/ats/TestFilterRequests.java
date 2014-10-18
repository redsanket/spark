package hadooptest.tez.ats;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.cluster.hadoop.HadoopComponent;
import hadooptest.node.hadoop.HadoopNode;
import hadooptest.tez.ats.ATSTestsBaseClass.EntityTypes;

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

		if (!timelineserverStarted) {
			// startTimelineServerOnRM(rmHostname);
		}
		Map<String, Boolean> expectedEntities = getExpectedFieldsMap(
				ResponseComposition.EVENTS.EXPECTED, 
				ResponseComposition.ENTITYTYPE.EXPECTED, 
				ResponseComposition.ENTITY.EXPECTED,
				ResponseComposition.STARTTIME.EXPECTED, 
				ResponseComposition.RELATEDENTITIES.NOT_EXPECTED,
				ResponseComposition.PRIMARYFILTERS.NOT_EXPECTED, 
				ResponseComposition.OTHERINFO.NOT_EXPECTED);
		
		EntityTypes entityTypeInRequest = EntityTypes.TEZ_DAG_ID;
		String url = getATSUrl() + "/" + entityTypeInRequest + "/" + "?fields=events";
		makeHttpCallAndEnqueueConsumedResponse(execService, url,
				HadooptestConstants.UserNames.HITUSR_1,
				entityTypeInRequest, dagIdQueue, expectedEntities);

		entityTypeInRequest = EntityTypes.TEZ_VERTEX_ID;
		url = getATSUrl() + "/" + entityTypeInRequest + "/" + "?fields=events";
		makeHttpCallAndEnqueueConsumedResponse(execService, url,
				HadooptestConstants.UserNames.HITUSR_1,
				entityTypeInRequest, vertexIdQueue, expectedEntities);
		
		entityTypeInRequest = EntityTypes.TEZ_TASK_ID;
		url = getATSUrl() + "/" + entityTypeInRequest + "/" + "?fields=events";
		makeHttpCallAndEnqueueConsumedResponse(execService, url,
				HadooptestConstants.UserNames.HITUSR_1,
				entityTypeInRequest, taskIdQueue, expectedEntities);
		
		entityTypeInRequest = EntityTypes.TEZ_TASK_ATTEMPT_ID;
		url = getATSUrl() + "/" + entityTypeInRequest + "/" + "?fields=events";
		makeHttpCallAndEnqueueConsumedResponse(execService, url,
				HadooptestConstants.UserNames.HITUSR_1,
				entityTypeInRequest, taskAttemptIdQueue, expectedEntities);
		
		entityTypeInRequest = EntityTypes.TEZ_APPLICATION_ATTEMPT;
		url = getATSUrl() + "/" + entityTypeInRequest + "/" + "?fields=events";
		makeHttpCallAndEnqueueConsumedResponse(execService, url,
				HadooptestConstants.UserNames.HITUSR_1,
				entityTypeInRequest, applicationAttemptQueue, expectedEntities);

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

		if (!timelineserverStarted) {
			// startTimelineServerOnRM(rmHostname);
		}
		Map<String, Boolean> expectedEntities = getExpectedFieldsMap(
				ResponseComposition.EVENTS.NOT_EXPECTED, 
				ResponseComposition.ENTITYTYPE.EXPECTED, 
				ResponseComposition.ENTITY.EXPECTED,
				ResponseComposition.STARTTIME.EXPECTED, 
				ResponseComposition.RELATEDENTITIES.EXPECTED,
				ResponseComposition.PRIMARYFILTERS.NOT_EXPECTED, 
				ResponseComposition.OTHERINFO.NOT_EXPECTED);
		String FILTER_STRING = "?fields=relatedentities";
		EntityTypes entityTypeInRequest = EntityTypes.TEZ_DAG_ID;
		String url = getATSUrl() + "/" + entityTypeInRequest + "/" + FILTER_STRING;
		makeHttpCallAndEnqueueConsumedResponse(execService, url,
				HadooptestConstants.UserNames.HITUSR_1,
				entityTypeInRequest, dagIdQueue, expectedEntities);

		entityTypeInRequest = EntityTypes.TEZ_VERTEX_ID;
		url = getATSUrl() + "/" + entityTypeInRequest + "/" + FILTER_STRING;
		makeHttpCallAndEnqueueConsumedResponse(execService, url,
				HadooptestConstants.UserNames.HITUSR_1,
				entityTypeInRequest, vertexIdQueue, expectedEntities);
		
		entityTypeInRequest = EntityTypes.TEZ_TASK_ID;
		url = getATSUrl() + "/" + entityTypeInRequest + "/" + FILTER_STRING;
		makeHttpCallAndEnqueueConsumedResponse(execService, url,
				HadooptestConstants.UserNames.HITUSR_1,
				entityTypeInRequest, taskIdQueue, expectedEntities);
		
		entityTypeInRequest = EntityTypes.TEZ_TASK_ATTEMPT_ID;
		url = getATSUrl() + "/" + entityTypeInRequest + "/" + FILTER_STRING;
		makeHttpCallAndEnqueueConsumedResponse(execService, url,
				HadooptestConstants.UserNames.HITUSR_1,
				entityTypeInRequest, taskAttemptIdQueue, expectedEntities);
		
		entityTypeInRequest = EntityTypes.TEZ_APPLICATION_ATTEMPT;
		url = getATSUrl() + "/" + entityTypeInRequest + "/" + FILTER_STRING;
		makeHttpCallAndEnqueueConsumedResponse(execService, url,
				HadooptestConstants.UserNames.HITUSR_1,
				entityTypeInRequest, applicationAttemptQueue, expectedEntities);

		execService.shutdown();
		while (!execService.isTerminated()) {
			TestSession.logger
					.info("Thread sleeping while awaiting DAG ID REST calls");
			Thread.sleep(1000);
		}

		Assert.assertEquals(errorCount.get(), 0);
	}

	@Test
	public void testTwoComboFiltering() throws Exception {
		ExecutorService execService = Executors.newFixedThreadPool(10);

		if (!timelineserverStarted) {
			// startTimelineServerOnRM(rmHostname);
		}
		String FILTER_STRING = "?primaryFilter=dagName:MRRSleepJob&fields=relatedentities";
		Map<String, Boolean> expectedEntities = getExpectedFieldsMap(
				ResponseComposition.EVENTS.NOT_EXPECTED, 
				ResponseComposition.ENTITYTYPE.EXPECTED, 
				ResponseComposition.ENTITY.EXPECTED,
				ResponseComposition.STARTTIME.EXPECTED, 
				ResponseComposition.RELATEDENTITIES.EXPECTED,
				ResponseComposition.PRIMARYFILTERS.NOT_EXPECTED, 
				ResponseComposition.OTHERINFO.NOT_EXPECTED);
		
		EntityTypes entityTypeInRequest = EntityTypes.TEZ_DAG_ID;
		String url = getATSUrl() + "/" + entityTypeInRequest + "/" + FILTER_STRING;
		makeHttpCallAndEnqueueConsumedResponse(execService, url,
				HadooptestConstants.UserNames.HITUSR_1,
				entityTypeInRequest, dagIdQueue, expectedEntities);

		entityTypeInRequest = EntityTypes.TEZ_VERTEX_ID;
		url = getATSUrl() + "/" + entityTypeInRequest + "/" + FILTER_STRING;
		makeHttpCallAndEnqueueConsumedResponse(execService, url,
				HadooptestConstants.UserNames.HITUSR_1,
				entityTypeInRequest, vertexIdQueue, expectedEntities);
		
		entityTypeInRequest = EntityTypes.TEZ_TASK_ID;
		url = getATSUrl() + "/" + entityTypeInRequest + "/" + FILTER_STRING;
		makeHttpCallAndEnqueueConsumedResponse(execService, url,
				HadooptestConstants.UserNames.HITUSR_1,
				entityTypeInRequest, taskIdQueue, expectedEntities);
		
		entityTypeInRequest = EntityTypes.TEZ_TASK_ATTEMPT_ID;
		url = getATSUrl() + "/" + entityTypeInRequest + "/" + FILTER_STRING;
		makeHttpCallAndEnqueueConsumedResponse(execService, url,
				HadooptestConstants.UserNames.HITUSR_1,
				entityTypeInRequest, taskAttemptIdQueue, expectedEntities);
		
		entityTypeInRequest = EntityTypes.TEZ_APPLICATION_ATTEMPT;
		url = getATSUrl() + "/" + entityTypeInRequest + "/" + FILTER_STRING;
		makeHttpCallAndEnqueueConsumedResponse(execService, url,
				HadooptestConstants.UserNames.HITUSR_1,
				entityTypeInRequest, applicationAttemptQueue, expectedEntities);

		execService.shutdown();
		while (!execService.isTerminated()) {
			TestSession.logger
					.info("Thread sleeping while awaiting REST calls");
			Thread.sleep(1000);
		}

		Assert.assertEquals(errorCount.get(), 0);
	}
	
	@Test
	public void testThreeComboFiltering() throws Exception {
		ExecutorService execService = Executors.newFixedThreadPool(10);

		if (!timelineserverStarted) {
			// startTimelineServerOnRM(rmHostname);
		}
		String FILTER_STRING = "?primaryFilter=dagName:MRRSleepJob&fields=relatedentities&limit=2";
		Map<String, Boolean> expectedEntities = getExpectedFieldsMap(
				ResponseComposition.EVENTS.EXPECTED, 
				ResponseComposition.ENTITYTYPE.EXPECTED, 
				ResponseComposition.ENTITY.EXPECTED,
				ResponseComposition.STARTTIME.EXPECTED, 
				ResponseComposition.RELATEDENTITIES.NOT_EXPECTED,
				ResponseComposition.PRIMARYFILTERS.NOT_EXPECTED, 
				ResponseComposition.OTHERINFO.NOT_EXPECTED);
		
		EntityTypes entityTypeInRequest = EntityTypes.TEZ_DAG_ID;
		String url = getATSUrl() + "/" + entityTypeInRequest + "/" + FILTER_STRING;
		makeHttpCallAndEnqueueConsumedResponse(execService, url,
				HadooptestConstants.UserNames.HITUSR_1,
				entityTypeInRequest, dagIdQueue, expectedEntities);

		entityTypeInRequest = EntityTypes.TEZ_VERTEX_ID;
		url = getATSUrl() + "/" + entityTypeInRequest + "/" + FILTER_STRING;
		makeHttpCallAndEnqueueConsumedResponse(execService, url,
				HadooptestConstants.UserNames.HITUSR_1,
				entityTypeInRequest, vertexIdQueue, expectedEntities);
		
		entityTypeInRequest = EntityTypes.TEZ_TASK_ID;
		url = getATSUrl() + "/" + entityTypeInRequest + "/" + FILTER_STRING;
		makeHttpCallAndEnqueueConsumedResponse(execService, url,
				HadooptestConstants.UserNames.HITUSR_1,
				entityTypeInRequest, taskIdQueue, expectedEntities);
		
		entityTypeInRequest = EntityTypes.TEZ_TASK_ATTEMPT_ID;
		url = getATSUrl() + "/" + entityTypeInRequest + "/" + FILTER_STRING;
		makeHttpCallAndEnqueueConsumedResponse(execService, url,
				HadooptestConstants.UserNames.HITUSR_1,
				entityTypeInRequest, taskAttemptIdQueue, expectedEntities);
		
		entityTypeInRequest = EntityTypes.TEZ_APPLICATION_ATTEMPT;
		url = getATSUrl() + "/" + entityTypeInRequest + "/" + FILTER_STRING;
		makeHttpCallAndEnqueueConsumedResponse(execService, url,
				HadooptestConstants.UserNames.HITUSR_1,
				entityTypeInRequest, applicationAttemptQueue, expectedEntities);

		execService.shutdown();
		while (!execService.isTerminated()) {
			TestSession.logger
					.info("Thread sleeping while awaiting REST calls");
			Thread.sleep(1000);
		}

		Assert.assertEquals(errorCount.get(), 0);
		Assert.assertTrue(dagIdQueue.size() <=2);
		Assert.assertTrue(vertexIdQueue.size() <=2);
		Assert.assertTrue(taskIdQueue.size() <=2);
		Assert.assertTrue(taskAttemptIdQueue.size() <=2);
		Assert.assertTrue(applicationAttemptQueue.size() <=2);
	}

}
