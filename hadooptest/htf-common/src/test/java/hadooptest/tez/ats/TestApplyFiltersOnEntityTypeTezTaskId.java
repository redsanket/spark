package hadooptest.tez.ats;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.tez.ats.ATSTestsBaseClass.ResponseComposition;
import hadooptest.tez.ats.SeedData.DAG;
import hadooptest.tez.ats.SeedData.DAG.Vertex;
import hadooptest.tez.ats.SeedData.DAG.Vertex.Task;
import hadooptest.tez.examples.extensions.OrderedWordCountExtendedForHtf;
import hadooptest.tez.examples.extensions.SimpleSessionExampleExtendedForTezHTF;
import hadooptest.tez.utils.HtfATSUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SerialTests.class)
public class TestApplyFiltersOnEntityTypeTezTaskId extends ATSTestsBaseClass {
	@Test
	@Ignore("http://bug.corp.yahoo.com/show_bug.cgi?id=7166198")
	public void testExpectEverythingFilterOnVertexId() throws Exception {
		ExecutorService execService = Executors.newFixedThreadPool(1);
		if (!timelineserverStarted) {
			// startTimelineServerOnRM(rmHostname);
		}
		int LIMIT = 2;
		/**
		 * There is just 1 DAG for OrderedWordCount
		 */
		for (Vertex vertex : seedDataForAutoLaunchedOrderedWordCount.dags
				.get(0).vertices) {
			String addendum = "?primaryFilter=TEZ_VERTEX_ID:" + vertex.id
					+ "&limit=" + LIMIT;

			EntityTypes entityTypeInRequest = EntityTypes.TEZ_TASK_ATTEMPT_ID;
			String url = getATSUrl() + entityTypeInRequest + addendum;
			makeHttpCallAndEnqueueConsumedResponse(execService, url,
					seedDataForAutoLaunchedOrderedWordCount.appStartedByUser,
					entityTypeInRequest, taskIdQueue, expectEverythingMap());

			execService.shutdown();
			while (!execService.isTerminated()) {
				Thread.sleep(1000);
			}
			GenericATSResponseBO genericATSResponse = taskIdQueue.poll();
			int entityCount = genericATSResponse.entities.size();
			Assert.assertTrue(entityCount > 0 && entityCount <= LIMIT);
		}

	}

	@Test
	public void testExpectOnlyEventsFilterOnVertexId() throws Exception {
		
		if (!timelineserverStarted) {
			// startTimelineServerOnRM(rmHostname);
		}
		int LIMIT = 2;
		/**
		 * There is just 1 DAG for OrderedWordCount
		 */
		for (Vertex vertex : seedDataForAutoLaunchedOrderedWordCount.dags
				.get(0).vertices) {
			ExecutorService execService = Executors.newFixedThreadPool(1);
			String addendum = "?primaryFilter=TEZ_VERTEX_ID:" + vertex.id
					+ "&fields=events&limit=" + LIMIT;
			HtfATSUtils atsUtils = new HtfATSUtils();
			Map<String, Boolean> expectedEntities = getExpectedFieldsMap(
					ResponseComposition.EVENTS.EXPECTED,
					ResponseComposition.ENTITYTYPE.EXPECTED,
					ResponseComposition.ENTITY.EXPECTED,
					ResponseComposition.STARTTIME.EXPECTED,
					ResponseComposition.RELATEDENTITIES.NOT_EXPECTED,
					ResponseComposition.PRIMARYFILTERS.NOT_EXPECTED,
					ResponseComposition.OTHERINFO.NOT_EXPECTED);

			EntityTypes entityTypeInRequest = EntityTypes.TEZ_TASK_ATTEMPT_ID;
			String url = getATSUrl() + entityTypeInRequest + addendum;
			makeHttpCallAndEnqueueConsumedResponse(execService, url,
					HadooptestConstants.UserNames.HITUSR_1,
					entityTypeInRequest, taskIdQueue, expectedEntities);

			execService.shutdown();
			while (!execService.isTerminated()) {
				Thread.sleep(1000);
			}

			Assert.assertEquals(0, errorCount.get());
			GenericATSResponseBO genericAtsResponseBo = taskIdQueue.poll();
			int entityCount = genericAtsResponseBo.entities.size();
			Assert.assertTrue(entityCount > 0 && entityCount <= LIMIT);
		}

	}

	@Test
	@Ignore("http://bug.corp.yahoo.com/show_bug.cgi?id=7166198")
	public void testExpectOnlyRelatedEntitiesFilterOnVertexId()
			throws Exception {
		ExecutorService execService = Executors.newFixedThreadPool(1);
		if (!timelineserverStarted) {
			// startTimelineServerOnRM(rmHostname);
		}
		int LIMIT = 2;
		/**
		 * There is just 1 DAG for OrderedWordCount
		 */
		for (Vertex vertex : seedDataForAutoLaunchedOrderedWordCount.dags
				.get(0).vertices) {

			String addendum = "?primaryFilter=TEZ_VERTEX_ID:" + vertex.id
					+ "&fields=relatedentities&limit=" + LIMIT;

			Map<String, Boolean> expectedEntities = getExpectedFieldsMap(
					ResponseComposition.EVENTS.NOT_EXPECTED,
					ResponseComposition.ENTITYTYPE.EXPECTED,
					ResponseComposition.ENTITY.EXPECTED,
					ResponseComposition.STARTTIME.EXPECTED,
					ResponseComposition.RELATEDENTITIES.EXPECTED,
					ResponseComposition.PRIMARYFILTERS.NOT_EXPECTED,
					ResponseComposition.OTHERINFO.NOT_EXPECTED);

			EntityTypes entityTypeInRequest = EntityTypes.TEZ_TASK_ATTEMPT_ID;
			String url = getATSUrl() + entityTypeInRequest + addendum;
			makeHttpCallAndEnqueueConsumedResponse(execService, url,
					HadooptestConstants.UserNames.HITUSR_1,
					entityTypeInRequest, taskIdQueue, expectedEntities);

			execService.shutdown();
			while (!execService.isTerminated()) {
				Thread.sleep(1000);
			}

			Assert.assertEquals(0, errorCount.get());
			GenericATSResponseBO genericAtsResponseBo = taskIdQueue.poll();
			int entityCount = genericAtsResponseBo.entities.size();
			Assert.assertTrue(entityCount > 0 && entityCount <= LIMIT);
		}

	}

	@Test
	public void testExpectOnlyOtherInfoFilterOnVertexId() throws Exception {
		
		if (!timelineserverStarted) {
			// startTimelineServerOnRM(rmHostname);
		}
		int LIMIT = 2;
		/**
		 * There is just 1 DAG for OrderedWordCount
		 */
		for (Vertex vertex : seedDataForAutoLaunchedOrderedWordCount.dags
				.get(0).vertices) {
			ExecutorService execService = Executors.newFixedThreadPool(1);
			String addendum = "?primaryFilter=TEZ_VERTEX_ID:" + vertex.id
					+ "&fields=otherinfo&limit=" + LIMIT;
			HtfATSUtils atsUtils = new HtfATSUtils();
			Map<String, Boolean> expectedEntities = getExpectedFieldsMap(
					ResponseComposition.EVENTS.NOT_EXPECTED,
					ResponseComposition.ENTITYTYPE.EXPECTED,
					ResponseComposition.ENTITY.EXPECTED,
					ResponseComposition.STARTTIME.EXPECTED,
					ResponseComposition.RELATEDENTITIES.NOT_EXPECTED,
					ResponseComposition.PRIMARYFILTERS.NOT_EXPECTED,
					ResponseComposition.OTHERINFO.EXPECTED);

			EntityTypes entityTypeInRequest = EntityTypes.TEZ_TASK_ATTEMPT_ID;
			String url = getATSUrl() + entityTypeInRequest + addendum;
			makeHttpCallAndEnqueueConsumedResponse(execService, url,
					HadooptestConstants.UserNames.HITUSR_1,
					entityTypeInRequest, taskIdQueue, expectedEntities);
			execService.shutdown();
			while (!execService.isTerminated()) {
				Thread.sleep(1000);
			}
			Assert.assertEquals(0, errorCount.get());
			GenericATSResponseBO genericAtsResponseBo = taskIdQueue.poll();
			int entityCount = genericAtsResponseBo.entities.size();
			Assert.assertTrue(entityCount > 0 && entityCount <= LIMIT);
		}

	}

	@Test
	public void testExpectOnlyPrimaryFilterFilterOnVertexId() throws Exception {
		
		if (!timelineserverStarted) {
			// startTimelineServerOnRM(rmHostname);
		}
		int LIMIT = 2;
		/**
		 * There is just 1 DAG for OrderedWordCount
		 */
		for (Vertex vertex : seedDataForAutoLaunchedOrderedWordCount.dags
				.get(0).vertices) {
			ExecutorService execService = Executors.newFixedThreadPool(10);
			String addendum = "?primaryFilter=TEZ_VERTEX_ID:" + vertex.id
					+ "&fields=primaryfilters&limit=" + LIMIT;
			Map<String, Boolean> expectedEntities = getExpectedFieldsMap(
					ResponseComposition.EVENTS.NOT_EXPECTED,
					ResponseComposition.ENTITYTYPE.EXPECTED,
					ResponseComposition.ENTITY.EXPECTED,
					ResponseComposition.STARTTIME.EXPECTED,
					ResponseComposition.RELATEDENTITIES.NOT_EXPECTED,
					ResponseComposition.PRIMARYFILTERS.EXPECTED,
					ResponseComposition.OTHERINFO.NOT_EXPECTED);

			EntityTypes entityTypeInRequest = EntityTypes.TEZ_TASK_ATTEMPT_ID;
			String url = getATSUrl() + entityTypeInRequest + addendum;
			makeHttpCallAndEnqueueConsumedResponse(execService, url,
					HadooptestConstants.UserNames.HITUSR_1,
					entityTypeInRequest, taskIdQueue, expectedEntities);
			execService.shutdown();
			while (!execService.isTerminated()) {
				Thread.sleep(1000);
			}
			Assert.assertEquals(0, errorCount.get());
			GenericATSResponseBO genericAtsResponseBo = taskIdQueue.poll();
			int entityCount = genericAtsResponseBo.entities.size();
			Assert.assertTrue(entityCount > 0 && entityCount <= LIMIT);
		}

	}

	// FILTER ON DAG ID

	@Test
	@Ignore("http://bug.corp.yahoo.com/show_bug.cgi?id=7166198")
	public void testExpectEverythingFilterOnDagId() throws Exception {
		ExecutorService execService = Executors.newFixedThreadPool(1);
		if (!timelineserverStarted) {
			// startTimelineServerOnRM(rmHostname);
		}
		int LIMIT = 2;
		/**
		 * There is just 1 DAG for OrderedWordCount
		 */
		for (DAG dag : seedDataForAutoLaunchedOrderedWordCount.dags) {
			String addendum = "?primaryFilter=TEZ_DAG_ID:" + dag.id + "&limit="
					+ LIMIT;

			EntityTypes entityTypeInRequest = EntityTypes.TEZ_TASK_ATTEMPT_ID;
			String url = getATSUrl() + entityTypeInRequest + addendum;
			makeHttpCallAndEnqueueConsumedResponse(execService, url,
					seedDataForAutoLaunchedOrderedWordCount.appStartedByUser,
					entityTypeInRequest, taskIdQueue, expectEverythingMap());

			execService.shutdown();
			while (!execService.isTerminated()) {
				Thread.sleep(1000);
			}
			GenericATSResponseBO genericATSResponse = taskIdQueue.poll();
			int entityCount = genericATSResponse.entities.size();
			Assert.assertTrue(entityCount > 0 && entityCount <= LIMIT);
		}

	}

	@Test
	public void testExpectOnlyEventsFilterOnDagId() throws Exception {
		ExecutorService execService = Executors.newFixedThreadPool(1);
		if (!timelineserverStarted) {
			// startTimelineServerOnRM(rmHostname);
		}
		int LIMIT = 2;
		/**
		 * There is just 1 DAG for OrderedWordCount
		 */
		for (DAG dag : seedDataForAutoLaunchedOrderedWordCount.dags) {

			String addendum = "?primaryFilter=TEZ_DAG_ID:" + dag.id
					+ "&fields=events&limit=" + LIMIT;
			HtfATSUtils atsUtils = new HtfATSUtils();
			Map<String, Boolean> expectedEntities = getExpectedFieldsMap(
					ResponseComposition.EVENTS.EXPECTED,
					ResponseComposition.ENTITYTYPE.EXPECTED,
					ResponseComposition.ENTITY.EXPECTED,
					ResponseComposition.STARTTIME.EXPECTED,
					ResponseComposition.RELATEDENTITIES.NOT_EXPECTED,
					ResponseComposition.PRIMARYFILTERS.NOT_EXPECTED,
					ResponseComposition.OTHERINFO.NOT_EXPECTED);

			EntityTypes entityTypeInRequest = EntityTypes.TEZ_TASK_ATTEMPT_ID;
			String url = getATSUrl() + entityTypeInRequest + addendum;
			makeHttpCallAndEnqueueConsumedResponse(execService, url,
					HadooptestConstants.UserNames.HITUSR_1,
					entityTypeInRequest, taskIdQueue, expectedEntities);

			execService.shutdown();
			while (!execService.isTerminated()) {
				Thread.sleep(1000);
			}

			Assert.assertEquals(0, errorCount.get());
			GenericATSResponseBO genericAtsResponseBo = taskIdQueue.poll();
			int entityCount = genericAtsResponseBo.entities.size();
			Assert.assertTrue(entityCount > 0 && entityCount <= LIMIT);
		}

	}

	@Test
	@Ignore("http://bug.corp.yahoo.com/show_bug.cgi?id=7166198")
	public void testExpectOnlyRelatedEntitiesFilterOnDagId() throws Exception {
		ExecutorService execService = Executors.newFixedThreadPool(1);
		if (!timelineserverStarted) {
			// startTimelineServerOnRM(rmHostname);
		}
		int LIMIT = 2;
		/**
		 * There is just 1 DAG for OrderedWordCount
		 */
		for (DAG dag : seedDataForAutoLaunchedOrderedWordCount.dags) {
			String addendum = "?primaryFilter=TEZ_DAG_ID:" + dag.id
					+ "&fields=relatedentities&limit=" + LIMIT;

			Map<String, Boolean> expectedEntities = getExpectedFieldsMap(
					ResponseComposition.EVENTS.NOT_EXPECTED,
					ResponseComposition.ENTITYTYPE.EXPECTED,
					ResponseComposition.ENTITY.EXPECTED,
					ResponseComposition.STARTTIME.EXPECTED,
					ResponseComposition.RELATEDENTITIES.EXPECTED,
					ResponseComposition.PRIMARYFILTERS.NOT_EXPECTED,
					ResponseComposition.OTHERINFO.NOT_EXPECTED);

			EntityTypes entityTypeInRequest = EntityTypes.TEZ_TASK_ATTEMPT_ID;
			String url = getATSUrl() + entityTypeInRequest + addendum;
			makeHttpCallAndEnqueueConsumedResponse(execService, url,
					HadooptestConstants.UserNames.HITUSR_1,
					entityTypeInRequest, taskIdQueue, expectedEntities);

			execService.shutdown();
			while (!execService.isTerminated()) {
				Thread.sleep(1000);
			}

			Assert.assertEquals(0, errorCount.get());
			GenericATSResponseBO genericAtsResponseBo = taskIdQueue.poll();
			int entityCount = genericAtsResponseBo.entities.size();
			Assert.assertTrue(entityCount > 0 && entityCount <= LIMIT);
		}

	}

	@Test
	public void testExpectOnlyOtherInfoFilterOnDagId() throws Exception {
		ExecutorService execService = Executors.newFixedThreadPool(1);
		if (!timelineserverStarted) {
			// startTimelineServerOnRM(rmHostname);
		}
		int LIMIT = 2;
		/**
		 * There is just 1 DAG for OrderedWordCount
		 */
		for (DAG dag : seedDataForAutoLaunchedOrderedWordCount.dags) {
			String addendum = "?primaryFilter=TEZ_DAG_ID:" + dag.id
					+ "&fields=otherinfo&limit=" + LIMIT;
			HtfATSUtils atsUtils = new HtfATSUtils();
			Map<String, Boolean> expectedEntities = getExpectedFieldsMap(
					ResponseComposition.EVENTS.NOT_EXPECTED,
					ResponseComposition.ENTITYTYPE.EXPECTED,
					ResponseComposition.ENTITY.EXPECTED,
					ResponseComposition.STARTTIME.EXPECTED,
					ResponseComposition.RELATEDENTITIES.NOT_EXPECTED,
					ResponseComposition.PRIMARYFILTERS.NOT_EXPECTED,
					ResponseComposition.OTHERINFO.EXPECTED);

			EntityTypes entityTypeInRequest = EntityTypes.TEZ_TASK_ATTEMPT_ID;
			String url = getATSUrl() + entityTypeInRequest + addendum;
			makeHttpCallAndEnqueueConsumedResponse(execService, url,
					HadooptestConstants.UserNames.HITUSR_1,
					entityTypeInRequest, taskIdQueue, expectedEntities);
			execService.shutdown();
			while (!execService.isTerminated()) {
				Thread.sleep(1000);
			}
			Assert.assertEquals(0, errorCount.get());
			GenericATSResponseBO genericAtsResponseBo = taskIdQueue.poll();
			int entityCount = genericAtsResponseBo.entities.size();
			Assert.assertTrue(entityCount > 0 && entityCount <= LIMIT);
		}

	}

	@Test
	public void testExpectOnlyPrimaryFilterFilterOnDagId() throws Exception {
		ExecutorService execService = Executors.newFixedThreadPool(10);
		if (!timelineserverStarted) {
			// startTimelineServerOnRM(rmHostname);
		}
		int LIMIT = 2;
		/**
		 * There is just 1 DAG for OrderedWordCount
		 */
		for (DAG dag : seedDataForAutoLaunchedOrderedWordCount.dags) {
			String addendum = "?primaryFilter=TEZ_DAG_ID:" + dag.id
					+ "&fields=primaryfilters&limit=" + LIMIT;
			Map<String, Boolean> expectedEntities = getExpectedFieldsMap(
					ResponseComposition.EVENTS.NOT_EXPECTED,
					ResponseComposition.ENTITYTYPE.EXPECTED,
					ResponseComposition.ENTITY.EXPECTED,
					ResponseComposition.STARTTIME.EXPECTED,
					ResponseComposition.RELATEDENTITIES.NOT_EXPECTED,
					ResponseComposition.PRIMARYFILTERS.EXPECTED,
					ResponseComposition.OTHERINFO.NOT_EXPECTED);

			EntityTypes entityTypeInRequest = EntityTypes.TEZ_TASK_ATTEMPT_ID;
			String url = getATSUrl() + entityTypeInRequest + addendum;
			makeHttpCallAndEnqueueConsumedResponse(execService, url,
					HadooptestConstants.UserNames.HITUSR_1,
					entityTypeInRequest, taskIdQueue, expectedEntities);
			execService.shutdown();
			while (!execService.isTerminated()) {
				Thread.sleep(1000);
			}
			Assert.assertEquals(0, errorCount.get());
			GenericATSResponseBO genericAtsResponseBo = taskIdQueue.poll();
			int entityCount = genericAtsResponseBo.entities.size();
			Assert.assertTrue(entityCount > 0 && entityCount <= LIMIT);
		}

	}

	// FILTER ON TASK ID

	@Test
	public void testExpectEverythingFilterOnTaskId() throws Exception {
		
		if (!timelineserverStarted) {
			// startTimelineServerOnRM(rmHostname);
		}
		int LIMIT = 2;
		/**
		 * There is just 1 DAG for OrderedWordCount
		 */
		for (DAG dag : seedDataForAutoLaunchedOrderedWordCount.dags) {
			for (Vertex vertex : dag.vertices) {
				for (Task task : vertex.tasks) {
					ExecutorService execService = Executors.newFixedThreadPool(1);
					String addendum = "?primaryFilter=TEZ_TASK_ID:" + task.id
							+ "&limit=" + LIMIT;

					EntityTypes entityTypeInRequest = EntityTypes.TEZ_TASK_ATTEMPT_ID;
					String url = getATSUrl() + entityTypeInRequest + addendum;
					makeHttpCallAndEnqueueConsumedResponse(
							execService,
							url,
							seedDataForAutoLaunchedOrderedWordCount.appStartedByUser,
							entityTypeInRequest, taskIdQueue,
							expectEverythingMap());

					execService.shutdown();
					while (!execService.isTerminated()) {
						Thread.sleep(1000);
					}
					GenericATSResponseBO genericATSResponse = taskIdQueue
							.poll();
					int entityCount = genericATSResponse.entities.size();
					Assert.assertTrue(entityCount > 0 && entityCount <= LIMIT);
				}
			}
		}

	}

	@Test
	public void testExpectOnlyEventsFilterOnTaskId() throws Exception {
		
		if (!timelineserverStarted) {
			// startTimelineServerOnRM(rmHostname);
		}
		int LIMIT = 2;
		/**
		 * There is just 1 DAG for OrderedWordCount
		 */
		for (DAG dag : seedDataForAutoLaunchedOrderedWordCount.dags) {
			for (Vertex vertex : dag.vertices) {
				for (Task task : vertex.tasks) {
					ExecutorService execService = Executors.newFixedThreadPool(1);
					String addendum = "?primaryFilter=TEZ_TASK_ID:" + task.id
							+ "&fields=events&limit=" + LIMIT;
					HtfATSUtils atsUtils = new HtfATSUtils();
					Map<String, Boolean> expectedEntities = getExpectedFieldsMap(
							ResponseComposition.EVENTS.EXPECTED,
							ResponseComposition.ENTITYTYPE.EXPECTED,
							ResponseComposition.ENTITY.EXPECTED,
							ResponseComposition.STARTTIME.EXPECTED,
							ResponseComposition.RELATEDENTITIES.NOT_EXPECTED,
							ResponseComposition.PRIMARYFILTERS.NOT_EXPECTED,
							ResponseComposition.OTHERINFO.NOT_EXPECTED);

					EntityTypes entityTypeInRequest = EntityTypes.TEZ_TASK_ATTEMPT_ID;
					String url = getATSUrl() + entityTypeInRequest + addendum;
					makeHttpCallAndEnqueueConsumedResponse(execService, url,
							HadooptestConstants.UserNames.HITUSR_1,
							entityTypeInRequest, taskIdQueue, expectedEntities);

					execService.shutdown();
					while (!execService.isTerminated()) {
						Thread.sleep(1000);
					}

					Assert.assertEquals(0, errorCount.get());
					GenericATSResponseBO genericAtsResponseBo = taskIdQueue
							.poll();
					int entityCount = genericAtsResponseBo.entities.size();
					Assert.assertTrue(entityCount > 0 && entityCount <= LIMIT);
				}
			}
		}

	}

	@Test
	public void testExpectOnlyRelatedEntitiesFilterOnTaskId() throws Exception {
		
		if (!timelineserverStarted) {
			// startTimelineServerOnRM(rmHostname);
		}
		int LIMIT = 2;
		/**
		 * There is just 1 DAG for OrderedWordCount
		 */
		for (DAG dag : seedDataForAutoLaunchedOrderedWordCount.dags) {
			for (Vertex vertex : dag.vertices) {
				for (Task task : vertex.tasks) {
					ExecutorService execService = Executors.newFixedThreadPool(1);
					String addendum = "?primaryFilter=TEZ_TASK_ID:" + task.id
							+ "&fields=relatedentities&limit=" + LIMIT;

					Map<String, Boolean> expectedEntities = getExpectedFieldsMap(
							ResponseComposition.EVENTS.NOT_EXPECTED,
							ResponseComposition.ENTITYTYPE.EXPECTED,
							ResponseComposition.ENTITY.EXPECTED,
							ResponseComposition.STARTTIME.EXPECTED,
							ResponseComposition.RELATEDENTITIES.EXPECTED,
							ResponseComposition.PRIMARYFILTERS.NOT_EXPECTED,
							ResponseComposition.OTHERINFO.NOT_EXPECTED);

					EntityTypes entityTypeInRequest = EntityTypes.TEZ_TASK_ATTEMPT_ID;
					String url = getATSUrl() + entityTypeInRequest + addendum;
					makeHttpCallAndEnqueueConsumedResponse(execService, url,
							HadooptestConstants.UserNames.HITUSR_1,
							entityTypeInRequest, taskIdQueue, expectedEntities);

					execService.shutdown();
					while (!execService.isTerminated()) {
						Thread.sleep(1000);
					}

					Assert.assertEquals(0, errorCount.get());
					GenericATSResponseBO genericAtsResponseBo = taskIdQueue
							.poll();
					int entityCount = genericAtsResponseBo.entities.size();
					Assert.assertTrue(entityCount > 0 && entityCount <= LIMIT);
				}
			}
		}
	}

	@Test
	public void testExpectOnlyOtherInfoFilterOnTaskId() throws Exception {
		ExecutorService execService = Executors.newFixedThreadPool(1);
		if (!timelineserverStarted) {
			// startTimelineServerOnRM(rmHostname);
		}
		int LIMIT = 2;
		/**
		 * There is just 1 DAG for OrderedWordCount
		 */
		for (DAG dag : seedDataForAutoLaunchedOrderedWordCount.dags) {
			for (Vertex vertex : dag.vertices) {
				for (Task task : vertex.tasks) {

					String addendum = "?primaryFilter=TEZ_TASK_ID:" + task.id
							+ "&fields=otherinfo&limit=" + LIMIT;
					HtfATSUtils atsUtils = new HtfATSUtils();
					Map<String, Boolean> expectedEntities = getExpectedFieldsMap(
							ResponseComposition.EVENTS.NOT_EXPECTED,
							ResponseComposition.ENTITYTYPE.EXPECTED,
							ResponseComposition.ENTITY.EXPECTED,
							ResponseComposition.STARTTIME.EXPECTED,
							ResponseComposition.RELATEDENTITIES.NOT_EXPECTED,
							ResponseComposition.PRIMARYFILTERS.NOT_EXPECTED,
							ResponseComposition.OTHERINFO.EXPECTED);

					EntityTypes entityTypeInRequest = EntityTypes.TEZ_TASK_ATTEMPT_ID;
					String url = getATSUrl() + entityTypeInRequest + addendum;
					makeHttpCallAndEnqueueConsumedResponse(execService, url,
							HadooptestConstants.UserNames.HITUSR_1,
							entityTypeInRequest, taskIdQueue, expectedEntities);
					execService.shutdown();
					while (!execService.isTerminated()) {
						Thread.sleep(1000);
					}
					Assert.assertEquals(0, errorCount.get());
					GenericATSResponseBO genericAtsResponseBo = taskIdQueue
							.poll();
					int entityCount = genericAtsResponseBo.entities.size();
					Assert.assertTrue(entityCount > 0 && entityCount <= LIMIT);
				}
			}
		}

	}

	@Test
	public void testExpectOnlyPrimaryFilterFilterOnTaskId() throws Exception {
		
		if (!timelineserverStarted) {
			// startTimelineServerOnRM(rmHostname);
		}
		int LIMIT = 2;
		/**
		 * There is just 1 DAG for OrderedWordCount
		 */
		for (DAG dag : seedDataForAutoLaunchedOrderedWordCount.dags) {
			for (Vertex vertex : dag.vertices) {
				for (Task task : vertex.tasks) {
					ExecutorService execService = Executors.newFixedThreadPool(10);
					String addendum = "?primaryFilter=TEZ_TASK_ID:" + task.id
							+ "&fields=primaryfilters&limit=" + LIMIT;
					Map<String, Boolean> expectedEntities = getExpectedFieldsMap(
							ResponseComposition.EVENTS.NOT_EXPECTED,
							ResponseComposition.ENTITYTYPE.EXPECTED,
							ResponseComposition.ENTITY.EXPECTED,
							ResponseComposition.STARTTIME.EXPECTED,
							ResponseComposition.RELATEDENTITIES.NOT_EXPECTED,
							ResponseComposition.PRIMARYFILTERS.EXPECTED,
							ResponseComposition.OTHERINFO.NOT_EXPECTED);

					EntityTypes entityTypeInRequest = EntityTypes.TEZ_TASK_ATTEMPT_ID;
					String url = getATSUrl() + entityTypeInRequest + addendum;
					makeHttpCallAndEnqueueConsumedResponse(execService, url,
							HadooptestConstants.UserNames.HITUSR_1,
							entityTypeInRequest, taskIdQueue, expectedEntities);
					execService.shutdown();
					while (!execService.isTerminated()) {
						Thread.sleep(1000);
					}
					Assert.assertEquals(0, errorCount.get());
					GenericATSResponseBO genericAtsResponseBo = taskIdQueue
							.poll();
					int entityCount = genericAtsResponseBo.entities.size();
					Assert.assertTrue(entityCount > 0 && entityCount <= LIMIT);
				}
			}
		}
	}

}
