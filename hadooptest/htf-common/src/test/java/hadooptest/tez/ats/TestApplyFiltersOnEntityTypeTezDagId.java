package hadooptest.tez.ats;

import hadooptest.SerialTests;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.tez.utils.HtfATSUtils;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SerialTests.class)
public class TestApplyFiltersOnEntityTypeTezDagId extends ATSTestsBaseClass {
	@Ignore("http://bug.corp.yahoo.com/show_bug.cgi?id=7167877")
	@Test
	public void testExpectEverything() throws Exception {
		ExecutorService execService = Executors.newFixedThreadPool(1);
		if (!timelineserverStarted) {
			// startTimelineServerOnRM(rmHostname);
		}
		int LIMIT=2;
		String addendum = "?primaryFilter=user:" + seedDataForAutoLaunchedOrderedWordCount.appStartedByUser +"&limit=" + LIMIT;
			EntityTypes entityTypeInRequest = EntityTypes.TEZ_DAG_ID;
			String url = getATSUrl() + entityTypeInRequest + addendum;
			makeHttpCallAndEnqueueConsumedResponse(execService, url,
					seedDataForAutoLaunchedOrderedWordCount.appStartedByUser,
					entityTypeInRequest, dagIdQueue, expectEverythingMap());
		execService.shutdown();
		while (!execService.isTerminated()) {
			Thread.sleep(1000);
		}
		GenericATSResponseBO genericATSResponse = dagIdQueue.poll();
		Assert.assertTrue(genericATSResponse.entities.size() <=LIMIT);
	}

//	@Test
	public void testExpectOnlyEvents() throws Exception {
		ExecutorService execService = Executors.newFixedThreadPool(1);
		if (!timelineserverStarted) {
			// startTimelineServerOnRM(rmHostname);
		}
		int LIMIT=2;
		String addendum = "?primaryFilter=user:" + seedDataForAutoLaunchedOrderedWordCount.appStartedByUser +"&fields=events&limit=" + LIMIT;
		HtfATSUtils atsUtils = new HtfATSUtils();
		Map<String, Boolean> expectedEntities = getExpectedFieldsMap(
				ResponseComposition.EVENTS.EXPECTED,
				ResponseComposition.ENTITYTYPE.EXPECTED,
				ResponseComposition.ENTITY.EXPECTED,
				ResponseComposition.STARTTIME.EXPECTED,
				ResponseComposition.RELATEDENTITIES.NOT_EXPECTED,
				ResponseComposition.PRIMARYFILTERS.NOT_EXPECTED,
				ResponseComposition.OTHERINFO.NOT_EXPECTED);


			EntityTypes entityTypeInRequest = EntityTypes.TEZ_DAG_ID;
			String url = getATSUrl() + entityTypeInRequest + addendum;
			makeHttpCallAndEnqueueConsumedResponse(execService, url,
					HadooptestConstants.UserNames.HITUSR_1,
					entityTypeInRequest, dagIdQueue, expectedEntities);


		execService.shutdown();
		while (!execService.isTerminated()) {
			Thread.sleep(1000);
		}

		Assert.assertEquals(0, errorCount.get());
		GenericATSResponseBO genericAtsResponseBo = dagIdQueue.poll();
		Assert.assertTrue(genericAtsResponseBo.entities.size()<=LIMIT);

	}
 
	@Ignore("http://bug.corp.yahoo.com/show_bug.cgi?id=7166198")
	@Test
	public void testExpectOnlyRelatedEntities() throws Exception {
		ExecutorService execService = Executors.newFixedThreadPool(1);
		if (!timelineserverStarted) {
			// startTimelineServerOnRM(rmHostname);
		}
		int LIMIT=2;
		String addendum = "?primaryFilter=user:" + seedDataForAutoLaunchedOrderedWordCount.appStartedByUser +"&fields=relatedentities&limit=" + LIMIT;
		
		Map<String, Boolean> expectedEntities = getExpectedFieldsMap(
				ResponseComposition.EVENTS.NOT_EXPECTED,
				ResponseComposition.ENTITYTYPE.EXPECTED,
				ResponseComposition.ENTITY.EXPECTED,
				ResponseComposition.STARTTIME.EXPECTED,
				ResponseComposition.RELATEDENTITIES.EXPECTED,
				ResponseComposition.PRIMARYFILTERS.NOT_EXPECTED,
				ResponseComposition.OTHERINFO.NOT_EXPECTED);

			EntityTypes entityTypeInRequest = EntityTypes.TEZ_DAG_ID;
			String url = getATSUrl() + entityTypeInRequest + addendum;
			makeHttpCallAndEnqueueConsumedResponse(execService, url,
					HadooptestConstants.UserNames.HITUSR_1,
					entityTypeInRequest, dagIdQueue, expectedEntities);

		execService.shutdown();
		while (!execService.isTerminated()) {
			Thread.sleep(1000);
		}

		Assert.assertEquals(0, errorCount.get());
		GenericATSResponseBO genericAtsResponseBo = dagIdQueue.poll();
		Assert.assertTrue(genericAtsResponseBo.entities.size()<=LIMIT);

	}

	@Test
	public void testExpectOnlyOtherInfo() throws Exception {
		ExecutorService execService = Executors.newFixedThreadPool(1);
		if (!timelineserverStarted) {
			// startTimelineServerOnRM(rmHostname);
		}
		int LIMIT=2;
		String addendum = "?primaryFilter=user:" + seedDataForAutoLaunchedOrderedWordCount.appStartedByUser +"&fields=otherinfo&limit=" + LIMIT;
		HtfATSUtils atsUtils = new HtfATSUtils();
		Map<String, Boolean> expectedEntities = getExpectedFieldsMap(
				ResponseComposition.EVENTS.NOT_EXPECTED,
				ResponseComposition.ENTITYTYPE.EXPECTED,
				ResponseComposition.ENTITY.EXPECTED,
				ResponseComposition.STARTTIME.EXPECTED,
				ResponseComposition.RELATEDENTITIES.NOT_EXPECTED,
				ResponseComposition.PRIMARYFILTERS.NOT_EXPECTED,
				ResponseComposition.OTHERINFO.EXPECTED);

			EntityTypes entityTypeInRequest = EntityTypes.TEZ_DAG_ID;
			String url = getATSUrl() + entityTypeInRequest + addendum;
			makeHttpCallAndEnqueueConsumedResponse(execService, url,
					HadooptestConstants.UserNames.HITUSR_1,
					entityTypeInRequest, dagIdQueue, expectedEntities);
		execService.shutdown();
		while (!execService.isTerminated()) {
			Thread.sleep(1000);
		}
		Assert.assertEquals(0, errorCount.get());
		GenericATSResponseBO genericAtsResponseBo = dagIdQueue.poll();
		int entityCount = genericAtsResponseBo.entities.size();
		Assert.assertTrue(entityCount>1 && entityCount<=LIMIT);

	}

	@Test
	public void testExpectOnlyPrimaryFilter() throws Exception {
		ExecutorService execService = Executors.newFixedThreadPool(10);
		if (!timelineserverStarted) {
			// startTimelineServerOnRM(rmHostname);
		}
		int LIMIT=2;
		String addendum = "?primaryFilter=user:" + seedDataForAutoLaunchedOrderedWordCount.appStartedByUser +"&fields=primaryfilters&limit=" + LIMIT;
		Map<String, Boolean> expectedEntities = getExpectedFieldsMap(
				ResponseComposition.EVENTS.NOT_EXPECTED,
				ResponseComposition.ENTITYTYPE.EXPECTED,
				ResponseComposition.ENTITY.EXPECTED,
				ResponseComposition.STARTTIME.EXPECTED,
				ResponseComposition.RELATEDENTITIES.NOT_EXPECTED,
				ResponseComposition.PRIMARYFILTERS.EXPECTED,
				ResponseComposition.OTHERINFO.NOT_EXPECTED);

			EntityTypes entityTypeInRequest = EntityTypes.TEZ_DAG_ID;
			String url = getATSUrl() + entityTypeInRequest + addendum;
			makeHttpCallAndEnqueueConsumedResponse(execService, url,
					HadooptestConstants.UserNames.HITUSR_1,
					entityTypeInRequest, dagIdQueue, expectedEntities);
		execService.shutdown();
		while (!execService.isTerminated()) {
			Thread.sleep(1000);
		}
		Assert.assertEquals(0, errorCount.get());
		GenericATSResponseBO genericAtsResponseBo = dagIdQueue.poll();
		int entityCount = genericAtsResponseBo.entities.size();
		Assert.assertTrue(entityCount>1 && entityCount<=LIMIT);


	}

}
