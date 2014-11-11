package hadooptest.tez.ats;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.tez.ats.ATSTestsBaseClass.ResponseComposition;
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
	public void testExpectEverything() throws Exception {
		ExecutorService execService = Executors.newFixedThreadPool(1);
		if (!timelineserverStarted) {
			// startTimelineServerOnRM(rmHostname);
		}
		int LIMIT=2;
		String addendum = "?primaryFilter=user:" + seedDataForAutomaticallyLaunchedOrderedWordCount.appStartedByUser +"&limit=" + LIMIT;
		
			EntityTypes entityTypeInRequest = EntityTypes.TEZ_TASK_ID;
			String url = getATSUrl() + entityTypeInRequest + addendum;
			makeHttpCallAndEnqueueConsumedResponse(execService, url,
					seedDataForAutomaticallyLaunchedOrderedWordCount.appStartedByUser,
					entityTypeInRequest, taskIdQueue, expectEverythingMap());
		execService.shutdown();
		while (!execService.isTerminated()) {
			Thread.sleep(1000);
		}
		GenericATSResponseBO genericATSResponse = taskIdQueue.poll();
		Assert.assertTrue(genericATSResponse.entities.size() <=LIMIT);
	}

	@Test
	public void testExpectOnlyEvents() throws Exception {
		ExecutorService execService = Executors.newFixedThreadPool(1);
		if (!timelineserverStarted) {
			// startTimelineServerOnRM(rmHostname);
		}
		int LIMIT=2;
		String addendum = "?primaryFilter=user:" + seedDataForAutomaticallyLaunchedOrderedWordCount.appStartedByUser +"&fields=events&limit=" + LIMIT;
		HtfATSUtils atsUtils = new HtfATSUtils();
		Map<String, Boolean> expectedEntities = getExpectedFieldsMap(
				ResponseComposition.EVENTS.EXPECTED,
				ResponseComposition.ENTITYTYPE.EXPECTED,
				ResponseComposition.ENTITY.EXPECTED,
				ResponseComposition.STARTTIME.EXPECTED,
				ResponseComposition.RELATEDENTITIES.NOT_EXPECTED,
				ResponseComposition.PRIMARYFILTERS.NOT_EXPECTED,
				ResponseComposition.OTHERINFO.NOT_EXPECTED);


			EntityTypes entityTypeInRequest = EntityTypes.TEZ_TASK_ID;
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
		Assert.assertTrue(genericAtsResponseBo.entities.size()<=LIMIT);

	}
 

	@Test
	public void testExpectOnlyRelatedEntities() throws Exception {
		ExecutorService execService = Executors.newFixedThreadPool(1);
		if (!timelineserverStarted) {
			// startTimelineServerOnRM(rmHostname);
		}
		int LIMIT=2;
		String addendum = "?primaryFilter=user:" + seedDataForAutomaticallyLaunchedOrderedWordCount.appStartedByUser +"&fields=relatedentities&limit=" + LIMIT;
		
		Map<String, Boolean> expectedEntities = getExpectedFieldsMap(
				ResponseComposition.EVENTS.NOT_EXPECTED,
				ResponseComposition.ENTITYTYPE.EXPECTED,
				ResponseComposition.ENTITY.EXPECTED,
				ResponseComposition.STARTTIME.EXPECTED,
				ResponseComposition.RELATEDENTITIES.EXPECTED,
				ResponseComposition.PRIMARYFILTERS.NOT_EXPECTED,
				ResponseComposition.OTHERINFO.NOT_EXPECTED);

			EntityTypes entityTypeInRequest = EntityTypes.TEZ_TASK_ID;
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
		Assert.assertTrue(genericAtsResponseBo.entities.size()<=LIMIT);

	}

	@Test
	public void testExpectOnlyOtherInfo() throws Exception {
		ExecutorService execService = Executors.newFixedThreadPool(1);
		if (!timelineserverStarted) {
			// startTimelineServerOnRM(rmHostname);
		}
		int LIMIT=2;
		String addendum = "?primaryFilter=user:" + seedDataForAutomaticallyLaunchedOrderedWordCount.appStartedByUser +"&fields=otherinfo&limit=" + LIMIT;
		HtfATSUtils atsUtils = new HtfATSUtils();
		Map<String, Boolean> expectedEntities = getExpectedFieldsMap(
				ResponseComposition.EVENTS.NOT_EXPECTED,
				ResponseComposition.ENTITYTYPE.EXPECTED,
				ResponseComposition.ENTITY.EXPECTED,
				ResponseComposition.STARTTIME.EXPECTED,
				ResponseComposition.RELATEDENTITIES.NOT_EXPECTED,
				ResponseComposition.PRIMARYFILTERS.NOT_EXPECTED,
				ResponseComposition.OTHERINFO.EXPECTED);

			EntityTypes entityTypeInRequest = EntityTypes.TEZ_TASK_ID;
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
		Assert.assertTrue(genericAtsResponseBo.entities.size()<=LIMIT);

	}

	@Test
	public void testExpectOnlyPrimaryFilter() throws Exception {
		ExecutorService execService = Executors.newFixedThreadPool(10);
		if (!timelineserverStarted) {
			// startTimelineServerOnRM(rmHostname);
		}
		int LIMIT=2;
		String addendum = "?primaryFilter=user:" + seedDataForAutomaticallyLaunchedOrderedWordCount.appStartedByUser +"&fields=primaryfilters&limit=" + LIMIT;
		Map<String, Boolean> expectedEntities = getExpectedFieldsMap(
				ResponseComposition.EVENTS.NOT_EXPECTED,
				ResponseComposition.ENTITYTYPE.EXPECTED,
				ResponseComposition.ENTITY.EXPECTED,
				ResponseComposition.STARTTIME.EXPECTED,
				ResponseComposition.RELATEDENTITIES.NOT_EXPECTED,
				ResponseComposition.PRIMARYFILTERS.EXPECTED,
				ResponseComposition.OTHERINFO.NOT_EXPECTED);

			EntityTypes entityTypeInRequest = EntityTypes.TEZ_TASK_ID;
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
		Assert.assertTrue(genericAtsResponseBo.entities.size()<=LIMIT);


	}

}
