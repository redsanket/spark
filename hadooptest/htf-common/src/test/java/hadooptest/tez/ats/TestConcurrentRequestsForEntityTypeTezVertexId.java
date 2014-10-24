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
public class TestConcurrentRequestsForEntityTypeTezVertexId extends ATSTestsBaseClass {
	@Test
	public void testExpectEverything() throws Exception {
		HtfATSUtils atsUtils = new HtfATSUtils();
		ExecutorService execService = Executors.newFixedThreadPool(10);
		if (!timelineserverStarted) {
			// startTimelineServerOnRM(rmHostname);
		}

		for (int xx = 0; xx <= 2; xx++) {

			EntityTypes entityTypeInRequest = EntityTypes.TEZ_VERTEX_ID;
			String url = getATSUrl() + entityTypeInRequest;
			makeHttpCallAndEnqueueConsumedResponse(execService, url,
					HadooptestConstants.UserNames.HITUSR_1,
					entityTypeInRequest, vertexIdQueue, expectEverythingMap());

		}
		execService.shutdown();
		while (!execService.isTerminated()) {
			Thread.sleep(1000);
		}
		Assert.assertTrue(atsUtils
				.peekQAndCmpAgainstOtherEnquedItems(vertexIdQueue));
	}

	@Test
	public void testExpectOnlyEvents() throws Exception {
		ExecutorService execService = Executors.newFixedThreadPool(10);
		if (!timelineserverStarted) {
			// startTimelineServerOnRM(rmHostname);
		}
		String addendum = "?fields=events";
		HtfATSUtils atsUtils = new HtfATSUtils();
		Map<String, Boolean> expectedEntities = getExpectedFieldsMap(
				ResponseComposition.EVENTS.EXPECTED,
				ResponseComposition.ENTITYTYPE.EXPECTED,
				ResponseComposition.ENTITY.EXPECTED,
				ResponseComposition.STARTTIME.EXPECTED,
				ResponseComposition.RELATEDENTITIES.NOT_EXPECTED,
				ResponseComposition.PRIMARYFILTERS.NOT_EXPECTED,
				ResponseComposition.OTHERINFO.NOT_EXPECTED);

		for (int xx = 0; xx < 200; xx++) {

			EntityTypes entityTypeInRequest = EntityTypes.TEZ_VERTEX_ID;
			String url = getATSUrl() + entityTypeInRequest + addendum;
			makeHttpCallAndEnqueueConsumedResponse(execService, url,
					HadooptestConstants.UserNames.HITUSR_1,
					entityTypeInRequest, vertexIdQueue, expectedEntities);

		}
		execService.shutdown();
		while (!execService.isTerminated()) {
			Thread.sleep(1000);
		}
		Assert.assertTrue(atsUtils
				.peekQAndCmpAgainstOtherEnquedItems(vertexIdQueue));

		Assert.assertEquals(0, errorCount.get());

	}

	@Test
	public void testExpectOnlyRelatedEntities() throws Exception {
		ExecutorService execService = Executors.newFixedThreadPool(10);
		if (!timelineserverStarted) {
			// startTimelineServerOnRM(rmHostname);
		}
		String addendum = "?fields=relatedentities";
		HtfATSUtils atsUtils = new HtfATSUtils();
		Map<String, Boolean> expectedEntities = getExpectedFieldsMap(
				ResponseComposition.EVENTS.NOT_EXPECTED,
				ResponseComposition.ENTITYTYPE.EXPECTED,
				ResponseComposition.ENTITY.EXPECTED,
				ResponseComposition.STARTTIME.EXPECTED,
				ResponseComposition.RELATEDENTITIES.EXPECTED,
				ResponseComposition.PRIMARYFILTERS.NOT_EXPECTED,
				ResponseComposition.OTHERINFO.NOT_EXPECTED);

		for (int xx = 0; xx < 200; xx++) {

			EntityTypes entityTypeInRequest = EntityTypes.TEZ_VERTEX_ID;
			String url = getATSUrl() + entityTypeInRequest + addendum;
			makeHttpCallAndEnqueueConsumedResponse(execService, url,
					HadooptestConstants.UserNames.HITUSR_1,
					entityTypeInRequest, vertexIdQueue, expectedEntities);
		}
		execService.shutdown();
		while (!execService.isTerminated()) {
			Thread.sleep(1000);
		}
		Assert.assertTrue(atsUtils
				.peekQAndCmpAgainstOtherEnquedItems(vertexIdQueue));

		Assert.assertEquals(0, errorCount.get());

	}

	@Test
	public void testExpectOnlyOtherInfo() throws Exception {
		ExecutorService execService = Executors.newFixedThreadPool(10);
		if (!timelineserverStarted) {
			// startTimelineServerOnRM(rmHostname);
		}
		String addendum = "?fields=otherinfo";
		HtfATSUtils atsUtils = new HtfATSUtils();
		Map<String, Boolean> expectedEntities = getExpectedFieldsMap(
				ResponseComposition.EVENTS.NOT_EXPECTED,
				ResponseComposition.ENTITYTYPE.EXPECTED,
				ResponseComposition.ENTITY.EXPECTED,
				ResponseComposition.STARTTIME.EXPECTED,
				ResponseComposition.RELATEDENTITIES.NOT_EXPECTED,
				ResponseComposition.PRIMARYFILTERS.NOT_EXPECTED,
				ResponseComposition.OTHERINFO.EXPECTED);

		for (int xx = 0; xx < 10; xx++) {

			EntityTypes entityTypeInRequest = EntityTypes.TEZ_VERTEX_ID;
			String url = getATSUrl() + entityTypeInRequest + addendum;
			makeHttpCallAndEnqueueConsumedResponse(execService, url,
					HadooptestConstants.UserNames.HITUSR_1,
					entityTypeInRequest, vertexIdQueue, expectedEntities);

		}
		execService.shutdown();
		while (!execService.isTerminated()) {
			Thread.sleep(1000);
		}
		Assert.assertTrue(atsUtils
				.peekQAndCmpAgainstOtherEnquedItems(vertexIdQueue));

		Assert.assertEquals(0, errorCount.get());

	}

	@Test
	public void testExpectOnlyPrimaryFilters() throws Exception {
		ExecutorService execService = Executors.newFixedThreadPool(10);
		if (!timelineserverStarted) {
			// startTimelineServerOnRM(rmHostname);
		}
		String addendum = "?fields=primaryfilters";
		HtfATSUtils atsUtils = new HtfATSUtils();
		Map<String, Boolean> expectedEntities = getExpectedFieldsMap(
				ResponseComposition.EVENTS.NOT_EXPECTED,
				ResponseComposition.ENTITYTYPE.EXPECTED,
				ResponseComposition.ENTITY.EXPECTED,
				ResponseComposition.STARTTIME.EXPECTED,
				ResponseComposition.RELATEDENTITIES.NOT_EXPECTED,
				ResponseComposition.PRIMARYFILTERS.EXPECTED,
				ResponseComposition.OTHERINFO.NOT_EXPECTED);

		for (int xx = 0; xx < 200; xx++) {

			EntityTypes entityTypeInRequest = EntityTypes.TEZ_VERTEX_ID;
			String url = getATSUrl() + entityTypeInRequest + addendum;
			makeHttpCallAndEnqueueConsumedResponse(execService, url,
					HadooptestConstants.UserNames.HITUSR_1,
					entityTypeInRequest, vertexIdQueue, expectedEntities);

		}
		execService.shutdown();
		while (!execService.isTerminated()) {
			Thread.sleep(1000);
		}
		Assert.assertTrue(atsUtils
				.peekQAndCmpAgainstOtherEnquedItems(vertexIdQueue));

		Assert.assertEquals(0, errorCount.get());

	}

}
