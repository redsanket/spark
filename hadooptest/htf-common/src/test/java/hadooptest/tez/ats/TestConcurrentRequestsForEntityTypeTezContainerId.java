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
public class TestConcurrentRequestsForEntityTypeTezContainerId extends
		ATSTestsBaseClass {
	@Ignore("http://bug.corp.yahoo.com/show_bug.cgi?id=7167877")
	@Test
	public void testExpectEverything() throws Exception {
		HtfATSUtils atsUtils = new HtfATSUtils();
		ExecutorService execService = Executors.newFixedThreadPool(10);
		if (!timelineserverStarted) {
			// startTimelineServerOnRM(rmHostname);
		}
		int LOOP = 2;
		for (int xx = 0; xx < LOOP; xx++) {

			EntityTypes entityTypeInRequest = EntityTypes.TEZ_CONTAINER_ID;
			String url = getATSUrl() + entityTypeInRequest;
			makeHttpCallAndEnqueueConsumedResponse(execService, url,
					HadooptestConstants.UserNames.HITUSR_1,
					entityTypeInRequest, containerIdQueue,
					expectEverythingMap());

		}
		execService.shutdown();
		while (!execService.isTerminated()) {
			Thread.sleep(1000);
		}
		Assert.assertTrue(atsUtils
				.peekQAndCmpAgainstOtherEnqueuedItems(containerIdQueue));
		int failedJobCount = countNumberOfFailedJobs(HadooptestConstants.UserNames.HITUSR_1);
		Assert.assertEquals(failedJobCount * LOOP, errorCount.get());
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

			EntityTypes entityTypeInRequest = EntityTypes.TEZ_CONTAINER_ID;
			String url = getATSUrl() + entityTypeInRequest + addendum;
			makeHttpCallAndEnqueueConsumedResponse(execService, url,
					HadooptestConstants.UserNames.HITUSR_1,
					entityTypeInRequest, containerIdQueue,
					expectedEntities);

		}
		execService.shutdown();
		while (!execService.isTerminated()) {
			Thread.sleep(1000);
		}
		Assert.assertTrue(atsUtils
				.peekQAndCmpAgainstOtherEnqueuedItems(containerIdQueue));

		Assert.assertEquals(0, errorCount.get());

	}

	@Ignore("http://bug.corp.yahoo.com/show_bug.cgi?id=7167877")
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

			EntityTypes entityTypeInRequest = EntityTypes.TEZ_CONTAINER_ID;
			String url = getATSUrl() + entityTypeInRequest + addendum;
			makeHttpCallAndEnqueueConsumedResponse(execService, url,
					HadooptestConstants.UserNames.HITUSR_1,
					entityTypeInRequest, containerIdQueue,
					expectedEntities);

		}
		execService.shutdown();
		while (!execService.isTerminated()) {
			Thread.sleep(1000);
		}
		Assert.assertTrue(atsUtils
				.peekQAndCmpAgainstOtherEnqueuedItems(containerIdQueue));

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

			EntityTypes entityTypeInRequest = EntityTypes.TEZ_CONTAINER_ID;
			String url = getATSUrl() + entityTypeInRequest + addendum;
			makeHttpCallAndEnqueueConsumedResponse(execService, url,
					HadooptestConstants.UserNames.HITUSR_1,
					entityTypeInRequest, containerIdQueue,
					expectedEntities);

		}
		execService.shutdown();
		while (!execService.isTerminated()) {
			Thread.sleep(1000);
		}
		Assert.assertTrue(atsUtils
				.peekQAndCmpAgainstOtherEnqueuedItems(containerIdQueue));

		Assert.assertEquals(0, errorCount.get());

	}

}
