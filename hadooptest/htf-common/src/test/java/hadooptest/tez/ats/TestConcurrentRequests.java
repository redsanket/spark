package hadooptest.tez.ats;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
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
public class TestConcurrentRequests extends ATSTestsBaseClass {
	@Ignore("Tested fine")
	@Test
	public void testCompleteResponses() throws Exception {
		ExecutorService execService = Executors.newFixedThreadPool(10);
		if (!timelineserverStarted) {
			// startTimelineServerOnRM(rmHostname);
		}
		HtfATSUtils atsUtils = new HtfATSUtils();
		for (int xx = 0; xx < 2; xx++) {
			EntityTypes entityTypeInRequest = EntityTypes.TEZ_DAG_ID;
			String url = getATSUrl() + entityTypeInRequest + "/";

			makeHttpCallAndEnqueueConsumedResponse(execService, url,
					HadooptestConstants.UserNames.HITUSR_1,
					entityTypeInRequest, dagIdQueue, expectEverythingMap());

			/**
			 * Uncomment this line once
			 * http://bug.corp.yahoo.com/show_bug.cgi?id=7167877 is fixed
			 * 
			 * entityTypeInRequest = EntityTypes.TEZ_CONTAINER_ID; url =
			 * getATSUrl() + entityTypeInRequest + "/";
			 * makeHttpCallAndEnqueueConsumedResponse(execService, url,
			 * HadooptestConstants.UserNames.HITUSR_1, entityTypeInRequest,
			 * containerIdQueue, expectEverythingMap());
			 */

			entityTypeInRequest = EntityTypes.TEZ_APPLICATION_ATTEMPT;
			url = getATSUrl() + entityTypeInRequest + "/";
			makeHttpCallAndEnqueueConsumedResponse(execService, url,
					HadooptestConstants.UserNames.HITUSR_1,
					entityTypeInRequest, applicationAttemptQueue,
					expectEverythingMap());

			entityTypeInRequest = EntityTypes.TEZ_VERTEX_ID;
			url = getATSUrl() + entityTypeInRequest + "/";
			makeHttpCallAndEnqueueConsumedResponse(execService, url,
					HadooptestConstants.UserNames.HITUSR_1,
					entityTypeInRequest, vertexIdQueue, expectEverythingMap());

			entityTypeInRequest = EntityTypes.TEZ_TASK_ID;
			url = getATSUrl() + entityTypeInRequest + "/";
			makeHttpCallAndEnqueueConsumedResponse(execService, url,
					HadooptestConstants.UserNames.HITUSR_1,
					entityTypeInRequest, taskIdQueue, expectEverythingMap());

			entityTypeInRequest = EntityTypes.TEZ_TASK_ATTEMPT_ID;
			url = getATSUrl() + entityTypeInRequest + "/";
			makeHttpCallAndEnqueueConsumedResponse(execService, url,
					HadooptestConstants.UserNames.HITUSR_1,
					entityTypeInRequest, taskAttemptIdQueue,
					expectEverythingMap());
		}
		execService.shutdown();
		while (!execService.isTerminated()) {
			TestSession.logger
					.info("Thread sleeping while awaiting HTTP calls");
			Thread.sleep(1000);
		}

		Assert.assertEquals(errorCount.get(), 0);
		Assert.assertTrue(atsUtils
				.takeFirstItemAndCompareItAgainstAllTheOtherItemsInQueue(dagIdQueue));
		/**
		 * http://bug.corp.yahoo.com/show_bug.cgi?id=7167877	
		 * Assert.assertTrue(atsUtils
		 * .takeFirstItemAndCompareItAgainstAllTheOtherItemsInQueue(containerIdQueue));		
		 */
		Assert.assertTrue(atsUtils
				.takeFirstItemAndCompareItAgainstAllTheOtherItemsInQueue(applicationAttemptQueue));
		Assert.assertTrue(atsUtils
				.takeFirstItemAndCompareItAgainstAllTheOtherItemsInQueue(vertexIdQueue));
		Assert.assertTrue(atsUtils
				.takeFirstItemAndCompareItAgainstAllTheOtherItemsInQueue(taskIdQueue));
		Assert.assertTrue(atsUtils
				.takeFirstItemAndCompareItAgainstAllTheOtherItemsInQueue(taskAttemptIdQueue));

	}

//	@Test
	public void testCascadingOnDagId() throws Exception {
		getCascadedEntitiesMap("dag_1413669561424_0007_1");
	}
	
	@Test
	public void testUGI() throws IOException, InterruptedException{
		UserGroupInformation ugi = getUgiForUser(HadooptestConstants.UserNames.HDFSQA);
		DoAs doAs = new DoAs(ugi,TestSession.cluster.getConf());
		doAs.doAction();
	}
}
