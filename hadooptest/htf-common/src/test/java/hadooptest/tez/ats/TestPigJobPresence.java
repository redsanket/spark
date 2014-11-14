package hadooptest.tez.ats;

import hadooptest.SerialTests;
import hadooptest.TestSession;
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
public class TestPigJobPresence extends ATSTestsBaseClass {
	/**
	 * Pig job is launched as hadoopqa which belongs to groups
	 * 'hadoop','hadoopqa'. Since 'hadoopqa' headless user does not have a
	 * bouncer cookie (as it does not conform to the xxx_yyy name format), I am
	 * using hitusr_1 who also belongs to the same group 'hadoop' to run the
	 * query instead. Since they belong to the same group, Pig job should get
	 * listed.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testPigJobGetsListed() throws Exception {
		ExecutorService execService = Executors.newFixedThreadPool(10);
		if (!timelineserverStarted) {
			// startTimelineServerOnRM(rmHostname);
		}

		EntityTypes entityTypeInRequest = EntityTypes.TEZ_DAG_ID;
		String url = getATSUrl() + entityTypeInRequest;
		makeHttpCallAndEnqueueConsumedResponse(execService, url,
				HadooptestConstants.UserNames.HITUSR_1, entityTypeInRequest,
				dagIdQueue, expectEverythingMap());
		execService.shutdown();
		while (!execService.isTerminated()) {
			Thread.sleep(1000);
		}
		Assert.assertEquals(0, errorCount.get());
		GenericATSResponseBO genericAtsResponseBo = dagIdQueue.poll();
		boolean pigJobFound = false;
		String pigApplicationId = seedDataForAutoLaunchedPigJob.appId.replace("application_", "");
		for (EntityInGenericATSResponseBO anEntityInResponse:genericAtsResponseBo.entities){
			if (anEntityInResponse.entity.contains(pigApplicationId)) {
				TestSession.logger.info("Found PIG app Id in response [" + pigApplicationId + "]");
				if (anEntityInResponse.primaryfilters.get("users").contains(seedDataForAutoLaunchedPigJob.appStartedByUser)){
					pigJobFound = true;
				}
			}
		}

		Assert.assertTrue(pigJobFound);
	}

}
