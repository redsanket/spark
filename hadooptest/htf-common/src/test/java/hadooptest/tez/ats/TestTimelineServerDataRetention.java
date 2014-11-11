package hadooptest.tez.ats;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.node.hadoop.HadoopNode;
import hadooptest.tez.ats.ATSTestsBaseClass.EntityTypes;
import hadooptest.tez.ats.ATSTestsBaseClass.ResponseComposition;
import hadooptest.tez.ats.OtherInfoTezDagIdBO.DagPlanBO.DagPlanVertexBO;
import hadooptest.tez.ats.SeedData.DAG;
import hadooptest.tez.ats.SeedData.DAG.Vertex;
import hadooptest.tez.examples.extensions.OrderedWordCountExtendedForHtf;
import hadooptest.tez.examples.extensions.SimpleSessionExampleExtendedForTezHTF;
import hadooptest.tez.utils.HtfATSUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SerialTests.class)
public class TestTimelineServerDataRetention extends ATSTestsBaseClass {
//	@Ignore("http://bug.corp.yahoo.com/show_bug.cgi?id=7184153,
//	http://bug.corp.yahoo.com/show_bug.cgi?id=7166198")
	
	@Test
	public void testDataRetention() throws Exception {
		int TIMELINESERVER_TIMEOUT = 60000; // Milliseconds

		HadoopNode hadoopNode = TestSession.cluster
				.getNode(HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
		String rmHost = hadoopNode.getHostname();

		int beforeDagIdCount = getCount(EntityTypes.TEZ_DAG_ID, dagIdQueue);
		TestSession.logger.info("[BEFORE] DagId count:" + beforeDagIdCount);
		
		//Uncomment these lines after http://bug.corp.yahoo.com/show_bug.cgi?id=7166198 is fixed
//		int beforeAppAttemptCount = getCount(
//				EntityTypes.TEZ_APPLICATION_ATTEMPT, applicationAttemptQueue);
//		TestSession.logger.info("[BEFORE] AttemptId count:" + beforeAppAttemptCount);
//		int beforeContainerIdCount = getCount(EntityTypes.TEZ_CONTAINER_ID,
//				containerIdQueue);
//		TestSession.logger.info("[BEFORE] ContainerId count:" + beforeContainerIdCount);
//		int beforeTaskAttemptId = getCount(EntityTypes.TEZ_TASK_ATTEMPT_ID,
//				taskAttemptIdQueue);
//		TestSession.logger.info("[BEFORE] TaskAttemptId count:" + taskAttemptIdQueue);

		List<String> args = new ArrayList<String>();
		String TIMELINE_SERVICE_PREFIX = "yarn.timeline-service.";
		/** Timeline service enable data age off */
		final String TIMELINE_SERVICE_TTL_ENABLE = TIMELINE_SERVICE_PREFIX
				+ "ttl-enable";

		/** Timeline service length of time to retain data */
		final String TIMELINE_SERVICE_TTL_MS = TIMELINE_SERVICE_PREFIX
				+ "ttl-ms";

		// final long DEFAULT_TIMELINE_SERVICE_TTL_MS = 1000 * 60 * 60 * 24 * 7;
		args.add(TIMELINE_SERVICE_TTL_ENABLE + "=true");
		// Set the history to disappear in a minute
		args.add(TIMELINE_SERVICE_TTL_MS + "=" + TIMELINESERVER_TIMEOUT);
		restartATSWithTheseArgs(rmHost, args);

		// Sleep for slightly over a minute
		Thread.sleep(TIMELINESERVER_TIMEOUT + 10000);
		
		int afterDagIdCount = getCount(EntityTypes.TEZ_DAG_ID, dagIdQueue);
		TestSession.logger.info("[AFTER] DagId count:" + afterDagIdCount);
//		int afterAppAttemptCount = getCount(
//				EntityTypes.TEZ_APPLICATION_ATTEMPT, applicationAttemptQueue);
//		TestSession.logger.info("[AFTER] DagId count:" + afterAppAttemptCount);
//		int afterContainerIdCount = getCount(EntityTypes.TEZ_CONTAINER_ID,
//				containerIdQueue);
//		TestSession.logger.info("[AFTER] ContainerId count:" + afterContainerIdCount);
//		int afterTaskAttemptId = getCount(EntityTypes.TEZ_TASK_ATTEMPT_ID,
//				taskAttemptIdQueue);
//		TestSession.logger.info("[AFTER] TaskAttemptId count:" + afterTaskAttemptId);

		Assert.assertTrue(beforeDagIdCount > afterDagIdCount);
//		Assert.assertTrue(beforeAppAttemptCount > afterAppAttemptCount);
//		Assert.assertTrue(beforeContainerIdCount > afterContainerIdCount);
//		Assert.assertTrue(beforeTaskAttemptId > afterTaskAttemptId);

	}
	
	public int getCount(EntityTypes entityType,
			Queue<GenericATSResponseBO> addProcessedResponseToThisQueue)
			throws InterruptedException {

		ExecutorService execService = Executors.newFixedThreadPool(1);

		String url = getATSUrl() + entityType;
		makeHttpCallAndEnqueueConsumedResponse(execService, url,
				HadooptestConstants.UserNames.HITUSR_1, entityType,
				addProcessedResponseToThisQueue, expectEverythingMap());

		execService.shutdown();
		while (!execService.isTerminated()) {
			Thread.sleep(1000);
		}

		GenericATSResponseBO deQueuedResponse = addProcessedResponseToThisQueue
				.poll();
		return deQueuedResponse.entities.size();
	}
	
	@After
	public void restoreTheTimelineServer() throws Exception{
		HadoopNode hadoopNode = TestSession.cluster
				.getNode(HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
		String rmHost = hadoopNode.getHostname();

		List<String> emptyList = new ArrayList<String>();
		restartATSWithTheseArgs(rmHost, emptyList);
		// Reset this variable, so that jobs are launched to help the next set of tests
		// along.
		jobsLaunchedOnceToSeedData = Boolean.FALSE;

	}

}
