package hadooptest.tez.ats;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.automation.utils.http.HTTPHandle;
import hadooptest.node.hadoop.HadoopNode;
import hadooptest.tez.ats.ATSTestsBaseClass.EntityTypes;
import hadooptest.tez.ats.ATSTestsBaseClass.ResponseComposition;

public class AclDomainBaseClass extends ATSTestsBaseClass{

	/**
	 * We don't want to launch seed data for these tests, hence
	 * override this method from the base class and remove
	 * the line where the seed tests were launched
	 */
	@Override
	public void cleanupAndPrepareForTestRun() throws Exception {
		TestSession.logger.info("Running cleanupAndPrepareForTestRun");
		// Fetch cookies
		HTTPHandle httpHandle = new HTTPHandle();
		String hitusr_1_cookie = null;
		String hitusr_2_cookie = null;
		String hitusr_3_cookie = null;
		String hitusr_4_cookie = null;

		hitusr_1_cookie = httpHandle
				.loginAndReturnCookie(HadooptestConstants.UserNames.HITUSR_1);
		TestSession.logger.info("Got cookie hitusr_1:" + hitusr_1_cookie);
		userCookies
				.put(HadooptestConstants.UserNames.HITUSR_1, hitusr_1_cookie);
		hitusr_2_cookie = httpHandle
				.loginAndReturnCookie(HadooptestConstants.UserNames.HITUSR_2);
		TestSession.logger.info("Got cookie hitusr_2:" + hitusr_2_cookie);
		userCookies
				.put(HadooptestConstants.UserNames.HITUSR_2, hitusr_2_cookie);
		hitusr_3_cookie = httpHandle
				.loginAndReturnCookie(HadooptestConstants.UserNames.HITUSR_3);
		TestSession.logger.info("Got cookie hitusr_3:" + hitusr_3_cookie);
		userCookies
				.put(HadooptestConstants.UserNames.HITUSR_3, hitusr_3_cookie);
		hitusr_4_cookie = httpHandle
				.loginAndReturnCookie(HadooptestConstants.UserNames.HITUSR_4);
		TestSession.logger.info("Got cookie hitusr_4:" + hitusr_4_cookie);
		userCookies
				.put(HadooptestConstants.UserNames.HITUSR_4, hitusr_4_cookie);

		// Reset the error count
		errorCount.set(0);

		drainQueues();

		HadoopNode hadoopNode = TestSession.cluster
				.getNode(HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
		String rmHost = hadoopNode.getHostname();
		TestSession.logger.info("RESOURCE MANAGER HOST:::::::::::::::::::::"
				+ rmHost);

		// restartRMWithTheseArgs(rmHost, new ArrayList<String>());

	}
	
	public void makeHttpRequestAndEnqueue(String url, EntityTypes entityType,
			String user,
			Queue<GenericATSResponseBO> enqueueProcessedResponseHere)
			throws InterruptedException {
		String filter = "?fields=events,relatedentities,primaryfilters";
		Map<String, Boolean> expectedEntities = getExpectedFieldsMap(
				ResponseComposition.EVENTS.EXPECTED,
				ResponseComposition.ENTITYTYPE.EXPECTED,
				ResponseComposition.ENTITY.EXPECTED,
				ResponseComposition.STARTTIME.EXPECTED,
				ResponseComposition.RELATEDENTITIES.EXPECTED,
				ResponseComposition.PRIMARYFILTERS.EXPECTED,
				ResponseComposition.OTHERINFO.NOT_EXPECTED);

		ExecutorService execService = Executors.newFixedThreadPool(1);
		makeHttpCallAndEnqueueConsumedResponse(execService, url, user,
				entityType, enqueueProcessedResponseHere, expectEverythingMap());
		execService.shutdown();
		while (!execService.isTerminated()) {
			Thread.sleep(1000);
		}

	}


}
