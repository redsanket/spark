package hadooptest.tez.ats;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.tez.ats.SeedData.DAG;
import hadooptest.tez.ats.SeedData.DAG.Vertex;
import hadooptest.tez.ats.SeedData.DAG.Vertex.Task;
import hadooptest.tez.ats.SeedData.DAG.Vertex.Task.Attempt;
import hadooptest.tez.utils.HtfATSUtils;

import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * This is how the users have been created on our grids
 * 
 * <pre>
 * 			hadoop hadoopqa	gdmdev	gdmqa
 * hitusr_1	 X
 * hitusr_2			X
 * hitusr_3					X
 * hitusr_4			X		X		X
 * hadoopqa  X      X
 * </pre>
 * 
 * This is how/who launches what jobs
 * 
 * <pre>
 * 			orderedwordcount	mrrsleep	simplesession   pig-job
 * hitusr_1		X
 * hitusr_2							X
 * hitusr_3										X
 * hitusr_4
 * hadoopqa														X
 * </pre>
 */

@Category(SerialTests.class)
public class TestAuthWhenUserSpecifiesUserAndGroupComboInACL extends
		ATSTestsBaseClass {
	@Test
	public void testSelf() throws Exception {
		String self = HadooptestConstants.UserNames.HITUSR_3;
		String explicitUser = HadooptestConstants.UserNames.HITUSR_2;
		String otherGroupAllowed = HadooptestConstants.UserGroups.HADOOP;
		String implicitUserBelongingToSpecifiedGroup = HadooptestConstants.UserNames.HITUSR_1;
		String implicitUserBelongingToSameGroup = HadooptestConstants.UserNames.HITUSR_4;
		String[] allowedUsers = new String[] { self, explicitUser,
				implicitUserBelongingToSpecifiedGroup,
				implicitUserBelongingToSameGroup };
		SeedData seedData = launchSimpleSessionExampleExtendedForTezHTFAndGetSeedData(
				self, explicitUser + "," + otherGroupAllowed);

		EntityTypes entityTypeBeingTested;
		Queue<GenericATSResponseBO> currentQueue;
		GenericATSResponseBO polled;
		String entity;
		HtfATSUtils atsUtils = new HtfATSUtils();
		for (String anAllowedUser : allowedUsers) {
			for (DAG aDAG : seedData.dags) {
				entity = aDAG.id;
				entityTypeBeingTested = EntityTypes.TEZ_DAG_ID;
				currentQueue = dagIdQueue;

				String url = getATSUrl() + entityTypeBeingTested + "/" + entity;
				TestSession.logger.info("Processing:" + url);
				makeHttpRequestAndEnqueue(url, entityTypeBeingTested,
						anAllowedUser, currentQueue);
				polled = currentQueue.poll();
				Assert.assertTrue(atsUtils.isEntityPresentInResponse(polled,
						entityTypeBeingTested, entity));
				for (Vertex aVertex : aDAG.vertices) {
					entity = aVertex.id;
					entityTypeBeingTested = EntityTypes.TEZ_VERTEX_ID;
					currentQueue = vertexIdQueue;
					url = getATSUrl() + entityTypeBeingTested + "/" + entity;
					TestSession.logger.info("Processing:" + url);
					makeHttpRequestAndEnqueue(url, entityTypeBeingTested,
							anAllowedUser, currentQueue);
					polled = currentQueue.poll();
					Assert.assertTrue(atsUtils.isEntityPresentInResponse(
							polled, entityTypeBeingTested, entity));
					for (Task aTask : aVertex.tasks) {
						entity = aTask.id;
						entityTypeBeingTested = EntityTypes.TEZ_TASK_ID;
						currentQueue = taskIdQueue;

						url = getATSUrl() + entityTypeBeingTested + "/"
								+ entity;
						TestSession.logger.info("Processing:" + url);
						makeHttpRequestAndEnqueue(url, entityTypeBeingTested,
								anAllowedUser, currentQueue);
						polled = currentQueue.poll();
						Assert.assertTrue(atsUtils.isEntityPresentInResponse(
								polled, entityTypeBeingTested, entity));
						for (Attempt anAttempt : aTask.attempts) {
							entity = anAttempt.id;
							entityTypeBeingTested = EntityTypes.TEZ_TASK_ATTEMPT_ID;
							currentQueue = taskAttemptIdQueue;
							url = getATSUrl() + entityTypeBeingTested + "/"
									+ entity;
							TestSession.logger.info("Processing:" + url);
							makeHttpRequestAndEnqueue(url,
									entityTypeBeingTested, anAllowedUser,
									currentQueue);
							polled = currentQueue.poll();
							Assert.assertTrue(atsUtils
									.isEntityPresentInResponse(polled,
											entityTypeBeingTested, entity));

						}

					}

				}

			}
		}
	}

	public void makeHttpRequestAndEnqueue(String url, EntityTypes entityType,
			String user,
			Queue<GenericATSResponseBO> enqueueProcessedResponseHere)
			throws InterruptedException {
		ExecutorService execService = Executors.newFixedThreadPool(1);
		makeHttpCallAndEnqueueConsumedResponse(execService, url, user,
				entityType, enqueueProcessedResponseHere, expectEverythingMap());
		execService.shutdown();
		while (!execService.isTerminated()) {
			Thread.sleep(1000);
		}

	}

}
