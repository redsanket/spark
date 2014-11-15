package hadooptest.tez.ats;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SerialTests.class)
public class TestCascadedData extends ATSTestsBaseClass {
	@Test
	@Ignore("http://bug.corp.yahoo.com/show_bug.cgi?id=7166198")
	public void testOrderedWordCount() throws Exception {
		ExecutorService execService = Executors.newFixedThreadPool(1);
		String addendum;
		if (!timelineserverStarted) {
			// startTimelineServerOnRM(rmHostname);
		}

		for (DAG aSeedDag : seedDataForAutoLaunchedOrderedWordCount.dags) {
			addendum = "?primaryFilter=dagName:" + aSeedDag.name;
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
			for (EntityInGenericATSResponseBO anEntityInGenericATSResponseBO : genericATSResponse.entities) {
				if (anEntityInGenericATSResponseBO.entity
						.contains(seedDataForAutoLaunchedOrderedWordCount.appId)) {
					if (!anEntityInGenericATSResponseBO.primaryfilters.get(
							"dagName").contains(aSeedDag.name)) {
						// DAG does not belong to this run
						continue;
					}
					// This is the DAG we are interested in
					compareAppIdUserNameAndVertexIdsInResponse(aSeedDag,
							anEntityInGenericATSResponseBO,
							seedDataForAutoLaunchedOrderedWordCount.appId,
							seedDataForAutoLaunchedOrderedWordCount.appStartedByUser);
				}
			}

		}

	}

	@Test
	@Ignore("http://bug.corp.yahoo.com/show_bug.cgi?id=7166198")
	public void testSimpleSessionExample() throws Exception {
		ExecutorService execService = Executors.newFixedThreadPool(1);
		String addendum;
		if (!timelineserverStarted) {
			// startTimelineServerOnRM(rmHostname);
		}

		for (DAG aSeedDag : seedDataForAutoLaunchedSimpleSessionExample.dags) {
			addendum = "?primaryFilter=dagName:" + aSeedDag.name;
			EntityTypes entityTypeInRequest = EntityTypes.TEZ_DAG_ID;
			String url = getATSUrl() + entityTypeInRequest + addendum;
			makeHttpCallAndEnqueueConsumedResponse(execService, url,
					seedDataForAutoLaunchedSimpleSessionExample.appStartedByUser,
					entityTypeInRequest, dagIdQueue, expectEverythingMap());
			execService.shutdown();
			while (!execService.isTerminated()) {
				Thread.sleep(1000);
			}
			GenericATSResponseBO genericATSResponse = dagIdQueue.poll();
			for (EntityInGenericATSResponseBO anEntityInGenericATSResponseBO : genericATSResponse.entities) {
				if (anEntityInGenericATSResponseBO.entity
						.contains(seedDataForAutoLaunchedSimpleSessionExample.appId)) {
					if (!anEntityInGenericATSResponseBO.primaryfilters.get(
							"dagName").contains(aSeedDag.name)) {
						// DAG does not belong to this run
						continue;
					}
					// This is the DAG we are interested in
					compareAppIdUserNameAndVertexIdsInResponse(aSeedDag,
							anEntityInGenericATSResponseBO,
							seedDataForAutoLaunchedSimpleSessionExample.appId,
							seedDataForAutoLaunchedSimpleSessionExample.appStartedByUser);
				}
			}

		}

	}

	@Test
	@Ignore("http://bug.corp.yahoo.com/show_bug.cgi?id=7166198")
	public void testSleepJob() throws Exception {
		ExecutorService execService = Executors.newFixedThreadPool(1);
		String addendum;
		if (!timelineserverStarted) {
			// startTimelineServerOnRM(rmHostname);
		}

		for (DAG aSeedDag : seedDataForAutoLaunchedSleepJob.dags) {
			addendum = "?primaryFilter=dagName:" + aSeedDag.name;
			EntityTypes entityTypeInRequest = EntityTypes.TEZ_DAG_ID;
			String url = getATSUrl() + entityTypeInRequest + addendum;
			makeHttpCallAndEnqueueConsumedResponse(execService, url,
					seedDataForAutoLaunchedSleepJob.appStartedByUser, entityTypeInRequest,
					dagIdQueue, expectEverythingMap());
			execService.shutdown();
			while (!execService.isTerminated()) {
				Thread.sleep(1000);
			}
			GenericATSResponseBO genericATSResponse = dagIdQueue.poll();
			for (EntityInGenericATSResponseBO anEntityInGenericATSResponseBO : genericATSResponse.entities) {
				if (anEntityInGenericATSResponseBO.entity
						.contains(seedDataForAutoLaunchedSleepJob.appId)) {
					if (!anEntityInGenericATSResponseBO.primaryfilters.get(
							"dagName").contains(aSeedDag.name)) {
						// DAG does not belong to this run
						continue;
					}
					// This is the DAG we are interested in
					compareAppIdUserNameAndVertexIdsInResponse(aSeedDag,
							anEntityInGenericATSResponseBO,
							seedDataForAutoLaunchedSleepJob.appId,
							seedDataForAutoLaunchedSleepJob.appStartedByUser);
				}
			}

		}

	}

	void compareAppIdUserNameAndVertexIdsInResponse(DAG aSeedDag,
			EntityInGenericATSResponseBO anEntityInGenericATSResponseBO,
			String appId, String user) {
		Assert.assertEquals(aSeedDag.id.trim(),
				anEntityInGenericATSResponseBO.entity.trim());
		Assert.assertTrue(anEntityInGenericATSResponseBO.primaryfilters.get(
				"dagName").contains(aSeedDag.name.trim()));
		for (Vertex aVertexId : aSeedDag.vertices) {
			TestSession.logger.info("Checking for presence of vertex:"
					+ aVertexId
					+ " in "
					+ anEntityInGenericATSResponseBO.relatedentities
							.get(EntityTypes.TEZ_VERTEX_ID.name()));
			Assert.assertTrue(anEntityInGenericATSResponseBO.relatedentities
					.get(EntityTypes.TEZ_VERTEX_ID.name()).contains(aVertexId));
		}
		Assert.assertTrue(((OtherInfoTezDagIdBO) anEntityInGenericATSResponseBO.otherinfo).applicationId
				.equals(appId));
		Assert.assertTrue(((OtherInfoTezDagIdBO) anEntityInGenericATSResponseBO.otherinfo).dagPlan.dagName
				.equals(aSeedDag.name));

		Assert.assertEquals(
				((OtherInfoTezDagIdBO) anEntityInGenericATSResponseBO.otherinfo).dagPlan.vertices
						.size(), aSeedDag.vertices.size());
		Assert.assertEquals(
				anEntityInGenericATSResponseBO.primaryfilters.get("user"), user);

	}
}
