package hadooptest.tez.ats;

import static com.jayway.restassured.RestAssured.given;
import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.cluster.hadoop.HadoopComponent;
import hadooptest.node.hadoop.HadoopNode;
import hadooptest.tez.utils.HtfATSUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Assert;
import org.junit.experimental.categories.Category;

import com.jayway.restassured.response.Response;

@Category(SerialTests.class)
public class TestConcurrentRequests extends ATSTestsBaseClass {
	// @Test
	public void testCompleteResponses() throws Exception {
		ExecutorService execService = Executors.newFixedThreadPool(10);
		if (!timelineserverStarted) {
			// startTimelineServerOnRM(rmHostname);
		}
		HtfATSUtils atsUtils = new HtfATSUtils();
		for (int xx = 0; xx < 2; xx++) {
			EntityTypes entityTypeInRequest = EntityTypes.TEZ_DAG_ID;
			String url = getATSUrl() + "/" + entityTypeInRequest + "/";

			makeHttpCallAndEnqueueConsumedResponse(execService, url,
					HadooptestConstants.UserNames.HITUSR_1,
					entityTypeInRequest, dagIdQueue, expectEverythingMap());

			entityTypeInRequest = EntityTypes.TEZ_CONTAINER_ID;
			url = getATSUrl() + "/" + entityTypeInRequest + "/";
			makeHttpCallAndEnqueueConsumedResponse(execService, url,
					HadooptestConstants.UserNames.HITUSR_1,
					entityTypeInRequest, containerIdQueue,
					expectEverythingMap());

			entityTypeInRequest = EntityTypes.TEZ_APPLICATION_ATTEMPT;
			url = getATSUrl() + "/" + entityTypeInRequest + "/";
			makeHttpCallAndEnqueueConsumedResponse(execService, url,
					HadooptestConstants.UserNames.HITUSR_1,
					entityTypeInRequest, applicationAttemptQueue,
					expectEverythingMap());

			entityTypeInRequest = EntityTypes.TEZ_VERTEX_ID;
			url = getATSUrl() + "/" + entityTypeInRequest + "/";
			makeHttpCallAndEnqueueConsumedResponse(execService, url,
					HadooptestConstants.UserNames.HITUSR_1,
					entityTypeInRequest, vertexIdQueue, expectEverythingMap());

			entityTypeInRequest = EntityTypes.TEZ_TASK_ID;
			url = getATSUrl() + "/" + entityTypeInRequest + "/";
			makeHttpCallAndEnqueueConsumedResponse(execService, url,
					HadooptestConstants.UserNames.HITUSR_1,
					entityTypeInRequest, taskIdQueue, expectEverythingMap());

			entityTypeInRequest = EntityTypes.TEZ_TASK_ATTEMPT_ID;
			url = getATSUrl() + "/" + entityTypeInRequest + "/";
			makeHttpCallAndEnqueueConsumedResponse(execService, url,
					HadooptestConstants.UserNames.HITUSR_1,
					entityTypeInRequest, taskAttemptIdQueue,
					expectEverythingMap());
		}
		execService.shutdown();
		while (!execService.isTerminated()) {
			TestSession.logger
					.info("Thread sleeping while awaiting REST calls");
			Thread.sleep(1000);
		}
		Assert.assertTrue(atsUtils
				.takeFirstItemAndCompareItAgainstAllTheOtherItemsInQueue(dagIdQueue));
		Assert.assertTrue(atsUtils
				.takeFirstItemAndCompareItAgainstAllTheOtherItemsInQueue(containerIdQueue));
		Assert.assertTrue(atsUtils
				.takeFirstItemAndCompareItAgainstAllTheOtherItemsInQueue(applicationAttemptQueue));
		Assert.assertTrue(atsUtils
				.takeFirstItemAndCompareItAgainstAllTheOtherItemsInQueue(vertexIdQueue));
		Assert.assertTrue(atsUtils
				.takeFirstItemAndCompareItAgainstAllTheOtherItemsInQueue(taskIdQueue));
		Assert.assertTrue(atsUtils
				.takeFirstItemAndCompareItAgainstAllTheOtherItemsInQueue(taskAttemptIdQueue));

	}

	// @Test
	public void testCascading() throws Exception {
		if (!timelineserverStarted) {
			// startTimelineServerOnRM(rmHostname);
		}
		ExecutorService execService = Executors.newFixedThreadPool(10);
//		ExecutorService vertexIdExecService = Executors.newFixedThreadPool(10);
//		ExecutorService taskIdExecService = Executors.newFixedThreadPool(10);
//		ExecutorService taskAttemptIdExecService = Executors.newFixedThreadPool(10);

		HtfATSUtils atsUtils = new HtfATSUtils();


		List<String> retrievedStringList = new ArrayList<String>();
		List<String> expectedPrimaryfilterList = new ArrayList<String>();
		Map<String, List<String>> expectedPrimaryfilterMap = new HashMap<String, List<String>>();
		List<String> vertexIds = new ArrayList<String>();
		List<String> taskIds = new ArrayList<String>();

		List<String> taskAttemptIds = new ArrayList<String>();

		/**
		 * Make calls into TEZ_DAG_ID
		 */
		EntityTypes entityTypeInRequest = EntityTypes.TEZ_DAG_ID;
		String url = getATSUrl() + "/" + entityTypeInRequest + "/";

		makeHttpCallAndEnqueueConsumedResponse(execService, 
				url+"dag_1412943684044_0003_1",
				HadooptestConstants.UserNames.HITUSR_1,
				entityTypeInRequest, dagIdQueue, expectEverythingMap());		
		execService.shutdown();	
		while (!execService.isTerminated()) {
			TestSession.logger
					.info("Thread sleeping while awaiting DAG ID REST calls");
			Thread.sleep(1000);
		}
		GenericATSResponseBO dagIdResponse = dagIdQueue.poll();
		retrievedStringList = atsUtils.retrieveValuesFromFormattedResponse(dagIdResponse,
				ResponseComposition.PRIMARYFILTERS.EXPECTED, "dagName", 0);
		//Check dagName
		expectedPrimaryfilterList.add("MRRSleepJob");
		Assert.assertTrue(retrievedStringList.containsAll(expectedPrimaryfilterList));
		//Check user
		expectedPrimaryfilterList.clear();
		retrievedStringList = atsUtils.retrieveValuesFromFormattedResponse(dagIdResponse,
				ResponseComposition.PRIMARYFILTERS.EXPECTED, "user", 0);
		expectedPrimaryfilterList.add(HadooptestConstants.UserNames.HADOOPQA);
		Assert.assertTrue(retrievedStringList.containsAll(expectedPrimaryfilterList));

		/**
		 * Make calls into TEZ_VERTEX_ID
		 */
		execService = Executors.newFixedThreadPool(10);
		vertexIds = atsUtils.retrieveValuesFromFormattedResponse(dagIdResponse,
			ResponseComposition.RELATEDENTITIES.EXPECTED, EntityTypes.TEZ_VERTEX_ID.name(),0);
		entityTypeInRequest = EntityTypes.TEZ_VERTEX_ID;		 
		for (String aVertexId:vertexIds){
			url = getATSUrl() + "/" + entityTypeInRequest + "/" + aVertexId;
			makeHttpCallAndEnqueueConsumedResponse(execService, 
				url,
				HadooptestConstants.UserNames.HITUSR_1,
				entityTypeInRequest, vertexIdQueue, expectEverythingMap());
		}
		execService.shutdown();	
		while (!execService.isTerminated()) {
			TestSession.logger
					.info("Thread sleeping while awaiting VERTEX ID REST calls");
			Thread.sleep(1000);
		}
		//Prepare the filter, Expect the dag id in the primary filter
		expectedPrimaryfilterList.clear();
		expectedPrimaryfilterList.add(dagIdResponse.entities.get(0).entity);
		GenericATSResponseBO vertexIdResponse;
		while((vertexIdResponse = vertexIdQueue.poll())!= null){
			taskIds.addAll(atsUtils.retrieveValuesFromFormattedResponse(vertexIdResponse,
					ResponseComposition.RELATEDENTITIES.EXPECTED, EntityTypes.TEZ_TASK_ID.name(),0));
			retrievedStringList = atsUtils.retrieveValuesFromFormattedResponse(vertexIdResponse,
					ResponseComposition.PRIMARYFILTERS.EXPECTED, EntityTypes.TEZ_DAG_ID.name(), 0);
			Assert.assertTrue(retrievedStringList.containsAll(expectedPrimaryfilterList));
		}

		/**
		 * Make calls into TEZ_TASK_ID
		 */
		execService= Executors.newFixedThreadPool(10);
		entityTypeInRequest = EntityTypes.TEZ_TASK_ID;;		 
		for (String aTaskId:taskIds){
			url = getATSUrl() + "/" + entityTypeInRequest + "/" + aTaskId;
			makeHttpCallAndEnqueueConsumedResponse(taskIdExecService, 
				url,
				HadooptestConstants.UserNames.HITUSR_1,
				entityTypeInRequest, taskIdQueue, expectEverythingMap());
		}
		taskIdExecService.shutdown();	
		while (!taskIdExecService.isTerminated()) {
			TestSession.logger
					.info("Thread sleeping while awaiting TASK ID REST calls");
			Thread.sleep(1000);
		}
		//Prepare the filter, Expect the dag id in the primary filter
		expectedPrimaryfilterList.add(dagIdResponse.entities.get(0).entity);
		GenericATSResponseBO vertexIdResponse;
		while((vertexIdResponse = vertexIdQueue.poll())!= null){
			taskIds.addAll(atsUtils.retrieveValuesFromFormattedResponse(vertexIdResponse,
					ResponseComposition.RELATEDENTITIES.EXPECTED, EntityTypes.TEZ_TASK_ID.name(),0));
			retrievedStringList = atsUtils.retrieveValuesFromFormattedResponse(vertexIdResponse,
					ResponseComposition.PRIMARYFILTERS.EXPECTED, EntityTypes.TEZ_DAG_ID.name(), 0);
			Assert.assertTrue(retrievedStringList.containsAll(expectedPrimaryfilterList));
		}

	}


}
