package hadooptest.tez.ats;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.cluster.hadoop.HadoopComponent;
import hadooptest.node.hadoop.HadoopNode;
import hadooptest.tez.ats.ATSTestsBaseClass.EntityTypes;
import hadooptest.tez.ats.ATSTestsBaseClass.ResponseComposition;
import hadooptest.tez.utils.HtfATSUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Stack;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SerialTests.class)
public class TestFilterRequests extends ATSTestsBaseClass {

	@Ignore("works fine")
	@Test
	public void testFilteringOnEvents() throws Exception {
		ExecutorService execService = Executors.newFixedThreadPool(10);

		if (!timelineserverStarted) {
			// startTimelineServerOnRM(rmHostname);
		}
		Map<String, Boolean> expectedEntities = getExpectedFieldsMap(
				ResponseComposition.EVENTS.EXPECTED, 
				ResponseComposition.ENTITYTYPE.EXPECTED, 
				ResponseComposition.ENTITY.EXPECTED,
				ResponseComposition.STARTTIME.EXPECTED, 
				ResponseComposition.RELATEDENTITIES.NOT_EXPECTED,
				ResponseComposition.PRIMARYFILTERS.NOT_EXPECTED, 
				ResponseComposition.OTHERINFO.NOT_EXPECTED);
		
		EntityTypes entityTypeInRequest = EntityTypes.TEZ_DAG_ID;
		String url = getATSUrl() + entityTypeInRequest + "/" + "?fields=events";
		makeHttpCallAndEnqueueConsumedResponse(execService, url,
				HadooptestConstants.UserNames.HITUSR_1,
				entityTypeInRequest, dagIdQueue, expectedEntities);

		entityTypeInRequest = EntityTypes.TEZ_VERTEX_ID;
		url = getATSUrl() + entityTypeInRequest + "/" + "?fields=events";
		makeHttpCallAndEnqueueConsumedResponse(execService, url,
				HadooptestConstants.UserNames.HITUSR_1,
				entityTypeInRequest, vertexIdQueue, expectedEntities);
		
		entityTypeInRequest = EntityTypes.TEZ_TASK_ID;
		url = getATSUrl() + entityTypeInRequest + "/" + "?fields=events";
		makeHttpCallAndEnqueueConsumedResponse(execService, url,
				HadooptestConstants.UserNames.HITUSR_1,
				entityTypeInRequest, taskIdQueue, expectedEntities);
		
		entityTypeInRequest = EntityTypes.TEZ_TASK_ATTEMPT_ID;
		url = getATSUrl() + entityTypeInRequest + "/" + "?fields=events";
		makeHttpCallAndEnqueueConsumedResponse(execService, url,
				HadooptestConstants.UserNames.HITUSR_1,
				entityTypeInRequest, taskAttemptIdQueue, expectedEntities);
		
		entityTypeInRequest = EntityTypes.TEZ_APPLICATION_ATTEMPT;
		url = getATSUrl() + entityTypeInRequest + "/" + "?fields=events";
		makeHttpCallAndEnqueueConsumedResponse(execService, url,
				HadooptestConstants.UserNames.HITUSR_1,
				entityTypeInRequest, applicationAttemptQueue, expectedEntities);

		execService.shutdown();
		while (!execService.isTerminated()) {
			TestSession.logger
					.info("Thread sleeping while awaiting DAG ID REST calls");
			Thread.sleep(1000);
		}

		Assert.assertEquals(errorCount.get(), 0);
	}

	@Ignore("Works fine")
	@Test
	public void testFilteringOnRelatedentities() throws Exception {
		ExecutorService execService = Executors.newFixedThreadPool(10);

		if (!timelineserverStarted) {
			// startTimelineServerOnRM(rmHostname);
		}
		Map<String, Boolean> expectedEntities = getExpectedFieldsMap(
				ResponseComposition.EVENTS.NOT_EXPECTED, 
				ResponseComposition.ENTITYTYPE.EXPECTED, 
				ResponseComposition.ENTITY.EXPECTED,
				ResponseComposition.STARTTIME.EXPECTED, 
				ResponseComposition.RELATEDENTITIES.EXPECTED,
				ResponseComposition.PRIMARYFILTERS.NOT_EXPECTED, 
				ResponseComposition.OTHERINFO.NOT_EXPECTED);
		String FILTER_STRING = "?fields=relatedentities";
		EntityTypes entityTypeInRequest = EntityTypes.TEZ_DAG_ID;
		String url = getATSUrl() + entityTypeInRequest + "/" + FILTER_STRING;
		makeHttpCallAndEnqueueConsumedResponse(execService, url,
				HadooptestConstants.UserNames.HITUSR_1,
				entityTypeInRequest, dagIdQueue, expectedEntities);

		entityTypeInRequest = EntityTypes.TEZ_VERTEX_ID;
		url = getATSUrl() + entityTypeInRequest + "/" + FILTER_STRING;
		makeHttpCallAndEnqueueConsumedResponse(execService, url,
				HadooptestConstants.UserNames.HITUSR_1,
				entityTypeInRequest, vertexIdQueue, expectedEntities);
		
		entityTypeInRequest = EntityTypes.TEZ_TASK_ID;
		url = getATSUrl() + entityTypeInRequest + "/" + FILTER_STRING;
		makeHttpCallAndEnqueueConsumedResponse(execService, url,
				HadooptestConstants.UserNames.HITUSR_1,
				entityTypeInRequest, taskIdQueue, expectedEntities);
		
		entityTypeInRequest = EntityTypes.TEZ_TASK_ATTEMPT_ID;
		url = getATSUrl() + entityTypeInRequest + "/" + FILTER_STRING;
		makeHttpCallAndEnqueueConsumedResponse(execService, url,
				HadooptestConstants.UserNames.HITUSR_1,
				entityTypeInRequest, taskAttemptIdQueue, expectedEntities);
		
		entityTypeInRequest = EntityTypes.TEZ_APPLICATION_ATTEMPT;
		url = getATSUrl() + entityTypeInRequest + "/" + FILTER_STRING;
		makeHttpCallAndEnqueueConsumedResponse(execService, url,
				HadooptestConstants.UserNames.HITUSR_1,
				entityTypeInRequest, applicationAttemptQueue, expectedEntities);

		execService.shutdown();
		while (!execService.isTerminated()) {
			TestSession.logger
					.info("Thread sleeping while awaiting DAG ID REST calls");
			Thread.sleep(1000);
		}

		Assert.assertEquals(errorCount.get(), 0);
	}

	@Ignore("Works fine")
	@Test
	public void testTwoComboFiltering() throws Exception {
		ExecutorService execService = Executors.newFixedThreadPool(10);

		String FILTER_STRING = "?fields=relatedentities&limit=2";
		Map<String, Boolean> expectedEntities = getExpectedFieldsMap(
				ResponseComposition.EVENTS.NOT_EXPECTED, 
				ResponseComposition.ENTITYTYPE.EXPECTED, 
				ResponseComposition.ENTITY.EXPECTED,
				ResponseComposition.STARTTIME.EXPECTED, 
				ResponseComposition.RELATEDENTITIES.EXPECTED,
				ResponseComposition.PRIMARYFILTERS.NOT_EXPECTED, 
				ResponseComposition.OTHERINFO.NOT_EXPECTED);
		
		if (!timelineserverStarted) {
			// startTimelineServerOnRM(rmHostname);
		}

		HtfATSUtils atsUtils = new HtfATSUtils();

		List<String> retrievedValuesList = new ArrayList<String>();
		List<String> vertexIds = new ArrayList<String>();
		List<String> taskIds = new ArrayList<String>();

		List<String> taskAttemptIds = new ArrayList<String>();

		/**
		 * Make calls into TEZ_DAG_ID
		 */
		EntityTypes entityTypeInRequest = EntityTypes.TEZ_DAG_ID;
		String url = getATSUrl() + entityTypeInRequest + "/";

		makeHttpCallAndEnqueueConsumedResponse(execService, 
				url+FILTER_STRING,
				HadooptestConstants.UserNames.HITUSR_1,
				entityTypeInRequest, dagIdQueue, expectedEntities);		
		execService.shutdown();	
		while (!execService.isTerminated()) {
			TestSession.logger
					.info("Thread sleeping while awaiting DAG ID REST calls");
			Thread.sleep(1000);
		}
		GenericATSResponseBO dagIdResponse = dagIdQueue.poll();		
		//Add all the vertex Ids
		retrievedValuesList.addAll(atsUtils.retrieveValuesFromFormattedResponse(dagIdResponse,
				ResponseComposition.RELATEDENTITIES.EXPECTED, EntityTypes.TEZ_VERTEX_ID.name(), 0));		
		retrievedValuesList.addAll(atsUtils.retrieveValuesFromFormattedResponse(dagIdResponse,
				ResponseComposition.RELATEDENTITIES.EXPECTED, EntityTypes.TEZ_VERTEX_ID.name(), 1));		

		/**
		 * Make calls into TEZ_VERTEX_ID
		 */
		execService = Executors.newFixedThreadPool(10);
		entityTypeInRequest = EntityTypes.TEZ_VERTEX_ID;		 
		for (String aVertexId:vertexIds){
			url = getATSUrl()  + entityTypeInRequest + "/" + aVertexId + FILTER_STRING;
			makeHttpCallAndEnqueueConsumedResponse(execService, 
				url,
				HadooptestConstants.UserNames.HITUSR_1,
				entityTypeInRequest, vertexIdQueue, expectedEntities);
		}
		execService.shutdown();	
		while (!execService.isTerminated()) {
			TestSession.logger
					.info("Thread sleeping while awaiting VERTEX ID REST calls");
			Thread.sleep(1000);
		}
		//Prepare the filter, Expect the dag id in the primary filter
		GenericATSResponseBO vertexIdResponse;
		while((vertexIdResponse = vertexIdQueue.poll())!= null){
			taskIds.addAll(atsUtils.retrieveValuesFromFormattedResponse(vertexIdResponse,
					ResponseComposition.RELATEDENTITIES.EXPECTED, EntityTypes.TEZ_TASK_ID.name(),0));
		}

		/**
		 * Make calls into TEZ_TASK_ID
		 */
		execService= Executors.newFixedThreadPool(10);
		entityTypeInRequest = EntityTypes.TEZ_TASK_ID;		
		for (String aTaskId:taskIds){
			url = getATSUrl()  + entityTypeInRequest + "/" + aTaskId + FILTER_STRING;
			makeHttpCallAndEnqueueConsumedResponse(execService, 
				url,
				HadooptestConstants.UserNames.HITUSR_1,
				entityTypeInRequest, taskIdQueue, expectedEntities);
		}
		execService.shutdown();	
		while (!execService.isTerminated()) {
			TestSession.logger
					.info("Thread sleeping while awaiting TASK ID REST calls");
			Thread.sleep(1000);
		}
		GenericATSResponseBO taskIdResponse;
		while((taskIdResponse = taskIdQueue.poll())!= null){
			//Gather related entities 
			taskAttemptIds.addAll(atsUtils.retrieveValuesFromFormattedResponse(taskIdResponse,
					ResponseComposition.RELATEDENTITIES.EXPECTED, EntityTypes.TEZ_TASK_ATTEMPT_ID.name(),0));
		}

		
		/**
		 * Make calls into TEZ_TASK_ATTEMPT_ID
		 * NOTE: Since, a call to task_attempt_id returns an empty relatedentities
		 * in spirit of the test method of testing two combo filtering, I am replacing the 1st entity,in the filter, with the enitytype
		 */
		execService= Executors.newFixedThreadPool(10);
		entityTypeInRequest = EntityTypes.TEZ_TASK_ATTEMPT_ID;		
		for (String aTaskAttemptId:taskAttemptIds){
			url = getATSUrl()  + entityTypeInRequest + "/" + aTaskAttemptId + "?entitytype=TEZ_TASK_ATTEMPT_ID&limit=2";
			makeHttpCallAndEnqueueConsumedResponse(execService, 
				url,
				HadooptestConstants.UserNames.HITUSR_1,
				entityTypeInRequest, taskAttemptIdQueue, expectedEntities);
		}
		execService.shutdown();	
		while (!execService.isTerminated()) {
			TestSession.logger
					.info("Thread sleeping while awaiting TASK ID REST calls");
			Thread.sleep(1000);
		}
		
		Assert.assertEquals(errorCount.get(), 0);		
	}
	
	@Test
	public void testThreeComboFilteringUsingFilterAsDagName() throws Exception {
		ExecutorService execService = Executors.newFixedThreadPool(10);
		Map<String, List<String>> cascadedEntitiesMap = getCascadedEntitiesMap("dag_1413669561424_0007_1");
		List<String> dagNames = cascadedEntitiesMap.get("dagName");
		String FILTER_STRING = "?primaryFilter=dagName:" + dagNames.get(0) +"&fields=relatedentities&limit=2";
		Map<String, Boolean> expectedEntities = getExpectedFieldsMap(
				ResponseComposition.EVENTS.NOT_EXPECTED, 
				ResponseComposition.ENTITYTYPE.EXPECTED, 
				ResponseComposition.ENTITY.EXPECTED,
				ResponseComposition.STARTTIME.EXPECTED, 
				ResponseComposition.RELATEDENTITIES.EXPECTED,
				ResponseComposition.PRIMARYFILTERS.NOT_EXPECTED, 
				ResponseComposition.OTHERINFO.NOT_EXPECTED);
		
		if (!timelineserverStarted) {
			// startTimelineServerOnRM(rmHostname);
		}

		HtfATSUtils atsUtils = new HtfATSUtils();

		List<String> retrievedValuesList = new ArrayList<String>();
		List<String> vertexIds = new ArrayList<String>();
		List<String> taskIds = new ArrayList<String>();

		List<String> taskAttemptIds = new ArrayList<String>();

		/**
		 * Make call into TEZ_DAG_ID
		 */
		EntityTypes entityTypeInRequest = EntityTypes.TEZ_DAG_ID;
		String url = getATSUrl() + entityTypeInRequest + "/" + "dag_1413669561424_0007_1";

		makeHttpCallAndEnqueueConsumedResponse(execService, 
				url+FILTER_STRING,
				HadooptestConstants.UserNames.HITUSR_1,
				entityTypeInRequest, dagIdQueue, expectedEntities);		
		execService.shutdown();	
		while (!execService.isTerminated()) {
			TestSession.logger
					.info("Thread sleeping while awaiting DAG ID REST calls");
			Thread.sleep(1000);
		}
		GenericATSResponseBO dagIdResponse = dagIdQueue.poll();		
		//Add all the vertex Ids
		retrievedValuesList.addAll(atsUtils.retrieveValuesFromFormattedResponse(dagIdResponse,
				ResponseComposition.RELATEDENTITIES.EXPECTED, EntityTypes.TEZ_VERTEX_ID.name(), 0));		

		/**
		 * Make calls into TEZ_VERTEX_ID
		 */
		vertexIds = cascadedEntitiesMap.get(EntityTypes.TEZ_VERTEX_ID.name());
		
		execService = Executors.newFixedThreadPool(10);
		entityTypeInRequest = EntityTypes.TEZ_VERTEX_ID;		 
		for (String aVertexId:vertexIds){
			FILTER_STRING = "?primaryFilter=TEZ_DAG_ID:" + "dag_1413669561424_0007_1" +"&fields=relatedentities&limit=2";
			url = getATSUrl()  + entityTypeInRequest + "/" + aVertexId + FILTER_STRING;
			makeHttpCallAndEnqueueConsumedResponse(execService, 
				url,
				HadooptestConstants.UserNames.HITUSR_1,
				entityTypeInRequest, vertexIdQueue, expectedEntities);
		}
		execService.shutdown();	
		while (!execService.isTerminated()) {
			TestSession.logger
					.info("Thread sleeping while awaiting VERTEX ID REST calls");
			Thread.sleep(1000);
		}
		//Prepare the filter, Expect the dag id in the primary filter
		GenericATSResponseBO vertexIdResponse;
		while((vertexIdResponse = vertexIdQueue.poll())!= null){
			taskIds.addAll(atsUtils.retrieveValuesFromFormattedResponse(vertexIdResponse,
					ResponseComposition.RELATEDENTITIES.EXPECTED, EntityTypes.TEZ_TASK_ID.name(),0));
		}

		/**
		 * Make calls into TEZ_TASK_ID
		 */
		execService= Executors.newFixedThreadPool(10);
		entityTypeInRequest = EntityTypes.TEZ_TASK_ID;		
		for (String aTaskId:taskIds){
			FILTER_STRING = "?primaryFilter=TEZ_DAG_ID:" + "dag_1413669561424_0007_1" +"&fields=relatedentities&limit=2";
			url = getATSUrl()  + entityTypeInRequest + "/" + aTaskId + FILTER_STRING;
			makeHttpCallAndEnqueueConsumedResponse(execService, 
				url,
				HadooptestConstants.UserNames.HITUSR_1,
				entityTypeInRequest, taskIdQueue, expectedEntities);
		}
		execService.shutdown();	
		while (!execService.isTerminated()) {
			TestSession.logger
					.info("Thread sleeping while awaiting TASK ID REST calls");
			Thread.sleep(1000);
		}
		GenericATSResponseBO taskIdResponse;
		while((taskIdResponse = taskIdQueue.poll())!= null){
			//Gather related entities 
			taskAttemptIds.addAll(atsUtils.retrieveValuesFromFormattedResponse(taskIdResponse,
					ResponseComposition.RELATEDENTITIES.EXPECTED, EntityTypes.TEZ_TASK_ATTEMPT_ID.name(),0));
		}
				
		Assert.assertEquals(errorCount.get(), 0);		
	}
}
