package hadooptest.tez.utils;

import hadooptest.TestSession;
import hadooptest.tez.ats.ATSEventsEntityBO;
import hadooptest.tez.ats.ATSTestsBaseClass;
import hadooptest.tez.ats.CounterGroup;
import hadooptest.tez.ats.EntityInGenericATSResponseBO;
import hadooptest.tez.ats.GenericATSResponseBO;
import hadooptest.tez.ats.OtherInfoTezApplicationAttemptBO;
import hadooptest.tez.ats.OtherInfoTezContainerIdBO;
import hadooptest.tez.ats.OtherInfoTezDagIdBO;
import hadooptest.tez.ats.OtherInfoTezTaskAttemptIdBO;
import hadooptest.tez.ats.OtherInfoTezTaskIdBO;
import hadooptest.tez.ats.OtherInfoTezVertexIdBO;
import hadooptest.tez.ats.ATSTestsBaseClass.EntityTypes;
import hadooptest.tez.ats.ATSTestsBaseClass.ResponseComposition;
import hadooptest.tez.ats.CounterGroup.Counter;
import hadooptest.tez.ats.OtherInfoTezDagIdBO.DagPlanBO;
import hadooptest.tez.ats.OtherInfoTezDagIdBO.DagPlanBO.DagPlanEdgeBO;
import hadooptest.tez.ats.OtherInfoTezDagIdBO.DagPlanBO.DagPlanVertexBO;
import hadooptest.tez.ats.OtherInfoTezDagIdBO.DagPlanBO.DagPlanVertexBO.DagVertexAdditionalInputBO;
import hadooptest.tez.ats.OtherInfoTezVertexIdBO.Stats;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.Assert;
/**
 * This class has all the methods needed to consume a REST response received from the
 * timelineserver.  
 * @author tiwari
 *
 */
public class HtfATSUtils {
/**
 * Tests would call this method and pass the JSON response body. EntityType 
 * e.g TEZ_TASK_ATTEMPT_ID or TEZ_DAG_ID etc is also passed to this method.
 * The reason being, in the response there is a field called primaryFilters
 * that can vary. So before we start consuming the data, the response "BO"
 * (Business Object) class would pre-populate the expected keys. This 
 * helps in keeping the functions generic and fosters reuse.
 * @param responseAsJsonString
 * @param entityType
 * @return
 * @throws ParseException
 */
	public GenericATSResponseBO processATSResponse(String responseAsJsonString, EntityTypes entityType,
			Map<String, Boolean>includes)throws ParseException {
		GenericATSResponseBO genericATSResponse = new GenericATSResponseBO();
		JSONParser parser = new JSONParser();
		Object obj = parser.parse(responseAsJsonString);
		JSONObject jsonObject = (JSONObject) obj;

		EntityInGenericATSResponseBO completelyProcessedSingleEntity;
		// ENTITIES
		JSONArray entities = (JSONArray) jsonObject.get("entities");
		if (entities != null) {
			for (int entityIdx = 0; entityIdx < entities.size(); entityIdx++) {
				JSONObject aDagEntityJson = (JSONObject) entities
						.get(entityIdx);				
				completelyProcessedSingleEntity = consumeResponse(aDagEntityJson, entityType, includes);
				genericATSResponse.entities.add(completelyProcessedSingleEntity);
			}
		} else {
			JSONObject aDagEntityJson = jsonObject;
			completelyProcessedSingleEntity = consumeResponse(aDagEntityJson, entityType, includes);
			genericATSResponse.entities.add(completelyProcessedSingleEntity);
		}

		return genericATSResponse;
	}
	
	/**
	 * The REST response contains two sections. One I've observed is common across all the
	 * responses and the other (called "otherinfo") is very specific to the call that has
	 * been made. Thats the reason why {@code ATSOtherInfoEntityBO} class has been made
	 * abstract. The {@code consumeOtherInfoFromDagId} method are called based upon the
	 * entity type. Thats where the correct Object is constructed and assigned to the
	 * otherInfo property.
	 * @param entityJsonObject
	 * @param entityType
	 * @return
	 */
	private EntityInGenericATSResponseBO consumeResponse(
			JSONObject entityJsonObject, EntityTypes entityType, 
			Map<String, Boolean>expectedEntities){
		
		EntityInGenericATSResponseBO anEntityInGenericATSResponseBO =
				new EntityInGenericATSResponseBO(entityType);
		
		anEntityInGenericATSResponseBO = consumeCommonPortionsInResponse(
				entityJsonObject, anEntityInGenericATSResponseBO, expectedEntities);
		
		
		if(expectedEntities.get("otherinfo") == true){
			switch (entityType){
				case TEZ_DAG_ID:				
					consumeOtherInfoFromDagId(anEntityInGenericATSResponseBO, entityJsonObject);				
					break;
				case TEZ_CONTAINER_ID:
					consumeOtherInfoFromContainerId(anEntityInGenericATSResponseBO, entityJsonObject);
					break;
				case TEZ_APPLICATION_ATTEMPT:
					consumeOtherInfoFromApplicationAttempt(anEntityInGenericATSResponseBO, entityJsonObject);
					break;
				case TEZ_TASK_ATTEMPT_ID:
					consumeOtherInfoFromTezTaskAttemptId(anEntityInGenericATSResponseBO, entityJsonObject);
					break;
				case TEZ_TASK_ID:
					consumeOtherInfoFromTezTaskId(anEntityInGenericATSResponseBO, entityJsonObject);
					break;
				case TEZ_VERTEX_ID:
					consumeOtherInfoFromTezVertexId(anEntityInGenericATSResponseBO, entityJsonObject);
					break;
			}
		}else{
			Assert.assertNull((entityJsonObject.get("otherinfo")));
		}
		return anEntityInGenericATSResponseBO;
	}
	
	/**
	 * This method consumes the common portion of the REST response. Calls such as
	 * /ws/v1/timeline/TEZ_TASK_ATTEMPT_ID  or /ws/v1/timeline/TEZ_TASK_ID etc
	 * have a set of keys that are common, so this is a common method to consume
	 * those values
	 * @param aDagEntityJson
	 * @param anEntityInGenericATSResponseBO
	 * @return
	 */
	EntityInGenericATSResponseBO consumeCommonPortionsInResponse(JSONObject aDagEntityJson, 
			EntityInGenericATSResponseBO anEntityInGenericATSResponseBO, Map<String,Boolean>expectedEntities) {

		if(expectedEntities.get("events")==true){
			JSONArray eventsJsonArray = (JSONArray) (aDagEntityJson.get("events"));
			for (int eventIdx = 0; eventIdx < eventsJsonArray.size(); eventIdx++) {
				ATSEventsEntityBO anATSEventsEntityBO = new ATSEventsEntityBO(expectedEntities.get("events"));
				JSONObject anEventJsonObject = (JSONObject) eventsJsonArray.get(eventIdx);
				anATSEventsEntityBO.timestamp = (Long) anEventJsonObject.get("timestamp");
				anATSEventsEntityBO.eventtype = (String) anEventJsonObject.get("eventtype");
				anATSEventsEntityBO.eventinfo = (JSONObject) anEventJsonObject.get("eventinfo");
				anEntityInGenericATSResponseBO.events.add(anATSEventsEntityBO);
			}
		}else{
			anEntityInGenericATSResponseBO.events=null;
		}
		// ENTITY TYPE
		anEntityInGenericATSResponseBO.entityType = (String) aDagEntityJson.get("entitytype");
		// ENTITY
		anEntityInGenericATSResponseBO.entity = (String) aDagEntityJson.get("entity");
		// START TIME
		anEntityInGenericATSResponseBO.starttime = (Long) aDagEntityJson.get("starttime");

		// RELATED ENTITIES
		if(expectedEntities.get("relatedentities") == true){
			JSONObject relatedEntitiesJson = (JSONObject) (aDagEntityJson.get("relatedentities"));
			for (String relatedEntityKey:anEntityInGenericATSResponseBO.relatedentities.keySet()){
				List<String>relatedEntitiesList = new ArrayList<String>();
				JSONArray relatedEntitiesJsonArray = (JSONArray) relatedEntitiesJson.get(relatedEntityKey);
	
				for (int ii = 0; ii < relatedEntitiesJsonArray.size(); ii++) {
					String aRelatedEntity = (String) relatedEntitiesJsonArray.get(ii);
					relatedEntitiesList.add(aRelatedEntity);
				}
				anEntityInGenericATSResponseBO.relatedentities.put(relatedEntityKey,relatedEntitiesList);			
			}
		}else{
			Assert.assertNull((aDagEntityJson.get("relatedentities")));
			anEntityInGenericATSResponseBO.relatedentities=null;
		}
		// PRIMARY FILTER
		if(expectedEntities.get("primaryfilters")== true){
			JSONObject primaryFiltersJson = (JSONObject) (aDagEntityJson.get("primaryfilters"));
			for (String aPrimaryFilterKey:anEntityInGenericATSResponseBO.primaryfilters.keySet()){
				List<String>primaryFiltersList = new ArrayList<String>();
				JSONArray primaryFiltersJsonArray = (JSONArray) primaryFiltersJson.get(aPrimaryFilterKey);			
				for (int ii = 0; ii < primaryFiltersJsonArray.size(); ii++) {
					String aPrimaryFilter = (String) primaryFiltersJsonArray.get(ii);
					primaryFiltersList.add(aPrimaryFilter);
				}
				anEntityInGenericATSResponseBO.primaryfilters.put(aPrimaryFilterKey,primaryFiltersList);
				
			}
		}else{
			Assert.assertNull((aDagEntityJson.get("primaryfilters")));
			anEntityInGenericATSResponseBO.primaryfilters=null;
		}

		return anEntityInGenericATSResponseBO;
	}
	
	/**
	 * The HTTP response across enities has certain common elements. Those are handled by the
	 * {@code} consumeCommonPortionsInResponse. But the otherInfo varies. Hence for each of the
	 * entities, we will just write specialized functions to consume the said otherInfo. This
	 * function consumes otherInfo for TEZ_DAG_ID and provides a specialized class to the 
	 * abstract implementation of OtherInfo.
	 * @param entityPopulatedThusFar
	 * @param aDagEntityJson
	 * @return
	 */
	EntityInGenericATSResponseBO consumeOtherInfoFromDagId(
			EntityInGenericATSResponseBO entityPopulatedThusFar,
			JSONObject aDagEntityJson) {

		// OTHER INFO
		OtherInfoTezDagIdBO dagOtherInfoBO = new OtherInfoTezDagIdBO();
		JSONObject otherInfoJson = (JSONObject) (aDagEntityJson.get("otherinfo"));
		dagOtherInfoBO.startTime = (Long) otherInfoJson.get("startTime");
		dagOtherInfoBO.status = (String) otherInfoJson.get("status");
		dagOtherInfoBO.initTime = (Long) otherInfoJson.get("initTime");
		dagOtherInfoBO.timeTaken = (Long) otherInfoJson.get("timeTaken");
		dagOtherInfoBO.applicationId = (String) otherInfoJson.get("applicationId");

		// DagPlan is a part of it
		OtherInfoTezDagIdBO.DagPlanBO dagPlanBO = new OtherInfoTezDagIdBO.DagPlanBO();
		JSONObject dagPlanJson = (JSONObject) otherInfoJson.get("dagPlan");
		dagPlanBO.dagName = (String) dagPlanJson.get("dagName");
		dagPlanBO.version = (Long) dagPlanJson.get("version");
		JSONArray dagPlanVerticesJsonArray = (JSONArray) dagPlanJson.get("vertices");
		// dagPlan vertices
		for (int vv = 0; vv < dagPlanVerticesJsonArray.size(); vv++) {
			JSONObject vertexJson = (JSONObject) dagPlanVerticesJsonArray.get(vv);
			OtherInfoTezDagIdBO.DagPlanBO.DagPlanVertexBO aVertexEntityInDagPlanBO = new OtherInfoTezDagIdBO.DagPlanBO.DagPlanVertexBO();
			aVertexEntityInDagPlanBO.vertexName = (String) vertexJson.get("vertexName");
			aVertexEntityInDagPlanBO.processorClass = (String) vertexJson.get("processorClass");
			// outedges
			JSONArray outEdgeIdsArrayJson = (JSONArray) vertexJson.get("outEdgeIds");
			if (outEdgeIdsArrayJson != null) {
				for (int oo = 0; oo < outEdgeIdsArrayJson.size(); oo++) {
					aVertexEntityInDagPlanBO.outEdgeIds.add((String) outEdgeIdsArrayJson.get(oo));
				}
			}
			// inedges
			JSONArray inEdgeIdsArrayJson = (JSONArray) vertexJson.get("inEdgeIds");
			if (inEdgeIdsArrayJson != null) {
				for (int ii = 0; ii < inEdgeIdsArrayJson.size(); ii++) {
					aVertexEntityInDagPlanBO.inEdgeIds.add((String) inEdgeIdsArrayJson.get(ii));
				}
			}

			// additionalInputs
			JSONArray additionalInputsJsonArray = (JSONArray) vertexJson.get("additionalInputs");
			if (additionalInputsJsonArray != null) {
				for (int aa = 0; aa < additionalInputsJsonArray.size(); aa++) {
					OtherInfoTezDagIdBO.DagPlanBO.DagPlanVertexBO.DagVertexAdditionalInputBO 
						dagVertexAdditionalInput = new OtherInfoTezDagIdBO.DagPlanBO.DagPlanVertexBO.DagVertexAdditionalInputBO();
					JSONObject additionalInputsJson = (JSONObject) additionalInputsJsonArray.get(aa);
					dagVertexAdditionalInput.name = (String) additionalInputsJson.get("name");
					dagVertexAdditionalInput.clazz = (String) additionalInputsJson.get("class");
					dagVertexAdditionalInput.initializer = (String) additionalInputsJson.get("initializer");
					//Add the additional input to the vertex entity that is being built
					aVertexEntityInDagPlanBO.additionalInputs.add(dagVertexAdditionalInput);

				}
			}

			dagPlanBO.vertices.add(aVertexEntityInDagPlanBO);

		}// End FOR loop processing of dagplan vertices
		
		//EDGES
		JSONArray edgesJsonArray = (JSONArray) (dagPlanJson.get("edges"));
		for (int ee = 0; ee < edgesJsonArray.size(); ee++) {
			JSONObject anEdgeJson = (JSONObject) edgesJsonArray.get(ee);
			OtherInfoTezDagIdBO.DagPlanBO.DagPlanEdgeBO aDagEdgeBO = new OtherInfoTezDagIdBO.DagPlanBO.DagPlanEdgeBO();
			aDagEdgeBO.edgeId = (String) anEdgeJson.get("edgeId");
			aDagEdgeBO.inputVertexName = (String) anEdgeJson
					.get("inputVertexName");
			aDagEdgeBO.outputVertexName = (String) anEdgeJson
					.get("outputVertexName");
			aDagEdgeBO.dataMovementType = (String) anEdgeJson
					.get("dataMovementType");
			aDagEdgeBO.dataSourceType = (String) anEdgeJson
					.get("dataSourceType");
			aDagEdgeBO.schedulingType = (String) anEdgeJson
					.get("schedulingType");
			aDagEdgeBO.edgeSourceClass = (String) anEdgeJson
					.get("edgeSourceClass");
			aDagEdgeBO.edgeDestinationClass = (String) anEdgeJson
					.get("edgeDestinationClass");

			dagPlanBO.edges.add(aDagEdgeBO);
		}

		// Add dagPlan to the OtherInfo
		dagOtherInfoBO.dagPlan = dagPlanBO;
		
		dagOtherInfoBO.endTime = (Long) otherInfoJson.get("endTime");
		dagOtherInfoBO.diagnostics = (String) otherInfoJson.get("diagnostics");

		//Retrieve the counters
		dagOtherInfoBO.counters = retrieveCounters(otherInfoJson);

		// Add otherInfo to DagEntityBO
		entityPopulatedThusFar.otherinfo = dagOtherInfoBO;

		return entityPopulatedThusFar;
	}

	/**
	 * The HTTP response across enities has certain common elements. Those are handled by the
	 * {@code} consumeCommonPortionsInResponse. But the otherInfo varies. Hence for each of the
	 * entities, we will just write specialized functions to consume the said otherInfo. This
	 * function consumes otherInfo for TEZ_CONTAINER_ID  and provides a specialized class to the
	 * abstract implementation.
	 * @param entityPopulatedThusFar
	 * @param aDagEntityJson
	 * @return
	 */

	EntityInGenericATSResponseBO consumeOtherInfoFromContainerId(
			EntityInGenericATSResponseBO entityPopulatedThusFar,
			JSONObject aDagEntityJson) {

		// OTHER INFO
		OtherInfoTezContainerIdBO containerIdgOtherInfoBO = new OtherInfoTezContainerIdBO();
		JSONObject otherInfoJson = (JSONObject) (aDagEntityJson.get("otherinfo"));
		containerIdgOtherInfoBO.exitStatus = (Long) otherInfoJson.get("exitStatus");
		containerIdgOtherInfoBO.endTime = (Long) otherInfoJson.get("endTime");

		// Add otherInfo to DagEntityBO
		entityPopulatedThusFar.otherinfo = containerIdgOtherInfoBO;

		return entityPopulatedThusFar;
	}

	/**
	 * The HTTP response across enities has certain common elements. Those are handled by the
	 * {@code} consumeCommonPortionsInResponse. But the otherInfo varies. Hence for each of the
	 * entities, we will just write specialized functions to consume the said otherInfo. This
	 * function consumes otherInfo for TEZ_APPLICATION_ATTEMPT and provides a specialized class to the
	 * abstract implementation.
	 * @param entityPopulatedThusFar
	 * @param aDagEntityJson
	 * @return
	 */

	EntityInGenericATSResponseBO consumeOtherInfoFromApplicationAttempt(
			EntityInGenericATSResponseBO entityPopulatedThusFar,
			JSONObject aDagEntityJson) {

		// OTHER INFO
		OtherInfoTezApplicationAttemptBO applicationAttemptOtherInfoBO = new OtherInfoTezApplicationAttemptBO();
		JSONObject otherInfoJson = (JSONObject) (aDagEntityJson.get("otherinfo"));
		applicationAttemptOtherInfoBO.appSubmitTime = (Long) otherInfoJson.get("appSubmitTime");

		// Add otherInfo to DagEntityBO
		entityPopulatedThusFar.otherinfo = applicationAttemptOtherInfoBO;

		return entityPopulatedThusFar;
	}

	/**
	 * The HTTP response across enities has certain common elements. Those are handled by the
	 * {@code} consumeCommonPortionsInResponse. But the otherInfo varies. Hence for each of the
	 * entities, we will just write specialized functions to consume the said otherInfo. This
	 * function consumes otherInfo for TEZ_VERTEX_ID and provides a specialized class to the
	 * abstract implementation.
	 * @param entityPopulatedThusFar
	 * @param aDagEntityJson
	 * @return
	 */

	EntityInGenericATSResponseBO consumeOtherInfoFromTezVertexId(
			EntityInGenericATSResponseBO entityPopulatedThusFar,
			JSONObject aDagEntityJson) {

		// OTHER INFO
		OtherInfoTezVertexIdBO tezVertexIdOtherInfoBO = new OtherInfoTezVertexIdBO();
		JSONObject otherInfoJson = (JSONObject) (aDagEntityJson.get("otherinfo"));
		tezVertexIdOtherInfoBO.numFailedTasks = (Long) otherInfoJson.get("numFailedTasks");
		tezVertexIdOtherInfoBO.numSucceededTasks = (Long) otherInfoJson.get("numSucceededTasks");
		tezVertexIdOtherInfoBO.status = (String) otherInfoJson.get("status");
		tezVertexIdOtherInfoBO.vertexName = (String) otherInfoJson.get("vertexName");
		
		Stats statsBO = new Stats();
		JSONObject statsJsonObject = (JSONObject) otherInfoJson.get("stats");
		//First task start time
		statsBO.firstTaskStartTime = (Long) statsJsonObject.get("firstTaskStartTime");
		JSONArray aGenericJsonArray = (JSONArray) statsJsonObject.get("firstTasksToStart");
		for(int xx=0;xx<aGenericJsonArray.size();xx++){
			statsBO.firstTasksToStart.add((String) aGenericJsonArray.get(xx));
		}
		//Last task finish time
		statsBO.lastTaskFinishTime = (Long) statsJsonObject.get("lastTaskFinishTime");
		aGenericJsonArray = (JSONArray) statsJsonObject.get("lastTasksToFinish");
		for(int xx=0;xx<aGenericJsonArray.size();xx++){
			statsBO.lastTasksToFinish.add((String) aGenericJsonArray.get(xx));
		}
		statsBO.minTaskDuration = (Long)statsJsonObject.get("minTaskDuration");
		statsBO.maxTaskDuration = (Long)statsJsonObject.get("maxTaskDuration");
		statsBO.avgTaskDuration = (Double)statsJsonObject.get("avgTaskDuration");
		
		//Shortest duration tasks
		aGenericJsonArray = (JSONArray) statsJsonObject.get("shortestDurationTasks");
		for(int xx=0;xx<aGenericJsonArray.size();xx++){
			statsBO.shortestDurationTasks.add((String) aGenericJsonArray.get(xx));
		}
		//Longest duration tasks
		aGenericJsonArray = (JSONArray) statsJsonObject.get("longestDurationTasks");
		for(int xx=0;xx<aGenericJsonArray.size();xx++){
			statsBO.longestDurationTasks.add((String) aGenericJsonArray.get(xx));
		}
		tezVertexIdOtherInfoBO.stats = statsBO;
		// Stats Object ends here
		
		
		tezVertexIdOtherInfoBO.processorClassName = (String) otherInfoJson.get("processorClassName");
		tezVertexIdOtherInfoBO.endTime = (Long) otherInfoJson.get("endTime");

		//Retrieve the counters
		tezVertexIdOtherInfoBO.counters = retrieveCounters(otherInfoJson);
		
		//There are still some elements after the counter, include 'em as well
		tezVertexIdOtherInfoBO.startTime = (Long) otherInfoJson.get("startTime");
		tezVertexIdOtherInfoBO.initTime = (Long) otherInfoJson.get("initTime");
		tezVertexIdOtherInfoBO.numTasks = (Long) otherInfoJson.get("numTasks");
		tezVertexIdOtherInfoBO.timeTaken = (Long) otherInfoJson.get("timeTaken");
		tezVertexIdOtherInfoBO.numKilledTasks = (Long) otherInfoJson.get("numKilledTasks");
		tezVertexIdOtherInfoBO.numCompletedTasks = (Long) otherInfoJson.get("numCompletedTasks");
		tezVertexIdOtherInfoBO.diagnostics = (String) otherInfoJson.get("diagnostics");
		tezVertexIdOtherInfoBO.initRequestedTime = (Long) otherInfoJson.get("initRequestedTime");
		tezVertexIdOtherInfoBO.startRequestedTime = (Long) otherInfoJson.get("startRequestedTime");
		
		// Add otherInfo to DagEntityBO
		entityPopulatedThusFar.otherinfo = tezVertexIdOtherInfoBO;

		return entityPopulatedThusFar;
	}

	/**
	 * The HTTP response across enities has certain common elements. Those are handled by the
	 * {@code} consumeCommonPortionsInResponse. But the otherInfo varies. Hence for each of the
	 * entities, we will just write specialized functions to consume the said otherInfo. This
	 * function consumes otherInfo for TEZ_TASK_ID and provides a specialized class to the
	 * abstract implementation.
	 * @param entityPopulatedThusFar
	 * @param aDagEntityJson
	 * @return
	 */

	EntityInGenericATSResponseBO consumeOtherInfoFromTezTaskId(
			EntityInGenericATSResponseBO entityPopulatedThusFar,
			JSONObject aDagEntityJson) {

		// OTHER INFO
		OtherInfoTezTaskIdBO tezTaskIdOtherInfoBO = new OtherInfoTezTaskIdBO();
		JSONObject otherInfoJson = (JSONObject) (aDagEntityJson.get("otherinfo"));
		tezTaskIdOtherInfoBO.startTime = (Long) otherInfoJson.get("startTime");
		tezTaskIdOtherInfoBO.status = (String) otherInfoJson.get("status");
		tezTaskIdOtherInfoBO.timeTaken = (Long) otherInfoJson.get("timeTaken");
		tezTaskIdOtherInfoBO.scheduledTime = (Long) otherInfoJson.get("scheduledTime");
		tezTaskIdOtherInfoBO.endTime = (Long) otherInfoJson.get("endTime");
		tezTaskIdOtherInfoBO.diagnostics = (String) otherInfoJson.get("diagnostics");
		
		//Retrieve the counters
		tezTaskIdOtherInfoBO.counters = retrieveCounters(otherInfoJson);
		
		// Add otherInfo to DagEntityBO
		entityPopulatedThusFar.otherinfo = tezTaskIdOtherInfoBO;

		return entityPopulatedThusFar;
	}
	
	/**
	 * The HTTP response across enities has certain common elements. Those are handled by the
	 * {@code} consumeCommonPortionsInResponse. But the otherInfo varies. Hence for each of the
	 * entities, we will just write specialized functions to consume the said otherInfo. This
	 * function consumes otherInfo for TEZ_TASK_ATTEMPT_ID and provides a specialized class to the
	 * abstract implementation.
	 * @param entityPopulatedThusFar
	 * @param aDagEntityJson
	 * @return
	 */

	EntityInGenericATSResponseBO consumeOtherInfoFromTezTaskAttemptId(
			EntityInGenericATSResponseBO entityPopulatedThusFar,
			JSONObject aDagEntityJson) {

		// OTHER INFO
		OtherInfoTezTaskAttemptIdBO tezTaskAttemptIdOtherInfoBO = new OtherInfoTezTaskAttemptIdBO();
		JSONObject otherInfoJson = (JSONObject) (aDagEntityJson.get("otherinfo"));
		tezTaskAttemptIdOtherInfoBO.startTime = (Long) otherInfoJson.get("startTime");
		tezTaskAttemptIdOtherInfoBO.status = (String) otherInfoJson.get("status");
		tezTaskAttemptIdOtherInfoBO.timeTaken = (Long) otherInfoJson.get("timeTaken");
		tezTaskAttemptIdOtherInfoBO.inProgressLogsURL = (String) otherInfoJson.get("inProgressLogsURL");
		tezTaskAttemptIdOtherInfoBO.completedLogsURL = (String) otherInfoJson.get("completedLogsURL");
		tezTaskAttemptIdOtherInfoBO.endTime = (Long) otherInfoJson.get("endTime");
		tezTaskAttemptIdOtherInfoBO.diagnostics = (String) otherInfoJson.get("diagnostics");
		
		//Retrieve the counters
		tezTaskAttemptIdOtherInfoBO.counters = retrieveCounters(otherInfoJson);
		
		// Add otherInfo to DagEntityBO
		entityPopulatedThusFar.otherinfo = tezTaskAttemptIdOtherInfoBO;

		return entityPopulatedThusFar;
	}
	

	/**
	 * Several responses have counters JSON present in them. This generalized routine
	 * should care for all the cases.
	 * @return
	 */
	List<CounterGroup> retrieveCounters(JSONObject otherInfoJson){
		List<CounterGroup> counterGroups = new ArrayList<CounterGroup>();
		JSONObject countersJsonObject =  (JSONObject) otherInfoJson.get("counters");
		if (countersJsonObject.containsKey("counterGroups")){
			JSONArray counterGroupsJsonArray =  (JSONArray) countersJsonObject.get("counterGroups");
			for (int xx=0;xx<counterGroupsJsonArray.size();xx++){
				JSONObject aCounterGroupJson = (JSONObject) counterGroupsJsonArray.get(xx);
				CounterGroup aCounterGroupBO = new CounterGroup();
				aCounterGroupBO.counterGroupName = (String) aCounterGroupJson.get("counterGroupName");
				aCounterGroupBO.counterGroupDisplayName = (String) aCounterGroupJson.get("counterGroupDisplayName");
				//Get the counters, off the array
				JSONArray countersJsonArray =  (JSONArray) aCounterGroupJson.get("counters");
				for(int yy=0;yy<countersJsonArray.size();yy++){
					JSONObject aCounterJson = (JSONObject) countersJsonArray.get(yy);
					Counter counterBO = new Counter();
					counterBO.counterName = (String) aCounterJson.get("counterName");
					counterBO.counterDisplayName = (String) aCounterJson.get("counterDisplayName");
					counterBO.counterValue = (Long) aCounterJson.get("counterValue");
				
					aCounterGroupBO.addCounter(counterBO);
				}
				counterGroups.add(aCounterGroupBO);
			}
		}
		
		return counterGroups;
	}
	
	public boolean takeFirstItemAndCompareItAgainstAllTheOtherItemsInQueue(Queue<GenericATSResponseBO> queue) {
		boolean verdict = true;
		if (queue.size() == 0)
			return true;
		GenericATSResponseBO referenceItem = queue.peek();
		GenericATSResponseBO anItem;
		while ((anItem = queue.poll()) != null) {
			if (!referenceItem.equals(anItem)) {
				verdict = false;
				break;
			}
		}
		return verdict;
	}
	
	public List<String> retrieveValuesFromFormattedResponse(GenericATSResponseBO dagIdResponse, 
			Object field, String lookupKey, int indexPos){
		List<String> retrievedStrings = new ArrayList<String>();
		
		if (field instanceof ResponseComposition.EVENTS){
			//Ignore EVENTS for now
		}else if(field instanceof ResponseComposition.ENTITYTYPE){
			retrievedStrings.add(dagIdResponse.entities.get(indexPos).entityType);		
			
		}else if(field instanceof ResponseComposition.ENTITY){
			retrievedStrings.add(dagIdResponse.entities.get(indexPos).entity);
			
		}else if(field instanceof ResponseComposition.STARTTIME){
			retrievedStrings.add(dagIdResponse.entities.get(indexPos).starttime.toString());			
			
		}else if(field instanceof ResponseComposition.RELATEDENTITIES){
			List<String>tempList;
			tempList = dagIdResponse.entities.get(indexPos).relatedentities.get(lookupKey);
			retrievedStrings.addAll(tempList);
			
		}else if(field instanceof ResponseComposition.PRIMARYFILTERS){
			List<String>tempList;
			tempList = dagIdResponse.entities.get(indexPos).primaryfilters.get(lookupKey);			
			retrievedStrings.addAll(tempList);
		}	
		
		return retrievedStrings;
	}

	
	public EntityInGenericATSResponseBO searchAndRetrieveSingleEntityFromBunch (
			GenericATSResponseBO bunch,
			EntityTypes entityType, GenericATSResponseBO singleResponse) {
		EntityInGenericATSResponseBO retrievedEntity = null;
		for(EntityInGenericATSResponseBO anEntityInBunch:bunch.entities){
			if (anEntityInBunch.entityType.equals(entityType.name())
					&& anEntityInBunch.entity.equals(singleResponse.entities.get(0).entity)){
				retrievedEntity = anEntityInBunch;
				break;
			}
		}
		return retrievedEntity;
	}

}
