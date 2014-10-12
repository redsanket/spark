package hadooptest.tez.ats;

import hadooptest.TestSession;
import hadooptest.tez.ats.ATSTestsBaseClass.EntityTypes;
import hadooptest.tez.ats.CounterGroup.Counterr;
import hadooptest.tez.ats.OtherInfoTezVertexIdBO.Stats;
import hadooptest.tez.ats.CounterGroup;

import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class ATSUtils {

	public GenericATSResponseBO processATSResponse(String responseAsJsonString, EntityTypes entityType)
			throws ParseException {
		GenericATSResponseBO genericATSResponse = new GenericATSResponseBO();
		JSONParser parser = new JSONParser();
		Object obj = parser.parse(responseAsJsonString);
		JSONObject jsonObject = (JSONObject) obj;

		EntityInGenericATSResponseBO responseBuiltThusFar;
		EntityInGenericATSResponseBO completelyProcessedSingleEntity;
		// ENTITIES
		JSONArray entities = (JSONArray) jsonObject.get("entities");
		if (entities != null) {
			for (int entityIdx = 0; entityIdx < entities.size(); entityIdx++) {
				JSONObject aDagEntityJson = (JSONObject) entities
						.get(entityIdx);
				 responseBuiltThusFar = consumeCommonPortionsInResponse(aDagEntityJson, entityType);
				 completelyProcessedSingleEntity = 
						 consumeOtherInfoBasedOnEntityType(responseBuiltThusFar,aDagEntityJson, entityType);
				 genericATSResponse.entities.add(completelyProcessedSingleEntity);
			}
		} else {
			JSONObject aDagEntityJson = jsonObject;
			 responseBuiltThusFar = consumeCommonPortionsInResponse(aDagEntityJson, entityType);
			 completelyProcessedSingleEntity = 
					 consumeOtherInfoBasedOnEntityType(responseBuiltThusFar,aDagEntityJson, entityType);
			 genericATSResponse.entities.add(completelyProcessedSingleEntity);
		}

		return genericATSResponse;
	}
	
	private EntityInGenericATSResponseBO consumeOtherInfoBasedOnEntityType(
			EntityInGenericATSResponseBO entityPopulatedThusFar,
			JSONObject aDagEntityJson, EntityTypes entityType){
		
		switch (entityType){
			case TEZ_DAG_ID:
				consumeOtherInfoFromDagId(entityPopulatedThusFar, aDagEntityJson);
				TestSession.logger.info("Ok in Tez_Gag_Id");
				break;
			case TEZ_CONTAINER_ID:
				consumeOtherInfoFromContainerId(entityPopulatedThusFar, aDagEntityJson);
				break;
			case TEZ_APPLICATION_ATTEMPT:
				consumeOtherInfoFromApplicationAttempt(entityPopulatedThusFar, aDagEntityJson);
				break;
			case TEZ_TASK_ATTEMPT_ID:
				TestSession.logger.info("Not implemented yet.... !");
				break;
			case TEZ_TASK_ID:
				TestSession.logger.info("Not implemented yet.... !");
				break;
			case TEZ_VERTEX_ID:
				consumeOtherInfoFromTezVertexId(entityPopulatedThusFar, aDagEntityJson);
				break;
		
		}
		return entityPopulatedThusFar;
	}

	EntityInGenericATSResponseBO consumeCommonPortionsInResponse(JSONObject aDagEntityJson, EntityTypes entityType) {
		EntityInGenericATSResponseBO anEntityInGenericATSResponseBO = null;
		switch (entityType){
			case TEZ_DAG_ID:
				anEntityInGenericATSResponseBO = new EntityInGenericATSResponseBO(EntityTypes.TEZ_DAG_ID);
				TestSession.logger.info("Ok in Tez_Gag_Id");
				break;
			case TEZ_CONTAINER_ID:
				anEntityInGenericATSResponseBO = new EntityInGenericATSResponseBO(EntityTypes.TEZ_CONTAINER_ID);
				break;
			case TEZ_APPLICATION_ATTEMPT:
				anEntityInGenericATSResponseBO = new EntityInGenericATSResponseBO(EntityTypes.TEZ_APPLICATION_ATTEMPT);
				break;
			case TEZ_TASK_ATTEMPT_ID:
				anEntityInGenericATSResponseBO = new EntityInGenericATSResponseBO(EntityTypes.TEZ_TASK_ATTEMPT_ID);
				break;
			case TEZ_TASK_ID:
				anEntityInGenericATSResponseBO = new EntityInGenericATSResponseBO(EntityTypes.TEZ_TASK_ID);
				break;
			case TEZ_VERTEX_ID:
				anEntityInGenericATSResponseBO = new EntityInGenericATSResponseBO(EntityTypes.TEZ_VERTEX_ID);
				break;			
		}		

		JSONArray eventsJsonArray = (JSONArray) (aDagEntityJson.get("events"));
		for (int eventIdx = 0; eventIdx < eventsJsonArray.size(); eventIdx++) {
			ATSEventsEntityBO anATSEventsEntityBO = new ATSEventsEntityBO();
			JSONObject anEventJsonObject = (JSONObject) eventsJsonArray.get(eventIdx);
			anATSEventsEntityBO.timestamp = (Long) anEventJsonObject.get("timestamp");
			anATSEventsEntityBO.eventtype = (String) anEventJsonObject.get("eventtype");
			anATSEventsEntityBO.eventinfo = (JSONObject) anEventJsonObject.get("eventinfo");
			anEntityInGenericATSResponseBO.events.add(anATSEventsEntityBO);
		}
		// ENTITY TYPE
		anEntityInGenericATSResponseBO.entityType = (String) aDagEntityJson.get("entitytype");
		// ENTITY
		anEntityInGenericATSResponseBO.entity = (String) aDagEntityJson.get("entity");
		// START TIME
		anEntityInGenericATSResponseBO.starttime = (Long) aDagEntityJson.get("starttime");

		// RELATED ENTITIES
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
		// PRIMARY FILTER
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
	 * Several responses have counters JSON present in them. This generalized routine
	 * should care for all the cases.
	 * @return
	 */
	List<CounterGroup> retrieveCounters(JSONObject otherInfoJson){
		List<CounterGroup> counterGroups = new ArrayList<CounterGroup>();
		JSONObject countersJsonObject =  (JSONObject) otherInfoJson.get("counters");
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
				Counterr counterBO = new Counterr();
				counterBO.counterName = (String) aCounterJson.get("counterName");
				counterBO.counterDisplayName = (String) aCounterJson.get("counterDisplayName");
				counterBO.counterValue = (Long) aCounterJson.get("counterValue");
				
				aCounterGroupBO.addCounter(counterBO);
			}
			counterGroups.add(aCounterGroupBO);
		}
		
		return counterGroups;
	}

}
