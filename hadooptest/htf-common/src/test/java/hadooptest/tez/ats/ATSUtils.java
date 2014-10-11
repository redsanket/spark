package hadooptest.tez.ats;

import hadooptest.TestSession;
import hadooptest.tez.ats.ATSTestsBaseClass.EntityTypes;

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
				consumeOtherInfoFromDagIdResponse(entityPopulatedThusFar, aDagEntityJson);
				TestSession.logger.info("Ok in Tez_Gag_Id");
				break;
			case TEZ_CONTAINER_ID:
				consumeOtherInfoFromContainerIdResponse(entityPopulatedThusFar, aDagEntityJson);
				break;
			case TEZ_APPLICATION_ATTEMPT:
				consumeOtherInfoFromApplicationAttemptResponse(entityPopulatedThusFar, aDagEntityJson);
				break;
			case TEZ_TASK_ATTEMPT_ID:
				TestSession.logger.info("Not implemented yet.... !");
				break;
			case TEZ_TASK_ID:
				TestSession.logger.info("Not implemented yet.... !");
				break;
			case TEZ_VERTEX_ID:
				TestSession.logger.info("Not implemented yet.... !");
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
			TestSession.logger.info(relatedEntityKey);
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
	 * function consumes otherInfo for TEZ_DAG_ID
	 * @param entityPopulatedThusFar
	 * @param aDagEntityJson
	 * @return
	 */
	EntityInGenericATSResponseBO consumeOtherInfoFromDagIdResponse(
			EntityInGenericATSResponseBO entityPopulatedThusFar,
			JSONObject aDagEntityJson) {

		// OTHER INFO
		TezDagIdOtherInfoBO dagOtherInfoBO = new TezDagIdOtherInfoBO();
		JSONObject otherInfoJson = (JSONObject) (aDagEntityJson.get("otherinfo"));
		dagOtherInfoBO.startTime = (Long) otherInfoJson.get("startTime");
		dagOtherInfoBO.status = (String) otherInfoJson.get("status");
		dagOtherInfoBO.initTime = (Long) otherInfoJson.get("initTime");
		dagOtherInfoBO.timeTaken = (Long) otherInfoJson.get("timeTaken");
		dagOtherInfoBO.applicationId = (String) otherInfoJson.get("applicationId");

		// DagPlan is a part of it
		TezDagIdOtherInfoBO.DagPlanBO dagPlanBO = new TezDagIdOtherInfoBO.DagPlanBO();
		JSONObject dagPlanJson = (JSONObject) otherInfoJson.get("dagPlan");
		dagPlanBO.dagName = (String) dagPlanJson.get("dagName");
		dagPlanBO.version = (Long) dagPlanJson.get("version");
		JSONArray dagPlanVerticesJsonArray = (JSONArray) dagPlanJson.get("vertices");
		// dagPlan vertices
		for (int vv = 0; vv < dagPlanVerticesJsonArray.size(); vv++) {
			JSONObject vertexJson = (JSONObject) dagPlanVerticesJsonArray.get(vv);
			TezDagIdOtherInfoBO.DagPlanBO.DagPlanVertexBO aVertexEntityInDagPlanBO = new TezDagIdOtherInfoBO.DagPlanBO.DagPlanVertexBO();
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
					TezDagIdOtherInfoBO.DagPlanBO.DagPlanVertexBO.DagVertexAdditionalInputBO 
						dagVertexAdditionalInput = new TezDagIdOtherInfoBO.DagPlanBO.DagPlanVertexBO.DagVertexAdditionalInputBO();
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
			TezDagIdOtherInfoBO.DagPlanBO.DagPlanEdgeBO aDagEdgeBO = new TezDagIdOtherInfoBO.DagPlanBO.DagPlanEdgeBO();
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

		// Add otherInfo to DagEntityBO
		entityPopulatedThusFar.otherinfo = dagOtherInfoBO;

		return entityPopulatedThusFar;
	}

	/**
	 * The HTTP response across enities has certain common elements. Those are handled by the
	 * {@code} consumeCommonPortionsInResponse. But the otherInfo varies. Hence for each of the
	 * entities, we will just write specialized functions to consume the said otherInfo. This
	 * function consumes otherInfo for TEZ_CONTAINER_ID
	 * @param entityPopulatedThusFar
	 * @param aDagEntityJson
	 * @return
	 */

	EntityInGenericATSResponseBO consumeOtherInfoFromContainerIdResponse(
			EntityInGenericATSResponseBO entityPopulatedThusFar,
			JSONObject aDagEntityJson) {

		// OTHER INFO
		TezContainerIdOtherInfoBO containerIdgOtherInfoBO = new TezContainerIdOtherInfoBO();
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
	 * function consumes otherInfo for TEZ_CONTAINER_ID
	 * @param entityPopulatedThusFar
	 * @param aDagEntityJson
	 * @return
	 */

	EntityInGenericATSResponseBO consumeOtherInfoFromApplicationAttemptResponse(
			EntityInGenericATSResponseBO entityPopulatedThusFar,
			JSONObject aDagEntityJson) {

		// OTHER INFO
		TezApplicationAttemptOtherInfoBO applicationAttemptOtherInfoBO = new TezApplicationAttemptOtherInfoBO();
		JSONObject otherInfoJson = (JSONObject) (aDagEntityJson.get("otherinfo"));
		applicationAttemptOtherInfoBO.appSubmitTime = (Long) otherInfoJson.get("appSubmitTime");

		// Add otherInfo to DagEntityBO
		entityPopulatedThusFar.otherinfo = applicationAttemptOtherInfoBO;

		return entityPopulatedThusFar;
	}

}
