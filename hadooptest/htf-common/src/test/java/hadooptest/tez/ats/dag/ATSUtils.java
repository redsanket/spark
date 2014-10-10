package hadooptest.tez.ats.dag;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class ATSUtils {

	public DagResponseBO processDagResponse(String responseAsString)
			throws ParseException {
		DagResponseBO dagResponseBO = new DagResponseBO();

		JSONParser parser = new JSONParser();
		Object obj = parser.parse(responseAsString);
		JSONObject jsonObject = (JSONObject) obj;

		// ENTITIES
		JSONArray entities = (JSONArray) jsonObject.get("entities");
		for (int entityIdx = 0; entityIdx < entities.size(); entityIdx++) {
			DagEntity aDagEntityBO = new DagEntity();
			JSONObject aDagEntityJson = (JSONObject) entities.get(entityIdx);

			JSONArray eventsJsonArray = (JSONArray) (aDagEntityJson
					.get("events"));
			for (int eventIdx = 0; eventIdx < eventsJsonArray.size(); eventIdx++) {
				DagEventsEntity aDagEventsEntityBO = new DagEventsEntity();
				JSONObject anEventJsonObject = (JSONObject) eventsJsonArray
						.get(eventIdx);
				aDagEventsEntityBO.timestamp = (Long) anEventJsonObject
						.get("timestamp");
				// TestSession.logger.info(anEvent.get("timestamp"));
				aDagEventsEntityBO.eventtype = (String) anEventJsonObject
						.get("eventtype");
				// TestSession.logger.info(anEvent.get("eventtype"));
				aDagEventsEntityBO.eventinfo = (JSONObject) anEventJsonObject
						.get("eventinfo");
				// TestSession.logger.info(anEvent.get("eventinfo"));
				aDagEntityBO.events.add(aDagEventsEntityBO);
			}
			aDagEntityBO.entityType = (String) aDagEntityJson.get("entitytype");
			aDagEntityBO.entity = (String) aDagEntityJson.get("entity");
			aDagEntityBO.starttime = (Long) aDagEntityJson.get("starttime");

			// RELATED ENTITIES
			String TEZ_VERTEX_ID = "TEZ_VERTEX_ID";
			JSONObject relatedEntitiesJson = (JSONObject) (aDagEntityJson
					.get("relatedentities"));
			JSONArray tezVertexIdsJsonArray = (JSONArray) relatedEntitiesJson
					.get(TEZ_VERTEX_ID);
			DagRelatedEntity aDagRelatedEntityBO = new DagRelatedEntity(
					TEZ_VERTEX_ID);
			for (int vrtxIdIdx = 0; vrtxIdIdx < tezVertexIdsJsonArray.size(); vrtxIdIdx++) {
				String tezVertexId = (String) tezVertexIdsJsonArray
						.get(vrtxIdIdx);
				aDagRelatedEntityBO.tezVertexIds.add(tezVertexId);
			}
			aDagEntityBO.relatedentities.add(aDagRelatedEntityBO);

			// PRIMARY FILTERS
			JSONObject primaryFiltersJson = (JSONObject) (aDagEntityJson
					.get("primaryfilters"));
			DagPrimaryFiltersBO dagPrimaryFilters = new DagPrimaryFiltersBO();
			// DagName
			DagPrimaryFilterEntity dagPrimaryFilterEntityBO = new DagPrimaryFilterEntity(
					"dagName");
			JSONArray dagNameArray = (JSONArray) primaryFiltersJson
					.get("dagName");
			for (int xx = 0; xx < dagNameArray.size(); xx++) {
				String aFilter = (String) dagNameArray.get(xx);
				dagPrimaryFilterEntityBO.filterList.add(aFilter);
			}
			dagPrimaryFilters.primaryFilters.add(dagPrimaryFilterEntityBO);

			// User
			dagPrimaryFilterEntityBO = new DagPrimaryFilterEntity("user");
			dagNameArray = (JSONArray) primaryFiltersJson.get("user");
			for (int xx = 0; xx < dagNameArray.size(); xx++) {
				String aFilter = (String) dagNameArray.get(xx);
				dagPrimaryFilterEntityBO.filterList.add(aFilter);
			}
			dagPrimaryFilters.primaryFilters.add(dagPrimaryFilterEntityBO);

			aDagEntityBO.primaryfilters.add(dagPrimaryFilters);

			// OTHER INFO
			DagOtherInfoBO dagOtherInfoBO = new DagOtherInfoBO();
			JSONObject otherInfoJson = (JSONObject) (aDagEntityJson
					.get("otherinfo"));
			dagOtherInfoBO.applicationId = (String) otherInfoJson
					.get("applicationId");
			dagOtherInfoBO.startTime = (Long) otherInfoJson.get("startTime");
			dagOtherInfoBO.initTime = (Long) otherInfoJson.get("initTime");
			dagOtherInfoBO.timeTaken = (Long) otherInfoJson.get("timeTaken");
			dagOtherInfoBO.status = (String) otherInfoJson.get("status");
			// DagPlan is a part of it
			DagPlan dagPlanBO = new DagPlan();
			JSONObject dagPlanJson = (JSONObject) otherInfoJson.get("dagPlan");
			dagPlanBO.dagName = (String) dagPlanJson.get("dagName");
			dagPlanBO.version = (Long) dagPlanJson.get("version");
			JSONArray dagPlanVerticesJsonArray = (JSONArray) dagPlanJson.get("vertices");
			// dagPlan vertices
			for (int vv = 0; vv < dagPlanVerticesJsonArray.size(); vv++) {
				JSONObject vertexJson = (JSONObject) dagPlanVerticesJsonArray
						.get(vv);
				DagOtherInfoDagPlanVertexEntity dagPlanVertexEntityBO = new DagOtherInfoDagPlanVertexEntity();
				dagPlanVertexEntityBO.vertexName = (String) vertexJson
						.get("vertexName");
				dagPlanVertexEntityBO.processorClass = (String) vertexJson
						.get("processorClass");
				// outedges
				JSONArray outEdgeIdsArrayJson = (JSONArray) vertexJson
						.get("outEdgeIds");
				if (outEdgeIdsArrayJson != null) {
					for (int oo = 0; oo < outEdgeIdsArrayJson.size(); oo++) {
						dagPlanVertexEntityBO.outEdgeIds
								.add((String) outEdgeIdsArrayJson.get(oo));
					}
				}
				// inedges
				JSONArray inEdgeIdsArrayJson = (JSONArray) vertexJson
						.get("inEdgeIds");
				if (inEdgeIdsArrayJson != null) {
					for (int ii = 0; ii < inEdgeIdsArrayJson.size(); ii++) {
						dagPlanVertexEntityBO.inEdgeIds
								.add((String) inEdgeIdsArrayJson.get(ii));
					}
				}

				//additionalInputs
				JSONArray additionalInputsJsonArray = (JSONArray) vertexJson.get("additionalInputs");
				if (additionalInputsJsonArray!=null){
					for(int aa=0;aa<additionalInputsJsonArray.size();aa++){
						DagVertexAdditionalInput dagVertexAdditionalInput = new DagVertexAdditionalInput();
						JSONObject additionalInputsJson = (JSONObject) additionalInputsJsonArray.get(aa);
						dagVertexAdditionalInput.name = (String) additionalInputsJson.get("name");
						dagVertexAdditionalInput.clazz = (String) additionalInputsJson.get("class");
						dagVertexAdditionalInput.initializer = (String) additionalInputsJson.get("initializer");
						
						dagPlanVertexEntityBO.additionalInputs.add(dagVertexAdditionalInput);
						
					}
				}
				
				dagPlanBO.vertices.add(dagPlanVertexEntityBO);
				
				//Add it to the OtherInfo
				dagOtherInfoBO.dagPlan = dagPlanBO;
				
				aDagEntityBO.otherinfo = dagOtherInfoBO;
			}

			// FINALLY ADD THE ENTITY
			dagResponseBO.entities.add(aDagEntityBO);
		}

		dagResponseBO.dump();
		return dagResponseBO;
	}
}
