package hadooptest.tez.ats;

import static com.jayway.restassured.RestAssured.given;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.node.hadoop.HadoopNode;
import hadooptest.tez.ats.ATSTestsBaseClass.EntityTypes;
import hadooptest.tez.utils.HtfATSUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.Test;

import com.jayway.restassured.response.Response;

public class TestTezPerfCountEntityIds extends ATSTestsBaseClass {
	static {
		ATSTestsBaseClass.jobsLaunchedOnceToSeedData = true;
	}
	static String outputFile = "/grid/0/tmp/entIdSet.txt";
	String lastEntId = null;
	@Test
	public void countEntityIds() throws InterruptedException {
		File file = new File(outputFile);
		if (file.exists())
			file.delete();
		HadoopNode hadoopNode = TestSession.cluster
				.getNode(HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
		String rmHost = hadoopNode.getHostname();
		Set<String> finalEntitiesSet = new LinkedHashSet<String>();
		int LIMIT = 100;
		boolean returnedCountLessThanLimit = false;
		Set<String> tempEntitiesSet = null;
		
		while (!returnedCountLessThanLimit) {
			String url;
			if (lastEntId == null) {
				url = "http://" + rmHost + ":4080/ws/v1/timeline/TEZ_DAG_ID"
						+ "?fields=relatedentities&limit=" + (LIMIT + 1);

			} else {
				TestSession.logger.info("Making next call with entId:"
						+ lastEntId);
				url = "http://" + rmHost + ":4080/ws/v1/timeline/TEZ_DAG_ID"
						+ "?fields=relatedentities&limit=" + (LIMIT + 1) + "&fromId=" + lastEntId;
			}

			tempEntitiesSet = run(url, HadooptestConstants.UserNames.HITUSR_1);
//			lastEntId = getLastEntId(tempEntitiesSet);
			if (tempEntitiesSet.size() < LIMIT) {
				returnedCountLessThanLimit = true;
				TestSession.logger.info("!OK! : returnedCountLessThanLimit :"
						+ tempEntitiesSet.size() + " while limit is: " + LIMIT);
			} else {
				TestSession.logger.info("OK : returnedCountLessThanLimit :"
						+ tempEntitiesSet.size() + " and limit is: " + LIMIT
						+ " ....keep going...!");
			}
			finalEntitiesSet.addAll(tempEntitiesSet);
			
//			writeSetToFile(tempEntitiesSet);
			tempEntitiesSet = null;
			System.gc();

//			Thread.sleep(5000);
		}
//		lastEntId = getLastEntId(finalEntitiesSet);
		TestSession.logger.info("Final entity in finalEntitiesSet is: "
				+ lastEntId + " and count is: " + finalEntitiesSet.size());
	}

	void writeSetToFile(Set<String> entitiesSet) {
		FileWriter file = null;
		BufferedWriter bw = null;
		try {
			file = new FileWriter("/grid/0/tmp/entIdSet.txt", true);
			bw = new BufferedWriter(file);
			for (String entId : entitiesSet) {
				bw.write(entId +"\n");
			}
			bw.close();
			file.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private String getLastEntId(Set<String> entitiesInSet) {
		String[] entriesInSet = entitiesInSet.toArray(new String[entitiesInSet
				.size()]);

		return entriesInSet[entriesInSet.length - 1];
	}

	Set<String> run(String url, String user) {
		TestSession.logger
				.info("RRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR");
		TestSession.logger.info("Url:" + url);
		TestSession.logger
				.info("RRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR");
		TestSession.logger.trace(userCookies);
		Response response = given().cookie(userCookies.get(user)).get(url);

		String responseAsString = response.getBody().asString();
		TestSession.logger.info(" R E S P O N S E  C O D E: "
				+ response.getStatusCode());
		TestSession.logger
				.info("R E S P O N S E  B O D Y :" + responseAsString);
		Set<String> tempEntitySet = new LinkedHashSet<String>();
		// http://axonitered-jt1.red.ygrid.yahoo.com:4080/ws/v1/timeline/TEZ_DAG_ID?limit=11&fromId=dag_1422053589558_29810_1
		try {
			JSONParser parser = new JSONParser();
			Object obj = parser.parse(responseAsString);
			JSONObject jsonObject = (JSONObject) obj;
			JSONArray entities = (JSONArray) jsonObject.get("entities");
			if (entities != null) {
				for (int entityIdx = 0; entityIdx < entities.size(); entityIdx++) {
					JSONObject aDagEntityJson = (JSONObject) entities
							.get(entityIdx);
					// ENTITY
					String entity = (String) aDagEntityJson.get("entity");
					tempEntitySet.add(entity);
					lastEntId = entity;

				}
				int xx = 1;
				for (String entityId : tempEntitySet) {
					TestSession.logger.info("EntityId [" + xx++ + "]: "
							+ entityId);
				}
				TestSession.logger
						.info("------------------------------------------------------------------------------------------------------------------");
				TestSession.logger.info("Last entity in this set: "
//						+ getLastEntId(tempEntitySet));
						+ lastEntId);
				TestSession.logger.info("Count of entities: "
						+ tempEntitySet.size());
				TestSession.logger
						.info("------------------------------------------------------------------------------------------------------------------");
			}

		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return tempEntitySet;
	}

}