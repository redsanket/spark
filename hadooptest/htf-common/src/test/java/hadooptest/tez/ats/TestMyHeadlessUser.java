package hadooptest.tez.ats;

import static com.jayway.restassured.RestAssured.given;
import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.cluster.hadoop.HadoopComponent;
import hadooptest.node.hadoop.HadoopNode;

import java.io.IOException;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.service.LifecycleEvent;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.client.api.impl.TimelineClientImpl;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGClientImpl;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.jayway.restassured.response.Header;
import com.jayway.restassured.response.Response;
import com.sun.jersey.api.client.ClientResponse;
@Category(SerialTests.class)
public class TestMyHeadlessUser extends ATSTestsBaseClass {
	
//	@Test
	public void test4() throws IOException, InterruptedException{
		TezClient tezClient =  TezClient.create("maaTezClient", new TezConfiguration());
		
		Cluster aCluster = new Cluster(TestSession.cluster.getConf());
		for (JobStatus aJobStatus:aCluster.getAllJobStatuses()){
			TestSession.logger.info("Amit: " + aJobStatus.getJobName());
		}
	}
	@Test
	public void test1() throws Exception {
		String rmHostname = null;
		HadoopComponent hadoopComp = TestSession.cluster.getComponents().get(
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER);

		Hashtable<String, HadoopNode> nodesHash = hadoopComp.getNodes();
		for (String key : nodesHash.keySet()) {
			TestSession.logger.info("Key:" + key);
			TestSession.logger.info("The associated hostname is:"
					+ nodesHash.get(key).getHostname());
			rmHostname = nodesHash.get(key).getHostname();
		}

		if (!timelineserverStarted){
//			startTimelineServerOnRM(rmHostname);
		}		

		String url = "http://" + rmHostname + ":" + HTTP_ATS_PORT + "/ws/v1/timeline/TEZ_DAG_ID/";
		Response response = given()
				.cookie(hitusr_1_cookie).get(url);
		String responseAsString = response.getBody().asString();
		TestSession.logger.info("R E S P O N S E  B O D Y :" + responseAsString);
		TestSession.logger.info("R E S P O N S E  STATUSLINE :"
				+ response.getStatusLine());
		TestSession.logger.info("R E S P O N S E  STATUSCODE :"
				+ response.getStatusCode());
		TestSession.logger.info("R E S P O N S E  CONTENTTYPE :"
				+ response.getContentType());

		for (Header header : response.getHeaders()) {
			TestSession.logger.info("R E S P O N S E  HEADER :" + header);
		}
		Map<String, String> cookies = response
				.getCookies();
		for (String key : cookies.keySet()) {
			TestSession.logger.info("C O O K I E: [key]" + key + " [value] "
					+ cookies.get(key));
		}
		ATSUtils atsUtils = new ATSUtils();
		atsUtils.processDagIdResponse(responseAsString);
	}
	
//	@Test
	public void testTimelineClient(){
		TimelineClientImpl tlc = createTimelineClient();
		TestSession.logger.info("TLC start time " + tlc.getStartTime());
		Map<String, String> blockerMap = tlc.getBlockers();
		for (String key:blockerMap.keySet()){
			TestSession.logger.info("Blocker key:" + key + " Value:" + blockerMap.get(key));
		}
		TestSession.logger.info("Failure state:" + tlc.getFailureState());

		List<LifecycleEvent> lifecycleHistory = tlc.getLifecycleHistory();
		for (LifecycleEvent aLifecycleEvent:lifecycleHistory){
			TestSession.logger.info("LifecycleEvent STATE:" + aLifecycleEvent.state);
			TestSession.logger.info("LifecycleEvent TIME:" + aLifecycleEvent.time);
		}
		TestSession.logger.info("NAME: " + tlc.getName());
		TimelineEntities entities = new TimelineEntities();
		TimelineEntity entity = new TimelineEntity();
		TimelineEvent tlEvent = new TimelineEvent();
		tlEvent.setEventType("AMIT EVENT");
		tlEvent.setTimestamp(1412607913351L);
		entity.addEvent(tlEvent);
		entities.addEntity(entity);
		
		
	}
	  private static TimelineClientImpl createTimelineClient() {
		    TimelineClientImpl client =
		        (TimelineClientImpl) TimelineClient.createTimelineClient();
		    client.init(TestSession.cluster.getConf());
		    client.start();
		    return client;
		  }

	
}
