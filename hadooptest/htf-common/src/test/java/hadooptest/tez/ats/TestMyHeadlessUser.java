package hadooptest.tez.ats;

import static com.jayway.restassured.RestAssured.given;
import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.cluster.hadoop.HadoopComponent;
import hadooptest.node.hadoop.HadoopNode;

import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.service.LifecycleEvent;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.client.api.impl.TimelineClientImpl;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.jayway.restassured.response.Header;
import com.jayway.restassured.response.Response;
import com.sun.jersey.api.client.ClientResponse;
@Category(SerialTests.class)
public class TestMyHeadlessUser extends ATSTestsBaseClass {
	
	
//	@Test
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
			startTimelineServerOnRM(rmHostname);
		}		

		String url = "http://" + rmHostname + ":" + HTTP_ATS_PORT + "/ws/v1/timeline/";
		Response response = given()
				.param("http.protocol.allow-circular-redirects", true)
				.cookie(hitusr_1_cookie).get(url);
		TestSession.logger.info("R E S P O N S E:" + response.asString());
		TestSession.logger.info("R E S P O N S E  B O D Y :" + response.body());
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
//		stopTimelineServerOnRM(rmHostname);

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
		ClientResponse clientResponse = tlc.doPostingEntities(entities);
		TestSession.logger.info("Got maa status:" + clientResponse.getStatus());
		TestSession.logger.info("Location " + clientResponse.getLocation());
		
		
	}
	  private static TimelineClientImpl createTimelineClient() {
		    TimelineClientImpl client =
		        (TimelineClientImpl) TimelineClient.createTimelineClient();
		    client.init(TestSession.cluster.getConf());
		    client.start();
		    return client;
		  }

	
}
