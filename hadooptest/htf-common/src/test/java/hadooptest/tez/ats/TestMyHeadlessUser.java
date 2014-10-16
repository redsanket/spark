package hadooptest.tez.ats;

import static com.jayway.restassured.RestAssured.given;
import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.cluster.hadoop.HadoopComponent;
import hadooptest.node.hadoop.HadoopNode;
import hadooptest.tez.utils.HtfATSUtils;

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
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.jayway.restassured.response.Header;
import com.jayway.restassured.response.Response;

@Category(SerialTests.class)
public class TestMyHeadlessUser extends ATSTestsBaseClass {
	
	@Test
	public void testDagIdResponse() throws Exception {

		if (!timelineserverStarted){
//			startTimelineServerOnRM(rmHostname);
		}		

		String url = getATSUrl() + "/TEZ_DAG_ID/";
		Response response = given()
				.cookie(userCookies.get(HadooptestConstants.UserNames.HITUSR_1)).get(url);
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
		HtfATSUtils atsUtils = new HtfATSUtils();
		GenericATSResponseBO consumedResponse = atsUtils.processATSResponse(responseAsString, EntityTypes.TEZ_DAG_ID,expectEverythingMap());
		consumedResponse.dump();

	}

	@Test
	public void testContainerIdResponse() throws Exception {

		if (!timelineserverStarted){
//			startTimelineServerOnRM(rmHostname);
		}		


		HtfATSUtils atsUtils = new HtfATSUtils();
		TestSession.logger.info("############################################ CONTAINER ID NOW ########################################################################");
		//Container ID
		String url = getATSUrl() +"/TEZ_CONTAINER_ID/";
		Response response = given().cookie(userCookies.get(HadooptestConstants.UserNames.HITUSR_1)).get(url);
		String responseAsString = response.getBody().asString();
		GenericATSResponseBO consumedResponse = atsUtils.processATSResponse(responseAsString, EntityTypes.TEZ_CONTAINER_ID, expectEverythingMap());
		consumedResponse.dump();

	}

	@Ignore("Unless http://bug.corp.yahoo.com/show_bug.cgi?id=7166198 is addressed")
	@Test
	public void testDagIdWithFilterResponse() throws Exception {

		if (!timelineserverStarted){
//			startTimelineServerOnRM(rmHostname);
		}		

		HtfATSUtils atsUtils = new HtfATSUtils();
		TestSession.logger.info("############################################ WITH FILTER NOW ########################################################################");
		//Container ID
		String url = getATSUrl() +"TEZ_DAG_ID?primaryFilter=dagName:MRRSleepJob";
		Response response = given().cookie(userCookies.get(HadooptestConstants.UserNames.HITUSR_1)).get(url);
		String responseAsString = response.getBody().asString();
		GenericATSResponseBO consumedResponse = atsUtils.processATSResponse(responseAsString, EntityTypes.TEZ_DAG_ID, expectEverythingMap());
		consumedResponse.dump();

	}
	
	@Test
	public void testApplicationAttemptResponse() throws Exception {
		if (!timelineserverStarted){
//			startTimelineServerOnRM(rmHostname);
		}		

		HtfATSUtils atsUtils = new HtfATSUtils();
		TestSession.logger.info("############################################ WITH APPLICATION ATTEMPT NOW ########################################################################");
		//Container ID
		String url = getATSUrl() +"TEZ_APPLICATION_ATTEMPT";
		Response response = given()
				.cookie(userCookies.get(HadooptestConstants.UserNames.HITUSR_1)).get(url);
		String responseAsString = response.getBody().asString();
		GenericATSResponseBO consumedResponse = atsUtils.processATSResponse(responseAsString, EntityTypes.TEZ_APPLICATION_ATTEMPT, expectEverythingMap());
		consumedResponse.dump();

	}
	
	@Test
	public void testVertexIdResponse() throws Exception {

		if (!timelineserverStarted){
//			startTimelineServerOnRM(rmHostname);
		}		

		HtfATSUtils atsUtils = new HtfATSUtils();

		TestSession.logger.info("############################################ WITH TEZ VERTEX ID NOW ########################################################################");
		//Container ID
		String url = getATSUrl() +"TEZ_VERTEX_ID";
		Response response = given()
				.cookie(userCookies.get(HadooptestConstants.UserNames.HITUSR_1)).get(url);
		String responseAsString = response.getBody().asString();
		GenericATSResponseBO  consumedResponse = atsUtils.processATSResponse(responseAsString, EntityTypes.TEZ_VERTEX_ID, expectEverythingMap());
		consumedResponse.dump();

	}
	
	@Test
	public void testTaskIdResponse() throws Exception {
		if (!timelineserverStarted){
//			startTimelineServerOnRM(rmHostname);
		}		

		HtfATSUtils atsUtils = new HtfATSUtils();

		TestSession.logger.info("############################################ WITH TEZ TASK ID NOW ########################################################################");
		//Container ID
		String url = getATSUrl() +"TEZ_TASK_ID";
		Response response = given()
				.cookie(userCookies.get(HadooptestConstants.UserNames.HITUSR_1)).get(url);
		String responseAsString = response.getBody().asString();
		GenericATSResponseBO consumedResponse = atsUtils.processATSResponse(responseAsString, EntityTypes.TEZ_TASK_ID, expectEverythingMap());
		consumedResponse.dump();

	}
	
	@Test
	public void testTaskAttemptIdResponse() throws Exception {

		if (!timelineserverStarted){
//			startTimelineServerOnRM(rmHostname);
		}		

		HtfATSUtils atsUtils = new HtfATSUtils();

		TestSession.logger.info("############################################ WITH TEZ TASK ATTEMPT ID NOW ########################################################################");
		//Container ID
		String url = getATSUrl() +"TEZ_TASK_ATTEMPT_ID";
		String url = "http://" + rmHostname + ":" + HadooptestConstants.Ports.HTTP_ATS_PORT + "/ws/v1/timeline/TEZ_TASK_ATTEMPT_ID";
		Response response = given().cookie(userCookies.get(HadooptestConstants.UserNames.HITUSR_1)).get(url);
		String responseAsString = response.getBody().asString();
		GenericATSResponseBO consumedResponse = atsUtils.processATSResponse(responseAsString, EntityTypes.TEZ_TASK_ATTEMPT_ID, expectEverythingMap());
		consumedResponse.dump();

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

//		@Test
		public void test4() throws IOException, InterruptedException{
			TezClient tezClient =  TezClient.create("maaTezClient", new TezConfiguration());
			
			Cluster aCluster = new Cluster(TestSession.cluster.getConf());
			for (JobStatus aJobStatus:aCluster.getAllJobStatuses()){
				TestSession.logger.info("Amit: " + aJobStatus.getJobName());
			}
		}

}
