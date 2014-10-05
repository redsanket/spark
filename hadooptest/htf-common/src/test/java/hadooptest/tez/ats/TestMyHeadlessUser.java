package hadooptest.tez.ats;

import static com.jayway.restassured.RestAssured.given;
import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.automation.utils.http.HTTPHandle;
import hadooptest.cluster.hadoop.HadoopComponent;
import hadooptest.node.hadoop.HadoopNode;

import java.util.Hashtable;
import java.util.Map;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.jayway.restassured.response.Header;
import com.jayway.restassured.response.Response;

@Category(SerialTests.class)
public class TestMyHeadlessUser extends ATSTestsBaseClass {
	
	
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
			startTimelineServerOnRM(rmHostname);
		}

		HTTPHandle httpHandle = new HTTPHandle();
		String hitusr_1_cookie = httpHandle.loginAndReturnCookie("hitusr_1");
		TestSession.logger
				.info("Got cookie for hitusr_1 as:" + hitusr_1_cookie);
		String hitusr_2_cookie = httpHandle.loginAndReturnCookie("hitusr_2");
		TestSession.logger
				.info("Got cookie for hitusr_2 as:" + hitusr_2_cookie);
		String hitusr_3_cookie = httpHandle.loginAndReturnCookie("hitusr_3");
		TestSession.logger
				.info("Got cookie for hitusr_3 as:" + hitusr_3_cookie);
		String hitusr_4_cookie = httpHandle.loginAndReturnCookie("hitusr_4");
		TestSession.logger
				.info("Got cookie for hitusr_4 as:" + hitusr_4_cookie);

		

		String url = "http://" + rmHostname + ":" + ATS_PORT + "/ws/v1/timeline/";
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
}
