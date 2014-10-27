package hadooptest.tez.ats;

import static com.jayway.restassured.RestAssured.given;
import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.tez.ats.ATSTestsBaseClass.ResponseComposition;
import hadooptest.tez.ats.OtherInfoTezDagIdBO.DagPlanBO.DagPlanVertexBO;
import hadooptest.tez.ats.SeedData.DAG;
import hadooptest.tez.ats.SeedData.DAG.Vertex;
import hadooptest.tez.examples.extensions.OrderedWordCountExtendedForHtf;
import hadooptest.tez.examples.extensions.SimpleSessionExampleExtendedForTezHTF;
import hadooptest.tez.utils.HtfATSUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.security.UserGroupInformation;
import org.json.simple.parser.ParseException;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.jayway.restassured.response.Response;
/**
 * This is how the users have been created on our grids
 * <pre>
 * 			hadoop hadoopqa	gdmdev	gdmqa
 * hitusr_1	 X
 * hitusr_2			X
 * hitusr_3					X
 * hitusr_4			X		X		X
 * </pre>
 * 
 * This is how/who launches what jobs
 * <pre>
 * 			orderedwordcount	mrrsleep	simplesession
 * hitusr_1		X
 * hitusr_2							X
 * hitusr_3										X
 * hitusr_4
 * </pre>
 */

@Category(SerialTests.class)
public class TestAuthorization extends ATSTestsBaseClass {
	@Test
	public void testHitUsr1Permissions() throws Exception {
		//hitusr_1
		String url = getATSUrl() + "TEZ_DAG_ID/" + orderedWordCountSeedData.dags.get(0);
		runAndAssertAuthorization(url, orderedWordCountSeedData.appStartedByUser, 200);
		url = getATSUrl() + "TEZ_DAG_ID/" + sleepJobSeedData.dags.get(0);
		runAndAssertAuthorization(url, sleepJobSeedData.appStartedByUser, 302);
		for (DAG dag :simpleSessionExampleSeedData.dags){
			url = getATSUrl() + "TEZ_DAG_ID/" + dag.name;
		}
		
	}

	public void runAndAssertAuthorization(String url, String user, int expectedReturnCode) {
		TestSession.logger
				.info("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
		TestSession.logger.info("User:" + user +" expectedReturnCode:" + expectedReturnCode + " Url:" + url);
		TestSession.logger
				.info("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
		TestSession.logger.info(userCookies);
		TestSession.logger.info("USer:" + user + " Cookie:" + userCookies.get(user));
		Response response = given().cookie(userCookies.get(user)).get(url);
		Assert.assertEquals(expectedReturnCode, response.getStatusCode());


	}

}
