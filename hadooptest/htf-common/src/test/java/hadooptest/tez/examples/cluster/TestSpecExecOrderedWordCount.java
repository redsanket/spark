package hadooptest.tez.examples.cluster;

import static com.jayway.restassured.RestAssured.given;
import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.automation.utils.http.HTTPHandle;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Force;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Recursive;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.SkipTrash;
import hadooptest.hadoop.regression.yarn.YarnCliCommands;
import hadooptest.hadoop.regression.yarn.YarnTestsBaseClass.YarnApplicationSubCommand;
import hadooptest.node.hadoop.HadoopNode;
import hadooptest.tez.ats.ATSTestsBaseClass;
import hadooptest.tez.examples.extensions.WordCountSpeculativeExecutor;
import hadooptest.tez.utils.HtfTezUtils.Session;
import hadooptest.tez.utils.HtfTezUtils.TimelineServer;

import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import com.jayway.restassured.response.Response;

/**
 * This class has the real test methods meant to be run on the cluster. Their
 * counterparts live under {@code}hadooptest.tez.mapreduce.examples.localmode
 * package. All test cases extend an intermediate class, ending in
 * *ExtendedForTezHTF which in turn extends the actual classes that are shipped
 * as a part of the Tez distro JAR. These test cases flesh out and implement
 * sub-tests that are provisioned in the original test class.
 * 
 */

@Category(SerialTests.class)
public class TestSpecExecOrderedWordCount extends WordCountSpeculativeExecutor {
	private static String source = "/grid/0/HTF/testdata/speculativeExec";
	private static String destination = "/tmp/";
	public static final HashMap<String, String> EMPTY_ENV_HASH_MAP = new HashMap<String, String>();
	@BeforeClass
	public static void beforeClass() {
		TestSession.start();
	}
	
	@Before
	public void copyData() throws Exception {
		DfsCliCommands dfsCommonCli = new DfsCliCommands();
		TestSession.logger.info("copying " + source +" to HDFS!");
		String aCluster = System.getProperty("CLUSTER_NAME");
		GenericCliResponseBO copyFileBO;
		copyFileBO = dfsCommonCli.copyFromLocal(EMPTY_ENV_HASH_MAP, HadooptestConstants.UserNames.HDFSQA, HadooptestConstants.Schema.NONE, aCluster,
				source, destination);
		TestSession.logger.info(copyFileBO.response);
		GenericCliResponseBO chmodBO;
		chmodBO = dfsCommonCli.chmod(EMPTY_ENV_HASH_MAP, HadooptestConstants.UserNames.HDFSQA, HadooptestConstants.Schema.NONE,
				aCluster, destination, "777", Recursive.YES);
		TestSession.logger.info(chmodBO.response);
		
	}

	@Rule
	public TestName testName = new TestName();

	@Test
	public void testOrderedWordCountWithPartitions() throws Exception {
		String taskIdToSpeculate = "000005";
		String vertexId = "00";
		String taskAttemptId = "0";
		HadoopNode hadoopNode = TestSession.cluster
				.getNode(HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
		String rmHost = hadoopNode.getHostname();

		HTTPHandle httpHandle = new HTTPHandle();
		String hitusr_2_cookie = httpHandle
				.loginAndReturnCookie(HadooptestConstants.UserNames.HITUSR_2);
		TestSession.logger.info("Got cookie hitusr_2:" + hitusr_2_cookie);
		ATSTestsBaseClass.userCookies.put(
				HadooptestConstants.UserNames.HITUSR_2, hitusr_2_cookie);

		ExecutorService executor = Executors.newFixedThreadPool(1);

		TezSpecExCallable callableTask = new TezSpecExCallable(this);
		executor.submit(callableTask);
		executor.shutdown();
		int TIMEOUT = 60;
		TestSession.logger
				.info("Letting the job run for :" + TIMEOUT + " secs");
		executor.awaitTermination(TIMEOUT, TimeUnit.SECONDS);
		TestSession.logger.info("Here...., the appId is "
				+ WordCountSpeculativeExecutor.appId);

		Assert.assertTrue(
				"Did not get expected resoponse, hence failing test.",
				waitTill("http://"
						+ rmHost
						+ ":4080/ws/v1/timeline/TEZ_TASK_ATTEMPT_ID/"
						+ WordCountSpeculativeExecutor.appId.toString()
								.replace("application", "attempt") + "_1_"
						+ vertexId + "_" + taskIdToSpeculate + "_"
						+ taskAttemptId, "KILLED"));

	}

	class ResponseTuple {
		int code;
		String rawResponse;
	}

	boolean waitTill(String url, String expectedStatus)
			throws InterruptedException, ParseException {
		int timeout = 500;
		boolean conditionMet = false;
		String readStatus = "";
		do {
			ResponseTuple responseTuple = makeHttpRequestAndGetRawResponse(url,
					HadooptestConstants.UserNames.HITUSR_2);
			if (responseTuple.code != 200) {
				timeout--;
				Thread.sleep(5000);
				continue;
			}
			JSONParser parser = new JSONParser();
			Object obj = parser.parse(responseTuple.rawResponse);
			JSONObject entireResponseAsJson = (JSONObject) obj;
			JSONObject otherInfoJson = (JSONObject) (entireResponseAsJson
					.get("otherinfo"));
			readStatus = (String) otherInfoJson.get("status");
			if (!readStatus.equals(expectedStatus)) {
				TestSession.logger.info("ReadStatus:" + readStatus + " != "
						+ expectedStatus
						+ " hence sleeping for 1 sec, tout in :" + timeout
						+ " secs.");
				Thread.sleep(1000);
				conditionMet = false;
			} else {
				String diagnostics = (String) otherInfoJson.get("diagnostics");
				Assert.assertTrue(diagnostics
						.contains("Killed this attempt as other speculative attempt"));
				conditionMet = true;
				break;
			}

		} while (timeout-- > 0);
		return conditionMet;
	}

	public ResponseTuple makeHttpRequestAndGetRawResponse(String url,
			String user) throws InterruptedException {
		ResponseTuple responseTuple = new ResponseTuple();
		TestSession.logger
				.info("IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII");
		TestSession.logger.info("Url:" + url);
		TestSession.logger
				.info("IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII");
		TestSession.logger.info("USer:" + user + " Cookie:"
				+ ATSTestsBaseClass.userCookies.get(user));
		Response response = given().cookie(
				ATSTestsBaseClass.userCookies.get(user)).get(url);

		responseTuple.rawResponse = response.getBody().asString();
		responseTuple.code = response.getStatusCode();
		TestSession.logger.info(" R E S P O N S E  C O D E: "
				+ responseTuple.code);
		TestSession.logger.info("R E S P O N S E  B O D Y: "
				+ responseTuple.rawResponse);

		return responseTuple;
	}

	class TezSpecExCallable implements Callable<Boolean> {
		TestSpecExecOrderedWordCount testClass;

		TezSpecExCallable(TestSpecExecOrderedWordCount testClass) {
			this.testClass = testClass;
		}

		public Boolean call() {
			try {
				return testClass.run("/tmp/teragen", "/tmp/specEx",
						TestSession.cluster.getConf(), 2,
						HadooptestConstants.Execution.TEZ_CLUSTER, Session.NO,
						TimelineServer.ENABLED, testName.getMethodName());
			} catch (Exception e) {
				e.printStackTrace();
				return false;
			}
		}

	}

	@After
	public void cleanup() throws Exception {
		HadoopNode hadoopNode = TestSession.cluster
				.getNode(HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
		String aCluster = System.getProperty("CLUSTER_NAME");
		YarnCliCommands yarnCliCommands = new YarnCliCommands();
		yarnCliCommands.application(DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA,
				HadooptestConstants.Schema.NONE, aCluster,
				YarnApplicationSubCommand.KILL,
				WordCountSpeculativeExecutor.appId.toString());
		TestSession.logger.info("Killed application:"
				+ WordCountSpeculativeExecutor.appId.toString());
	}

}
