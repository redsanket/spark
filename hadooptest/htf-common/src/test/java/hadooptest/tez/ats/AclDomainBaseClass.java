package hadooptest.tez.ats;

import static com.jayway.restassured.RestAssured.given;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.jayway.restassured.response.Response;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.automation.utils.http.HTTPHandle;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.node.hadoop.HadoopNode;
import hadooptest.tez.ats.ATSTestsBaseClass.EntityTypes;
import hadooptest.tez.ats.ATSTestsBaseClass.ResponseComposition;

public class AclDomainBaseClass extends ATSTestsBaseClass {

	public void backupConfigDirAndRestartTimelineServer() throws Exception {
		if (!jobsLaunchedOnceToSeedData) {

			// Backup config and replace file, on Resource Manager
			FullyDistributedCluster fullyDistributedCluster = (FullyDistributedCluster) TestSession
					.getCluster();

			fullyDistributedCluster.getConf(
					HadooptestConstants.NodeTypes.RESOURCE_MANAGER)
					.backupConfDir();
			String dirWhereRMConfHasBeenCopied = fullyDistributedCluster
					.getConf(HadooptestConstants.NodeTypes.RESOURCE_MANAGER)
					.getHadoopConfDir();
			TestSession.logger.info("Dir where conf has been copied:"
					+ dirWhereRMConfHasBeenCopied);

			HadoopNode hadoopNode = TestSession.cluster
					.getNode(HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
			String rmHost = hadoopNode.getHostname();

			// Edit the yarn-site.xml file inplace, in the newly backed up dir
			String command = "perl -pi -e 's/gridadmin,hadoop,hadoopqa/gridadmin/' "
					+ dirWhereRMConfHasBeenCopied + "/yarn-site.xml";
			doJavaSSHClientExec(HadooptestConstants.UserNames.HADOOPQA, rmHost,
					command, "/homes/hadoopqa/.ssh/id_rsa");
			
//			doJavaSSHClientExec(HadooptestConstants.UserNames.MAPREDQA, rmHost,
//					command, HadooptestConstants.Location.Identity.HADOOPQA_AS_MAPREDQA_IDENTITY_FILE);
			
			//Now bounce the timelineserver
			List<String> newConfigLocation = new ArrayList<String>();
			newConfigLocation.add(" --config " + dirWhereRMConfHasBeenCopied);
			restartATSWithTheseArgs(rmHost, newConfigLocation);

		}
	}

	/**
	 * We don't want to launch seed data for these tests, hence override this
	 * method from the base class and remove the line where the seed tests were
	 * launched
	 */
	@Override
	public void cleanupAndPrepareForTestRun() throws Exception {
			
		
		TestSession.logger.info("Running cleanupAndPrepareForTestRun");
		
		backupConfigDirAndRestartTimelineServer();
		
		// Fetch cookies
		HTTPHandle httpHandle = new HTTPHandle();
		String hitusr_1_cookie = null;
		String hitusr_2_cookie = null;
		String hitusr_3_cookie = null;
		String hitusr_4_cookie = null;

		hitusr_1_cookie = httpHandle
				.loginAndReturnCookie(HadooptestConstants.UserNames.HITUSR_1);
		TestSession.logger.info("Got cookie hitusr_1:" + hitusr_1_cookie);
		userCookies
				.put(HadooptestConstants.UserNames.HITUSR_1, hitusr_1_cookie);
		hitusr_2_cookie = httpHandle
				.loginAndReturnCookie(HadooptestConstants.UserNames.HITUSR_2);
		TestSession.logger.info("Got cookie hitusr_2:" + hitusr_2_cookie);
		userCookies
				.put(HadooptestConstants.UserNames.HITUSR_2, hitusr_2_cookie);
		hitusr_3_cookie = httpHandle
				.loginAndReturnCookie(HadooptestConstants.UserNames.HITUSR_3);
		TestSession.logger.info("Got cookie hitusr_3:" + hitusr_3_cookie);
		userCookies
				.put(HadooptestConstants.UserNames.HITUSR_3, hitusr_3_cookie);
		hitusr_4_cookie = httpHandle
				.loginAndReturnCookie(HadooptestConstants.UserNames.HITUSR_4);
		TestSession.logger.info("Got cookie hitusr_4:" + hitusr_4_cookie);
		userCookies
				.put(HadooptestConstants.UserNames.HITUSR_4, hitusr_4_cookie);

		// Reset the error count
		errorCount.set(0);

		drainQueues();

		HadoopNode hadoopNode = TestSession.cluster
				.getNode(HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
		String rmHost = hadoopNode.getHostname();
		TestSession.logger.info("RESOURCE MANAGER HOST:::::::::::::::::::::"
				+ rmHost);

		createUserGroupMapping();		

	}

	public void makeHttpRequestAndEnqueue(String url, EntityTypes entityType,
			String user,
			Queue<GenericATSResponseBO> enqueueProcessedResponseHere)
			throws InterruptedException {
		String filter = "?fields=events,relatedentities,primaryfilters";
		Map<String, Boolean> expectedEntities = getExpectedFieldsMap(
				ResponseComposition.EVENTS.EXPECTED,
				ResponseComposition.ENTITYTYPE.EXPECTED,
				ResponseComposition.ENTITY.EXPECTED,
				ResponseComposition.STARTTIME.EXPECTED,
				ResponseComposition.RELATEDENTITIES.EXPECTED,
				ResponseComposition.PRIMARYFILTERS.EXPECTED,
				ResponseComposition.OTHERINFO.NOT_EXPECTED);

		ExecutorService execService = Executors.newFixedThreadPool(1);
		makeHttpCallAndEnqueueConsumedResponse(execService, url, user,
				entityType, enqueueProcessedResponseHere, expectEverythingMap());
		execService.shutdown();
		while (!execService.isTerminated()) {
			Thread.sleep(1000);
		}

	}

	public int makeHttpRequestAndGetResponseCode(String url, String user)
			throws InterruptedException {

		TestSession.logger
				.info("IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII");
		TestSession.logger.info("Url:" + url);
		TestSession.logger
				.info("IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII");
		TestSession.logger.info("USer:" + user + " Cookie:"
				+ userCookies.get(user));
		Response response = given().cookie(userCookies.get(user)).get(url);

		String responseAsString = response.getBody().asString();
		int responseCode = response.getStatusCode();
		TestSession.logger.info(" R E S P O N S E  C O D E: " + responseCode);
		TestSession.logger
				.info("R E S P O N S E  B O D Y: " + responseAsString);

		return responseCode;
	}

	/**
	 * Remove hadoop and hadoopqa from RM's yarn-site.xml. This is needed
	 * because for ACL tests if hitusr_* users are members of admin group then
	 * one cannot do ACL tests.
	 * 
	 * @throws Exception
	 */
	
//	public void removeUsersHadoopAndHadoopqaAsAdminsInTimelineServer()
//			throws Exception {
//		// Get the RM host
//		HadoopNode hadoopNode = TestSession.cluster
//				.getNode(HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
//		String rmHost = hadoopNode.getHostname();
//		// Prepare the arg list
//		List<String> modifyYarnAdminAclList = new ArrayList<String>();
//		modifyYarnAdminAclList.add("\"yarn.admin.acl= gridadmin\"");
//		TestSession.logger.info("prepared  list:" + modifyYarnAdminAclList);
//		restartATSWithTheseArgs(rmHost, modifyYarnAdminAclList);
//	}

	@AfterClass
	public static void restoreTheAdminsAfterATest() throws Exception {
		// Get the RM host
		HadoopNode hadoopNode = TestSession.cluster
				.getNode(HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
		String rmHost = hadoopNode.getHostname();
		// Prepare the arg list
		List<String> modifyYarnAdminAclList = new ArrayList<String>();
		ATSTestsBaseClass atsTestBaseClass = new ATSTestsBaseClass();
//		atsTestBaseClass.restartATSWithTheseArgs(rmHost, modifyYarnAdminAclList);
		

	}

}
