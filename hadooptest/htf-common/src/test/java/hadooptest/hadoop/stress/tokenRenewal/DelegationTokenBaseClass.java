package hadooptest.hadoop.stress.tokenRenewal;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Assert;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.cluster.hadoop.HadoopCluster.Action;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.config.hadoop.HadoopConfiguration;
import hadooptest.hadoop.regression.dfs.DfsCliCommands;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;

public class DelegationTokenBaseClass extends DfsTestsBaseClass {
	Thread threadThatWouldKeepCleaningKinitCache;
	KinitCacheDestroyer kinitCacheDestroyer;

//	static String FILE_USED_IN_THIS_TEST = "file_257MB";
	static String FILE_USED_IN_THIS_TEST = INPUT_TO_WORD_COUNT;
	//file_64MB
	static String KEYTAB_DIR = "keytabDir";
	static String KEYTAB_USER = "keytabUser";
	static String OWNED_FILE_WITH_COMPLETE_PATH = "ownedFile";
	static String USER_WHO_DOESNT_HAVE_PERMISSIONS = "userWhoDoesntHavePermissions";
	static int TOKEN_RENEW_INTERVAL_IN_SECONDS = 30;
	static int TOKEN_MAX_LIFE_IN_SECONDS_SET_TO_A_LOW_VALUE_TO_MAKE_JOBS_FAIL = 40;

	static int EXTRA_TIME = 10;

	Configuration conf = new Configuration();
	int failFlags = 10000; // test result init but not updated yet

	void testPrepSetTokenRenewAndMaxLifeInterval() throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse;

		// mkdir on HDFS
		genericCliResponse = dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "",
				System.getProperty("CLUSTER_NAME"), DATA_DIR_IN_HDFS);
		Assert.assertTrue("Command exited with non-zero exit code",
				genericCliResponse.process.exitValue() == 0);
		pathsChmodedSoFar = new HashMap<String, Boolean>();

		doChmodRecursively(System.getProperty("CLUSTER_NAME"), DATA_DIR_IN_HDFS);

		genericCliResponse = dfsCliCommands.test(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HDFSQA, "",
				System.getProperty("CLUSTER_NAME"), DATA_DIR_IN_HDFS
						+ FILE_USED_IN_THIS_TEST,
				DfsCliCommands.FILE_SYSTEM_ENTITY_FILE);
		if (genericCliResponse.process.exitValue() != 0) {
			genericCliResponse = dfsCliCommands.put(EMPTY_ENV_HASH_MAP,
					HadooptestConstants.UserNames.HADOOPQA, "",
					System.getProperty("CLUSTER_NAME"), DATA_DIR_IN_LOCAL_FS
							+ FILE_USED_IN_THIS_TEST, DATA_DIR_IN_HDFS);
			Assert.assertTrue("Command exited with non-zero exit code",
					genericCliResponse.process.exitValue() == 0);
		}

		FullyDistributedCluster fullyDistributedCluster = (FullyDistributedCluster) TestSession
				.getCluster();

		// Backup config and replace file, for Resource Manager and Namenode
		fullyDistributedCluster.getConf(
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER).backupConfDir();
		HashMap<String, String> keyValuesForResourceManagerConfigs = new HashMap<String, String>();
		keyValuesForResourceManagerConfigs.put(
				"yarn.resourcemanager.delegation.token.renew-interval",
				Integer.toString(TOKEN_RENEW_INTERVAL_IN_SECONDS * 1000));
		fullyDistributedCluster
				.getConf(HadooptestConstants.NodeTypes.RESOURCE_MANAGER)
				.setHadoopConfFileProp(keyValuesForResourceManagerConfigs,
						HadooptestConstants.ConfFileNames.MAPRED_SITE_XML, null);

		fullyDistributedCluster.getConf(HadooptestConstants.NodeTypes.NAMENODE)
				.backupConfDir();

		HashMap<String, String> keyValuesForNamenodeConfigs = new HashMap<String, String>();
		keyValuesForNamenodeConfigs.put(
				"dfs.namenode.delegation.token.renew-interval",
				Integer.toString(TOKEN_RENEW_INTERVAL_IN_SECONDS * 1000));
		// keyValuesForResourceManagerConfigs.put("dfs.namenode.delegation.token.max-lifetime",
		// Integer.toString(TOKEN_MAX_LIFE_IN_SECONDS * 1000));
		fullyDistributedCluster.getConf(HadooptestConstants.NodeTypes.NAMENODE)
				.setHadoopConfFileProp(keyValuesForNamenodeConfigs,
						HadooptestConstants.ConfFileNames.HDFS_SITE_XML, null);

		fullyDistributedCluster.hadoopDaemon(Action.STOP,
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER);
		fullyDistributedCluster.hadoopDaemon(Action.START,
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER);

		fullyDistributedCluster.hadoopDaemon(Action.STOP,
				HadooptestConstants.NodeTypes.NAMENODE);
		fullyDistributedCluster.hadoopDaemon(Action.START,
				HadooptestConstants.NodeTypes.NAMENODE);

		// Wait for NN to be out of safemode.
		TestSession.logger.info("NN and RM nodes have been bounced, "
				+ "wait for NN to be out of Safemode");

                // wait up to 5 minutes for NN to be out of safemode
                for (int waitCounter = 0; waitCounter < 30; waitCounter++) {
                  genericCliResponse = dfsCliCommands.dfsadmin(
				DfsTestsBaseClass.EMPTY_ENV_HASH_MAP,
				DfsTestsBaseClass.Report.NO, 
				"get",
				DfsTestsBaseClass.ClearQuota.NO, 
				DfsTestsBaseClass.SetQuota.NO, 
				0, 
				DfsTestsBaseClass.ClearSpaceQuota.NO,
                                DfsTestsBaseClass.SetSpaceQuota.NO, 
				0, 
				DfsTestsBaseClass.PrintTopology.NO, 
				null);

                  if (genericCliResponse.response.contains("Safe mode is OFF")) {
                    TestSession.logger.info("NN is out of Safemode after " + (waitCounter*10) + " seconds");
                    break;
                  } else if (waitCounter >= 30) {
                    TestSession.logger.error("NN never left Safemode after " + (waitCounter*10) + " seconds!");
                    Assert.fail();
                  }
                  else
                  {
                    Thread.sleep(10000);
                  }
                }

		// Also set the value on the local client
		conf.setInt("yarn.resourcemanager.delegation.token.renew-interval",
				TOKEN_RENEW_INTERVAL_IN_SECONDS * 1000);
		conf.setInt("dfs.namenode.delegation.token.renew-interval",
				TOKEN_RENEW_INTERVAL_IN_SECONDS * 1000);
		// conf.setInt("yarn.resourcemanager.delegation.token.max-lifetime",
		// TOKEN_MAX_LIFE_IN_SECONDS * 1000);
		// conf.setInt("dfs.namenode.delegation.token.max-lifetime",
		// TOKEN_MAX_LIFE_IN_SECONDS * 1000);
		conf.setBoolean("mapreduce.job.complete.cancel.delegation.tokens",
				false);

	}

	public void startKinitCacheDestroyer() {
		kinitCacheDestroyer = new KinitCacheDestroyer();
		threadThatWouldKeepCleaningKinitCache = new Thread(kinitCacheDestroyer);
		threadThatWouldKeepCleaningKinitCache.setPriority(Thread.MIN_PRIORITY);
		threadThatWouldKeepCleaningKinitCache.start();
	}

	public void stopKinitCacheDestroyer() {
		kinitCacheDestroyer.keepRunning = false;
	}

	public UserGroupInformation loginUserFromKeytabAndReturnUGI(String keytabUser,
			String keytabDir) {
		logger.info("Set keytab user=" + keytabUser);
		logger.info("Set keytab dir=" + keytabDir);
		UserGroupInformation ugi;
		try {

			ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
					keytabUser, keytabDir);
			logger.info("UGI=" + ugi);
			logger.info("credentials:" + ugi.getCredentials());
			logger.info("group names" + ugi.getGroupNames());
			logger.info("real user:" + ugi.getRealUser());
			logger.info("short user name:" + ugi.getShortUserName());
			logger.info("token identifiers:" + ugi.getTokenIdentifiers());
			logger.info("tokens:" + ugi.getTokens());
			logger.info("username:" + ugi.getUserName());
			logger.info("current user:" + UserGroupInformation.getCurrentUser());
			logger.info("login user:" + UserGroupInformation.getLoginUser());

		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return ugi;
	}

	public String getPrefixForProtocol(String protocol, String cluster) {
		if (protocol.equals("hdfs")) {
			return DfsCliCommands.getNNUrlForHdfs(cluster);
		}
		if (protocol.equals("webhdfs")) {
			return DfsCliCommands.getNNUrlForWebhdfs(cluster);
		}
		return "";
	}

	void printTokens(HashMap<Text, byte[]> aTokenHashMap) {
		StringBuilder sb;
		for (Text aTokenKind : aTokenHashMap.keySet()) {
			sb = new StringBuilder();
			TestSession.logger.info(aTokenKind);
			for (byte aByte : aTokenHashMap.get(aTokenKind)) {
				sb.append(String.format("%02X ", aByte));
			}
			TestSession.logger.info(sb);
		}

	}

	void assertTokenRenewals(HashMap<Text, byte[]> previousTokenMap,
			HashMap<Text, byte[]> newTokenMap) {
		Assert.assertTrue((newTokenMap.size() == previousTokenMap.size())
				&& previousTokenMap.size() >= 1);
		for (Text aTextFromPreviousToken : previousTokenMap.keySet()) {
			Assert.assertTrue(
					"Tke key sets between previous and new tokens did not match",
					newTokenMap.containsKey(aTextFromPreviousToken));
			Assert.assertTrue(
					"",
					previousTokenMap.get(aTextFromPreviousToken).length == newTokenMap
							.get(aTextFromPreviousToken).length);
			for (int xx = 0; xx < newTokenMap.get(aTextFromPreviousToken).length; xx++) {
				Assert.assertTrue(
						"The byte contents did not match, across tokens",
						previousTokenMap.get(aTextFromPreviousToken)[xx] == newTokenMap
								.get(aTextFromPreviousToken)[xx]);
			}
		}

	}

	public class KinitCacheDestroyer implements Runnable {
		public volatile boolean keepRunning;

		public KinitCacheDestroyer() {
			keepRunning = true;
		}

		public void run() {
			while (keepRunning) {
				kdestroy();
				try {
					Thread.sleep(20000);
					TestSession.logger
							.info("Background Thread cleaned up krb cache..........!");
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		}

		public void stop() {
			keepRunning = false;
		}

		void kdestroy() {
			DfsCliCommands dfsCommonCli = new DfsCliCommands();
			try {
				String cacheLocation = dfsCommonCli.getKerberosCacheLocation();
				if (cacheLocation.isEmpty() || cacheLocation == "") {
					TestSession.logger.info("Just returning since cacheLocation is: "+cacheLocation);
					return;
				}
				TestSession.logger.info("Going to kdestroy cache location: "+cacheLocation);
				dfsCommonCli.kdestroy(cacheLocation);
				
//				StringBuilder sb = new StringBuilder();
//				sb.append("rm");
//				sb.append(" ");
//				sb.append(cacheLocation);
//
//				Process proc = TestSession.exec.runProcBuilderGetProc(sb
//						.toString().split("\\s+"));
//				proc.waitFor();
//				Assert.assertTrue(
//						"Got non-zero exit value while removing cache:"
//								+ proc.exitValue(), proc.exitValue() == 0);
				TestSession.logger.info("removed " + cacheLocation);

				

			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}

}
