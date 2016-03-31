package hadooptest.hadoop.regression.dfs;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.cluster.hadoop.HadoopCluster.Action;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;

import java.io.File;
import java.util.HashMap;

import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

@Category(SerialTests.class)
public class TestDelegationTokens extends DfsTestsBaseClass {

    private String protocol;
	private static final String DELEGATION_TOKEN_TESTS_DIR_ON_DFS = "/user/"
			+ HadooptestConstants.UserNames.HADOOPQA
			+ "/delegation_token_tests/";

	File fileStoringDelagationTokenReceivedViaFetchdt;
	File fileStoringDelegationTokenReceivedViaKinit;

	@Rule
	public TemporaryFolder tempDelegationTokenFolder = new TemporaryFolder();

	public void beforeEachTest(String protocol) throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		dfsCliCommands.mkdir(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				DELEGATION_TOKEN_TESTS_DIR_ON_DFS);
		fileStoringDelagationTokenReceivedViaFetchdt = tempDelegationTokenFolder
				.newFile("delegation.token.received.via.dfs.command");
		fileStoringDelegationTokenReceivedViaKinit = tempDelegationTokenFolder
				.newFile("delegation.token.received.via.kinit");

		/*
		 * Fetch and store a delegation token locally.
		 */
		dfsCliCommands.fetchdt(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				null, HadooptestConstants.UserNames.HADOOPQA, null, null, null,
				fileStoringDelagationTokenReceivedViaFetchdt.getPath(), null,
				null, null, null, null, null);

	}

	@Test public void test_SF110_1_WebHDFS() throws Exception { test_SF110_1(HadooptestConstants.Schema.WEBHDFS); }
    @Test public void test_SF110_1_HDFS() throws Exception { test_SF110_1(HadooptestConstants.Schema.HDFS); }
    @Test public void test_SF110_2_WebHDFS() throws Exception { test_SF110_2(HadooptestConstants.Schema.WEBHDFS); }
    @Test public void test_SF110_2_HDFS() throws Exception { test_SF110_2(HadooptestConstants.Schema.HDFS); }
    @Test public void test_SF110_6_WebHDFS() throws Exception { test_SF110_6(HadooptestConstants.Schema.WEBHDFS); }
    @Test public void test_SF110_6_HDFS() throws Exception { test_SF110_6(HadooptestConstants.Schema.HDFS); }
    @Test public void test_SF170_2_WebHDFS() throws Exception { test_SF110_6(HadooptestConstants.Schema.WEBHDFS); }
    @Test public void test_SF170_2_HDFS() throws Exception { test_SF110_6(HadooptestConstants.Schema.HDFS); }
    @Test public void test_SF175_1_WebHDFS() throws Exception { test_SF110_6(HadooptestConstants.Schema.WEBHDFS); }
    @Test public void test_SF175_1_HDFS() throws Exception { test_SF110_6(HadooptestConstants.Schema.HDFS); }
    @Test public void test_SF190_1_WebHDFS() throws Exception { test_SF110_6(HadooptestConstants.Schema.WEBHDFS); }
    @Test public void test_SF190_1_HDFS() throws Exception { test_SF110_6(HadooptestConstants.Schema.HDFS); }
    
	public void test_SF110_1(String protocol) throws Exception {
	    beforeEachTest(protocol);
	    this.protocol = protocol;
	    
		GenericCliResponseBO genericCliResponseBO;
		logger.info("Absolute file:"
				+ fileStoringDelagationTokenReceivedViaFetchdt
						.getAbsoluteFile());
		logger.info("Absolute path:"
				+ fileStoringDelagationTokenReceivedViaFetchdt
						.getAbsolutePath());
		logger.info("Canonical path:"
				+ fileStoringDelagationTokenReceivedViaFetchdt
						.getCanonicalPath());
		logger.info("Name:"
				+ fileStoringDelagationTokenReceivedViaFetchdt.getName());
		logger.info("Path:"
				+ fileStoringDelagationTokenReceivedViaFetchdt.getPath());

		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		dfsCliCommands.createCustomizedKerberosTicket(
				HadooptestConstants.UserNames.HADOOPQA,
				fileStoringDelegationTokenReceivedViaKinit.getPath(), null);

		dfsCliCommands.kdestroy(fileStoringDelegationTokenReceivedViaKinit
				.getPath());
		/*
		 * Now fetch the delegation token
		 */
		HashMap<String, String> testSpecificEnvVars = new HashMap<String, String>();
		testSpecificEnvVars.put(KRB5CCNAME,
				fileStoringDelegationTokenReceivedViaKinit.getPath());
		genericCliResponseBO = dfsCliCommands.ls(testSpecificEnvVars,
				HadooptestConstants.UserNames.HDFSQA, protocol, localCluster,
				"/", Recursive.NO);
		Assert.assertTrue(genericCliResponseBO.process.exitValue() != 0);

		// Now, use the token received via fetchdt, it should work
		testSpecificEnvVars = new HashMap<String, String>();
		testSpecificEnvVars.put(HADOOP_TOKEN_FILE_LOCATION,
				fileStoringDelagationTokenReceivedViaFetchdt.getPath());
		genericCliResponseBO = dfsCliCommands.ls(testSpecificEnvVars,
				HadooptestConstants.UserNames.HDFSQA, protocol, localCluster,
				"/", Recursive.NO);
		Assert.assertTrue(genericCliResponseBO.process.exitValue() == 0);

		logger.info("Absolute file:"
				+ fileStoringDelegationTokenReceivedViaKinit.getAbsoluteFile());
		logger.info("Absolute path:"
				+ fileStoringDelegationTokenReceivedViaKinit.getAbsolutePath());
		logger.info("Canonical path:"
				+ fileStoringDelegationTokenReceivedViaKinit.getCanonicalPath());
		logger.info("Name:"
				+ fileStoringDelegationTokenReceivedViaKinit.getName());
		logger.info("Path:"
				+ fileStoringDelegationTokenReceivedViaKinit.getPath());

	}

	public void test_SF110_2(String protocol) throws Exception {
        beforeEachTest(protocol);
        this.protocol = protocol;
        
		GenericCliResponseBO genericCliResponseBO;

		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		dfsCliCommands.createCustomizedKerberosTicket(
				HadooptestConstants.UserNames.HADOOPQA,
				fileStoringDelegationTokenReceivedViaKinit.getPath(), "60s");

		// Wait for longer than 60s, let the ticket expire
		Thread.sleep(600000);
		HashMap<String, String> testSpecificEnvVars = new HashMap<String, String>();
		testSpecificEnvVars.put(KRB5CCNAME,
				fileStoringDelegationTokenReceivedViaKinit.getPath());

		genericCliResponseBO = dfsCliCommands.ls(testSpecificEnvVars,
				HadooptestConstants.UserNames.HDFSQA, protocol, localCluster,
				"/", Recursive.NO);
		Assert.assertTrue(genericCliResponseBO.process.exitValue() != 0);

		// Now, use the token received via fetchdt, it should work
		testSpecificEnvVars = new HashMap<String, String>();
		testSpecificEnvVars.put(HADOOP_TOKEN_FILE_LOCATION,
				fileStoringDelagationTokenReceivedViaFetchdt.getPath());

		genericCliResponseBO = dfsCliCommands.ls(testSpecificEnvVars,
				HadooptestConstants.UserNames.HDFSQA, protocol, localCluster,
				"/", Recursive.NO);
		Assert.assertTrue(genericCliResponseBO.process.exitValue() == 0);

		logger.info("Absolute file:"
				+ fileStoringDelegationTokenReceivedViaKinit.getAbsoluteFile());
		logger.info("Absolute path:"
				+ fileStoringDelegationTokenReceivedViaKinit.getAbsolutePath());
		logger.info("Canonical path:"
				+ fileStoringDelegationTokenReceivedViaKinit.getCanonicalPath());
		logger.info("Name:"
				+ fileStoringDelegationTokenReceivedViaKinit.getName());
		logger.info("Path:"
				+ fileStoringDelegationTokenReceivedViaKinit.getPath());

	}

	/*
	 * test_SF160_1 #Delegation tokens expire according to their expiration date
	 * #Steps: #Verify delegation tokens expire on expiration date/time.
	 * #1.Create delegation tokens for user #2. Forward clock/wait till
	 * delegation token expiration time is elapsed. #3.After expiration time
	 * verify that delegation token is expired and it's not usable for Hadoop
	 * operations(HDFS commands and Map reduce job). # #You can use the
	 * following settings in the hdfs-site.xml to control the time for which the
	 * delegation tokens are valid # #<property>
	 * #<name>dfs.namenode.delegation.key.update-interval</name>
	 * #<value>120000</value> #<description>The update interval for master key
	 * for delegation tokens in the namenode in milliseconds.</description>
	 * #</property> # #<property>
	 * #<name>dfs.namenode.delegation.token.max-lifetime</name>
	 * #<value>60000</value> #<description>The maximum lifetime in milliseconds
	 * for which a delegation token is valid.</description> #</property> #
	 * #<property> #<name>dfs.namenode.delegation.token.renew-interval</name>
	 * #<value>30000</value> #<description>The renewal interval for delegation
	 * token in milliseconds.</description> #</property> # # #Expected: #Hadoop
	 * operations should fail with access control exception. #With the settings
	 * above delegation token will be valid for 2 mins. wait 3 mins and run an
	 * ls command and it should fail #TC ID=1172569
	 */
	public void test_SF110_6(String protocol) throws Exception {
        beforeEachTest(protocol);
        this.protocol = protocol;
        
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponseBO;
		HashMap<String, String> testSpecificEnvVars = new HashMap<String, String>();

		FullyDistributedCluster cluster = (FullyDistributedCluster) TestSession.cluster;
		String[] componentsToUpdate = new String[] {
				HadooptestConstants.NodeTypes.NAMENODE,
				HadooptestConstants.NodeTypes.DATANODE,
				HadooptestConstants.NodeTypes.RESOURCE_MANAGER };
		String hdfsSiteXml = HadooptestConstants.ConfFileNames.HDFS_SITE_XML;
		for (String aComponentToUpdate : componentsToUpdate) {

			cluster.getConf(aComponentToUpdate).backupConfDir();

			// Update the values
			cluster.getConf(aComponentToUpdate).setHadoopConfFileProp(
					"dfs.namenode.delegation.key.update-interval", "60000",
					hdfsSiteXml);
			cluster.getConf(aComponentToUpdate).setHadoopConfFileProp(
					"dfs.namenode.delegation.token.max-lifetime", "30000",
					hdfsSiteXml);
			cluster.getConf(aComponentToUpdate).setHadoopConfFileProp(
					"dfs.namenode.delegation.token.renew-interval", "10000",
					hdfsSiteXml);
			cluster.hadoopDaemon(Action.STOP, aComponentToUpdate);
			cluster.hadoopDaemon(Action.START, aComponentToUpdate);

		}

                // wait up to 5 minutes for NN to be out of safemode
                DfsCliCommands dfsCliCommands = new DfsCliCommands();
                GenericCliResponseBO genericCliResponse;
                for (int waitCounter = 0; waitCounter < 30; waitCounter++) {
                  genericCliResponse = dfsCliCommands.dfsadmin(EMPTY_ENV_HASH_MAP, Report.NO, "get",
                                ClearQuota.NO, SetQuota.NO, 0, ClearSpaceQuota.NO,
                                SetSpaceQuota.NO, 0, PrintTopology.NO, EMPTY_FS_ENTITY);

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

		/*
		 * Fetch and store a delegation token locally.
		 */
		dfsCliCommands.fetchdt(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				null, HadooptestConstants.UserNames.HADOOPQA, null, null, null,
				fileStoringDelagationTokenReceivedViaFetchdt.getPath(), null,
				null, null, null, null, null);

		dfsCliCommands.createCustomizedKerberosTicket(
				HadooptestConstants.UserNames.HADOOPQA,
				fileStoringDelegationTokenReceivedViaKinit.getPath(), null);

		dfsCliCommands.kdestroy(fileStoringDelegationTokenReceivedViaKinit
				.getPath());

		testSpecificEnvVars = new HashMap<String, String>();
		testSpecificEnvVars.put(HADOOP_TOKEN_FILE_LOCATION,
				fileStoringDelagationTokenReceivedViaFetchdt.getPath());

		genericCliResponseBO = dfsCliCommands.ls(testSpecificEnvVars,
				HadooptestConstants.UserNames.HDFSQA, protocol, localCluster,
				"/", Recursive.NO);
		Assert.assertTrue(genericCliResponseBO.process.exitValue() == 0);

		// Wait 2 mins, let the token expire
		Thread.sleep(120000);
		genericCliResponseBO = dfsCliCommands.ls(testSpecificEnvVars,
				HadooptestConstants.UserNames.HDFSQA, protocol, localCluster,
				"/", Recursive.NO);
		Assert.assertTrue(genericCliResponseBO.process.exitValue() != 0);

		for (String aComponentToUpdate : componentsToUpdate) {

			cluster.getConf(aComponentToUpdate).resetHadoopConfDir();
			cluster.hadoopDaemon(Action.STOP, aComponentToUpdate);
			cluster.hadoopDaemon(Action.START, aComponentToUpdate);
			
		}

                // wait up to 5 minutes for NN to be out of safemode
                DfsCliCommands dfsCliCommands = new DfsCliCommands();
                GenericCliResponseBO genericCliResponse;
                for (int waitCounter = 0; waitCounter < 30; waitCounter++) {
                  genericCliResponse = dfsCliCommands.dfsadmin(EMPTY_ENV_HASH_MAP, Report.NO, "get",
                                ClearQuota.NO, SetQuota.NO, 0, ClearSpaceQuota.NO,
                                SetSpaceQuota.NO, 0, PrintTopology.NO, EMPTY_FS_ENTITY);

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


	}

	/*
	 * test_SF170_2 #For Headless users Verify that delegation token can be used
	 * in it's life time even after Name node restart. #1.Create delegation
	 * token. #2.Restart Name node. #3.Verify that Hadoop operations(HDFS
	 * commands and Map reduce job) are allowed and there are no errors.
	 * #Expected #Hadoop operations should complete successfully. #TC ID=1172572
	 */
	public void test_SF170_2(String protocol) throws Exception {
        beforeEachTest(protocol);
        this.protocol = protocol;
        
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponseBO;
		HashMap<String, String> testSpecificEnvVars = new HashMap<String, String>();

		dfsCliCommands.createCustomizedKerberosTicket(
				HadooptestConstants.UserNames.HADOOPQA,
				fileStoringDelegationTokenReceivedViaKinit.getPath(), null);

		dfsCliCommands.kdestroy(fileStoringDelegationTokenReceivedViaKinit
				.getPath());

		testSpecificEnvVars = new HashMap<String, String>();
		testSpecificEnvVars.put(KRB5CCNAME,
				fileStoringDelegationTokenReceivedViaKinit.getPath());

		genericCliResponseBO = dfsCliCommands.ls(testSpecificEnvVars,
				HadooptestConstants.UserNames.HDFSQA, protocol, localCluster,
				"/", Recursive.NO);
		Assert.assertTrue(genericCliResponseBO.process.exitValue() != 0);

		FullyDistributedCluster cluster = (FullyDistributedCluster) TestSession.cluster;
		String[] componentsToUpdate = new String[] { HadooptestConstants.NodeTypes.NAMENODE };
		for (String aComponentToUpdate : componentsToUpdate) {
			cluster.hadoopDaemon(Action.STOP, aComponentToUpdate);
			cluster.hadoopDaemon(Action.START, aComponentToUpdate);

		}

                // wait up to 5 minutes for NN to be out of safemode
                DfsCliCommands dfsCliCommands = new DfsCliCommands();
                GenericCliResponseBO genericCliResponse;
                for (int waitCounter = 0; waitCounter < 30; waitCounter++) {
                  genericCliResponse = dfsCliCommands.dfsadmin(EMPTY_ENV_HASH_MAP, Report.NO, "get",
                                ClearQuota.NO, SetQuota.NO, 0, ClearSpaceQuota.NO,
                                SetSpaceQuota.NO, 0, PrintTopology.NO, EMPTY_FS_ENTITY);

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

		testSpecificEnvVars = new HashMap<String, String>();
		testSpecificEnvVars.put(HADOOP_TOKEN_FILE_LOCATION,
				fileStoringDelagationTokenReceivedViaFetchdt.getPath());

		genericCliResponseBO = dfsCliCommands.ls(testSpecificEnvVars,
				HadooptestConstants.UserNames.HDFSQA, protocol, localCluster,
				"/", Recursive.NO);
		Assert.assertTrue(genericCliResponseBO.process.exitValue() == 0);

	}

	/*
	 * test_SF175_1 #Delegation tokens are still valid after two or more
	 * NameNode restarts within delegation token persistence time on NameNode
	 * #Steps: #Verify that delegation token can be used in it's life time even
	 * after Name node restart is done 3 times. #1.Create delegation token.
	 * #2.Restart Name node. #3.Verify that Hadoop operations(HDFS commands and
	 * Map reduce job) are allowed and there are no errors. # #Expected: #Hadoop
	 * operations should complete successfully #TC ID=1172574
	 */
	public void test_SF175_1(String protocol) throws Exception {
        beforeEachTest(protocol);
        this.protocol = protocol;
        
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponseBO;
		HashMap<String, String> testSpecificEnvVars = new HashMap<String, String>();

		dfsCliCommands.createCustomizedKerberosTicket(
				HadooptestConstants.UserNames.HADOOPQA,
				fileStoringDelegationTokenReceivedViaKinit.getPath(), null);

		dfsCliCommands.kdestroy(fileStoringDelegationTokenReceivedViaKinit
				.getPath());

		testSpecificEnvVars = new HashMap<String, String>();
		testSpecificEnvVars.put(KRB5CCNAME,
				fileStoringDelegationTokenReceivedViaKinit.getPath());

		genericCliResponseBO = dfsCliCommands.ls(testSpecificEnvVars,
				HadooptestConstants.UserNames.HDFSQA, protocol, localCluster,
				"/", Recursive.NO);
		Assert.assertTrue(genericCliResponseBO.process.exitValue() != 0);

		FullyDistributedCluster cluster = (FullyDistributedCluster) TestSession.cluster;
		String[] componentsToUpdate = new String[] { HadooptestConstants.NodeTypes.NAMENODE };

		for (int count = 0; count < 3; count++) {
			cluster.hadoopDaemon(Action.STOP,
					HadooptestConstants.NodeTypes.NAMENODE);
			cluster.hadoopDaemon(Action.START,
					HadooptestConstants.NodeTypes.NAMENODE);
			Thread.sleep(5000);

		}

                // wait up to 5 minutes for NN to be out of safemode
                DfsCliCommands dfsCliCommands = new DfsCliCommands();
                GenericCliResponseBO genericCliResponse;
                for (int waitCounter = 0; waitCounter < 30; waitCounter++) {
                  genericCliResponse = dfsCliCommands.dfsadmin(EMPTY_ENV_HASH_MAP, Report.NO, "get",
                                ClearQuota.NO, SetQuota.NO, 0, ClearSpaceQuota.NO,
                                SetSpaceQuota.NO, 0, PrintTopology.NO, EMPTY_FS_ENTITY);

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

		testSpecificEnvVars = new HashMap<String, String>();
		testSpecificEnvVars.put(HADOOP_TOKEN_FILE_LOCATION,
				fileStoringDelagationTokenReceivedViaFetchdt.getPath());

		genericCliResponseBO = dfsCliCommands.ls(testSpecificEnvVars,
				HadooptestConstants.UserNames.HDFSQA, protocol, localCluster,
				"/", Recursive.NO);
		Assert.assertTrue(genericCliResponseBO.process.exitValue() == 0);

	}

	/*
	 * #Once delegation token is canceled, it cannot be used anymore #(This is a
	 * 0.22 test case) #Steps: #Verify that after delegation token cancellation
	 * no hadoop operations are allowed using delegation token and without
	 * kerberos ticket. # #Expected: #Hadoop operations using delegation token
	 * (without kerberos ticket) should fail with access control exception when
	 * delegation tokens are canceled.
	 */
	public void test_SF190_1(String protocol) throws Exception {
        beforeEachTest(protocol);
        this.protocol = protocol;
        
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponseBO;
		HashMap<String, String> testSpecificEnvVars = new HashMap<String, String>();

		testSpecificEnvVars = new HashMap<String, String>();
		testSpecificEnvVars.put(HADOOP_TOKEN_FILE_LOCATION,
				fileStoringDelagationTokenReceivedViaFetchdt.getPath());

		genericCliResponseBO = dfsCliCommands.ls(testSpecificEnvVars,
				HadooptestConstants.UserNames.HDFSQA, protocol, localCluster,
				"/", Recursive.NO);
		Assert.assertTrue(genericCliResponseBO.process.exitValue() == 0);

		dfsCliCommands.fetchdt(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				null, HadooptestConstants.UserNames.HADOOPQA, "cancel", null,
				null, fileStoringDelagationTokenReceivedViaFetchdt.getPath(),
				null, null, null, null, null, null);

		testSpecificEnvVars = new HashMap<String, String>();
		testSpecificEnvVars.put(HADOOP_TOKEN_FILE_LOCATION,
				fileStoringDelagationTokenReceivedViaFetchdt.getPath());

		genericCliResponseBO = dfsCliCommands.ls(testSpecificEnvVars,
				HadooptestConstants.UserNames.HDFSQA, protocol, localCluster,
				"/", Recursive.NO);
		Assert.assertTrue(genericCliResponseBO.process.exitValue() != 0);

	}

	@After
	public void afterTest() throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse = null;
		genericCliResponse = dfsCliCommands.rm(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				Recursive.YES, Force.YES, SkipTrash.YES,
				DELEGATION_TOKEN_TESTS_DIR_ON_DFS);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

	}

}
