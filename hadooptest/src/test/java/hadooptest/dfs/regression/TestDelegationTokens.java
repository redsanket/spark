package hadooptest.dfs.regression;

import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.cluster.hadoop.HadoopCluster.Action;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.config.hadoop.HadoopConfiguration;
import hadooptest.dfs.regression.DfsCliCommands.GenericCliResponseBO;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import hadooptest.SerialTests;
import hadooptest.TestSession;

@RunWith(Parameterized.class)
@Category(SerialTests.class)
public class TestDelegationTokens extends DfsBaseClass {
	String protocol;

	public TestDelegationTokens(String protocol) {
		this.protocol = protocol;
		logger.info("Test invoked for protocol/schema:" + protocol);
	}

	static Logger logger = Logger.getLogger(TestDelegationTokens.class);
	private static final String DELEGATION_TOKEN_TESTS_DIR_ON_DFS = "/user/"
			+ HadooptestConstants.UserNames.HADOOPQA
			+ "/delegation_token_tests/";

	File fileStoringDelagationTokenReceivedViaFetchdt;
	File fileStoringDelegationTokenReceivedViaKinit;

	@Parameters
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][] {
		// Schemas
		// { HadooptestConstants.Schema.WEBHDFS },
		// { "" },
		{ HadooptestConstants.Schema.HDFS }, });
	}

	@Rule
	public TemporaryFolder tempDelegationTokenFolder = new TemporaryFolder();

	@Before
	public void beforeEachTest() throws Exception {
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

	/*
	 * test_SF110_1
	 */
	 @Test
	public void test_SF110_1() throws Exception {
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
				"/", false);
		Assert.assertTrue(genericCliResponseBO.process.exitValue() != 0);

		// Now, use the token received via fetchdt, it should work
		testSpecificEnvVars = new HashMap<String, String>();
		testSpecificEnvVars.put(HADOOP_TOKEN_FILE_LOCATION,
				fileStoringDelagationTokenReceivedViaFetchdt.getPath());
		genericCliResponseBO = dfsCliCommands.ls(testSpecificEnvVars,
				HadooptestConstants.UserNames.HDFSQA, protocol, localCluster,
				"/", false);
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
	 * test_SF110_2
	 */
	 @Test
	public void test_SF110_2() throws Exception {
		GenericCliResponseBO genericCliResponseBO;

		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		dfsCliCommands.createCustomizedKerberosTicket(
				HadooptestConstants.UserNames.HADOOPQA,
				fileStoringDelegationTokenReceivedViaKinit.getPath(), "60s");

		// Wait for longer than 60s, let the ticket expire
		Thread.sleep(90000);
		HashMap<String, String> testSpecificEnvVars = new HashMap<String, String>();
		testSpecificEnvVars.put(KRB5CCNAME,
				fileStoringDelegationTokenReceivedViaKinit.getPath());

		genericCliResponseBO = dfsCliCommands.ls(testSpecificEnvVars,
				HadooptestConstants.UserNames.HDFSQA, protocol, localCluster,
				"/", false);
		Assert.assertTrue(genericCliResponseBO.process.exitValue() != 0);

		// Now, use the token received via fetchdt, it should work
		testSpecificEnvVars = new HashMap<String, String>();
		testSpecificEnvVars.put(HADOOP_TOKEN_FILE_LOCATION,
				fileStoringDelagationTokenReceivedViaFetchdt.getPath());

		genericCliResponseBO = dfsCliCommands.ls(testSpecificEnvVars,
				HadooptestConstants.UserNames.HDFSQA, protocol, localCluster,
				"/", false);
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
	 @Test
	public void test_SF110_6() throws Exception {
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
		// Get NN out of sademode
		dfsCliCommands.dfsadmin(EMPTY_ENV_HASH_MAP, "get", false, false, 0,
				false, false, 0, null);
		dfsCliCommands.dfsadmin(EMPTY_ENV_HASH_MAP, "leave", false, false, 0,
				false, false, 0, null);
		dfsCliCommands.dfsadmin(EMPTY_ENV_HASH_MAP, "get", false, false, 0,
				false, false, 0, null);
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
				"/", false);
		Assert.assertTrue(genericCliResponseBO.process.exitValue() == 0);

		// Wait 2 mins, let the token expire
		Thread.sleep(120000);
		genericCliResponseBO = dfsCliCommands.ls(testSpecificEnvVars,
				HadooptestConstants.UserNames.HDFSQA, protocol, localCluster,
				"/", false);
		Assert.assertTrue(genericCliResponseBO.process.exitValue() != 0);

		for (String aComponentToUpdate : componentsToUpdate) {

			cluster.getConf(aComponentToUpdate).resetHadoopConfDir();
			cluster.hadoopDaemon(Action.STOP, aComponentToUpdate);
			cluster.hadoopDaemon(Action.START, aComponentToUpdate);
		}

		// Get NN out of sademode
		dfsCliCommands.dfsadmin(EMPTY_ENV_HASH_MAP, "get", false, false, 0,
				false, false, 0, null);
		dfsCliCommands.dfsadmin(EMPTY_ENV_HASH_MAP, "leave", false, false, 0,
				false, false, 0, null);

	}

	/*
	 * test_SF170_2 #For Headless users Verify that delegation token can be used
	 * in it's life time even after Name node restart. #1.Create delegation
	 * token. #2.Restart Name node. #3.Verify that Hadoop operations(HDFS
	 * commands and Map reduce job) are allowed and there are no errors.
	 * #Expected #Hadoop operations should complete successfully. #TC ID=1172572
	 */
	 @Test
	public void test_SF170_2() throws Exception {
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
				"/", false);
		Assert.assertTrue(genericCliResponseBO.process.exitValue() != 0);

		FullyDistributedCluster cluster = (FullyDistributedCluster) TestSession.cluster;
		String[] componentsToUpdate = new String[] { HadooptestConstants.NodeTypes.NAMENODE };
		for (String aComponentToUpdate : componentsToUpdate) {
			cluster.hadoopDaemon(Action.STOP, aComponentToUpdate);
			cluster.hadoopDaemon(Action.START, aComponentToUpdate);

		}
		// Get NN out of sademode
		dfsCliCommands.dfsadmin(EMPTY_ENV_HASH_MAP, "get", false, false, 0,
				false, false, 0, null);
		dfsCliCommands.dfsadmin(EMPTY_ENV_HASH_MAP, "leave", false, false, 0,
				false, false, 0, null);
		dfsCliCommands.dfsadmin(EMPTY_ENV_HASH_MAP, "get", false, false, 0,
				false, false, 0, null);

		testSpecificEnvVars = new HashMap<String, String>();
		testSpecificEnvVars.put(HADOOP_TOKEN_FILE_LOCATION,
				fileStoringDelagationTokenReceivedViaFetchdt.getPath());

		genericCliResponseBO = dfsCliCommands.ls(testSpecificEnvVars,
				HadooptestConstants.UserNames.HDFSQA, protocol, localCluster,
				"/", false);
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
	@Test
	public void test_SF175_1() throws Exception {
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
				"/", false);
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
		// Get NN out of sademode
		dfsCliCommands.dfsadmin(EMPTY_ENV_HASH_MAP, "get", false, false, 0,
				false, false, 0, null);
		dfsCliCommands.dfsadmin(EMPTY_ENV_HASH_MAP, "leave", false, false, 0,
				false, false, 0, null);
		dfsCliCommands.dfsadmin(EMPTY_ENV_HASH_MAP, "get", false, false, 0,
				false, false, 0, null);

		testSpecificEnvVars = new HashMap<String, String>();
		testSpecificEnvVars.put(HADOOP_TOKEN_FILE_LOCATION,
				fileStoringDelagationTokenReceivedViaFetchdt.getPath());

		genericCliResponseBO = dfsCliCommands.ls(testSpecificEnvVars,
				HadooptestConstants.UserNames.HDFSQA, protocol, localCluster,
				"/", false);
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
	@Test
	public void test_SF190_1() throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponseBO;
		HashMap<String, String> testSpecificEnvVars = new HashMap<String, String>();

		testSpecificEnvVars = new HashMap<String, String>();
		testSpecificEnvVars.put(HADOOP_TOKEN_FILE_LOCATION,
				fileStoringDelagationTokenReceivedViaFetchdt.getPath());

		genericCliResponseBO = dfsCliCommands.ls(testSpecificEnvVars,
				HadooptestConstants.UserNames.HDFSQA, protocol, localCluster,
				"/", false);
		Assert.assertTrue(genericCliResponseBO.process.exitValue() == 0);

		
		dfsCliCommands.fetchdt(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				null, HadooptestConstants.UserNames.HADOOPQA, "cancel", null, null,
				fileStoringDelagationTokenReceivedViaFetchdt.getPath(), null,
				null, null, null, null, null);

		testSpecificEnvVars = new HashMap<String, String>();
		testSpecificEnvVars.put(HADOOP_TOKEN_FILE_LOCATION,
				fileStoringDelagationTokenReceivedViaFetchdt.getPath());

		genericCliResponseBO = dfsCliCommands.ls(testSpecificEnvVars,
				HadooptestConstants.UserNames.HDFSQA, protocol, localCluster,
				"/", false);
		Assert.assertTrue(genericCliResponseBO.process.exitValue() != 0);

	}

	@After
	public void afterTest() throws Exception {
		DfsCliCommands dfsCliCommands = new DfsCliCommands();
		GenericCliResponseBO genericCliResponse = null;
		genericCliResponse = dfsCliCommands.rm(EMPTY_ENV_HASH_MAP,
				HadooptestConstants.UserNames.HADOOPQA, protocol, localCluster,
				true, true, true, DELEGATION_TOKEN_TESTS_DIR_ON_DFS);
		Assert.assertTrue(genericCliResponse.process.exitValue() == 0);

	}

	@After
	public void logTaskResportSummary() {
		// Override to hide the Test Session logs
	}

}
